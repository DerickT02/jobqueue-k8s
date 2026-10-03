package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

// jobQueue is the Redis list job IDs are pushed onto (LPUSH). A worker should
// pop from the other end (BRPOP / BLMOVE) to process jobs in FIFO order.
const jobQueue = "jobs:queue"

const (
	StatusPending    = "pending"
	StatusProcessing = "processing"
	StatusDone       = "done"
	StatusFailed     = "failed"
)

type Job struct {
	ID        int64           `json:"id"`
	Type      string          `json:"type"`
	Payload   json.RawMessage `json:"payload"`
	Status    string          `json:"status"`
	CreatedAt time.Time       `json:"created_at"`
	UpdatedAt time.Time       `json:"updated_at"`
}

const jobColumns = `id, type, payload, status, created_at, updated_at`

type server struct {
	db    *pgxpool.Pool
	redis *redis.Client
}

func main() {
	dsn := getenv("DATABASE_URL", "postgres://postgres:postgres@localhost:5433/goservices?sslmode=disable")
	redisURL := getenv("REDIS_URL", "redis://localhost:6379/0")
	addr := getenv("ADDR", ":8080")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	db, err := pgxpool.New(ctx, dsn)
	if err != nil {
		log.Fatalf("create pool: %v", err)
	}
	defer db.Close()

	if err := db.Ping(ctx); err != nil {
		log.Fatalf("connect to database: %v", err)
	}

	if _, err := db.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS jobs (
			id         BIGSERIAL PRIMARY KEY,
			type       TEXT NOT NULL,
			payload    JSONB NOT NULL DEFAULT '{}',
			status     TEXT NOT NULL DEFAULT 'pending'
			           CHECK (status IN ('pending', 'processing', 'done', 'failed')),
			created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`); err != nil {
		log.Fatalf("migrate: %v", err)
	}

	redisOpts, err := redis.ParseURL(redisURL)
	if err != nil {
		log.Fatalf("parse redis url: %v", err)
	}
	rdb := redis.NewClient(redisOpts)
	defer rdb.Close()

	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("connect to redis: %v", err)
	}

	s := &server{db: db, redis: rdb}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", s.health)
	mux.HandleFunc("GET /jobs", s.listJobs)
	mux.HandleFunc("POST /jobs", s.createJob)
	mux.HandleFunc("GET /jobs/{id}", s.getJob)

	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		log.Printf("listening on %s", addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("server: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	srv.Shutdown(shutdownCtx)
}

func (s *server) health(w http.ResponseWriter, r *http.Request) {
	if err := s.db.Ping(r.Context()); err != nil {
		writeError(w, http.StatusServiceUnavailable, "database unavailable")
		return
	}
	if err := s.redis.Ping(r.Context()).Err(); err != nil {
		writeError(w, http.StatusServiceUnavailable, "redis unavailable")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// listJobs returns all jobs, optionally filtered with ?status=pending|processing|done|failed.
func (s *server) listJobs(w http.ResponseWriter, r *http.Request) {
	status := r.URL.Query().Get("status")
	if status != "" && !validStatus(status) {
		writeError(w, http.StatusBadRequest, "invalid status")
		return
	}
	rows, err := s.db.Query(r.Context(),
		`SELECT `+jobColumns+` FROM jobs WHERE $1::text = '' OR status = $1 ORDER BY id`, status)
	if err != nil {
		internalError(w, err)
		return
	}
	jobs, err := pgx.CollectRows(rows, pgx.RowToStructByPos[Job])
	if err != nil {
		internalError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, jobs)
}

func (s *server) getJob(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	rows, _ := s.db.Query(r.Context(), `SELECT `+jobColumns+` FROM jobs WHERE id = $1`, id)
	job, err := pgx.CollectExactlyOneRow(rows, pgx.RowToStructByPos[Job])
	if errors.Is(err, pgx.ErrNoRows) {
		writeError(w, http.StatusNotFound, "job not found")
		return
	}
	if err != nil {
		internalError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, job)
}

// createJob stores the job in Postgres, then pushes its ID onto the Redis queue.
// The insert is committed before enqueueing so a worker can never pop an ID
// whose row isn't visible yet. If the enqueue fails, the row is removed so no
// job is left pending without being queued.
func (s *server) createJob(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Type    string          `json:"type"`
		Payload json.RawMessage `json:"payload"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if body.Type == "" {
		writeError(w, http.StatusBadRequest, "type is required")
		return
	}
	if len(body.Payload) == 0 {
		body.Payload = json.RawMessage(`{}`)
	}

	rows, _ := s.db.Query(r.Context(),
		`INSERT INTO jobs (type, payload) VALUES ($1, $2) RETURNING `+jobColumns,
		body.Type, body.Payload)
	job, err := pgx.CollectExactlyOneRow(rows, pgx.RowToStructByPos[Job])
	if err != nil {
		internalError(w, err)
		return
	}

	if err := s.redis.LPush(r.Context(), jobQueue, job.ID).Err(); err != nil {
		log.Printf("enqueue job %d: %v", job.ID, err)
		if _, delErr := s.db.Exec(context.WithoutCancel(r.Context()), `DELETE FROM jobs WHERE id = $1`, job.ID); delErr != nil {
			log.Printf("remove unqueued job %d: %v", job.ID, delErr)
		}
		writeError(w, http.StatusServiceUnavailable, "failed to enqueue job")
		return
	}

	writeJSON(w, http.StatusCreated, job)
}

func validStatus(s string) bool {
	switch s {
	case StatusPending, StatusProcessing, StatusDone, StatusFailed:
		return true
	}
	return false
}

func pathID(w http.ResponseWriter, r *http.Request) (int64, bool) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid id")
		return 0, false
	}
	return id, true
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func internalError(w http.ResponseWriter, err error) {
	log.Printf("internal error: %v", err)
	writeError(w, http.StatusInternalServerError, "internal server error")
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
