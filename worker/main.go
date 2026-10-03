// Command worker pulls job IDs off the Redis queue, runs them, and records the
// result in Postgres.
//
// Flow for each job:
//  1. BLMOVE the ID from jobs:queue to jobs:processing, so it isn't lost if the
//     worker dies mid-job.
//  2. Claim the job in Postgres (pending -> processing). The claim only succeeds
//     for pending jobs, so a job is never run twice.
//  3. Run the handler registered for the job's type.
//  4. Mark the job done or failed, then remove its ID from jobs:processing.
//
// On startup, any IDs left in jobs:processing by a previous run are reset to
// pending and put back on the queue. This assumes a single worker process
// (with WORKER_CONCURRENCY goroutines); multiple worker processes would need
// a processing list per process.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

// These must match the API in ../main.go.
const (
	jobQueue        = "jobs:queue"
	processingQueue = "jobs:processing"
)

// Handler does the work for one job type. Returning an error marks the job failed.
type Handler func(ctx context.Context, payload json.RawMessage) error

// handlers maps a job's type to the code that runs it. Add new job types here.
var handlers = map[string]Handler{
	"send_email": sendEmail,
	"resize":     resize,
}

type worker struct {
	db         *pgxpool.Pool
	redis      *redis.Client
	jobTimeout time.Duration
}

func main() {
	dsn := getenv("DATABASE_URL", "postgres://postgres:postgres@localhost:5433/goservices?sslmode=disable")
	redisURL := getenv("REDIS_URL", "redis://localhost:6379/0")
	concurrency := getenvInt("WORKER_CONCURRENCY", 4)
	jobTimeout := time.Duration(getenvInt("JOB_TIMEOUT_SECONDS", 60)) * time.Second

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

	redisOpts, err := redis.ParseURL(redisURL)
	if err != nil {
		log.Fatalf("parse redis url: %v", err)
	}
	rdb := redis.NewClient(redisOpts)
	defer rdb.Close()

	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("connect to redis: %v", err)
	}

	w := &worker{db: db, redis: rdb, jobTimeout: jobTimeout}

	if err := w.recover(ctx); err != nil {
		log.Fatalf("recover in-flight jobs: %v", err)
	}

	log.Printf("worker started with concurrency %d", concurrency)
	var wg sync.WaitGroup
	for i := range concurrency {
		wg.Go(func() { w.loop(ctx, i) })
	}

	<-ctx.Done()
	log.Println("shutting down, waiting for in-flight jobs")
	wg.Wait()
	log.Println("worker stopped")
}

// recover re-queues jobs that a previous run took off the queue but never finished.
func (w *worker) recover(ctx context.Context) error {
	ids, err := w.redis.LRange(ctx, processingQueue, 0, -1).Result()
	if err != nil {
		return err
	}
	for _, raw := range ids {
		id, err := strconv.ParseInt(raw, 10, 64)
		if err == nil {
			if _, err := w.db.Exec(ctx,
				`UPDATE jobs SET status = 'pending', updated_at = now() WHERE id = $1 AND status = 'processing'`,
				id); err != nil {
				return err
			}
		}
		if err := w.redis.LMove(ctx, processingQueue, jobQueue, "RIGHT", "RIGHT").Err(); err != nil {
			return err
		}
	}
	if len(ids) > 0 {
		log.Printf("re-queued %d unfinished job(s)", len(ids))
	}
	return nil
}

// loop takes jobs off the queue until ctx is cancelled. A job that has started
// is always allowed to finish.
func (w *worker) loop(ctx context.Context, n int) {
	for ctx.Err() == nil {
		// Block for a few seconds at a time so shutdown is noticed promptly.
		raw, err := w.redis.BLMove(ctx, jobQueue, processingQueue, "RIGHT", "LEFT", 5*time.Second).Result()
		if errors.Is(err, redis.Nil) || ctx.Err() != nil {
			continue
		}
		if err != nil {
			log.Printf("[%d] dequeue: %v", n, err)
			time.Sleep(time.Second)
			continue
		}
		w.process(context.WithoutCancel(ctx), n, raw)
	}
}

func (w *worker) process(ctx context.Context, n int, raw string) {
	// Whatever happens, the ID is done with once we return.
	defer func() {
		if err := w.redis.LRem(ctx, processingQueue, 1, raw).Err(); err != nil {
			log.Printf("[%d] job %s: remove from processing list: %v", n, raw, err)
		}
	}()

	id, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		log.Printf("[%d] skipping invalid job id %q", n, raw)
		return
	}

	var jobType string
	var payload json.RawMessage
	err = w.db.QueryRow(ctx,
		`UPDATE jobs SET status = 'processing', updated_at = now()
		 WHERE id = $1 AND status = 'pending'
		 RETURNING type, payload`, id).Scan(&jobType, &payload)
	if errors.Is(err, pgx.ErrNoRows) {
		log.Printf("[%d] job %d: not found or not pending, skipping", n, id)
		return
	}
	if err != nil {
		// Leave the job pending and put it back so it isn't lost.
		log.Printf("[%d] job %d: claim: %v; re-queueing", n, id, err)
		w.redis.LPush(ctx, jobQueue, raw)
		return
	}

	log.Printf("[%d] job %d (%s): started", n, id, jobType)
	start := time.Now()
	runErr := w.run(ctx, jobType, payload)

	status := "done"
	if runErr != nil {
		status = "failed"
		log.Printf("[%d] job %d (%s): failed after %s: %v", n, id, jobType, time.Since(start).Round(time.Millisecond), runErr)
	} else {
		log.Printf("[%d] job %d (%s): done in %s", n, id, jobType, time.Since(start).Round(time.Millisecond))
	}

	if _, err := w.db.Exec(ctx,
		`UPDATE jobs SET status = $1, updated_at = now() WHERE id = $2`, status, id); err != nil {
		log.Printf("[%d] job %d: mark %s: %v", n, id, status, err)
	}
}

// run calls the job's handler with a timeout, turning panics into errors.
func (w *worker) run(ctx context.Context, jobType string, payload json.RawMessage) (err error) {
	h, ok := handlers[jobType]
	if !ok {
		return fmt.Errorf("no handler for job type %q", jobType)
	}
	ctx, cancel := context.WithTimeout(ctx, w.jobTimeout)
	defer cancel()
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: %v", r)
		}
	}()
	return h(ctx, payload)
}

// Example handlers. Replace these with real work.

func sendEmail(ctx context.Context, payload json.RawMessage) error {
	var p struct {
		To string `json:"to"`
	}
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("invalid payload: %w", err)
	}
	if p.To == "" {
		return errors.New(`payload.to is required`)
	}
	log.Printf("sending email to %s", p.To)
	return sleep(ctx, 500*time.Millisecond)
}

func resize(ctx context.Context, payload json.RawMessage) error {
	return sleep(ctx, time.Second)
}

func sleep(ctx context.Context, d time.Duration) error {
	select {
	case <-time.After(d):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getenvInt(key string, fallback int) int {
	v, err := strconv.Atoi(os.Getenv(key))
	if err != nil || v <= 0 {
		return fallback
	}
	return v
}
