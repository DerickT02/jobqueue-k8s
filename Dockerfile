FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /out/api . \
 && CGO_ENABLED=0 go build -ldflags="-s -w" -o /out/worker ./worker

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/api /api
COPY --from=build /out/worker /worker
EXPOSE 8080
ENTRYPOINT ["/api"]
