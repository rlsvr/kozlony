# Kozlony (Közlöny)

Kozlony is a high-throughput, low-latency message board and conversation service implemented in Go. It combines an asynchronous write-path backed by **NATS JetStream**, a micro-batch consumer pipeline that drains into **PostgreSQL**, real-time **Server-Sent Events (SSE)**, and optimistic concurrency version control.

---

## Architecture & System Design

```mermaid
flowchart TD
    Client[Client / Web UI]

    subgraph "Ingress & REST API"
        API["HTTP API (Echo v4)<br/>POST /v1/interactions<br/>PUT /v1/interactions/:id<br/>GET /v1/interactions/:id<br/>GET /v1/interactions/events"]
    end

    subgraph "NATS JetStream (Write Path)"
        JS[("Stream: BOARD<br/>Subjects: BOARD.*.evt.>")]
        DLQ[("Subject: BOARD.dlq<br/>Dead Letter Queue")]
    end

    subgraph "Worker Pipeline"
        Drainer["Micro-Batch Drainer<br/>500 items / 50ms window"]
    end

    subgraph "Persistence"
        Postgres[("PostgreSQL 17<br/>Tree & Flat Feed")]
    end

    %% Client Interactions
    Client -->|"POST /v1/interactions (Idempotency-Key)"| API
    Client -->|"GET /v1/interactions/events (SSE)"| API
    Client -->|"GET /v1/interactions/:id"| API

    %% Write Path
    API -->|"Publish Event (UUIDv7)"| JS
    API -->|"200/201 Fast ACK (<5ms)"| Client

    %% Real-time SSE
    JS -.->|"Subject Subscription (BOARD.*.evt.>)"| API

    %% Batch Ingestion
    JS -->|"Pull Batch (FetchMaxWait)"| Drainer
    Drainer -->|"Atomic Transaction (InsertBatch + Reply Counts)"| Postgres
    Drainer -->|"Poison / Malformed Payload"| DLQ

    %% Read Path
    API -->|"Recursive CTE Query"| Postgres
```

### Key Architectural Concepts

1. **Async Write Path (<5ms Fast ACK)**:
   - When a client sends `POST /v1/interactions`, the server validates the input, generates a sortable **UUIDv7**, and publishes the event to NATS JetStream.
   - The HTTP response returns `200/201` as soon as NATS confirms publish, completely decoupling the client write latency from database writes.
2. **Idempotency**:
   - Clients supply an `Idempotency-Key` header on `POST /v1/interactions`.
   - Replays with the same idempotency key return the existing interaction without publishing duplicate events.
3. **Micro-Batch Drainer**:
   - A durable pull consumer pulls up to `DrainBatchSize` messages within `DrainFlushInterval` (default 500 items / 50ms).
   - Writes the batch in an atomic PostgreSQL transaction with `ON CONFLICT (id) DO NOTHING` and atomically increments `reply_count` on parents and root posts.
4. **Dead Letter Queue (DLQ)**:
   - Poison messages that fail JSON deserialization or schema validation are automatically routed to `BOARD.dlq` with diagnostic headers (`X-DLQ-Reason`, `X-Original-Subject`) and acknowledged, preventing pipeline stalls.
5. **Real-Time Streaming (SSE)**:
   - Clients connect to `GET /v1/interactions/events?group_id=...` to receive a `text/event-stream` of live interaction created and edited events.
   - Includes periodic `:ping\n\n` heartbeats every 15s to keep connections alive through proxies.
6. **Optimistic Concurrency Control**:
   - `PUT /v1/interactions/{id}` requires a `version` field matching the current state in PostgreSQL.
   - Increments `version` on success; returns `409 Conflict` if modified concurrently.
7. **Readiness Probe**:
   - `GET /v1/readyz` verifies active connectivity to both PostgreSQL connection pool and NATS broker.

---

## Prerequisites

Before running or developing, ensure the following are installed:

- **Go**: Version `1.24+`
- **Docker & Docker Compose**: For local PostgreSQL and NATS services
- **oapi-codegen**: Version `v2.4+`
  ```bash
  go install github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen@latest
  ```
- **golangci-lint**: Version `v2+` (e.g. `v2.1.2` or later)
  ```bash
  go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest
  ```
- **go-jsonschema**: Handled automatically via `go run` in the Makefile (`github.com/atombender/go-jsonschema@v0.22.0`).
- **mockgen**: Handled automatically via `go run go.uber.org/mock/mockgen@v0.6.0` in `make mocks`.

---

## Makefile Targets

| Target | Description |
|---|---|
| `make all` | Generate stubs (`gen`), generate mocks (`mocks`), compile binary (`build`), and run tests (`test`). |
| `make gen` | Runs `openapi-gen` and `asyncapi-gen`. |
| `make openapi-gen` | Generates Echo server interfaces from `api/openapi.yaml` into `internal/api/server/server.gen.go`. |
| `make asyncapi-gen` | Generates Go structs from AsyncAPI schemas in `schemas/`. |
| `make mocks` | Generates Uber gomock mocks for package interfaces across `internal/`. |
| `make build` | Compiles the binary into `bin/kozlony`. |
| `make run` | Builds and executes the server. |
| `make test` | Runs unit and integration tests with the Go race detector (`-race`). |
| `make test-verbose` | Runs tests in verbose mode (`-v`). |
| `make test-blackbox` | Runs the end-to-end blackbox test suite against a running service. |
| `make fmt` | Formats code with `golangci-lint fmt`. |
| `make lint` | Runs `golangci-lint run ./...`. |
| `make check` | Runs `fmt`, `lint`, and `go mod tidy`. |
| `make clean` | Removes compiled binaries. |

---

## Local Development & Running

### 1. Start Infrastructure via Docker Compose

Start the PostgreSQL and NATS JetStream services:

```bash
docker compose up -d
```

Verify services are healthy:
```bash
docker compose ps
```

### 2. Environment Configuration

Configuration is loaded from environment variables (or `.env`):

| Variable | Default | Description |
|---|---|---|
| `ADDR` | `:8080` | HTTP server listen address |
| `DATABASE_URL` | `postgres://postgres:postgres@localhost:5433/messageboard?sslmode=disable` | PostgreSQL connection string |
| `NATS_URL` | `nats://localhost:4222` | NATS JetStream broker URL |
| `NATS_STREAM_NAME` | `BOARD` | JetStream stream name |
| `NATS_CONSUMER_NAME` | `board-drainer` | Durable pull consumer name |
| `DRAIN_BATCH_SIZE` | `500` | Drainer batch size |
| `DRAIN_FLUSH_INTERVAL`| `50ms` | Drainer flush window timeout |
| `LOG_LEVEL` | `info` | Zerolog level (`debug`, `info`, `warn`, `error`) |
| `PRETTY_LOGGING` | `true` | Human-readable console log format |

### 3. Run the Application

```bash
make run
```

---

## API Endpoints

| Method | Path | Description |
|---|---|---|
| `GET` | `/v1/healthz` | Liveness probe (returns `200 OK`) |
| `GET` | `/v1/readyz` | Readiness probe (checks DB pool & NATS broker) |
| `GET` | `/v1/interactions` | Feed listing of root interactions with scrollback cursor pagination |
| `POST` | `/v1/interactions` | Create an interaction (title required for root post, `parent_id` for reply, supports `Idempotency-Key`) |
| `GET` | `/v1/interactions/{id}` | Retrieve interaction details with recursive conversation thread |
| `PUT` | `/v1/interactions/{id}` | Edit an interaction body/title with optimistic concurrency version check |
| `GET` | `/v1/interactions/events` | Real-time Server-Sent Events (SSE) stream (`group_id` filter optional) |

---

## Testing

### Unit and Mock Tests
```bash
make test
```

Runs all unit tests with the race detector enabled, using Uber Go Mock (`gomock`) for database, consumer, and publisher mocks.

### Code Quality and Linters
```bash
make check
```

Runs `golangci-lint` (checking revive, nestif, bodyclose, gosec, zerologlint, and errcheck) and ensures clean module tidiness.

### End-to-End Blackbox Suite
With the server and dependencies running:
```bash
make test-blackbox
```
Or directly with custom parameters:
```bash
go run ./scripts/blackbox -api http://localhost:8080 -nats nats://localhost:4222 -group my-board
```

The blackbox suite verifies:
1. Health and readiness endpoints.
2. Direct NATS event stream verification.
3. SSE real-time event streaming (`/v1/interactions/events`).
4. Root post creation and `Idempotency-Key` replay deduplication.
5. Nested reply creation with multi-level depth.
6. Micro-batch drainer flush to PostgreSQL and thread tree retrieval.
7. Optimistic concurrency edit and `409 Conflict` detection on stale versions.
