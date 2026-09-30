# Kozlony (Közlöny)

Kozlony is a high-throughput, low-latency message board and conversation service implemented in Go. It combines an asynchronous write path backed by **NATS JetStream**, a micro-batch consumer pipeline that drains into **PostgreSQL**, an **in-memory hot read cache**, real-time **Server-Sent Events (SSE)**, and optimistic concurrency version control.

---

## Architecture & System Design

```mermaid
flowchart TD
    subgraph ClientLayer["Client Layer"]
        Client["Client / Web UI / Mobile"]
    end

    subgraph APITier["API & Hot Cache Tier"]
        API["HTTP API (Echo v4)<br/>POST /v1/interactions<br/>GET /v1/interactions<br/>PUT /v1/interactions/:id<br/>GET /v1/interactions/:id<br/>GET /v1/interactions/events"]
        Cache[("In-Memory Hot Cache<br/>eko/gocache (go-cache)<br/>Top Interactions (~1 µs Reads)")]
    end

    subgraph IngestionBuffer["Ingestion & Realtime Buffer"]
        JS[("NATS JetStream (FileStorage)<br/>Sliding Window: 14 Days<br/>Stream: BOARD (BOARD.*.evt.>)")]
        DLQ[("Dead Letter Queue<br/>Subject: BOARD.dlq")]
    end

    subgraph PersistenceTier["Relational Database Tier"]
        Drainer["Micro-Batch Drainer<br/>500 items / 50ms flush"]
        Postgres[("Vanilla PostgreSQL 17 on NVMe<br/>Hierarchical Tree & Group Feeds")]
    end

    %% Write Path
    Client -->|"1. POST /v1/interactions (required Idempotency-Key)"| API
    API -->|"2. Publish Event (deterministic UUID from key)"| JS
    JS -->|"3. Sync Ack (<0.5ms)"| API
    API -->|"4. HTTP 201 Created"| Client
    API -.->|"5. Push Live SSE Event"| Client

    %% Read Path & Cache Shield
    Client -->|"GET /v1/interactions/:id or /feed"| API
    API <-->|"Hot Cache Hit (~1 µs)"| Cache
    API -.->|"Cache Miss (~1-2ms seek)"| Postgres
    API -.->|"Warm Cache"| Cache

    %% Real-time SSE
    JS -.->|"Ephemeral Stream Subscription"| API

    %% Background Ingestion
    JS -->|"Batch Pull Fetch"| Drainer
    Drainer -->|"Atomic Transaction (InsertBatch + Reply Counts)"| Postgres
    Drainer -->|"Poison / Malformed Payload"| DLQ
    Postgres -->|"Commit OK"| Drainer
    Drainer -->|"Batch msg.Ack()"| JS
```

### Core Architectural Pillars

1. **Decoupled Asynchronous Write Path (<1ms Fast ACK)**:
   - When a client issues `POST /v1/interactions`, the server validates constraints, derives a **deterministic UUID** from the required `Idempotency-Key` header, stores the interaction in the local hot cache, and publishes an `InteractionCreatedEvent` to NATS JetStream.
   - The `Idempotency-Key` header is **mandatory**: the interaction ID is a pure function of that key (`UUID v5` via SHA-1 of the key, or the key itself when it is already a valid UUID). Replaying a request with the same key therefore resolves to the same row — the write is deduplicated at the database (`ON CONFLICT (id) DO NOTHING`) and at JetStream (`Nats-Msg-Id`), and the API answers `200 OK` with the existing interaction instead of creating a duplicate.
   - **Tradeoff**: because the ID must be a pure function of the key for idempotency to hold, IDs are no longer time-ordered `UUIDv7` values. They are `UUID v5` (SHA-1) values, so B-Tree inserts are no longer append-mostly and index locality is worse than a v7 layout would give. Deterministic identity was chosen over insert locality.
   - The HTTP `201 Created` response returns immediately upon JetStream confirmation, decoupling user-facing write latency from relational database writes.

2. **Hot Read Shield (In-Memory Cache)**:
   - Powered by `eko/gocache` with an in-memory `go-cache` store, holding the most active interactions and parent hierarchy metadata in RAM.
   - Delivers sub-microsecond (~1 µs) lookups on parent resolution, individual interactions, and warmed thread hierarchies, shielding PostgreSQL from connection contention during traffic spikes.

3. **Micro-Batch Ingestion Pipeline**:
   - A durable JetStream pull consumer fetches up to `DRAIN_BATCH_SIZE` messages within `DRAIN_FLUSH_INTERVAL` (defaults: 500 items / 50ms).
   - Writes the batch inside an atomic PostgreSQL transaction using multi-row multi-value inserts with `ON CONFLICT (id) DO NOTHING`.
   - In the same transaction, aggregates reply increments and atomically updates `reply_count` on parent replies and thread root posts.

4. **Dead Letter Queue (DLQ) Poison Message Handling**:
   - Malformed payloads or unmarshal failures are trapped and diverted to `BOARD.dlq` with diagnostic headers (`X-DLQ-Reason`, `X-Original-Subject`) and acknowledged, preventing worker deadlocks.

5. **Storage Strategy (Vanilla PostgreSQL on NVMe + Sliding JetStream)**:
   - **No Duplicate Storage**: JetStream retains messages within a 14-day sliding window (`LimitsPolicy`, `MaxAge: 336h`), acting strictly as a high-speed write-ahead buffer and real-time event pipeline. PostgreSQL remains the permanent source of truth.
   - **No TimescaleDB / NoSQL Complexity**: 100M posts and 500M replies take ~310 GB on disk. A standard enterprise NVMe SSD comfortably accommodates decades of forum history. PostgreSQL's `shared_buffers` + OS page cache keeps the active working set in RAM, serving cold threads in 1–3 ms.

6. **Hierarchical Thread Tree Schema**:
   - Recursive domain model using `root_id`, `parent_id`, and `depth`.
   - `GET /v1/interactions/{id}` retrieves an entire conversation thread in a single indexed seek (`WHERE id = $1 OR root_id = $1 ORDER BY created_at ASC`).

7. **Optimistic Concurrency Control (OCC)**:
   - `PUT /v1/interactions/{id}` verifies version matching (`WHERE id = $1 AND version = $2`).
   - Increments `version = version + 1`, updates `updated_at = NOW()`, and broadcasts an `InteractionEditedEvent` on JetStream. Stale updates return `409 Conflict`.
   - An omitted or blank `title` is preserved rather than overwritten (`SET title = COALESCE($3, title)`), so a body-only edit can never wipe a root post's title.

8. **Real-Time Streaming (SSE)**:
   - Clients subscribe to `GET /v1/interactions/events?group_id=...` for a continuous `text/event-stream`. Periodic `:ping\n\n` heartbeats every 15s prevent intermediate proxy timeouts.

---

### Latency Profile Summary

| Operation | In-Memory Hot Cache | PostgreSQL Warm NVMe | PostgreSQL Under Heavy Contention |
|---|---|---|---|
| **Write Interaction (Sync JetStream Ack)** | **~0.25 ms** *(JetStream NVMe)* | Async batch worker | Async batch worker (unaffected) |
| **Top Feed Listing (Recent Posts)** | **~1.1 µs** *(RAM hit)* | 1.0 – 2.0 ms | 40 – 120 ms *(connection contention)* |
| **Conversation Thread (Root + Replies)** | **~1.2 µs** *(RAM hit)* | 1.5 – 3.0 ms | 50 – 180 ms |
| **Historical Thread (Cold Archive)** | Cache Miss (Fallthrough) | **1.5 – 3.0 ms** *(NVMe seek)* | 10 – 30 ms |

---

## Prerequisites

Ensure the following tools are installed:

- **Go**: Version `1.26+` (`go.mod` declares `go 1.26.1`)
- **Docker & Docker Compose**: For local PostgreSQL and NATS services
- **oapi-codegen**: Version `v2.4+`
  ```bash
  go install github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen@latest
  ```
- **golangci-lint**: Version `v2+`
  ```bash
  go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest
  ```
- **go-jsonschema**: Handled automatically via `go run` in `make asyncapi-gen`.
- **mockgen**: Handled automatically via `go run` in `make mocks`.

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

```bash
docker compose up -d
```

Verify services are healthy:
```bash
docker compose ps
```

* PostgreSQL runs on port `5433` (preventing host port collisions), with migrations automatically applied.
* NATS JetStream runs on port `4222` (monitoring on `8223`).

### 2. Environment Configuration

Configuration is loaded from environment variables (or `.env`):

| Variable | Default | Description |
|---|---|---|
| `ADDR` | `:8080` | HTTP server listen address |
| `DATABASE_URL` | `postgres://postgres:postgres@localhost:5433/messageboard?sslmode=disable` | PostgreSQL connection string |
| `NATS_URL` | `nats://localhost:4222` | NATS JetStream broker URL |
| `NATS_STREAM_NAME` | `BOARD` | JetStream stream name |
| `NATS_CONSUMER_NAME` | `board-drainer` | Durable pull consumer name |
| `MAX_CACHED_INTERACTIONS` | `500000` | In-memory cache capacity limit (FIFO eviction) |
| `CACHE_TTL` | `1h` | In-memory cache item time-to-live |
| `DRAIN_BATCH_SIZE` | `500` | Drainer micro-batch size |
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
| `GET` | `/v1/readyz` | Readiness probe (verifies active DB pool & NATS broker connectivity) |
| `GET` | `/v1/interactions` | Feed listing of root posts with cursor-based pagination |
| `POST` | `/v1/interactions` | Create an interaction (**`Idempotency-Key` header required**; title required for root post, `parent_id` for reply) |
| `GET` | `/v1/interactions/{id}` | Retrieve interaction details with recursive conversation thread |
| `PUT` | `/v1/interactions/{id}` | Edit interaction body/title with optimistic concurrency version check |
| `GET` | `/v1/interactions/events` | Real-time Server-Sent Events (SSE) stream (`group_id` filter optional) |

### Quick Example

```bash
# Create a root post (Idempotency-Key is required)
curl -X POST http://localhost:8080/v1/interactions \
  -H 'Content-Type: application/json' \
  -H "Idempotency-Key: $(uuidgen)" \
  -d '{"title":"First post","body":"Hello board","author":"alice"}'

# Reply to it (parent_id from the create response)
curl -X POST http://localhost:8080/v1/interactions \
  -H 'Content-Type: application/json' \
  -H "Idempotency-Key: $(uuidgen)" \
  -d '{"body":"Nice one","author":"bob","parent_id":"<interaction-id>"}'

# Edit: omit "title" to change only the body; the existing title is preserved
curl -X PUT http://localhost:8080/v1/interactions/<interaction-id> \
  -H 'Content-Type: application/json' \
  -d '{"body":"Updated text","version":1}'
```

---

## Testing

### Unit and Mock Tests
```bash
make test
```
Executes all unit tests with the Go race detector enabled, utilizing Uber gomock mocks for database repository, messaging publishers, and in-memory cache interfaces.

### Code Quality and Linters
```bash
make check
```
Runs `golangci-lint` with strict checks (gocost, revive, errcheck, gosec, zerologlint) and checks `go mod tidy`.

### End-to-End Blackbox Suite
With the server and dependencies running:
```bash
make test-blackbox
```
Or directly:
```bash
go run ./scripts/blackbox -api http://localhost:8080 -nats nats://localhost:4222 -group my-board
```

The blackbox suite verifies:
1. `/v1/healthz` and `/v1/readyz` operational status.
2. Direct NATS JetStream event publishing and receipt.
3. SSE real-time event streaming over `/v1/interactions/events`.
4. Root post creation with mandatory title validation.
5. `Idempotency-Key` replay deduplication.
6. Multi-level nested replies (`depth >= 2`) with recursive parent lookup.
7. Micro-batch drainer flush to PostgreSQL and thread hierarchy retrieval.
8. Optimistic concurrency edit (`version`) and `409 Conflict` rejection on stale versions.

---

## Tradeoffs & Production Considerations

Given the half-day time constraint of this assignment, several pragmatic architectural tradeoffs were made. In a production rollout, the following enhancements would be prioritized:

1. **User Authentication & Authorization (AuthN/AuthZ)**:
   - *Current Implementation*: Interactions accept a user display name (`author: string`), suitable for open/guest discussion boards without signup friction.
   - *Production Architecture*: Introduce an OIDC / OAuth2 JWT Bearer authentication middleware (e.g. Keycloak, Auth0, or Authentik). The authenticated subject `user_id` would be injected into the Echo request context, preventing identity spoofing and enforcing granular Role-Based Access Control (RBAC) so that only authors or moderators can edit content.

2. **Hierarchical Soft Deletion & Moderation**:
   - *Current Implementation*: Content mutations are performed via version-controlled edits; deletion endpoints are omitted.
   - *Production Architecture*: Implement soft deletion (`deleted_at TIMESTAMPTZ`) with tombstone rendering (`"[deleted by author]"`). This preserves conversation thread integrity so that nested reply subtrees are never orphaned when a parent interaction is removed.

3. **Horizontal Sharding & Multi-Board Partitioning**:
   - *Current Implementation*: Boards are logically partitioned via `group_id` with composite B-Tree indexes on `(group_id, created_at DESC)`.
   - *Production Architecture*: Under massive scale, PostgreSQL table partitioning (declarative hash or list partitioning by `group_id`) combined with NATS JetStream subject filtering (`BOARD.<group_id>.*`) enables horizontal scaling across independent database nodes and localized caching per tenant/board.
