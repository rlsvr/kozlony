# Kozlony (Közlöny)

Kozlony is a high-throughput, low-latency message board and conversation service implemented in Go. It combines an asynchronous write path backed by **NATS JetStream**, a micro-batch consumer pipeline that drains into **PostgreSQL**, an **in-memory hot read cache**, real-time **Server-Sent Events (SSE)**, and optimistic concurrency version control.

---

## Architecture & System Design

```text
===================================================================================
                                     CLIENTS
                  (Web Browsers  •  Mobile Apps  •  Stress Loaders)
===================================================================================
        │                                    ▲        │                    ▲
 [1] Write Post                              │        │ [2] Read Request   │ [3] Live Stream
  POST /v1/interactions              Response│        │  GET /feed, /thread│  GET /events (SSE)
        │                                    │        ▼                    │
        ▼                                 ┌─────────────────────────┐      │
┌─────────────────────────┐               │        READ API         │      │
│      WRITE API          │               │  • Serve cached reads   │      │
│  • Validate inputs      │               │  • Fallback to Postgres │      │
│  • Deterministic UUID   │               └───────────▲─────────────┘      │
└───────────┬─────────────┘                           │                    │
            │                                  Cache  │ Lookup /           │
   Fast ACK │ (<0.5ms)                         Hit    │ Warm               │
            ▼                                (~1µs)   ▼                    │
┌─────────────────────────┐               ┌─────────────────────────┐      │
│      NATS JETSTREAM     │               │   IN-MEMORY HOT CACHE   │      │
│  • High-speed NVMe log  │               │  • Sub-microsecond RAM  │      │
│  • 14-day sliding buffer│               │  • 80-90% hit ratio     │      │
└─────┬──────────────┬────┘               └───────────▲─────────────┘      │
      │              │                                │                    │
      │ Pull Batch   │ Realtime SSE            Return │ Query on           │
      │ (500 items)  └────────────────────────────────┼────────────────────┘
      ▼                                        Data   │ Cache Miss
┌─────────────────────────┐                           ▼
│   MICRO-BATCH DRAINER   │               ┌─────────────────────────┐
│  • Aggregate replies    │──────────────►│      POSTGRESQL 17      │
│  • Atomic SQL commit    │ Batch Insert  │  • Permanent store      │
│  • Poison pills to DLQ  │ (Multi-Row)   │  • Hierarchical trees   │
└─────────────────────────┘               │  • Serves cold reads    │
                                          └─────────────────────────┘
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

The defaults below already match the Docker Compose setup, so **no configuration is required** to run locally. To override anything, copy the sample — the `Makefile` loads `.env` automatically (`-include .env` plus `export`), so the values are applied whenever you launch via `make run`:

```bash
cp .env.example .env    # optional; edit as needed
```

Note that `.env` is honoured by `make` targets only. The binary itself reads plain environment variables and never parses `.env`, so running `./bin/kozlony` (or `go run ./cmd/kozlony`) directly will ignore the file and use the defaults in the table below.

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

4. **Load, Stress & Resilience Testing**:
   - *Current Implementation*: Coverage is unit tests (gomock, `-race`) plus the `scripts/blackbox` end-to-end suite, which asserts functional correctness only — it runs a single root post and one reply, not throughput. There is no measurement of the latency figures quoted above; they are design targets, not observed numbers.
   - *Gap to close*: The blackbox suite `t.Skip`s when the API is unreachable, so `make test` reports success in an environment where nothing was actually exercised end-to-end. It should run as a gated CI stage that fails rather than skips.
   - *Production Architecture*: Add a k6 (or `hey`/`vegeta`) load profile — sustained write throughput, burst absorption during the 500-item/50 ms micro-batch window, and concurrent read fan-out — with p50/p95/p99 latency and error-rate assertions. Pair it with Go benchmarks (`go test -bench`) on the hot read path (`GetByID` cache hit vs. PostgreSQL seek) so the ~1 µs / ~1–3 ms claims become reproducible measurements, plus a soak test long enough to cross the JetStream 14-day retention window. Fault-injection tests should cover the failure modes this architecture specifically introduces: PostgreSQL stalling mid-batch (messages `Nak` and redeliver), the drainer process being killed between commit and ack (idempotent `ON CONFLICT (id) DO NOTHING` must absorb the replay), and JetStream message deduplication expiring while a retry is still in flight.

5. **Observability — Tracing & Metrics**:
   - *Current Implementation*: Structured `zerolog` logging plus Echo request logging (method, URI, status, latency). `middleware.RequestID()` is installed, but its generated ID is discarded — `LogValuesFunc` accepts `_ echo.Context` and never reads it back — so no correlation ID reaches any log line. There is no tracing and no metrics endpoint; `prometheus/client_golang` is present in `go.mod` only as an indirect dependency of `gocache`.
   - *Production Architecture*: Instrument with OpenTelemetry, propagating W3C `traceparent` onto the JetStream message header so a single trace spans HTTP accept → publish → drainer batch → PostgreSQL insert. Without that propagation the system has an unavoidable blind spot: it cannot distinguish a slow `POST` caused by the API from one caused by a drainer batch waiting on the `50ms` flush window or a stalled database. Export Prometheus metrics with deliberately bounded label cardinality — `group_id` is client-supplied and must never become a label. The signals that matter for this architecture are the drainer batch-size histogram, JetStream consumer lag on `board-drainer`, `Nak`/redelivery counts, cache hit ratio, and RED (rate, errors, duration) per route.

6. **Secret Management**:
   - *Current Implementation*: `DATABASE_URL` falls back to a hardcoded credential baked into the Go source (`postgres://postgres:postgres@...`, `internal/config/config.go`), so a production deployment that forgets to set the variable does not fail fast — it silently connects with a well-known password. Docker Compose runs PostgreSQL as the `postgres` superuser with `postgres/postgres` published on host port 5433, and NATS is started with `-js -m 8222 -sd /data`: no authentication, no TLS, and port 4222 published to the host, meaning anything network-reachable can publish to `BOARD.*` and inject content into a board.
   - *Production Architecture*: Make `DATABASE_URL` and `NATS_URL` required with no default outside development so a missing value is a startup failure rather than a silent insecure default. Source credentials from a secret manager (HashiCorp Vault, Kubernetes Secrets, or the cloud provider's equivalent) with automated rotation. Require TLS on both hops — `sslmode=verify-full` for PostgreSQL, and NATS token or nkey credentials over TLS — and keep broker and database ports off the host network outside local development.

7. **Graceful Shutdown**:
   - *Current Implementation*: `SIGINT`/`SIGTERM` are trapped via `signal.NotifyContext` and `e.Shutdown()` is called with a 10s timeout, but that covers the HTTP server only. The micro-batch drainer is never flushed, the PostgreSQL pool is never closed, and the JetStream connection is never drained.
   - *Production Architecture*: On `SIGTERM`, stop accepting new requests, flush and ack the in-flight micro-batch, then close the database pool and the JetStream connection within the shutdown budget. As written, a rolling deploy can kill a drainer mid-batch; the idempotent `ON CONFLICT (id) DO NOTHING` insert means the replay is absorbed, so the cost is a redelivery latency spike rather than data loss — but that upsert is a data-integrity guarantee and should not be doubling as the shutdown strategy.

8. **SSE Fan-Out**:
   - *Current Implementation*: Every connected SSE client opens its own JetStream subscription, so N open browser tabs mean N broker-side subscriptions.
   - *Production Architecture*: Use one shared subscription per replica feeding a bounded in-process fan-out buffer, dropping slow consumers rather than letting them apply backpressure to the broker.

9. **Rate Limiting & Request Limits**:
   - *Current Implementation*: Body and title length are capped in the handler (65535 and 255 characters) on both create and update, but nothing bounds request *rate* — a client may create interactions without limit, and the number of concurrent SSE connections is likewise uncapped.
   - *Production Architecture*: Apply Echo's rate-limiter middleware with a token bucket keyed by authenticated subject (falling back to IP for anonymous boards), stricter per-route limits on `POST /v1/interactions` to protect the JetStream stream from write floods, and an explicit cap on concurrent SSE connections per replica.

10. **Resiliency — Retry, Backoff & Circuit Breaking**:
    - *Current Implementation*: There is no retry, backoff, or circuit breaker anywhere in the service. When `InsertBatch` fails the drainer `Nak`s every message and relies on JetStream redelivery after `AckWait` (30s), while the run loop's only throttle is a fixed `100ms` sleep — so a downed PostgreSQL means roughly ten failing batches per second, indefinitely, with nothing to stop the pressure. The consumer sets neither `MaxDeliver` nor `BackOff`, so a message that can never be inserted is redelivered forever and never reaches the DLQ; the DLQ currently handles only poison payloads (unmarshal and validation failures) on first failure. The publish path makes a single attempt and surfaces a 500 on failure.
    - *Production Architecture*: Put a circuit breaker in front of each dependency (PostgreSQL, JetStream) so an outage trips it open rather than hammering a dead service, and replace the fixed sleep with exponential backoff plus jitter. Configure `MaxDeliver` with a `BackOff` schedule on the consumer so a permanently-failing message lands in the DLQ after a bounded number of attempts instead of looping indefinitely, reserving the DLQ for genuinely poison traffic rather than transient dependency failure. Retry the publish path with jittered, bounded attempts before returning 5xx.
