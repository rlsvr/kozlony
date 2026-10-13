# Kozlony (Közlöny)

Kozlony is a high-throughput, low-latency message board and conversation service implemented in Go. It combines an asynchronous write path backed by **NATS JetStream**, a micro-batch consumer pipeline that drains into **PostgreSQL**, an **in-memory hot read cache**, real-time **Server-Sent Events (SSE)**, and optimistic concurrency version control.

---

## Architecture & System Design

```text
===================================================================================================
                                              CLIENTS
                           (Web Browsers  •  Mobile Apps  •  Stress Loaders)
===================================================================================================
        │                                             ▲        │                    ▲
 [1] Write Post                                       │        │ [2] Read Request   │ [3] Live Stream
  POST /v1/interactions                       Response│        │  GET /feed, /thread│  GET /events (SSE)
        │                                             │        ▼                    │  ?group_id=&root_id=
        ▼                                          ┌─────────────────────────┐      │
┌─────────────────────────┐                        │        READ API         │      │
│      WRITE API          │                        │  • Serve cached reads   │      │
│  • Validate inputs      │                        │  • Fallback to Postgres │      │
│  • Idempotent UUID v5   │                        └───────────▲─────────────┘      │
│  • Hash partition % 16  │                                    │                    │
└───────────┬─────────────┘                             Cache  │ Lookup /           │
            │                                           Hit    │ Warm               │
   Fast ACK │ (<0.5ms)                                  (~30ns)▼                    │
            ▼                                      ┌─────────────────────────┐      │
┌─────────────────────────┐                        │   IN-MEMORY HOT CACHE   │      │
│  NATS JETSTREAM INGEST  │                        │  • 64-way Sharded LRU   │      │
│  • 16 Pinned Partitions │                        │  • B-Tree Feed Index    │      │
│    INGEST.p.0 .. p.15   │                        └───────────▲─────────────┘      │
│  • High-speed NVMe log  │                                    │                    │
└─────┬───────────────────┘                                    │                    │
      │                                                        │                    │
      │ 16 Parallel Pull Micro-Batches                         │                    │
      ▼                                                        │                    │
┌─────────────────────────┐   Non-blocking Async Broadcast     │                    │
│   16 DRAINER WORKERS    │────────────────────────────────────┼────────────────┐   │
│  • 1-to-1 pinned        │   Core NATS Live Subjects          │                │   │
│  • Multi-row unnest CTE │   LIVE.<group>.feed                │                ▼   │
│  • ON CONFLICT DO NOTH. │   LIVE.<group>.thread.<id>         │    ┌─────────────────────────┐
└───────────┬─────────────┘                                    │    │   ZERO-FILTER FAN-OUT   │
            │                                                  │    │  • Per-topic Core NATS  │
            ▼                                                  │    │  • Direct raw byte pipe │
┌─────────────────────────┐        Batch Bulk Insert           │    │  • Dynamic ref-counting │
│     COUNTER WORKER      │───────────────────────┐            │    │  • Non-blocking drop    │
│  • Async reply counts   │                       │            │    │  • ~2.89M events/sec    │
│  • fillfactor = 85 HOT  │                       ▼            │    └───────────┬─────────────┘
└─────────────────────────┘               ┌─────────────────────────┐       │
                                          │      POSTGRESQL 17      │       │
                                          │  • Dual Pools (30w/20r) │       │
                                          │  • Keyspace index       │       │
                                          │  • Permanent store      │       │
                                          └─────────────────────────┘       │
                                                                            ▼
                                                                Raw SSE Byte Streams
```

### Core Architectural Pillars

1. **Decoupled Asynchronous Write Path with 16-Way Partitioning (<1ms Fast ACK)**:
   - When a client issues `POST /v1/interactions`, the server validates constraints, derives a **deterministic UUID** from the required `Idempotency-Key` header, stores the interaction in the local hot cache, and computes a deterministic partition index: `PartitionForThread(threadRoot, 16)`.
   - The message is published to the dedicated JetStream partition `INGEST.p.<i>`. This guarantees strict causal ordering for all replies within a conversation thread while distributing root post traffic uniformly across 16 independent parallel stream partitions.
   - The `Idempotency-Key` header is **mandatory**: the interaction ID is a pure function of that key (`UUID v5` via SHA-1 of the key, or the key itself when it is already a valid UUID). Replaying a request with the same key resolves to the same row — deduplicated at JetStream (`Nats-Msg-Id`) and PostgreSQL (`ON CONFLICT (id) DO NOTHING`).
   - The HTTP `201 Created` response returns immediately upon JetStream confirmation, decoupling user-facing write latency from relational database disk I/O.

2. **Hot Read Shield (In-Memory Cache)**:
   - Powered by a custom 64-way sharded LRU with doubly-linked lists, thread-based hierarchical caching, a global $O(1)$ node-to-thread lookup table, and in-memory `google/btree` feed indexing.
   - Delivers sub-microsecond (~30 ns single node, ~450 ns full thread, ~2 µs feed pagination page) lookups on parent resolution, individual interactions, warmed thread hierarchies, and cursor-based feed pagination, shielding PostgreSQL from connection contention during traffic spikes.

3. **16-Way Partitioned Micro-Batch Ingestion & PostgreSQL Multi-Row Unnest Engine**:
   - 16 independent, durable JetStream pull consumers (`board-drainer-0` through `board-drainer-15`) are pinned 1-to-1 to dedicated drainer worker goroutines. This eliminates consumer lock contention and eliminates JetStream backlog.
   - Batch insertion utilizes a single-statement multi-row unnest CTE: `SELECT * FROM unnest(...) ON CONFLICT (id) DO NOTHING`. Instead of parsing hundreds of individual SQL statements, PostgreSQL parses and plans 1 statement per batch.
   - **Dedicated Dual Connection Pools**: Writes (drainer and counter workers) and reads (HTTP API queries) are segregated into separate pools (`writePool: 30`, `readPool: 20`) with strict acquire timeouts to prevent read starvation.
   - **Storage Tuning & HOT Updates**: The `interactions` table is configured with `fillfactor = 85` and composite index `(group_id, created_at DESC, id DESC)`. This enables Heap-Only Tuple (HOT) updates for asynchronous `reply_count` updates, preventing table bloat and index writes.
   - Reply count recalculations remain decoupled: the drainer emits a `reply-counts-recalculated` event to JetStream, consumed by dedicated `CounterConsumer` workers that apply batched reply counts in separate, short transactions.

4. **Dead Letter Queue (DLQ) Poison Message Handling**:
   - Malformed payloads or unmarshal failures are trapped and diverted to `BOARD.dlq` with diagnostic headers (`X-DLQ-Reason`, `X-Original-Subject`) and acknowledged, preventing worker deadlocks.

5. **Storage Strategy (Vanilla PostgreSQL on NVMe + Sliding JetStream)**:
   - **No Duplicate Storage**: JetStream retains messages within a 14-day sliding window (`LimitsPolicy`, `MaxAge: 336h`, `DiscardOld`), acting strictly as a high-speed write-ahead buffer and real-time event pipeline. PostgreSQL remains the permanent source of truth.
   - **Tuned PostgreSQL Engine**: Configured with `max_wal_size=10GB`, `checkpoint_completion_target=0.9`, `checkpoint_timeout=15min`, `shared_buffers=1GB`, and `synchronous_commit=off`, eliminating periodic checkpoint disk I/O thrashing.

6. **Hierarchical Thread Tree Schema**:
   - Recursive domain model using `root_id`, `parent_id`, and `depth`.
   - `GET /v1/interactions/{id}` retrieves an entire conversation thread in a single indexed seek (`WHERE id = $1 OR root_id = $1 ORDER BY created_at ASC`).

7. **Optimistic Concurrency Control (OCC)**:
   - `PUT /v1/interactions/{id}` verifies version matching (`WHERE id = $1 AND version = $2`).
   - Increments `version = version + 1`, updates `updated_at = NOW()`, and broadcasts an `InteractionEditedEvent` on JetStream. Stale updates return `409 Conflict`.
   - An omitted or blank `title` is preserved rather than overwritten (`SET title = COALESCE($3, title)`), so a body-only edit can never wipe a root post's title.

8. **Zero-Filter Core NATS Real-Time SSE Fan-Out Pipeline**:
   - Built on ephemeral Core NATS pub/sub topics with zero retention overhead: `LIVE.<group_id>.feed` and `LIVE.<group_id>.thread.<root_id>`.
   - The Go HTTP layer establishes **per-topic Core NATS subscriptions dynamically on-demand with atomic reference counting**, delegating all subject trie routing to the NATS broker's native C networking layer.
   - **Zero Go JSON Deserialization / Zero Payload Filtering**: Incoming Core NATS messages are piped as raw bytes (`m.Data`) directly into client channels.
   - Delivers over **2.89M events/sec peak broadcast throughput** across 8,000 live streams with zero server connection errors.
   - Features 512-event client buffers for mobile jitter resilience, view-epoch tracking preventing stale view leaks, and non-blocking HTTP micro-draining to minimize kernel syscall overhead.

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
| `make build-stress` | Compiles the stress testing CLI binary into `bin/stresstest`. |
| `make stress` | Runs the stress testing tool (pass `ARGS="..."` to customize). |
| `make build-tui` | Compiles the interactive TUI load monitor into `bin/kozlony-tui`. |
| `make tui` | Runs the interactive TUI load & stack monitor (pass `ARGS="..."` to customize). |
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
| `DATABASE_WRITE_MAX_CONNS` | `30` | Maximum dedicated write connections for drainer and counter pools |
| `DATABASE_READ_MAX_CONNS` | `20` | Maximum dedicated read connections for HTTP query pool |
| `DATABASE_MAX_CONNS` | `50` | Maximum connections across fallback pool |
| `DATABASE_MIN_CONNS` | `10` | Minimum idle connections across database pools |
| `NATS_URL` | `nats://localhost:4222` | NATS broker URL |
| `NATS_STREAM_NAME` | `BOARD` | JetStream stream name |
| `NATS_CONSUMER_NAME` | `board-drainer` | Durable pull consumer prefix for partition workers (`board-drainer-0` .. `15`) |
| `NATS_COUNTER_CONSUMER_NAME` | `board-counters` | Durable pull consumer name for reply counter worker |
| `NATS_CONCURRENT_SUBSCRIBERS` | `32` | Number of concurrent subscriber worker routines |
| `INGEST_PARTITIONS` | `16` | Number of deterministic hash partitions for JetStream ingestion |
| `LIVE_PREFIX` | `LIVE` | Subject prefix for Core NATS ephemeral real-time topic broadcast |
| `MAX_CACHED_INTERACTIONS` | `500000` | In-memory cache capacity limit (global thread-based LRU eviction) |
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
| `GET` | `/v1/interactions/events` | Real-time Server-Sent Events (SSE) stream (`group_id` board filter, `root_id` thread filter) |

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

### Stress Testing & Workload Pressure Controller (`tools/stress`)
A dedicated load-generation and pressure-controlling CLI app inspired by production workload runners is provided in `tools/stress`.

#### Features:
- **Hierarchical Conversation Growth**: Automatically generates root discussions and attaches multi-level nested replies (`depth >= 1`), building realistic conversation trees.
- **Optimistic Concurrency Stress**: Continuously issues concurrent edits (`PUT /v1/interactions/:id`), asserting version updates and conflict (`409`) absorption.
- **Configurable Pressure Control**:
  - Simulated active concurrent users (`-users`, default `20`).
  - Global requests-per-second rate limiter ceiling (`-rps`, default unconstrained `0`).
  - Per-user think-time pacing jitter (`-min-think` to `-max-think`, default `1s` to `3s`).
  - Action probability weights (`-weight-root`, `-weight-reply`, `-weight-edit`, `-weight-read`).
- **Telemetry Publishing**: Live runner console line and atomic 1-second status broadcast to `/tmp/kozlony-stress-status.json` for external observers.

#### Running the Stress Test:
```bash
# Compile and run with defaults (20 users, 1..3s think time, indefinite duration)
make stress

# Custom run: 50 concurrent users, rate-limited at 200 req/s, 10s test duration
make stress ARGS="-users 50 -rps 200 -duration 10s"

# Directly via binary
./bin/stresstest -url http://localhost:8080 -users 100 -rps 500 -min-think 200ms -max-think 800ms
```

---

### Interactive TUI Stack & Load Monitor (`tools/tui`)
An interactive, **pure read-only** single-pane terminal dashboard built with **Bubble Tea** and **Lip Gloss**:

> *The TUI is strictly a monitor — it does not start runs, stop runs, or mutate the system. It discovers and renders active status from the workload runner, PostgreSQL, and Docker containers.*

```
┌────────────────────────────────────────────────────────────────────────────────────────┐
│ ⚡ KOZLONY TUI STACK & LOAD MONITOR  16:50:12  ● RUNNER ACTIVE (PID 2954575)            │
├────────────────────────────────────────────────────────────────────────────────────────┤
│ WORKLOAD & THROUGHPUT (MONITORING TOOLS/STRESS)                                        │
│   Throughput:  1074.2 req/s (overall: 1060.1 req/s)   Requests: 21,500 (99.4% ok)      │
│   Latencies (Window): P50: 700µs | P90: 2.2ms | P99: 9.0ms | Max: 53.4ms               │
│                                                                                        │
│   ACTIONS BREAKDOWN                    HTTP STATUS RESPONSES                           │
│     create_reply      11759 (54.7%)      HTTP 200 OK:         5259 (24.5%)             │
│     create_root        4350 (20.2%)      HTTP 201 Created:   16109 (74.9%)             │
│     edit (OCC)         3292 (15.3%)      HTTP 409 Conflict:      4 ( 0.0%)             │
│     read_feed          1034 ( 4.8%)      HTTP 404 Cold Miss:   128 ( 0.6%)             │
│     read_interaction   1065 ( 5.0%)      HTTP 5xx Server:        0 ( 0.0%)             │
├────────────────────────────────────────────────────────────────────────────────────────┤
│ SYSTEM RESOURCES (CPU & RAM)                                                           │
│   Kozlony Service (Echo+App)      CPU:   0.9% [■░░░░░░░░░░░░░░░░░░░]  RAM:    50.4 MB  │
│   PostgreSQL 17 (Batched Ingest)  CPU:  24.5% [■■■■■░░░░░░░░░░░░░░░]  RAM:   122.2 MB  │
│   NATS JetStream (File Engine)    CPU:  18.9% [■■■■░░░░░░░░░░░░░░░░]  RAM:    50.9 MB  │
├────────────────────────────────────────────────────────────────────────────────────────┤
│ DATABASE & HIERARCHICAL CONVERSATION STATE                                             │
│   PostgreSQL State: ONLINE | Runner Pool: 10,000 items                                 │
│   Total Ingested:    16315 rows | Root Discussions:    4408 | Replies:       11907     │
│   Max Tree Depth:       13 levels | Reply Counts Incremented:     7275 posts           │
└────────────────────────────────────────────────────────────────────────────────────────┘
```

#### Hotkeys:
- `[q]` / `Ctrl+C`: Clean exit and restore terminal screen.
- `[c]`: Reset local snapshot display.

#### Running the Workflow:
```bash
# Terminal 1: Start the read-only monitor
make tui

# Terminal 2: Run the workload generator
make stress ARGS="-users 100 -rps 1000"
```

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
   - *Current Implementation*: Deterministic thread hash partitioning (`hash(thread_root_id) % 16`) maps incoming writes across 16 JetStream partitions (`INGEST.p.<0..15>`), guaranteeing thread causality and 1-to-1 pinned consumer scaling. Boards are logically partitioned via `group_id` with composite keyset B-Tree indexes on `(group_id, created_at DESC, id DESC)`.
   - *Production Architecture*: Under massive multi-cluster scale, partition ranges (e.g. partitions 0–3, 4–7) can be mapped directly to dedicated pod replicas, and PostgreSQL table declarative hash partitioning can align 1-to-1 with the JetStream partition keys for multi-node database sharding.

4. **Load, Stress & Resilience Testing**:
   - *Current Implementation*: In addition to unit tests (`gomock`, `-race`) and the `scripts/blackbox` suite, the repository includes a custom high-concurrency workload generator (`tools/stress`), reproducible cache benchmarks (`internal/cache/cache_bench_test.go`), and a live single-pane terminal dashboard (`tools/tui`).
   - *Measured Performance*:
     - In-memory hot cache achieves ~30 ns for single node seek, ~450 ns for thread hierarchy traversal, and ~2 µs for feed page index seeks.
     - Asynchronous write path sustained **29,624 req/s** (with **35,376 req/s peak**) on a 3.7-million-row database with 98.4% success rate and zero JetStream consumer lag across all 16 partitions.
     - Live SSE limit testing demonstrated sustained delivery of **2.89M events/second** across 8,000 active concurrent SSE streams with zero server connection errors and zero Go struct filtering overhead.
   - *Production Architecture*: Pair the local test runners with distributed load injectors (e.g. k6 clusters) across multi-node topologies, combined with automated chaos fault-injection (killing PostgreSQL primary or NATS leader mid-transaction) in continuous integration pipelines.

5. **Observability — Tracing & Metrics**:
   - *Current Implementation*: Structured `zerolog` logging plus Echo request logging (method, URI, status, latency), complemented by real-time CLI telemetry and a live single-pane terminal dashboard (`tools/tui`) monitoring JetStream stream limits, consumer lag, and database state.
   - *Production Architecture*: Instrument with OpenTelemetry, propagating W3C `traceparent` onto the JetStream message header so a single trace spans HTTP accept → publish → drainer batch → PostgreSQL insert. Export Prometheus metrics for RED (Rate, Errors, Duration) metrics per route, drainer batch sizing, and cache hit/miss ratios.

6. **Secret Management**:
   - *Current Implementation*: `DATABASE_URL` falls back to a development credential (`postgres://postgres:postgres@...`, `internal/config/config.go`). Local Docker Compose publishes PostgreSQL and NATS on host ports without TLS.
   - *Production Architecture*: Make `DATABASE_URL` and `NATS_URL` strictly required in non-development environments. Source credentials from an enterprise secret manager (HashiCorp Vault, AWS Secrets Manager) with automated rotation, and enforce TLS with mutual certificate verification.

7. **Graceful Shutdown**:
   - *Current Implementation*: `SIGINT`/`SIGTERM` are trapped via `signal.NotifyContext`. The HTTP server initiates graceful shutdown with a 10s budget (`e.Shutdown`), stopping active listeners. Dedicated counter workers (`replyCounter.Stop()`) are drained and closed before database pool termination (`defer dbPool.Close()`) and NATS client termination (`defer msgClient.Close()`), eliminating teardown race conditions.
   - *Production Architecture*: Extend the graceful drain to explicitly wait for in-flight JetStream micro-batch drainer commits before terminating the main consumer loop, ensuring no unacked messages require redelivery on standard rolling deployments.

8. **Zero-Filter SSE Fan-Out & Edge Broadcasting**:
   - *Current Implementation*: Solved in-process via `internal/messaging/fanout/pipeline.go`. Rather than per-client JetStream subscriptions or monolithic Go-side worker filtering, the service establishes dynamic per-topic Core NATS subscriptions (`LIVE.<group_id>.feed` and `LIVE.<group_id>.thread.<root_id>`) with atomic reference counting. The NATS broker performs subject routing at the C networking layer, and Kozlony pipes raw payload bytes (`m.Data`) directly into client channels with zero JSON deserialization. Slow consumers are protected with non-blocking drops.
   - *Production Architecture*: In a multi-replica cluster behind an L4/L7 load balancer (e.g., Envoy or Cloudflare), run an internal Redis or NATS pub/sub backplane to route user-to-thread presence, or terminate client SSE connections at edge points (e.g., Cloudflare Workers or Fastly Fanout) while Kozlony streams multiplexed events to edge POPs.

9. **Rate Limiting & Request Limits**:
   - *Current Implementation*: Body and title length are capped in the handler (65535 and 255 characters) on both create and update, but nothing bounds request *rate* — a client may create interactions without limit, and the number of concurrent SSE connections is likewise uncapped.
   - *Production Architecture*: Apply Echo's rate-limiter middleware with a token bucket keyed by authenticated subject (falling back to IP for anonymous boards), stricter per-route limits on `POST /v1/interactions` to protect the JetStream stream from write floods, and an explicit cap on concurrent SSE connections per replica.

10. **Resiliency — Retry, Backoff & Circuit Breaking**:
    - *Current Implementation*: There is no retry, backoff, or circuit breaker anywhere in the service. When `InsertBatch` fails the drainer `Nak`s every message and relies on JetStream redelivery after `AckWait` (30s), while the run loop's only throttle is a fixed `100ms` sleep — so a downed PostgreSQL means roughly ten failing batches per second, indefinitely, with nothing to stop the pressure. The consumer sets neither `MaxDeliver` nor `BackOff`, so a message that can never be inserted is redelivered forever and never reaches the DLQ; the DLQ currently handles only poison payloads (unmarshal and validation failures) on first failure. The publish path makes a single attempt and surfaces a 500 on failure.
    - *Production Architecture*: Put a circuit breaker in front of each dependency (PostgreSQL, JetStream) so an outage trips it open rather than hammering a dead service, and replace the fixed sleep with exponential backoff plus jitter. Configure `MaxDeliver` with a `BackOff` schedule on the consumer so a permanently-failing message lands in the DLQ after a bounded number of attempts instead of looping indefinitely, reserving the DLQ for genuinely poison traffic rather than transient dependency failure. Retry the publish path with jittered, bounded attempts before returning 5xx.
