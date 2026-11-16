# Roadmap

A learning project: an airline ticketing API in Go, used to practise systems design,
the CAP theorem, concurrency and the foundations of data engineering.

The domain needs **both sides of CAP**:

| Path | Example | Choice under partition | Why |
|------|---------|------------------------|-----|
| **Booking** | `POST /v1/bookings` | **CP**: refuse (503) rather than risk it | Selling seat 14C twice is unacceptable |
| **Search** | `GET /v1/flights` | **AP**: serve possibly stale data | A 5-second-old seat count harms nobody |

Every phase teaches **one** main concept and ends with a **failure experiment**
(you break something on purpose and write down what happened).

Code layout, conventions and diagrams live in [ARCHITECTURE.md](ARCHITECTURE.md).
Read that before writing code in each phase.

---

## Prerequisites

- Go 1.23+ (`go version`)
- Docker + docker compose
- `kind`, `kubectl`, `helm` (from Phase 2)
- `psql` client (handy for poking at the database)

---

## Concurrency track

Concurrency comes up at three levels. Each phase covers at least one of them.

| Level | What it is | Where you learn it |
|-------|-----------|--------------------|
| **Go (in-process)** | goroutines, channels, `sync.Mutex`, `context`, `errgroup`, the race detector | 0a, 1, 4, 5, `loadgen` |
| **Database** | transactions, isolation levels, row locks, optimistic locking, `SKIP LOCKED` | 0b, 1, 2 |
| **Distributed** | replicas, replication lag, leader election, failover, at-least-once delivery | 2, 3, 5, 7 |

The key lesson: **a `sync.Mutex` only protects one process.** Phase 0a works with one
process; Phase 0b and the Kubernetes phases show why it stops working once you have
more than one.

---

## Phase 0a: In-memory, single process (Go concurrency)

**Goal:** a working API with no database, so you learn Go concurrency on its own first.

Build:
- `go mod init`, `cmd/api/main.go`, stdlib `net/http` router (`mux.HandleFunc("POST /v1/bookings", ...)`)
- `booking.Repository` interface + an **in-memory** implementation (`map` + `sync.Mutex`)
- Endpoints: list flights, list seats, create booking, get booking (see ARCHITECTURE.md)
- Seed a handful of flights on startup
- Graceful shutdown (`signal.NotifyContext` + `server.Shutdown`)
- `cmd/loadgen`: your own load generator. **Write it yourself, don't use k6**: N workers,
  a jobs channel, a results channel, a final summary. It's the best Go concurrency exercise in the project.

Experiment `01-inmemory-race`:
1. Write the in-memory store **without** a mutex. Run `loadgen` with 100 workers booking the same seat.
2. Run the tests with `go test -race`. Read the race report.
3. Add the mutex. Is "check seat is free" + "mark seat taken" now atomic? It is only if both steps run under the **same** lock acquisition.

Done when: `go test -race ./...` is clean and 100 concurrent bookings of one seat give exactly 1 success and 99 `409 Conflict`.

## Phase 0b: Postgres, single node (database concurrency)

**Goal:** swap the in-memory store for Postgres **without touching handlers or services**.
That's what the repository interface is for.

Build:
- `deploy/compose/docker-compose.yaml` with Postgres 17
- Migrations (goose or golang-migrate, see ARCHITECTURE.md)
- `storage/postgres` implementation of the same interfaces, using `pgx/v5` and `pgxpool`
- Config switch: `STORAGE=memory|postgres`

Experiment `02-double-booking`:
1. Write the naive version first: `SELECT` the seat, check it in Go, then `UPDATE`/`INSERT`. Run two `api` processes against the same DB, then run `loadgen`. **The mutex no longer helps.** Count the double bookings.
2. Fix it three different ways, one at a time, and measure the throughput and error rate of each:
   - pessimistic: `SELECT ... FOR UPDATE`
   - optimistic: `version` column + `UPDATE ... WHERE version = $n`
   - constraint: let a unique index reject the duplicate
3. Try `SERIALIZABLE` isolation and handle serialization failures (SQLSTATE `40001`) with a retry.
4. Write down which one you'd pick and why.

Done when: you can explain each fix's trade-off (contention, retries, where the truth lives).

## Phase 1: Seat holds + idempotency

**Goal:** a real booking flow: hold → confirm, plus safe retries.

Build:
- `POST /v1/bookings` creates a **hold** that expires after 10 min; `POST /v1/bookings/{id}/confirm` confirms it (payment is stubbed)
- A background **expirer** goroutine (a ticker plus a context) that releases expired holds
- An `Idempotency-Key` header on `POST /v1/bookings`, with responses stored in `idempotency_keys`
- Middleware: request ID, structured logging, per-request timeout

Concurrency points:
- The expirer must stop cleanly on shutdown (no leaked goroutines).
- Race: the expirer releases a hold **at the same moment** the user confirms it. Who wins? Make the outcome deterministic.
- Two requests with the same idempotency key arrive at the same time. The second must not run the booking again.

Experiment `03-retry-storm`: `loadgen` sends every request twice with the same key, and randomly kills the API mid-request (`kill -9`). Verify zero duplicate bookings.

## Phase 2: Kubernetes on kind

**Goal:** move the infrastructure onto Kubernetes so the CAP experiments have real pods, nodes and networks to break.

Build:
- `deploy/kind/cluster.yaml`: 1 control plane + 3 workers
- **Postgres via the CloudNativePG operator** (Helm: `cnpg/cloudnative-pg`)
  - `deploy/helm/cnpg-operator-values.yaml`
  - `deploy/k8s/postgres-cluster.yaml`: `Cluster` with `instances: 3`, pod anti-affinity across workers
  - CNPG creates `<name>-rw`, `<name>-ro` and `<name>-r` Services, which the API uses in Phase 3
  - Why not the Bitnami chart: since 2025 Bitnami's free images are no longer maintained (`bitnamilegacy`)
- **Redis** (or Valkey) via a maintained Helm chart: `deploy/helm/redis-values.yaml`, standalone for now
- **API**: multi-stage Dockerfile (distroless), loaded with `kind load docker-image`
- **API Helm chart** written by you: `deploy/helm/api/` (Deployment, Service, ConfigMap, probes, migration Job as a pre-install/pre-upgrade hook)
- `Makefile`: `cluster-up`, `cluster-down`, `image`, `deploy`, `undeploy`

Concurrency points:
- Run the API with `replicas: 3`. Now **3 expirers** run at once. Fix it with either:
  - `SELECT ... FOR UPDATE SKIP LOCKED` (every replica works and they share the load), or
  - leader election with a Kubernetes `Lease` (one replica works).
- Readiness: should a pod be "ready" when it can't reach the primary? (Search: yes. Booking: no. Same pod, two answers. How do you model that?)

Experiment `04-primary-failover`: `kubectl delete pod <primary>` while `loadgen` books seats. Measure the failover time, the failed requests, and whether any booking was **lost** or **duplicated**.

## Phase 3: Replication and CAP

**Goal:** see CAP and PACELC (the extension of CAP: even without a partition you trade latency for consistency) with your own numbers.

Build:
- Two connection pools: `writer` → `-rw` Service, `reader` → `-ro` Service
- Search reads from replicas (AP); booking reads and writes go to the primary (CP)

Experiments:
- `05-read-your-writes`: book a seat, then immediately `GET` it from a replica. How often is it missing? Fixes: read from the primary after a write, a session token with the WAL LSN (log sequence number), sticky reads.
- `06-sync-replication`: set `minSyncReplicas: 1` in CNPG. Measure the change in booking p50/p99 latency. That's the "else, Latency vs Consistency" half of PACELC.
- `07-partition`: cut the API off from the primary (a NetworkPolicy or Chaos Mesh) but keep the replicas reachable. Search must keep working; booking must return `503` with `Retry-After`. Then partition a sync replica: what happens to writes?

## Phase 4: Cache for search (AP read path)

Build: cache-aside in Redis for flight search and seat maps, with TTLs. Invalidate on booking events.

Concurrency points: thundering herd (many requests rebuilding the same expired key at once) → `singleflight`; fan-out search over several routes with `errgroup` and a shared `context`.

Experiment `08-stale-cache`: measure the staleness window. Kill Redis: does search degrade gracefully (fall back to replicas) or fail?

## Phase 5: Events (bridge to data engineering)

Build:
- **Outbox pattern**: insert the booking and an `outbox` row in the same transaction; a relay publishes to Kafka (Redpanda on kind)
- Alternative: **CDC** with Debezium reading the Postgres WAL. Try both and compare.
- Event schema with Protobuf or Avro + a schema registry

Concurrency points: relay worker pool, consumer goroutines, ordering per key (partition by `flight_id`), backpressure with bounded channels.

Experiment `09-duplicates-and-order`: kill the relay after publishing but before marking the row as published. Consumers must be idempotent.

## Phase 6: Analytics pipeline

Build: consume events → ClickHouse (or DuckDB + Parquet files). Metrics: load factor per flight, bookings per route per hour, hold→confirm conversion.

Concepts: OLTP vs OLAP modelling, star schema, batch vs streaming, late events, schema evolution.

## Phase 7 (optional): Consensus

Swap the seat inventory store for etcd or CockroachDB, or write a toy Raft in Go.
Kill nodes and watch the minority side refuse writes.

---

## Side track (after the API is complete): `chaos-proxy` in Rust

**Goal:** replace the off-the-shelf fault injection (toxiproxy, Chaos Mesh, NetworkPolicies)
with your own TCP proxy written in Rust, then rerun the earlier experiments with it.
It can live in its own repository; this project is just its first user.

```
api pod ──► chaos-proxy ──► postgres-rw / redis / kafka
                 ▲
                 │  control API (HTTP)
            you / loadgen
```

**Why:** async Rust networking (`tokio`) next to Go's goroutines gives you two concurrency
models side by side, and building partitions yourself teaches how they really look on the wire.

Milestones:

| # | Milestone | You learn | Done when |
|---|-----------|-----------|-----------|
| R1 | **TCP passthrough**: listen, connect upstream, copy both ways | `tokio`, `TcpListener`, `copy_bidirectional`, one task per connection | The Go API works normally through the proxy (`psql` too) |
| R2 | **Config + multiple listeners**: one proxy, several upstreams (Postgres, Redis) | `serde`, config files, spawning and supervising tasks | One process proxies all dependencies |
| R3 | **Latency + jitter** per direction | async timers, wrapping streams, per-connection state | `06-sync-replication` latency numbers shift by the injected amount |
| R4 | **Fault modes**: `refuse` (RST), `blackhole` (accept, never forward), `close-after-N-bytes`, bandwidth limit | the difference between failing fast and hanging; why deadlines matter | The Go API returns `503` quickly under `refuse` and only after its timeout under `blackhole` |
| R5 | **Control API**: `GET/POST/DELETE /faults`, changes apply to **live** connections | `axum`, shared state (`Arc<RwLock<..>>` or `tokio::sync::watch`) | `loadgen` can turn faults on and off during a run |
| R6 | **Observability**: structured logs, Prometheus metrics (connections, bytes, faults hit) | `tracing`, `metrics` crates | A dashboard shows when faults were active |
| R7 | **Kubernetes**: container image, Helm chart, deploy as a sidecar or a standalone Deployment on kind | packaging Rust (static musl build, distroless) | The API Helm chart can route its DB traffic through the proxy |

Experiment `10-own-partition`: rerun `04-primary-failover` and `07-partition` through
`chaos-proxy` and compare the results with the toxiproxy/NetworkPolicy runs. Do they match?
If not, why? (Hint: refused vs blackholed connections.)

Stretch: a UDP mode, packet-loss percentage, or a `tc netem` backend for faults below
the TCP layer.

---

## Experiment template

Every experiment gets `experiments/NN-name/README.md`:

```markdown
# NN: Title
## Hypothesis       what I expect to happen and why
## Setup            commands, config, loadgen flags
## Results          numbers, logs, graphs
## Conclusion       was I right? what trade-off did I see?
## What I'd do in production
```

These write-ups are your portfolio: they show you understand the trade-offs,
not just that you can wire services together.
