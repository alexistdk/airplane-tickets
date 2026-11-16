# Architecture & Code Guidelines

The reference to follow while writing the code. When in doubt, choose
**boring and explicit** over clever.

Contents:
1. [Target architecture](#1-target-architecture)
2. [Domain model](#2-domain-model)
3. [Seat lifecycle](#3-seat-lifecycle)
4. [Booking flow](#4-booking-flow)
5. [HTTP API](#5-http-api)
6. [Project layout](#6-project-layout)
7. [Layering rules](#7-layering-rules)
8. [Go conventions](#8-go-conventions)
9. [Concurrency rules](#9-concurrency-rules)
10. [Database conventions](#10-database-conventions)
11. [Testing](#11-testing)
12. [Configuration & operations](#12-configuration--operations)

---

## 1. Target architecture

Where the project ends up after Phase 6. Each phase adds a piece.

```mermaid
flowchart LR
    client([Client / loadgen])

    subgraph k8s [kind cluster]
        subgraph api [API Deployment x3]
            search[Search handler<br/>AP path]
            booking[Booking handler<br/>CP path]
            expirer[Hold expirer]
            relay[Outbox relay]
        end

        redis[(Redis<br/>search cache)]

        subgraph pg [CloudNativePG Cluster]
            primary[(Primary)]
            r1[(Replica 1)]
            r2[(Replica 2)]
        end

        kafka[[Kafka / Redpanda]]
        consumer[Analytics consumer]
        olap[(ClickHouse)]
    end

    client --> search
    client --> booking
    search -->|cache-aside| redis
    search -->|"-ro" service| r1
    search -->|"-ro" service| r2
    booking -->|"-rw" service| primary
    expirer -->|"-rw"| primary
    primary -. streaming replication .-> r1
    primary -. streaming replication .-> r2
    relay -->|reads outbox| primary
    relay --> kafka
    kafka --> consumer --> olap
```

**Rule of thumb:** anything that **decides** who owns a seat talks to the primary.
Anything that only **shows** seats may read from a replica or the cache.

---

## 2. Domain model

```mermaid
erDiagram
    AIRPORT ||--o{ FLIGHT : "origin / destination"
    FLIGHT ||--|{ FLIGHT_SEAT : has
    FLIGHT_SEAT ||--o{ BOOKING : "is booked by"
    PASSENGER ||--o{ BOOKING : makes

    AIRPORT {
        char3 code PK "IATA, e.g. EZE"
        text name
        text city
        text timezone "IANA, e.g. America/Argentina/Buenos_Aires"
    }
    FLIGHT {
        uuid id PK
        text number "e.g. AR1140"
        char3 origin FK
        char3 destination FK
        timestamptz departs_at
        timestamptz arrives_at
    }
    FLIGHT_SEAT {
        uuid flight_id PK,FK
        text seat_no PK "e.g. 14C"
        text cabin "economy | business"
        bigint price_cents
        bigint version "optimistic locking (Phase 0b)"
    }
    PASSENGER {
        uuid id PK
        text full_name
        text email
    }
    BOOKING {
        uuid id PK
        uuid flight_id FK
        text seat_no FK
        uuid passenger_id FK
        text status "held | confirmed | expired | cancelled"
        timestamptz hold_expires_at
        timestamptz created_at
        timestamptz confirmed_at
    }
```

Added in later phases:

| Table | Phase | Purpose |
|-------|-------|---------|
| `idempotency_keys` (`key` PK, `request_hash`, `status_code`, `response_body`, `created_at`) | 1 | Replay the stored response when a client retries |
| `outbox` (`id`, `aggregate_id`, `event_type`, `payload jsonb`, `created_at`, `published_at`) | 5 | Events written in the same transaction as the booking |

> **Open on purpose:** *how* the database guarantees "at most one active booking
> per seat" is the Phase 0b exercise. Don't add that constraint until you've
> watched the double booking happen.

---

## 3. Seat lifecycle

A seat's availability comes from its bookings: a seat is **taken** when it has a
booking in `held` (not expired) or `confirmed` state.

```mermaid
stateDiagram-v2
    [*] --> held: POST /bookings
    held --> confirmed: POST /bookings/{id}/confirm<br/>(before hold_expires_at)
    held --> expired: expirer, after hold_expires_at
    held --> cancelled: DELETE /bookings/{id}
    confirmed --> cancelled: DELETE /bookings/{id}
    expired --> [*]
    cancelled --> [*]
    confirmed --> [*]
```

Invariants (write a test for each):
- At most one `held` or `confirmed` booking per `(flight_id, seat_no)`.
- `confirmed` is only reachable from `held`, and only if `now() < hold_expires_at`.
- A state change is a **conditional update** (`... WHERE status = 'held'`). Never read the status in Go and write it back blindly.
- State changes in the domain package are methods returning an error, e.g. `func (b *Booking) Confirm(now time.Time) error`.

---

## 4. Booking flow

```mermaid
sequenceDiagram
    autonumber
    participant C as Client
    participant H as HTTP handler
    participant S as booking.Service
    participant R as Repository
    participant DB as Postgres (primary)

    C->>H: POST /v1/bookings<br/>Idempotency-Key: k1
    H->>H: decode + validate JSON
    H->>S: Hold(ctx, HoldRequest)
    S->>R: FindIdempotent(ctx, k1)
    alt key seen before
        R-->>S: stored response
        S-->>H: replay
    else new key
        S->>R: CreateHold(ctx, ...) [single tx]
        R->>DB: BEGIN, claim seat, INSERT booking,<br/>INSERT idempotency_key, COMMIT
        alt seat already taken
            DB-->>R: conflict
            R-->>S: booking.ErrSeatUnavailable
            S-->>H: error
            H-->>C: 409 seat_unavailable
        else primary unreachable
            R-->>S: ErrUnavailable
            H-->>C: 503 + Retry-After (CP: refuse, don't guess)
        else ok
            R-->>S: Booking{status: held}
            S-->>H: Booking
            H-->>C: 201 Created
        end
    end
```

How the "claim seat" step works (locking, optimistic, constraint) is your call in Phase 0b.

---

## 5. HTTP API

Base path `/v1`. JSON in and out. Times are RFC 3339 in UTC. Money is integer cents plus a currency.

| Method | Path | Path type | Success | Errors |
|--------|------|-----------|---------|--------|
| GET | `/healthz` | - | 200 | - |
| GET | `/readyz` | - | 200 | 503 |
| GET | `/v1/flights?origin=EZE&destination=MAD&date=2026-12-20` | AP | 200 | 422 |
| GET | `/v1/flights/{flightID}` | AP | 200 | 404 |
| GET | `/v1/flights/{flightID}/seats` | AP | 200 | 404 |
| POST | `/v1/bookings` | CP | 201 | 404, 409, 422, 503 |
| GET | `/v1/bookings/{bookingID}` | CP | 200 | 404 |
| POST | `/v1/bookings/{bookingID}/confirm` | CP | 200 | 404, 409 (expired / not held), 503 |
| DELETE | `/v1/bookings/{bookingID}` | CP | 204 | 404, 409 |

`GET /v1/bookings/{id}` is CP so that a user always sees their own booking right after making it (read-your-writes).

Request example:
```json
POST /v1/bookings
Idempotency-Key: 6f1c...   (required from Phase 1)

{ "flight_id": "…", "seat_no": "14C",
  "passenger": { "full_name": "Ada Lovelace", "email": "ada@example.com" } }
```

**Error shape**, the same everywhere:
```json
{ "error": { "code": "seat_unavailable", "message": "seat 14C is already held" } }
```

Status code mapping (done in **one** place, in the `httpapi` package):

| Domain error | Status | `code` |
|--------------|--------|--------|
| validation | 422 | `invalid_request` |
| `ErrNotFound` | 404 | `not_found` |
| `ErrSeatUnavailable` | 409 | `seat_unavailable` |
| `ErrHoldExpired` / `ErrInvalidTransition` | 409 | `invalid_state` |
| `ErrIdempotencyMismatch` (same key, different body) | 422 | `idempotency_key_reused` |
| `ErrUnavailable` (dependency down, CP path) | 503 + `Retry-After` | `unavailable` |
| anything else | 500 | `internal` (log the details, never return them) |

---

## 6. Project layout

```
airplane-tickets/
├── cmd/
│   ├── api/main.go          # wiring only: config → deps → server → shutdown
│   └── loadgen/main.go      # your load/chaos client
├── internal/
│   ├── flight/              # domain: Flight, Seat, search Service, Repository interface
│   ├── booking/             # domain: Booking, state machine, Service, Repository interface, errors
│   ├── storage/
│   │   ├── memory/          # Phase 0a implementations
│   │   └── postgres/        # Phase 0b+ implementations (SQL lives here only)
│   ├── cache/               # Phase 4 (Redis decorator around flight.Repository)
│   ├── events/              # Phase 5 (outbox relay, event types)
│   ├── httpapi/             # router, handlers, middleware, JSON + error mapping
│   └── platform/
│       ├── config/          # env → struct
│       └── logging/         # slog setup
├── migrations/              # 0001_init.sql, ...
├── deploy/
│   ├── compose/             # Phase 0b–1
│   ├── kind/                # cluster.yaml
│   ├── helm/                # values files + your api chart
│   └── k8s/                 # raw manifests (CNPG Cluster, NetworkPolicies)
├── experiments/NN-name/     # README.md per experiment
├── docs/
└── Makefile
```

Package-by-feature (`flight`, `booking`), **not** package-by-layer (`models`, `controllers`, `services`).

---

## 7. Layering rules

```mermaid
flowchart TB
    main[cmd/api] --> httpapi
    main --> storage
    httpapi --> booking
    httpapi --> flight
    storage --> booking
    storage --> flight
    booking -.->|"defines Repository interface"| storage
```

- **Domain packages (`booking`, `flight`) import nothing from this repo.** They hold types, business rules, errors, the `Service`, and the `Repository` **interface**.
- **`storage/*` implements the domain interfaces.** It's the only place that knows SQL or Redis.
- **`httpapi` knows HTTP.** It decodes requests, calls services, maps errors to status codes. No SQL, no business rules.
- **`cmd/api/main.go` is the only place things get wired together.** No `init()` side effects, no package-level globals (except `var ErrX = errors.New(...)`).
- Interfaces are defined **where they are used** (the consumer), and kept small.

Example shape (signatures only; the implementation is yours):

```go
// internal/booking/repository.go
type Repository interface {
    CreateHold(ctx context.Context, h Hold) (Booking, error)
    Get(ctx context.Context, id uuid.UUID) (Booking, error)
    Confirm(ctx context.Context, id uuid.UUID, now time.Time) (Booking, error)
    ExpireHolds(ctx context.Context, now time.Time, limit int) (int, error)
}
```

A new storage backend (memory → postgres → cached) must work **without changing** `booking` or `httpapi`.

---

## 8. Go conventions

**Style**
- `gofmt`/`goimports` always; `golangci-lint` with defaults + `errorlint`, `bodyclose`, `contextcheck`.
- Short names in small scopes (`b`, `ctx`), descriptive names in large ones.
- Constructors: `func NewService(repo Repository, clock Clock, log *slog.Logger) *Service`.
- Accept interfaces, return concrete types.

**Errors**
- Sentinel errors in the domain: `var ErrSeatUnavailable = errors.New("seat unavailable")`.
- Wrap with context on the way up: `fmt.Errorf("create hold for %s: %w", seatNo, err)`.
- Check with `errors.Is` / `errors.As`, never by comparing strings.
- Translate infrastructure errors (pgx error codes like `23505` for unique violations) into domain errors **inside `storage/postgres`**.
- Handle each error once: either log it or return it, not both.

**Context**
- `ctx context.Context` is the first parameter of anything that does I/O or may block.
- Never store a context in a struct. Never pass `nil`; use `context.TODO()` if you must.
- Every outbound call (DB, Redis, Kafka) runs under a deadline derived from the request.

**Time**
- Inject a clock (`type Clock interface{ Now() time.Time }`) so expiry logic is testable.
- Store and compute in UTC; convert to the airport's timezone only for display.

**Logging**
- `log/slog` with a JSON handler. Always include `request_id`; add `flight_id`/`booking_id` where relevant.
- Never log emails or full names (PII).

**Dependencies** (keep the list short)
| Need | Use |
|------|-----|
| HTTP routing | stdlib `net/http` (Go 1.22+ patterns) |
| Postgres | `github.com/jackc/pgx/v5` + `pgxpool` |
| Migrations | `github.com/pressly/goose/v3` |
| UUIDs | `github.com/google/uuid` (v7 for time-ordered IDs) |
| Concurrency helpers | `golang.org/x/sync` (`errgroup`, `singleflight`) |
| Redis (Phase 4) | `github.com/redis/go-redis/v9` |
| Kafka (Phase 5) | `github.com/twmb/franz-go` |
| Tests | stdlib `testing`; `testcontainers-go` for Postgres |

Write SQL by hand with pgx. You'll learn more than with an ORM. `sqlc` is an option later.

---

## 9. Concurrency rules

1. **Every goroutine has an owner and an exit.** Whoever starts it must be able to stop it (a `ctx`) and wait for it (`sync.WaitGroup`/`errgroup`). No fire-and-forget.
2. **Shared memory needs a lock, and the lock covers the whole check-then-act.** Checking under the lock and acting after unlocking is a bug.
3. **Hold locks briefly; never do I/O while holding a `sync.Mutex`.**
4. **Prefer channels to hand off ownership** (worker pools, pipelines), and **mutexes to protect state** (maps, counters).
5. **Bound everything:** channel sizes, worker counts, connection pool size (`pgxpool` `MaxConns`), request timeouts.
6. **`go test -race ./...` must pass** before every commit. Run `loadgen` under `-race` too.
7. **In-process locks don't survive `replicas: 2`.** Correctness across processes must come from the database (transactions, conditional updates, constraints) or from explicit coordination (a `Lease`, `SKIP LOCKED`).
8. **Retries need idempotency.** Never retry a non-idempotent operation without an idempotency key.
9. **Database transactions are short.** No network calls (payments, Kafka) inside a transaction. That's what the outbox is for.

---

## 10. Database conventions

- Migrations are numbered, forward-only SQL files in `migrations/`. Never edit one that has already been applied.
- `snake_case`, plural table names (`bookings`), `timestamptz` everywhere, `bigint` cents for money.
- Primary keys: UUID v7 generated in Go (or a natural composite key like `(flight_id, seat_no)`).
- `status` columns use `text` + a `CHECK` constraint (easier to evolve than Postgres enums).
- Index for your queries: search by `(origin, destination, departs_at)`; the expirer scans `(status, hold_expires_at)`.
- Every query takes a `ctx`; every transaction uses `pgx.BeginTxFunc` (or an equivalent helper) so rollback can't be forgotten.
- Choose the isolation level per operation, explicitly. Write a comment explaining why when it's not `READ COMMITTED`.

---

## 11. Testing

| Kind | Where | What |
|------|-------|------|
| Unit | `internal/booking`, `internal/flight` | state machine, validation, service logic with a fake repository; table-driven |
| Repository contract | `internal/storage/...` | **one shared test suite** run against both `memory` and `postgres` implementations |
| HTTP | `internal/httpapi` | `httptest` for status codes, the error shape and headers |
| Concurrency | storage + service | N goroutines book one seat → exactly 1 success; run with `-race` |
| Experiments | `experiments/` | `loadgen` + chaos, results written up by hand |

The repository contract suite is the most valuable test in the project: it proves
memory and postgres behave the same, including under concurrency.

---

## 12. Configuration & operations

- Config only from environment variables, parsed once into a struct in `platform/config`; fail fast on startup if something's missing.

| Var | Example |
|-----|---------|
| `HTTP_ADDR` | `:8080` |
| `STORAGE` | `memory` / `postgres` |
| `DATABASE_URL` | `postgres://app:app@localhost:5432/tickets` |
| `DATABASE_READ_URL` | (Phase 3) points to the `-ro` service |
| `REDIS_ADDR` | (Phase 4) |
| `HOLD_TTL` | `10m` |
| `SHUTDOWN_TIMEOUT` | `15s` |

- `/healthz` = the process is alive (liveness). `/readyz` = it can serve traffic (readiness); decide per path what "can serve" means.
- Graceful shutdown order: stop accepting connections → let in-flight requests finish → stop background workers → close pools.
- Container: multi-stage build, `gcr.io/distroless/static` base, non-root user, `CGO_ENABLED=0`.
