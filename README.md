# IdemLedger

A double-entry ledger with idempotent transfers, concurrency-safe balances, and an
immutable audit journal. Built in Go + PostgreSQL.

Designed as an interview-ready engineering project demonstrating the core concepts
behind Stripe-style payment systems: ACID transactions, three concurrency strategies,
idempotency, the outbox pattern, and reconciliation.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│  HTTP API  (stdlib net/http ServeMux, Go 1.22+ patterns)        │
│                                                                  │
│  POST /accounts          POST /transfers                        │
│  GET  /accounts/:id      GET  /accounts/:id/history             │
│  GET  /health            GET  /metrics                          │
└────────────────────────────┬────────────────────────────────────┘
                             │
              ┌──────────────▼──────────────┐
              │  Ledger (single DB tx)       │
              │                             │
              │  1. Claim idempotency key   │
              │  2. Debit from_account      │  ← one of 3 strategies
              │  3. Credit to_account       │
              │  4. INSERT transfer row     │
              │  5. INSERT 2 postings       │  ← sum to zero
              │  6. INSERT outbox event     │  ← atomic with transfer
              │  7. Complete idem key       │
              │  8. COMMIT                  │
              └──────────────┬──────────────┘
                             │
          ┌──────────────────┼──────────────────┐
          │                  │                  │
   ┌──────▼──────┐  ┌────────▼──────┐  ┌───────▼──────┐
   │  accounts   │  │  transfers    │  │   outbox     │
   │  ─────────  │  │  ──────────   │  │  ─────────   │
   │  id         │  │  id (UUID)    │  │  id (UUID)   │
   │  balance    │  │  idem_key     │  │  event_type  │
   │  version    │  │  status       │  │  payload     │
   │  webhook_url│  │  amount       │  │  target_url  │
   └─────────────┘  └───────────────┘  │  status      │
                                       │  attempt_cnt │
   ┌─────────────┐  ┌───────────────┐  └──────────────┘
   │  postings   │  │  idem_keys    │
   │  ─────────  │  │  ──────────   │
   │  transfer_id│  │  key (PK)     │
   │  account_id │  │  request_hash │
   │  amount     │  │  status       │
   └─────────────┘  │  response     │
                    └───────────────┘

   ┌──────────────────────────────────────────────────────────┐
   │  Outbox Poller (goroutine, 100ms tick)                   │
   │  SELECT ... FOR UPDATE SKIP LOCKED → POST to webhook     │
   │  Exponential backoff (5s × 2^n + jitter), 8 retries     │
   └──────────────────────────────────────────────────────────┘
```

## Design Decisions

### Double-entry accounting

Every transfer creates exactly two postings that sum to zero:

```
from_account: -amount  (debit)
to_account:   +amount  (credit)
```

Conservation is enforced by construction: `SUM(all postings) == 0` always. The
`accounts.balance` column is a materialized cache; the `postings` table is the
source of truth.

### Money as int64

All amounts are stored as integer minor units (paise, cents, etc.) — never floats.
Floating-point arithmetic has rounding errors that compound over millions of
transactions. An `int64` can represent values up to ~92 trillion minor units.

### Concurrency strategies

Three strategies for the debit step, selectable via `STRATEGY` env var:

| Strategy | Mechanism | Best when | Weakness |
|---|---|---|---|
| **A: conditional UPDATE** *(default)* | `WHERE balance >= amount` atomically re-checks the condition at lock time | Check expressible in SQL | Cannot handle multi-row decisions |
| **B: SELECT FOR UPDATE** | Lock both rows in ascending ID order, then read-check-write | Complex computation between read and write | Holds locks longer; requires lock ordering |
| **C: Optimistic (version)** | Read version, update only if unchanged, retry on conflict | Low contention | Retry storm under high contention |

### Idempotency

`Idempotency-Key` is **required** on `POST /transfers`. Without a stable client-supplied
key, a network retry after the server commits creates a duplicate transfer — a double
spend. Stripe enforces the same requirement on all mutating endpoints.

```
Idempotency-Key header (required) + SHA-256(JSON-normalized body) → idempotency_keys

INSERT ON CONFLICT DO NOTHING → claim the key
RowsAffected == 1 → this request owns the work → execute transfer
RowsAffected == 0, status == done → replay stored response (Idempotent-Replayed: true)
RowsAffected == 0, status == in_progress, age < 30s → 409 Conflict
RowsAffected == 0, status == in_progress, age > 30s → steal stale claim, retry
hash mismatch → 422 ErrDuplicateRequest
```

The key claim and transfer commit are in the same transaction — atomic. The request
body is JSON-normalized before hashing so `{"amount":100,"currency":"USD"}` and
`{"currency":"USD","amount":100}` are treated as the same request.

### Outbox pattern

Writing a webhook after the DB commit is broken:

- Crash between COMMIT and POST → transfer committed, merchant never notified
- POST succeeds, crash before recording it → retry → merchant gets duplicate

Fix: write the outbox event *inside* the transfer transaction. The event exists
iff the transfer committed. A background poller delivers it with at-least-once
semantics. Consumers deduplicate on the stable `X-Event-ID` header.

## Benchmark Results

Apple Silicon (M-series) + Docker Postgres 16. All runs: 20 workers, `Invariant violations: 0 ✓`.

### Low contention (500 accounts, 50,000 transfers)

| Strategy | TPS | p50 | p99 |
|---|---|---|---|
| conditional_update *(default)* | **4,412** | 4.43 ms | 6.47 ms |
| select_for_update | 3,454 | 5.66 ms | 8.03 ms |
| optimistic | 3,860 | 5.03 ms | 9.33 ms |

### High contention (20 accounts) — strategy comparison

| Strategy | TPS | p50 | p99 | Errors |
|---|---|---|---|---|
| optimistic | 162 | 8.76 ms | **2,022 ms** | 262 |
| conditional_update | 20 | 22.9 ms | **6,087 ms** | 1 |
| select_for_update | **1,790** | 7.46 ms | 48.9 ms | 0 |

**90× TPS gap** between conditional_update (20 TPS) and select_for_update (1,790 TPS)
at identical contention. The ascending-order pre-lock makes deadlocks mathematically
impossible; the other two strategies collapse under the same load.

## Running Locally

```bash
# Start Postgres
docker compose up -d

# Apply migrations
make migrate-up

# Run the API server
DATABASE_URL=postgres://idem:idem@localhost:5432/idemledger?sslmode=disable \
PORT=8080 go run ./cmd/api

# Run the correctness + benchmark harness
go run ./cmd/loadtest -accounts 500 -transfers 50000 -workers 20 -strategy conditional_update

# Run all three strategies
go run ./cmd/loadtest -accounts 500 -transfers 50000 -workers 20 -strategy select_for_update
go run ./cmd/loadtest -accounts 500 -transfers 50000 -workers 20 -strategy optimistic

# High-contention run (shows optimistic degrading)
go run ./cmd/loadtest -accounts 20 -transfers 10000 -workers 20 -strategy optimistic

# Chaos test (proves outbox delivers all events through a simulated crash)
go run ./cmd/chaostest

# Reconciliation job
go run ./cmd/recon
```

## API Reference

```
POST /accounts
  Body: {"name": "alice", "currency": "INR", "webhook_url": "https://..."}
  → 201 {"id": 1, "name": "alice", "currency": "INR", "balance": 0, ...}

GET /accounts/:id
  → 200 {"id": 1, "balance": 50000, ...}

GET /accounts/:id/history?after=<cursor>
  → 200 {"postings": [{"id": 42, "amount": -500, ...}]}

POST /transfers
  Headers: Idempotency-Key: <uuid>   (required)
  Body: {"from_account": 1, "to_account": 2, "amount": 500, "currency": "INR"}
  → 201 {"id": "<uuid>", "status": "posted", "amount": 500, ...}

GET /health
  → 200 {"status": "ok"}

GET /metrics
  → 200 {"outbox": {"pending": 0, "in_flight": 0, "delivered": 142, "dead_letter": 0}}
```

## Error Handling

| Error | HTTP status |
|---|---|
| Invalid request body | 400 |
| Account not found | 404 |
| Conflict — concurrent duplicate in progress | 409 |
| Missing `Idempotency-Key` header | 422 |
| Same key, different request body | 422 |
| Insufficient funds | 422 |
| Currency mismatch between accounts and transfer | 422 |
| Invalid amount (zero or negative) | 422 |
| Invalid currency (not 3 letters) | 422 |
| `webhook_url` is private/loopback/invalid (SSRF protection) | 422 |
| Database unavailable (`GET /health`) | 503 |
| Internal server error | 500 |
| Replayed idempotent response | original status + `Idempotent-Replayed: true` |

## Project Structure

```
cmd/
  api/        HTTP server
  loadtest/   Concurrency + benchmark harness
  chaostest/  Outbox resilience proof
  recon/      Reconciliation job
internal/
  config/     Env-based config
  idempotency/ Key storage, hash, replay
  ledger/     Transfer logic, three strategies
  outbox/     Poller goroutine
  recon/      Invariant checks
  store/      pgx queries (accounts, outbox)
migrations/   golang-migrate SQL files
```

## What This Project Demonstrates

- **ACID transactions**: single-transaction transfer — debit, credit, postings, outbox
  event, and idempotency key all commit together or not at all
- **Concurrency safety**: conditional UPDATE as default; SELECT FOR UPDATE with ascending
  lock order eliminates deadlocks; optimistic versioning benchmarked to show its
  retry-storm failure mode (19× TPS collapse under high contention)
- **Idempotency**: SHA-256 request hashing with JSON normalization, `INSERT ON CONFLICT
  DO NOTHING` as a distributed mutex, stored-response replay, stale-claim reclaim for
  crashed holders (30s TTL on `claimed_at`)
- **Outbox pattern**: atomic event write, `FOR UPDATE SKIP LOCKED` delivery,
  at-least-once + consumer idempotency via `X-Event-ID`; chaos test proves
  Lost: 0, Duplicates: 0 through a simulated mid-delivery crash
- **Reconciliation**: independent conservation and per-account balance-integrity checks;
  exits non-zero on drift for CI integration
- **Production hardening**: SSRF protection on webhook URLs (RFC1918 + loopback blocking),
  graceful HTTP shutdown (15s drain on SIGTERM), `MaxBytesReader` on all POST bodies,
  DB health ping on `/health`, cross-currency transfer rejection
- **Observability**: `/metrics` endpoint (pending/in_flight/delivered/dead_letter counts),
  structured error responses with typed sentinel errors
