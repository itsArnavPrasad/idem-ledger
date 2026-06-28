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

```
Idempotency-Key header + SHA-256(request body) → idempotency_keys table

INSERT ON CONFLICT DO NOTHING → claim the key
RowsAffected == 0, status == done → replay stored response
RowsAffected == 0, status == in_progress → 409 Conflict
hash mismatch → 422 (different request, same key)
```

The key claim and transfer commit are in the same transaction — atomic.

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

### High contention (20 accounts, 10,000 transfers, optimistic only)

| Strategy | TPS | p50 | p99 |
|---|---|---|---|
| optimistic | 384 | 5.91 ms | **1,030 ms** |

**10× TPS collapse and 159× p99 spike** under high contention demonstrates the
retry-storm failure mode of optimistic locking empirically.

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
  Headers: Idempotency-Key: <uuid>   (optional but recommended)
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
| Same idempotency key, different body | 422 |
| Insufficient funds | 422 |
| Request in progress (concurrent duplicate) | 409 |
| Server error | 500 |
| Replayed response | original status + `Idempotent-Replayed: true` |

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

- **ACID transactions**: single-transaction transfer ensures crash-safe double-entry
- **Concurrency safety**: conditional UPDATE as default; SELECT FOR UPDATE and optimistic
  versioning as alternatives, with benchmark proof of when each degrades
- **Idempotency**: SHA-256 request hashing, `INSERT ON CONFLICT DO NOTHING` as
  distributed mutex, stored-response replay
- **Outbox pattern**: atomic event write, `FOR UPDATE SKIP LOCKED` delivery,
  at-least-once + consumer idempotency via X-Event-ID
- **Reconciliation**: independent conservation and balance-integrity checks
- **Observability**: `/metrics` endpoint, structured error responses
