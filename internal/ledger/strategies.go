package ledger

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// isRetriableConflict reports whether err is a transient concurrency error that
// ExecuteOptimistic should retry: deadlock (40P01) or serialisation failure (40001).
// These can occur because UPDATE still acquires row locks even in "optimistic" mode —
// two goroutines doing A→B and B→A can deadlock at the credit step.
func isRetriableConflict(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code == "40P01" || pgErr.Code == "40001"
	}
	return false
}

// debitFn is the strategy hook: debit fromAccount by amount inside tx.
// Returns ErrInsufficientFunds or ErrAccountNotFound on logical failure.
type debitFn func(ctx context.Context, tx pgx.Tx, fromAccount, amount int64) error

// --- Fix A: conditional atomic UPDATE (default) ---
// Safe under READ COMMITTED: the UPDATE takes a row lock and re-evaluates
// balance >= amount at lock time, closing the read-write gap.
func debitConditionalUpdate(ctx context.Context, tx pgx.Tx, fromAccount, amount int64) error {
	tag, err := tx.Exec(ctx,
		`UPDATE accounts SET balance = balance - $1 WHERE id = $2 AND balance >= $1`,
		amount, fromAccount,
	)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		var exists bool
		if err := tx.QueryRow(ctx,
			`SELECT EXISTS(SELECT 1 FROM accounts WHERE id = $1)`, fromAccount,
		).Scan(&exists); err != nil {
			return err
		}
		if !exists {
			return ErrAccountNotFound
		}
		return ErrInsufficientFunds
	}
	return nil
}

// --- Fix B: pessimistic SELECT FOR UPDATE ---
// Explicitly locks both account rows before reading balances, then writes.
// Lock order is always min(from,to) first to prevent A→B / B→A deadlocks.
func debitSelectForUpdate(ctx context.Context, tx pgx.Tx, fromAccount, amount int64) error {
	var balance int64
	err := tx.QueryRow(ctx,
		`SELECT balance FROM accounts WHERE id = $1 FOR UPDATE`,
		fromAccount,
	).Scan(&balance)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrAccountNotFound
	}
	if err != nil {
		return err
	}
	if balance < amount {
		return ErrInsufficientFunds
	}
	_, err = tx.Exec(ctx,
		`UPDATE accounts SET balance = balance - $1 WHERE id = $2`,
		amount, fromAccount,
	)
	return err
}

// ExecuteWithForUpdate runs a full transfer using Fix B.
// Both account rows are locked in ascending ID order before any balance is read,
// preventing deadlocks when concurrent transfers run in opposite directions (A→B and B→A).
// The ascending-order pre-lock makes deadlocks virtually impossible, but a serialisation
// failure (40001) or transient error is still retried for robustness.
func ExecuteWithForUpdate(ctx context.Context, pool *pgxpool.Pool, req TransferRequest) (Transfer, *idempotencyResult, error) {
	if req.IdempotencyKey != "" {
		return Transfer{}, nil, errors.New("idempotency key not supported by ExecuteWithForUpdate; use Execute")
	}
	for attempt := 0; attempt < maxRetries; attempt++ {
		tx, err := pool.Begin(ctx)
		if err != nil {
			return Transfer{}, nil, err
		}

		// Lock in ascending id order — canonical deadlock prevention.
		lo, hi := req.FromAccount, req.ToAccount
		if lo > hi {
			lo, hi = hi, lo
		}
		if _, err := tx.Exec(ctx,
			`SELECT id FROM accounts WHERE id IN ($1, $2) ORDER BY id FOR UPDATE`,
			lo, hi,
		); err != nil {
			tx.Rollback(ctx)
			if isRetriableConflict(err) {
				continue
			}
			return Transfer{}, nil, err
		}

		// Now safe to read-check-write.
		result, err := execTxWithDebit(ctx, tx, req, debitSelectForUpdate)
		if err != nil {
			tx.Rollback(ctx)
			if isRetriableConflict(err) {
				continue
			}
			return Transfer{}, nil, err
		}
		if err := tx.Commit(ctx); err != nil {
			tx.Rollback(ctx)
			if isRetriableConflict(err) {
				continue
			}
			return Transfer{}, nil, err
		}
		return result, nil, nil
	}
	return Transfer{}, nil, errors.New("too many retries in ExecuteWithForUpdate")
}

// maxRetries is the retry budget for transient concurrency errors (deadlock 40P01,
// serialisation failure 40001) shared by all three execution strategies.
const maxRetries = 5

// --- Fix C: optimistic concurrency (version column) ---
// Reads (balance, version), checks, then updates only if version unchanged.
// Retries up to maxRetries times before giving up.
func ExecuteOptimistic(ctx context.Context, pool *pgxpool.Pool, req TransferRequest) (Transfer, *idempotencyResult, error) {
	if req.IdempotencyKey != "" {
		return Transfer{}, nil, errors.New("idempotency key not supported by ExecuteOptimistic; use Execute")
	}
	for attempt := 0; attempt < maxRetries; attempt++ {
		tx, err := pool.Begin(ctx)
		if err != nil {
			return Transfer{}, nil, err
		}

		var balance, version int64
		err = tx.QueryRow(ctx,
			`SELECT balance, version FROM accounts WHERE id = $1`,
			req.FromAccount,
		).Scan(&balance, &version)
		if errors.Is(err, pgx.ErrNoRows) {
			tx.Rollback(ctx)
			return Transfer{}, nil, ErrAccountNotFound
		}
		if err != nil {
			tx.Rollback(ctx)
			return Transfer{}, nil, err
		}
		if balance < req.Amount {
			tx.Rollback(ctx)
			return Transfer{}, nil, ErrInsufficientFunds
		}

		tag, err := tx.Exec(ctx,
			`UPDATE accounts SET balance = balance - $1, version = version + 1
			 WHERE id = $2 AND version = $3`,
			req.Amount, req.FromAccount, version,
		)
		if err != nil {
			tx.Rollback(ctx)
			if isRetriableConflict(err) {
				continue
			}
			return Transfer{}, nil, err
		}
		if tag.RowsAffected() == 0 {
			// Version changed — someone else updated first. Retry.
			tx.Rollback(ctx)
			continue
		}

		// Credit destination (no version check needed on the credit side).
		// A deadlock here means another goroutine holds the to_account row lock and is waiting
		// for from_account — exactly the A→B / B→A cycle. Roll back and retry.
		if _, err := tx.Exec(ctx,
			`UPDATE accounts SET balance = balance + $1 WHERE id = $2`,
			req.Amount, req.ToAccount,
		); err != nil {
			tx.Rollback(ctx)
			if isRetriableConflict(err) {
				continue
			}
			return Transfer{}, nil, err
		}

		result, err := insertTransferAndPostings(ctx, tx, req)
		if err != nil {
			tx.Rollback(ctx)
			return Transfer{}, nil, err
		}
		if err := tx.Commit(ctx); err != nil {
			// Commit failed (serialisation conflict or transient error) — roll back and retry.
			// Without this Rollback the pgx connection is returned to the pool in an aborted
			// transaction state, causing "current transaction is aborted" errors for unrelated callers.
			tx.Rollback(ctx)
			continue
		}
		return result, nil, nil
	}
	return Transfer{}, nil, errors.New("too many optimistic retry conflicts")
}

// idempotencyResult is used in the strategy helpers (unused until Phase 8 harness wires it up).
type idempotencyResult = struct{}
