package ledger

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/arnavprasad/idem-ledger/internal/idempotency"
	"github.com/arnavprasad/idem-ledger/internal/store"
)

var (
	ErrInsufficientFunds = errors.New("insufficient funds")
	ErrAccountNotFound   = errors.New("account not found")
	ErrCurrencyMismatch  = errors.New("currency mismatch: from_account, to_account, and transfer currency must all match")
	ErrInvalidAmount     = errors.New("amount must be positive")
	ErrSameAccount       = errors.New("from_account and to_account must differ")
	ErrInvalidCurrency   = errors.New("currency must be a 3-letter ISO 4217 code")
)

type TransferRequest struct {
	FromAccount    int64
	ToAccount      int64
	Amount         int64
	Currency       string
	IdempotencyKey string
	RequestHash    string // SHA-256 of request body; required when IdempotencyKey is set
}

type Transfer struct {
	ID             uuid.UUID `json:"id"`
	IdempotencyKey string    `json:"idempotency_key"`
	Status         string    `json:"status"`
	Amount         int64     `json:"amount"`
	Currency       string    `json:"currency"`
	FromAccount    int64     `json:"from_account"`
	ToAccount      int64     `json:"to_account"`
	CreatedAt      time.Time `json:"created_at"`
}

// maxDeadlockRetries is the retry budget for deadlock (40P01) and serialisation
// failure (40001) errors in Execute. Both are transient: PostgreSQL rolls back one
// side of the deadlock and the transaction can be retried safely. The idempotency
// key claim is inside the same transaction, so a rollback also unwinds the claim —
// retrying re-claims it from scratch, keeping atomicity intact.
const maxDeadlockRetries = 5

// Execute runs a transfer as a single atomic DB transaction.
// Validates the request before touching the database; callers that bypass the HTTP
// layer get clean domain errors instead of raw DB constraint failures.
// When IdempotencyKey is set, it claims the key, does the work, and records the
// response — all inside one transaction. Replays return the stored response.
// Deadlocks (40P01) and serialisation failures (40001) are retried automatically;
// these arise when two concurrent A→B and B→A transfers race on the same rows.
func Execute(ctx context.Context, pool *pgxpool.Pool, req TransferRequest) (Transfer, *idempotency.StoredResponse, error) {
	if req.Amount <= 0 {
		return Transfer{}, nil, ErrInvalidAmount
	}
	if req.FromAccount == req.ToAccount {
		return Transfer{}, nil, ErrSameAccount
	}
	if len(req.Currency) != 3 {
		return Transfer{}, nil, ErrInvalidCurrency
	}

	for attempt := 0; attempt < maxDeadlockRetries; attempt++ {
		t, stored, err := executeOnce(ctx, pool, req)
		if err != nil && isRetriableConflict(err) {
			continue
		}
		return t, stored, err
	}
	return Transfer{}, nil, errors.New("too many deadlock retries on transfer")
}

func executeOnce(ctx context.Context, pool *pgxpool.Pool, req TransferRequest) (Transfer, *idempotency.StoredResponse, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return Transfer{}, nil, err
	}
	defer tx.Rollback(ctx)

	// Idempotency check — runs inside the same transaction so the key claim
	// and the transfer are atomic: the key is 'done' iff the transfer committed.
	if req.IdempotencyKey != "" {
		won, stored, err := idempotency.ClaimInTx(ctx, tx, req.IdempotencyKey, req.RequestHash)
		if err != nil {
			return Transfer{}, nil, err
		}
		if !won {
			// Replay the stored response — the transaction was never needed.
			tx.Rollback(ctx)
			return Transfer{}, stored, nil
		}
	}

	result, err := execTx(ctx, tx, req)
	if err != nil {
		return Transfer{}, nil, err
	}

	// Record the idempotent result before committing so it's atomic.
	if req.IdempotencyKey != "" {
		body, err := json.Marshal(result)
		if err != nil {
			return Transfer{}, nil, fmt.Errorf("marshal transfer result: %w", err)
		}
		if err := idempotency.Complete(ctx, tx, req.IdempotencyKey, 201, body); err != nil {
			return Transfer{}, nil, err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return Transfer{}, nil, err
	}
	return result, nil, nil
}

// execTx runs the transfer inside tx using the default Fix A debit strategy.
func execTx(ctx context.Context, tx pgx.Tx, req TransferRequest) (Transfer, error) {
	return execTxWithDebit(ctx, tx, req, debitConditionalUpdate)
}

// execTxWithDebit runs the transfer using the provided debit strategy.
// Called by Fix A (execTx), Fix B (ExecuteWithForUpdate), and Fix C (ExecuteOptimistic).
// Validates that both accounts exist and share the same currency as the transfer
// before any balance mutation, so the caller gets a clean domain error.
func execTxWithDebit(ctx context.Context, tx pgx.Tx, req TransferRequest, debit debitFn) (Transfer, error) {
	// Validate that both accounts exist and their currencies match the transfer currency.
	// Done in one query to avoid two round trips. ErrNoRows means at least one account
	// is missing; we return ErrAccountNotFound in that case.
	var fromCurr, toCurr string
	err := tx.QueryRow(ctx,
		`SELECT a1.currency, a2.currency
		 FROM accounts a1, accounts a2
		 WHERE a1.id = $1 AND a2.id = $2`,
		req.FromAccount, req.ToAccount,
	).Scan(&fromCurr, &toCurr)
	if errors.Is(err, pgx.ErrNoRows) {
		return Transfer{}, ErrAccountNotFound
	}
	if err != nil {
		return Transfer{}, err
	}
	if fromCurr != req.Currency || toCurr != req.Currency {
		return Transfer{}, ErrCurrencyMismatch
	}

	if err := debit(ctx, tx, req.FromAccount, req.Amount); err != nil {
		return Transfer{}, err
	}

	// Credit destination.
	creditTag, err := tx.Exec(ctx,
		`UPDATE accounts SET balance = balance + $1 WHERE id = $2`,
		req.Amount, req.ToAccount,
	)
	if err != nil {
		return Transfer{}, err
	}
	if creditTag.RowsAffected() == 0 {
		return Transfer{}, ErrAccountNotFound
	}

	return insertTransferAndPostings(ctx, tx, req)
}

// insertTransferAndPostings writes the transfers row and the two balanced postings.
func insertTransferAndPostings(ctx context.Context, tx pgx.Tx, req TransferRequest) (Transfer, error) {
	idempotencyKey := req.IdempotencyKey
	if idempotencyKey == "" {
		idempotencyKey = uuid.New().String()
	}
	transferID := uuid.New()

	var t Transfer
	err := tx.QueryRow(ctx,
		`INSERT INTO transfers (id, idempotency_key, status, amount, currency, from_account, to_account)
		 VALUES ($1, $2, 'posted', $3, $4, $5, $6)
		 RETURNING id, idempotency_key, status, amount, currency, from_account, to_account, created_at`,
		transferID, idempotencyKey, req.Amount, req.Currency, req.FromAccount, req.ToAccount,
	).Scan(&t.ID, &t.IdempotencyKey, &t.Status, &t.Amount, &t.Currency, &t.FromAccount, &t.ToAccount, &t.CreatedAt)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23503" {
			return Transfer{}, ErrAccountNotFound
		}
		return Transfer{}, err
	}

	// Two postings summing to zero — conservation by construction.
	if _, err := tx.Exec(ctx,
		`INSERT INTO postings (transfer_id, account_id, amount) VALUES ($1,$2,$3),($1,$4,$5)`,
		t.ID, req.FromAccount, -req.Amount, req.ToAccount, req.Amount,
	); err != nil {
		return Transfer{}, err
	}

	// Write outbox event in the same transaction so the event is atomic with the
	// transfer: exists iff the transfer committed, never lost on a crash between
	// COMMIT and the old naive "fire webhook after commit" approach.
	webhookURL, err := store.GetWebhookURLInTx(ctx, tx, req.ToAccount)
	if err != nil {
		return Transfer{}, err
	}
	payload, err := json.Marshal(t)
	if err != nil {
		return Transfer{}, fmt.Errorf("marshal outbox payload: %w", err)
	}
	if err := store.InsertOutboxEventInTx(ctx, tx, "transfer.created", payload, webhookURL); err != nil {
		return Transfer{}, err
	}

	return t, nil
}
