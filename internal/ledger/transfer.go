package ledger

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/arnavprasad/idem-ledger/internal/idempotency"
)

var (
	ErrInsufficientFunds = errors.New("insufficient funds")
	ErrAccountNotFound   = errors.New("account not found")
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

// Execute runs a transfer as a single atomic DB transaction.
// When IdempotencyKey is set, it claims the key, does the work, and records the
// response — all inside one transaction. Replays return the stored response.
func Execute(ctx context.Context, pool *pgxpool.Pool, req TransferRequest) (Transfer, *idempotency.StoredResponse, error) {
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
		body, _ := json.Marshal(result)
		if err := idempotency.Complete(ctx, tx, req.IdempotencyKey, 201, body); err != nil {
			return Transfer{}, nil, err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return Transfer{}, nil, err
	}
	return result, nil, nil
}

// execTx performs the core transfer logic inside an already-open transaction.
// Exported separately so Phase 5 (idempotency) can wrap it.
func execTx(ctx context.Context, tx pgx.Tx, req TransferRequest) (Transfer, error) {
	// Fix A: conditional atomic UPDATE — safe under READ COMMITTED.
	// The UPDATE takes a row lock and re-evaluates balance >= amount at lock time,
	// so there is no read-write gap for another transaction to sneak through.
	tag, err := tx.Exec(ctx,
		`UPDATE accounts SET balance = balance - $1 WHERE id = $2 AND balance >= $1`,
		req.Amount, req.FromAccount,
	)
	if err != nil {
		return Transfer{}, err
	}
	if tag.RowsAffected() == 0 {
		// Could be: account missing, or funds too low. Distinguish with a quick SELECT.
		var exists bool
		if err := tx.QueryRow(ctx,
			`SELECT EXISTS(SELECT 1 FROM accounts WHERE id = $1)`, req.FromAccount,
		).Scan(&exists); err != nil {
			return Transfer{}, err
		}
		if !exists {
			return Transfer{}, ErrAccountNotFound
		}
		return Transfer{}, ErrInsufficientFunds
	}

	// Credit destination. A missing to_account produces 0 rows here and then a
	// FK violation on the transfers INSERT below, which we map to ErrAccountNotFound.
	if _, err := tx.Exec(ctx,
		`UPDATE accounts SET balance = balance + $1 WHERE id = $2`,
		req.Amount, req.ToAccount,
	); err != nil {
		return Transfer{}, err
	}

	idempotencyKey := req.IdempotencyKey
	if idempotencyKey == "" {
		idempotencyKey = uuid.New().String()
	}
	transferID := uuid.New()

	var t Transfer
	err = tx.QueryRow(ctx,
		`INSERT INTO transfers (id, idempotency_key, status, amount, currency, from_account, to_account)
		 VALUES ($1, $2, 'posted', $3, $4, $5, $6)
		 RETURNING id, idempotency_key, status, amount, currency, from_account, to_account, created_at`,
		transferID, idempotencyKey, req.Amount, req.Currency, req.FromAccount, req.ToAccount,
	).Scan(&t.ID, &t.IdempotencyKey, &t.Status, &t.Amount, &t.Currency, &t.FromAccount, &t.ToAccount, &t.CreatedAt)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23503" { // FK violation
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

	return t, nil
}
