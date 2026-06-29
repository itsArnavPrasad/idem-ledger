package idempotency

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

var (
	// ErrDuplicateRequest is returned when the same key is used with a different request body.
	ErrDuplicateRequest = errors.New("idempotency key already used with a different request")
	// ErrInProgress is returned when a concurrent request is already processing the same key.
	ErrInProgress = errors.New("request with this idempotency key is already in progress")
)

// StoredResponse is the response we captured and replay on retries.
type StoredResponse struct {
	Code int
	Body json.RawMessage
}

// HashRequest produces a stable SHA-256 hex digest of the request body bytes.
func HashRequest(body []byte) string {
	sum := sha256.Sum256(body)
	return fmt.Sprintf("%x", sum)
}

// Complete marks the key as done and stores the response, inside a provided transaction.
// Call this within the transfer transaction so the result and the transfer are atomic.
func Complete(ctx context.Context, tx pgx.Tx, key string, code int, body []byte) error {
	_, err := tx.Exec(ctx,
		`UPDATE idempotency_keys SET status='done', response_code=$1, response_body=$2 WHERE key=$3`,
		code, body, key,
	)
	return err
}

// ClaimInTx inserts the idempotency record inside an already-open transaction.
// Returns (true, nil) if inserted (this request owns the work).
// Returns (false, stored) if a done record already exists (replay).
// Returns error on hash mismatch or in_progress collision.
func ClaimInTx(ctx context.Context, tx pgx.Tx, key, requestHash string) (bool, *StoredResponse, error) {
	tag, err := tx.Exec(ctx,
		`INSERT INTO idempotency_keys (key, request_hash, status)
		 VALUES ($1, $2, 'in_progress')
		 ON CONFLICT (key) DO NOTHING`,
		key, requestHash,
	)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) {
			return false, nil, fmt.Errorf("idempotency insert: %w", err)
		}
		return false, nil, err
	}

	if tag.RowsAffected() == 1 {
		// We inserted the row — we own this request.
		return true, nil, nil
	}

	// Row already existed. Read it to determine what to do.
	var existingHash, status string
	var respCode *int
	var respBody []byte
	err = tx.QueryRow(ctx,
		`SELECT request_hash, status, response_code, response_body FROM idempotency_keys WHERE key = $1`,
		key,
	).Scan(&existingHash, &status, &respCode, &respBody)
	if err != nil {
		return false, nil, err
	}

	if existingHash != requestHash {
		return false, nil, ErrDuplicateRequest
	}
	if status == "in_progress" {
		return false, nil, ErrInProgress
	}
	// status == "done"
	return false, &StoredResponse{Code: *respCode, Body: json.RawMessage(respBody)}, nil
}
