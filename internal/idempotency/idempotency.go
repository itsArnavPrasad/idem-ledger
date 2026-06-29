package idempotency

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

var (
	// ErrDuplicateRequest is returned when the same key is used with a different request body.
	ErrDuplicateRequest = errors.New("idempotency key already used with a different request")
	// ErrInProgress is returned when a concurrent request is already processing the same key.
	ErrInProgress = errors.New("request with this idempotency key is already in progress")
)

// staleClaimThreshold is how long a key may sit in in_progress before the claim is
// considered abandoned (process or connection died between claiming and completing).
// If the original holder is still alive, they will win the re-claim race because their
// transaction holds the row lock; only truly abandoned keys get stolen.
const staleClaimThreshold = 30 * time.Second

// StoredResponse is the response we captured and replay on retries.
type StoredResponse struct {
	Code int
	Body json.RawMessage
}

// HashRequest produces a stable SHA-256 hex digest of the request body bytes.
// The body is normalized (JSON unmarshal+remarshal) before hashing so that
// logically identical payloads with different key order or whitespace produce
// the same hash. Falls back to raw bytes for non-JSON bodies.
func HashRequest(body []byte) string {
	var v any
	if json.Unmarshal(body, &v) == nil {
		if norm, err := json.Marshal(v); err == nil {
			// json.Marshal sorts map keys alphabetically and strips extra whitespace.
			body = norm
		}
	}
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
//
// Stale-claim reclaim: if a done record exists with in_progress status and the claim
// is older than staleClaimThreshold (the original holder likely crashed), this function
// attempts to steal the claim by updating the row. If the UPDATE wins (RowsAffected=1),
// this request owns the work. If another concurrent request just stole it first
// (RowsAffected=0), ErrInProgress is returned.
func ClaimInTx(ctx context.Context, tx pgx.Tx, key, requestHash string) (bool, *StoredResponse, error) {
	tag, err := tx.Exec(ctx,
		`INSERT INTO idempotency_keys (key, request_hash, status, claimed_at)
		 VALUES ($1, $2, 'in_progress', now())
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
	var claimedAt time.Time
	err = tx.QueryRow(ctx,
		`SELECT request_hash, status, response_code, response_body, claimed_at
		 FROM idempotency_keys WHERE key = $1`,
		key,
	).Scan(&existingHash, &status, &respCode, &respBody, &claimedAt)
	if err != nil {
		return false, nil, err
	}

	if existingHash != requestHash {
		return false, nil, ErrDuplicateRequest
	}

	if status == "in_progress" {
		// Check if the claim is stale (original holder likely crashed).
		if time.Since(claimedAt) > staleClaimThreshold {
			// Attempt to steal: update only if the row is still in_progress and still stale.
			// The WHERE clause is optimistic: if the original holder completes between our
			// SELECT and this UPDATE, their commit wins and RowsAffected will be 0.
			stealTag, err := tx.Exec(ctx,
				`UPDATE idempotency_keys
				 SET status='in_progress', request_hash=$1, claimed_at=now()
				 WHERE key=$2 AND status='in_progress' AND claimed_at < now()-$3`,
				requestHash, key, staleClaimThreshold,
			)
			if err != nil {
				return false, nil, err
			}
			if stealTag.RowsAffected() == 1 {
				return true, nil, nil // we stole the stale claim
			}
			// Someone else just stole it, or the original holder completed. Fall through.
		}
		return false, nil, ErrInProgress
	}

	// status == "done" — replay the stored response.
	if respCode == nil {
		// This should not happen if the DB CHECK constraint is enforced, but guard
		// defensively: a done row with NULL response_code would panic on dereference.
		return false, nil, errors.New("idempotency key is done but response_code is NULL — data corruption")
	}
	return false, &StoredResponse{Code: *respCode, Body: json.RawMessage(respBody)}, nil
}
