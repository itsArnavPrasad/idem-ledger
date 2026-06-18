package store

import (
	"context"
	"encoding/json"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// OutboxEvent is a row in the outbox table.
type OutboxEvent struct {
	ID        uuid.UUID
	EventType string
	Payload   json.RawMessage
	TargetURL *string
	Status    string
}

// InsertOutboxEventInTx writes one outbox event inside an existing transaction.
// Call inside the same transaction that creates the transfer so the event is
// atomic with the ledger change: the event exists iff the transfer committed.
func InsertOutboxEventInTx(ctx context.Context, tx pgx.Tx, eventType string, payload json.RawMessage, targetURL *string) error {
	_, err := tx.Exec(ctx,
		`INSERT INTO outbox (event_type, payload, target_url)
		 VALUES ($1, $2, $3)`,
		eventType, payload, targetURL,
	)
	return err
}

// ClaimPendingEvents locks up to limit pending/overdue outbox rows using
// FOR UPDATE SKIP LOCKED so concurrent pollers don't double-deliver.
// Claimed rows are atomically flipped to in_flight.
func ClaimPendingEvents(ctx context.Context, db *pgxpool.Pool, limit int) ([]OutboxEvent, error) {
	rows, err := db.Query(ctx,
		`UPDATE outbox
		 SET status = 'in_flight'
		 WHERE id IN (
		     SELECT id FROM outbox
		     WHERE status = 'pending'
		       AND (next_attempt_at IS NULL OR next_attempt_at <= now())
		     ORDER BY created_at
		     LIMIT $1
		     FOR UPDATE SKIP LOCKED
		 )
		 RETURNING id, event_type, payload, target_url, status`,
		limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var events []OutboxEvent
	for rows.Next() {
		var e OutboxEvent
		if err := rows.Scan(&e.ID, &e.EventType, &e.Payload, &e.TargetURL, &e.Status); err != nil {
			return nil, err
		}
		events = append(events, e)
	}
	return events, rows.Err()
}

// MarkDelivered sets an outbox event as delivered.
func MarkDelivered(ctx context.Context, db *pgxpool.Pool, id uuid.UUID) error {
	_, err := db.Exec(ctx,
		`UPDATE outbox SET status='delivered', delivered_at=now() WHERE id=$1`,
		id,
	)
	return err
}

// OutboxMetrics summarises outbox health for the /metrics endpoint.
type OutboxMetrics struct {
	Pending    int `json:"pending"`
	InFlight   int `json:"in_flight"`
	Delivered  int `json:"delivered"`
	DeadLetter int `json:"dead_letter"`
}

// GetOutboxMetrics returns current row counts per status.
func GetOutboxMetrics(ctx context.Context, db *pgxpool.Pool) (OutboxMetrics, error) {
	rows, err := db.Query(ctx, `SELECT status, COUNT(*) FROM outbox GROUP BY status`)
	if err != nil {
		return OutboxMetrics{}, err
	}
	defer rows.Close()
	var m OutboxMetrics
	for rows.Next() {
		var status string
		var n int
		if err := rows.Scan(&status, &n); err != nil {
			return OutboxMetrics{}, err
		}
		switch status {
		case "pending":
			m.Pending = n
		case "in_flight":
			m.InFlight = n
		case "delivered":
			m.Delivered = n
		case "dead_letter":
			m.DeadLetter = n
		}
	}
	return m, rows.Err()
}

// MarkFailed increments attempt_count and either schedules a retry (pending) or
// dead-letters the event after maxRetries total attempts.
// Backoff: 5s * 2^attempt_count + up to 5s random jitter, so:
//   attempt 1 → ~10s, attempt 2 → ~20s, attempt 3 → ~40s … attempt 8 → dead_letter
func MarkFailed(ctx context.Context, db *pgxpool.Pool, id uuid.UUID, reason string, maxRetries int) error {
	_, err := db.Exec(ctx,
		`UPDATE outbox
		 SET attempt_count   = attempt_count + 1,
		     failure_reason  = $2,
		     status          = CASE
		         WHEN attempt_count + 1 >= $3 THEN 'dead_letter'
		         ELSE 'pending'
		     END,
		     next_attempt_at = CASE
		         WHEN attempt_count + 1 >= $3 THEN NULL
		         ELSE now()
		              + (interval '5 seconds' * power(2, attempt_count + 1))
		              + (random() * interval '5 seconds')
		     END
		 WHERE id = $1`,
		id, reason, maxRetries,
	)
	return err
}
