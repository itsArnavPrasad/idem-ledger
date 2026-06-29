// Package outbox provides the poller goroutine that delivers events to merchant webhooks.
// Events are written atomically with their transfers; the poller reads and delivers them
// with at-least-once semantics. Consumers must deduplicate on the X-Event-ID header.
package outbox

import (
	"context"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/arnavprasad/idem-ledger/internal/store"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	pollInterval   = 100 * time.Millisecond
	batchSize      = 50
	maxRetries     = 8
	deliverTimeout = 10 * time.Second
	// markTimeout is the budget for post-delivery DB writes.
	// It intentionally uses context.Background() so it survives poller shutdown;
	// the transfer is already delivered and the DB state must reflect that.
	markTimeout = 5 * time.Second
)

// Poller delivers outbox events via HTTP. Run it as a goroutine; cancel the ctx
// to shut it down cleanly.
type Poller struct {
	db     *pgxpool.Pool
	client *http.Client
}

func New(db *pgxpool.Pool) *Poller {
	return &Poller{
		db:     db,
		client: &http.Client{Timeout: deliverTimeout},
	}
}

// Run polls the outbox every pollInterval. Blocks until ctx is cancelled.
func (p *Poller) Run(ctx context.Context) {
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := p.poll(ctx); err != nil {
				log.Printf("outbox poll error: %v", err)
			}
		}
	}
}

func (p *Poller) poll(ctx context.Context) error {
	events, err := store.ClaimPendingEvents(ctx, p.db, batchSize)
	if err != nil {
		return err
	}
	for _, e := range events {
		p.deliver(ctx, e)
	}
	return nil
}

// markCtx returns a context for DB state-update calls after delivery.
// It uses context.Background() — not the poller's ctx — so that marking
// succeeds even when the poller is shutting down. Without this, a shutdown
// signal between HTTP success and MarkDelivered leaves the row in_flight
// permanently (until the stale-recovery mechanism reclaims it, causing a
// duplicate delivery to the merchant).
func markCtx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), markTimeout)
}

func (p *Poller) deliver(ctx context.Context, e store.OutboxEvent) {
	// No webhook configured — treat as delivered immediately.
	if e.TargetURL == nil || *e.TargetURL == "" {
		mCtx, cancel := markCtx()
		defer cancel()
		if err := store.MarkDelivered(mCtx, p.db, e.ID); err != nil {
			log.Printf("outbox mark delivered (no-op) id=%s: %v", e.ID, err)
		}
		return
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, *e.TargetURL,
		strings.NewReader(string(e.Payload)))
	if err != nil {
		// Malformed URL stored in target_url — permanent failure, dead-letter immediately.
		mCtx, cancel := markCtx()
		defer cancel()
		if err2 := store.MarkDeadLetter(mCtx, p.db, e.ID, "bad target_url: "+err.Error()); err2 != nil {
			log.Printf("outbox dead-letter (bad url) id=%s: %v", e.ID, err2)
		}
		return
	}
	req.Header.Set("Content-Type", "application/json")
	// Stable per event across all retry attempts — the merchant deduplicates on this.
	req.Header.Set("X-Event-ID", e.ID.String())
	req.Header.Set("X-Event-Type", e.EventType)

	resp, err := p.client.Do(req)
	if err != nil {
		// Network/timeout error — schedule retry with backoff.
		mCtx, cancel := markCtx()
		defer cancel()
		if err2 := store.MarkFailed(mCtx, p.db, e.ID, err.Error(), maxRetries); err2 != nil {
			log.Printf("outbox mark failed (network err) id=%s: %v", e.ID, err2)
		}
		return
	}
	resp.Body.Close()

	mCtx, cancel := markCtx()
	defer cancel()
	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		// Retry MarkDelivered up to 3 times to shrink the duplicate delivery window.
		// If all retries fail, the row stays in_flight and will be redelivered after
		// the stale-recovery threshold (~30s) — inherent at-least-once semantics.
		p.markDeliveredWithRetry(e.ID)
	case resp.StatusCode >= 400 && resp.StatusCode < 500:
		// Client error (404, 410, etc.) — retrying cannot help, dead-letter immediately.
		if err := store.MarkDeadLetter(mCtx, p.db, e.ID, http.StatusText(resp.StatusCode)); err != nil {
			log.Printf("outbox dead-letter (4xx) id=%s: %v", e.ID, err)
		}
	default:
		// 5xx / redirect — schedule retry with exponential backoff.
		if err := store.MarkFailed(mCtx, p.db, e.ID, http.StatusText(resp.StatusCode), maxRetries); err != nil {
			log.Printf("outbox mark failed (5xx) id=%s: %v", e.ID, err)
		}
	}
}

// markDeliveredWithRetry retries MarkDelivered up to 3 times (1s apart).
// The duplicate delivery window shrinks to the gap between the HTTP 200 response
// and the third DB write attempt — typically well under 3 seconds. Events that
// still fail after 3 attempts are left in_flight and redelivered after 30s
// (stale-recovery), which is correct at-least-once behavior.
func (p *Poller) markDeliveredWithRetry(id uuid.UUID) {
	for i := 0; i < 3; i++ {
		mCtx, cancel := markCtx()
		err := store.MarkDelivered(mCtx, p.db, id)
		cancel()
		if err == nil {
			return
		}
		if i < 2 {
			time.Sleep(time.Second)
		}
	}
	log.Printf("outbox mark delivered failed after 3 attempts id=%s — will redeliver after stale timeout", id)
}
