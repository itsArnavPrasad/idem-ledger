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
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	pollInterval = 100 * time.Millisecond
	batchSize    = 50
	maxRetries   = 8
	deliverTimeout = 10 * time.Second
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

func (p *Poller) deliver(ctx context.Context, e store.OutboxEvent) {
	// No webhook configured — treat as delivered immediately.
	if e.TargetURL == nil || *e.TargetURL == "" {
		if err := store.MarkDelivered(ctx, p.db, e.ID); err != nil {
			log.Printf("outbox mark delivered (no-op): %v", err)
		}
		return
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, *e.TargetURL,
		strings.NewReader(string(e.Payload)))
	if err != nil {
		store.MarkFailed(ctx, p.db, e.ID, err.Error(), maxRetries)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	// Stable per event across all retry attempts — the merchant deduplicates on this.
	req.Header.Set("X-Event-ID", e.ID.String())
	req.Header.Set("X-Event-Type", e.EventType)

	resp, err := p.client.Do(req)
	if err != nil {
		store.MarkFailed(ctx, p.db, e.ID, err.Error(), maxRetries)
		return
	}
	resp.Body.Close()

	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		store.MarkDelivered(ctx, p.db, e.ID)
	case resp.StatusCode >= 400 && resp.StatusCode < 500:
		// Client error — dead-letter immediately (retrying won't help).
		store.MarkFailed(ctx, p.db, e.ID,
			http.StatusText(resp.StatusCode), maxRetries+1) // maxRetries+1 forces dead_letter
	default:
		// 5xx / redirect — schedule retry with backoff.
		store.MarkFailed(ctx, p.db, e.ID,
			http.StatusText(resp.StatusCode), maxRetries)
	}
}
