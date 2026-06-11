-- Add webhook_url to accounts so each account can receive transfer notifications.
ALTER TABLE accounts ADD COLUMN webhook_url TEXT;

-- Outbox table: events written atomically with their transfers.
-- The poller reads pending rows, delivers via HTTP, and updates status.
CREATE TABLE outbox (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_type      TEXT NOT NULL,
    payload         JSONB NOT NULL,
    -- Snapshot of to_account.webhook_url at event creation time.
    -- Stored here so URL changes after the transfer don't affect delivery.
    target_url      TEXT,
    status          TEXT NOT NULL DEFAULT 'pending',
      CONSTRAINT outbox_status_check CHECK (status IN ('pending','in_flight','delivered','dead_letter')),
    attempt_count   INT  NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ,
    failure_reason  TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    delivered_at    TIMESTAMPTZ
);

-- Partial index: the poller only reads pending/in_flight rows.
-- Filtering before the ORDER BY keeps the index small and fast.
CREATE INDEX idx_outbox_pending ON outbox (status, next_attempt_at)
    WHERE status IN ('pending', 'in_flight');
