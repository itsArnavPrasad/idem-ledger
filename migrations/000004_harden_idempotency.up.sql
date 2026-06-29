-- Track when each key was claimed so stale in_progress rows can be reclaimed after a crash.
-- Without this, a process that crashes mid-transfer leaves the key stuck in_progress forever
-- and all retries on that key receive 409 indefinitely.
ALTER TABLE idempotency_keys ADD COLUMN claimed_at TIMESTAMPTZ NOT NULL DEFAULT now();

-- Enforce that a done row always has a response_code.
-- Prevents nil pointer panic in ClaimInTx when replaying a done key whose response_code
-- was somehow left NULL (data corruption, manual DB edit, or partial write).
ALTER TABLE idempotency_keys
  ADD CONSTRAINT idempotency_keys_done_has_response_code
  CHECK (status = 'in_progress' OR response_code IS NOT NULL);
