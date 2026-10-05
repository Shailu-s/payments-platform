-- Unknown outcomes must wait for confirmation, not be retried or reversed blindly.
ALTER TABLE transfers DROP CONSTRAINT transfers_status_check;
ALTER TABLE transfers ADD CONSTRAINT transfers_status_check
    CHECK (status IN ('created', 'processing', 'settled', 'failed', 'unresolved'));

-- A provider reference must identify only one transfer for webhook lookup.
ALTER TABLE transfers ADD COLUMN provider_ref text;

-- Count claims and persist their backoff so a restart does not reset reservations.
ALTER TABLE transfers ADD COLUMN attempt_count   integer     NOT NULL DEFAULT 0;
ALTER TABLE transfers ADD COLUMN next_attempt_at timestamptz;

-- The last thing the provider said, kept for an operator to read. Not parsed.
ALTER TABLE transfers ADD COLUMN last_error text;

CREATE UNIQUE INDEX transfers_provider_ref_idx
    ON transfers (provider_ref) WHERE provider_ref IS NOT NULL;

-- Match claim ordering: unattempted rows first, then oldest retry deadline.
-- Exclude terminal history so the index tracks processing work, not all transfers.
CREATE INDEX transfers_claimable_idx
    ON transfers (next_attempt_at NULLS FIRST, created_at) WHERE status = 'processing';
