-- Phase 4: the outside world.
--
-- 'unresolved' is a real state, not a comment. Our call to the provider timed
-- out, so we genuinely do not know whether the money moved. Retrying risks
-- paying twice; failing risks losing a payment that happened. Being unable to
-- decide is the correct behaviour, so the schema has a name for it.
ALTER TABLE transfers DROP CONSTRAINT transfers_status_check;
ALTER TABLE transfers ADD CONSTRAINT transfers_status_check
    CHECK (status IN ('created', 'processing', 'settled', 'failed', 'unresolved'));

-- The provider's own identifier for this payment. Nullable until the provider
-- has accepted it, and unique because two transfers sharing one provider
-- reference would mean we cannot tell which payment a webhook is about.
ALTER TABLE transfers ADD COLUMN provider_ref text;

-- Retry bookkeeping. attempt_count is for observability and for giving up;
-- next_attempt_at is what makes backoff possible without a second table or a
-- sleeping goroutine — the worker claims rows whose time has come, so a
-- restart loses nothing.
ALTER TABLE transfers ADD COLUMN attempt_count   integer     NOT NULL DEFAULT 0;
ALTER TABLE transfers ADD COLUMN next_attempt_at timestamptz;

-- The last thing the provider said, kept for an operator to read. Not parsed.
ALTER TABLE transfers ADD COLUMN last_error text;

CREATE UNIQUE INDEX transfers_provider_ref_idx
    ON transfers (provider_ref) WHERE provider_ref IS NOT NULL;

-- The worker's claim query, and the column order matters.
--
-- Partial on status = 'processing' because settled transfers are the vast
-- majority and the worker never wants them: without the predicate this index
-- grows forever while staying useful for a shrinking fraction of its rows.
--
-- The claim query must ORDER BY next_attempt_at to use this, and that is also
-- the correct order to work in: oldest due first, so a transfer backed off to a
-- later time does not jump ahead of one that has been waiting. Ordering by
-- created_at instead was measured at 11,047 buffers and 6.6ms against a backlog
-- of 50,000 — Postgres abandons this index and scans the created_at index
-- backwards. Ordering by next_attempt_at is 3 buffers and 0.085ms.
--
-- NULLS FIRST because a transfer that has never been attempted has no
-- next_attempt_at and should be claimed before one that is merely due again.
CREATE INDEX transfers_claimable_idx
    ON transfers (next_attempt_at NULLS FIRST, created_at) WHERE status = 'processing';
