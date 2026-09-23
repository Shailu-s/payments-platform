DROP INDEX IF EXISTS transfers_claimable_idx;
DROP INDEX IF EXISTS transfers_provider_ref_idx;

ALTER TABLE transfers DROP COLUMN IF EXISTS last_error;
ALTER TABLE transfers DROP COLUMN IF EXISTS next_attempt_at;
ALTER TABLE transfers DROP COLUMN IF EXISTS attempt_count;
ALTER TABLE transfers DROP COLUMN IF EXISTS provider_ref;

-- Anything left at 'unresolved' has no representation in the old constraint.
-- Moved to 'processing' rather than dropped: a transfer whose outcome is
-- unknown must not silently become a transfer that never happened.
UPDATE transfers SET status = 'processing' WHERE status = 'unresolved';

ALTER TABLE transfers DROP CONSTRAINT transfers_status_check;
ALTER TABLE transfers ADD CONSTRAINT transfers_status_check
    CHECK (status IN ('created', 'processing', 'settled', 'failed'));
