DROP INDEX IF EXISTS transfers_claimable_idx;
DROP INDEX IF EXISTS transfers_provider_ref_idx;

ALTER TABLE transfers DROP COLUMN IF EXISTS last_error;
ALTER TABLE transfers DROP COLUMN IF EXISTS next_attempt_at;
ALTER TABLE transfers DROP COLUMN IF EXISTS attempt_count;
ALTER TABLE transfers DROP COLUMN IF EXISTS provider_ref;

-- Preserve unresolved rows when restoring the old status constraint.
UPDATE transfers SET status = 'processing' WHERE status = 'unresolved';

ALTER TABLE transfers DROP CONSTRAINT transfers_status_check;
ALTER TABLE transfers ADD CONSTRAINT transfers_status_check
    CHECK (status IN ('created', 'processing', 'settled', 'failed'));
