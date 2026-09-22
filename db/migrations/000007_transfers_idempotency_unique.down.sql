ALTER TABLE transfers DROP COLUMN IF EXISTS request_fingerprint;
DROP INDEX IF EXISTS transfers_idempotency_key_idx;
