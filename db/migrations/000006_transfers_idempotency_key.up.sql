-- Uniqueness is added separately in migration 000007.
ALTER TABLE transfers ADD COLUMN idempotency_key text;
