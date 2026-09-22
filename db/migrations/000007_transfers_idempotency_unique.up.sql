-- Phase 3.2: the fix.
--
-- This index is not a validation rule, it is a concurrency primitive. When two
-- transactions insert the same key at once, Postgres blocks the second on the
-- first's index entry until that one commits, then fails it with 23505. That
-- blocking IS the serialisation, and it is the thing application code cannot
-- do: the two requests may be in different processes, so no mutex reaches them
-- both, and checking before inserting only moves the gap.
--
-- Partial, because idempotency_key is nullable: transfers created by anything
-- other than a client instruction — a reversal in phase 4, say — have no key,
-- and NULLs would otherwise be excluded from uniqueness anyway. Saying so in
-- the predicate keeps the index small and the intent explicit.
CREATE UNIQUE INDEX transfers_idempotency_key_idx
    ON transfers (idempotency_key)
    WHERE idempotency_key IS NOT NULL;

-- The request fingerprint. Same key with a DIFFERENT body is a client bug, not
-- a retry: without this, a caller who reuses a key for a new payment silently
-- receives the old transfer and never learns the new one did not happen.
ALTER TABLE transfers ADD COLUMN request_fingerprint text;
