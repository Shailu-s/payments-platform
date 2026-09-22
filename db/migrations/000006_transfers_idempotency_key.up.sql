-- Phase 3.1: the column, with NO constraint on it yet.
--
-- The UNIQUE index is deliberately absent. This migration exists so the broken
-- implementation can be written and watched failing first: SELECT by key, then
-- INSERT if nothing came back. That reads correct and is wrong under
-- concurrency, and the failure is the deliverable.
--
-- The constraint arrives in 3.2, which is the fix.
ALTER TABLE transfers ADD COLUMN idempotency_key text;
