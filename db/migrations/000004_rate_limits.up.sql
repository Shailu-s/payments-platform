-- Fixed-window rate limiting, one row per key per window.
--
-- The constraint: an in-memory counter is wrong the instant there are two API
-- instances, because each process allows the full quota and the real limit is
-- silently double what was configured. The limit has to live in shared state,
-- and the only shared state in V1 is Postgres.
CREATE TABLE rate_limits (
    api_key_id   text        NOT NULL REFERENCES api_keys (id) ON DELETE CASCADE,
    window_start timestamptz NOT NULL,
    count        integer     NOT NULL DEFAULT 0 CHECK (count >= 0),

    -- The composite key is the whole mechanism. ON CONFLICT on this constraint
    -- is what makes increment-and-read a single atomic statement; without it
    -- two concurrent requests both read the same count and both write back the
    -- same increment, and the limit silently allows more than it should.
    -- This is phase 3's idempotency race in a different costume.
    PRIMARY KEY (api_key_id, window_start)
);

-- Stale windows are deleted by window_start, so the sweep is an index scan
-- rather than a full table scan.
CREATE INDEX rate_limits_window_start_idx ON rate_limits (window_start);
