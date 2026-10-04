-- Shared counters keep the quota consistent across API instances.
CREATE TABLE rate_limits (
    api_key_id   text        NOT NULL REFERENCES api_keys (id) ON DELETE CASCADE,
    window_start timestamptz NOT NULL,
    count        integer     NOT NULL DEFAULT 0 CHECK (count >= 0),

    -- ON CONFLICT serialises increments for the same key and window.
    PRIMARY KEY (api_key_id, window_start)
);

-- Supports expiry sweeps without scanning every key.
CREATE INDEX rate_limits_window_start_idx ON rate_limits (window_start);
