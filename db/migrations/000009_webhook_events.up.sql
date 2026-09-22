-- Received provider events, recorded so a redelivery can be recognised.
--
-- The contract says webhook delivery is at least once: the same event_id may
-- arrive any number of times. A receiver that is not idempotent double-counts
-- money, which is guarantee 4.
--
-- The UNIQUE constraint on event_id is the mechanism, exactly as in phase 3:
-- two concurrent deliveries of one event cannot both insert, because the second
-- blocks on the first's index entry and then fails. Application code cannot
-- produce that, and a SELECT-then-INSERT check here would be the phase 3 bug
-- for the third time.
CREATE TABLE webhook_events (
    id          text        PRIMARY KEY,
    event_id    text        NOT NULL UNIQUE,
    provider_ref text       NOT NULL,
    status      text        NOT NULL,
    payload     jsonb       NOT NULL,
    received_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX webhook_events_provider_ref_idx ON webhook_events (provider_ref);
