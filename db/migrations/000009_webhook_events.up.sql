-- Unique event IDs serialise duplicate deliveries; SELECT-then-INSERT would race.
CREATE TABLE webhook_events (
    id          text        PRIMARY KEY,
    event_id    text        NOT NULL UNIQUE,
    provider_ref text       NOT NULL,
    status      text        NOT NULL,
    payload     jsonb       NOT NULL,
    received_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX webhook_events_provider_ref_idx ON webhook_events (provider_ref);
