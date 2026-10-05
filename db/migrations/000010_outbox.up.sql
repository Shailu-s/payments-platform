-- Commit events with their business change: Postgres and Kafka cannot commit atomically.
-- The relay retries publication from these durable rows.
CREATE TABLE outbox_events (
    id           text        PRIMARY KEY,
    aggregate_id text        NOT NULL,
    event_type   text        NOT NULL,
    topic        text        NOT NULL,
    payload      jsonb       NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now(),
    published_at timestamptz NULL
);

-- Index only the backlog; published history is retained.
CREATE INDEX outbox_events_unpublished_idx
    ON outbox_events (created_at)
    WHERE published_at IS NULL;
