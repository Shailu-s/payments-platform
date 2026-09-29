-- Events owed to Kafka, written in the same transaction as the change they
-- describe.
--
-- The constraint: a Postgres commit and a Kafka publish are two systems and
-- cannot commit together, so publishing from the request handler loses the
-- event whenever the process dies between the two. Writing the event here
-- instead makes it part of the transfer's own transaction: both rows exist or
-- neither does. A relay publishes from this table afterwards and can retry
-- forever, because what it is retrying from is durable.
--
-- The row is generic on purpose. The event body is jsonb, so a new event type
-- is a new payload shape, not a migration.
CREATE TABLE outbox_events (
    id           text        PRIMARY KEY,
    aggregate_id text        NOT NULL,
    event_type   text        NOT NULL,
    topic        text        NOT NULL,
    payload      jsonb       NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now(),
    published_at timestamptz NULL
);

-- Rows are kept after publishing, so the table only grows. The relay asks for
-- the oldest unpublished rows, and a partial index holds only those: it stays
-- as small as the backlog no matter how large the history gets.
CREATE INDEX outbox_events_unpublished_idx
    ON outbox_events (created_at)
    WHERE published_at IS NULL;
