CREATE TABLE customer_webhook_deliveries (
    event_id         text PRIMARY KEY,
    endpoint         text NOT NULL,
    payload          jsonb NOT NULL,
    status           text NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'delivered', 'dead')),
    attempt_count    integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    next_attempt_at  timestamptz NOT NULL DEFAULT now(),
    last_http_status integer,
    last_error       text,
    delivered_at     timestamptz,
    created_at       timestamptz NOT NULL DEFAULT now(),
    updated_at       timestamptz NOT NULL DEFAULT now(),
    CHECK ((status = 'delivered') = (delivered_at IS NOT NULL))
);

CREATE INDEX customer_webhook_deliveries_due
    ON customer_webhook_deliveries (next_attempt_at, created_at)
    WHERE status = 'pending';
