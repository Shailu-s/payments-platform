-- held_amount is the money an authorization still holds; a release moves it to
-- released_amount under the row lock, so no hold can be released twice. An
-- incremental chain keeps its whole hold on the initial row.
ALTER TABLE card_authorizations
    ADD COLUMN held_amount     bigint      NOT NULL DEFAULT 0 CHECK (held_amount >= 0),
    ADD COLUMN released_amount bigint      NOT NULL DEFAULT 0 CHECK (released_amount >= 0),
    ADD COLUMN expired_at      timestamptz NULL;

UPDATE card_authorizations SET held_amount = approved_amount - hold_shortfall
WHERE decision = 'approved';

-- The expirer's queue: only holds that can still expire.
CREATE INDEX card_authorizations_open_holds_idx ON card_authorizations (created_at)
    WHERE held_amount > 0 AND expired_at IS NULL;

-- Reversals that arrived before their authorization, applied when it is approved.
CREATE TABLE pending_reversals (
    reversal_id text        PRIMARY KEY,
    auth_id     text        NOT NULL,
    amount      bigint      NOT NULL CHECK (amount > 0),
    received_at timestamptz NOT NULL DEFAULT now(),
    applied_at  timestamptz NULL
);

CREATE INDEX pending_reversals_auth_idx ON pending_reversals (auth_id) WHERE applied_at IS NULL;
