-- One row per network auth_id: the primary key is the deduplication key, so a
-- retried or duplicated authorization can only ever have one answer and one hold.
-- No foreign key on card_id: a tap on an unknown card is declined and stored too.
CREATE TABLE card_authorizations (
    auth_id             text        PRIMARY KEY,
    card_id             text        NOT NULL,
    account_id          text        NULL,
    held_account_id     text        NULL,
    amount              bigint      NOT NULL CHECK (amount > 0),
    currency            text        NOT NULL,
    merchant_id         text        NOT NULL,
    merchant_name       text        NOT NULL,
    mcc                 text        NOT NULL,
    kind                text        NOT NULL CHECK (kind IN ('initial', 'incremental')),
    original_auth_id    text        NULL,
    decision            text        NOT NULL CHECK (decision IN ('approved', 'declined')),
    approved_amount     bigint      NOT NULL CHECK (approved_amount >= 0),
    decline_reason      text        NULL,
    -- The network approved it on our behalf (an advice); the merchant was told yes.
    stand_in            boolean     NOT NULL DEFAULT false,
    -- Approved by stand-in but not fully held: credit exposure for reconciliation.
    hold_shortfall      bigint      NOT NULL DEFAULT 0 CHECK (hold_shortfall >= 0),
    -- Chosen before the ledger write, which comes last in the transaction.
    ledger_txn_id       text        NULL REFERENCES ledger_transactions (id) DEFERRABLE INITIALLY DEFERRED,
    request_fingerprint text        NOT NULL,
    created_at          timestamptz NOT NULL DEFAULT now(),
    updated_at          timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX card_authorizations_card_idx ON card_authorizations (card_id, created_at);

-- The daily limit as a counter row checked and incremented in one statement,
-- the same idea as the balance CHECK: no read-then-write window.
CREATE TABLE card_daily_spend (
    card_id text   NOT NULL REFERENCES cards (id),
    day     date   NOT NULL,
    amount  bigint NOT NULL CHECK (amount >= 0),
    PRIMARY KEY (card_id, day)
);

-- Advice and reversal ids already applied: the insert is the deduplication.
CREATE TABLE network_messages (
    message_id  text        PRIMARY KEY,
    kind        text        NOT NULL CHECK (kind IN ('advice', 'reversal')),
    auth_id     text        NOT NULL,
    received_at timestamptz NOT NULL DEFAULT now()
);

ALTER TABLE cards ADD COLUMN blocked_mccs text[] NOT NULL DEFAULT '{}';
