-- One held account per funding account, shared by all its cards. A card hold
-- moves money from the funding account's balance to this one, so "available"
-- and "held" are both ordinary stored balances.
CREATE TABLE card_accounts (
    account_id      text        PRIMARY KEY REFERENCES accounts (id),
    held_account_id text        NOT NULL UNIQUE REFERENCES accounts (id),
    created_at      timestamptz NOT NULL DEFAULT now()
);

-- A card is a handle on a card account, never a card number: no PAN, CVV or
-- PIN is stored, so this system stays out of PCI scope.
CREATE TABLE cards (
    id                    text        PRIMARY KEY,
    account_id            text        NOT NULL REFERENCES card_accounts (account_id),
    -- Cosmetic, random, and not derived from anything.
    last4                 text        NOT NULL CHECK (last4 ~ '^[0-9]{4}$'),
    status                text        NOT NULL CHECK (status IN ('active', 'frozen', 'closed')),
    per_transaction_limit bigint      NOT NULL CHECK (per_transaction_limit > 0),
    daily_limit           bigint      NOT NULL,
    api_key_id            text        NOT NULL REFERENCES api_keys (id),
    idempotency_key       text        NOT NULL,
    request_fingerprint   text        NOT NULL,
    created_at            timestamptz NOT NULL DEFAULT now(),
    updated_at            timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT cards_daily_covers_transaction CHECK (daily_limit >= per_transaction_limit)
);

-- The idempotency guarantee: concurrent issues with one key cannot both commit.
CREATE UNIQUE INDEX cards_idempotency_key_idx ON cards (idempotency_key);
CREATE INDEX cards_account_id_idx ON cards (account_id);
