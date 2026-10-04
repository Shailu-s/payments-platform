-- The hash, never the key. A stolen database dump must not yield working keys.
CREATE TABLE api_keys (
    id         text        PRIMARY KEY,
    key_hash   text        NOT NULL UNIQUE,
    -- pk_live_ plus eight random characters: identifiable, not a working key.
    prefix     text        NOT NULL,
    name       text        NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    -- Timestamp revocation preserves the key referenced by existing transfers.
    revoked_at timestamptz
);

CREATE INDEX api_keys_prefix_idx ON api_keys (prefix);

-- Business state is separate from immutable accounting: one transfer has several movements.
CREATE TABLE transfers (
    id                  text        PRIMARY KEY,
    source_account      text        NOT NULL REFERENCES accounts (id),
    destination_account text        NOT NULL REFERENCES accounts (id),
    amount              bigint      NOT NULL CHECK (amount > 0),
    currency            text        NOT NULL,
    status              text        NOT NULL CHECK (status IN ('created', 'processing', 'settled', 'failed')),
    -- Linked before the creation transaction commits; only the initial movement.
    ledger_txn_id       text        REFERENCES ledger_transactions (id),
    -- Retain the identity that authorised this instruction.
    api_key_id          text        NOT NULL REFERENCES api_keys (id),
    created_at          timestamptz NOT NULL DEFAULT now(),
    updated_at          timestamptz NOT NULL DEFAULT now(),

    -- Double-entry balancing alone would not reject a self-payment.
    CONSTRAINT transfers_distinct_accounts CHECK (source_account <> destination_account)
);

CREATE INDEX transfers_status_idx     ON transfers (status);
CREATE INDEX transfers_created_at_idx ON transfers (created_at);
