-- Phase 2: the ledger becomes a product other people can call.

-- The hash, never the key. A stolen database dump must not yield working keys.
CREATE TABLE api_keys (
    id         text        PRIMARY KEY,
    key_hash   text        NOT NULL UNIQUE,
    -- First 8 characters of the plaintext, so a key is identifiable in a log
    -- line or revocable from a dashboard without the secret being stored.
    prefix     text        NOT NULL,
    name       text        NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    -- NULL means live. Revocation is a timestamp rather than a DELETE so the
    -- audit trail survives: transfers reference the key that authorised them.
    revoked_at timestamptz
);

CREATE INDEX api_keys_prefix_idx ON api_keys (prefix);

-- The business intent, sitting on top of the ledger rather than replacing it.
-- The ledger records the accounting fact; this records what a caller asked for.
-- One transfer produces several ledger transactions over its life, so
-- collapsing the two would mean either editing ledger rows or losing the link
-- between a movement and the transfer it belonged to.
CREATE TABLE transfers (
    id                  text        PRIMARY KEY,
    source_account      text        NOT NULL REFERENCES accounts (id),
    destination_account text        NOT NULL REFERENCES accounts (id),
    amount              bigint      NOT NULL CHECK (amount > 0),
    currency            text        NOT NULL,
    -- Mutable, and that is coherent: this is business state, not a ledger row.
    -- The append-only rule applies to ledger_entries.
    status              text        NOT NULL CHECK (status IN ('created', 'processing', 'settled', 'failed')),
    -- Nullable: a transfer exists before its movement is recorded, and later
    -- gains further ledger transactions this column does not try to hold.
    ledger_txn_id       text        REFERENCES ledger_transactions (id),
    -- Who asked. Guarantee 5: every financial state change has an audit trail.
    api_key_id          text        NOT NULL REFERENCES api_keys (id),
    created_at          timestamptz NOT NULL DEFAULT now(),
    updated_at          timestamptz NOT NULL DEFAULT now(),

    -- A transfer must move money between two different accounts. Without this
    -- an account can pay itself: debit and credit cancel, the ledger still
    -- balances, and the invariant never notices.
    CONSTRAINT transfers_distinct_accounts CHECK (source_account <> destination_account)
);

CREATE INDEX transfers_status_idx     ON transfers (status);      -- the worker polls by status in phase 4
CREATE INDEX transfers_created_at_idx ON transfers (created_at);  -- listing is newest-first
