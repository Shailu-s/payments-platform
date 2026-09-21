-- Phase 1: the ledger. Three tables, no balance column anywhere.

CREATE TABLE accounts (
    id         text        PRIMARY KEY,
    currency   text        NOT NULL,
    type       text        NOT NULL CHECK (type IN ('asset', 'liability', 'settlement')),
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE ledger_transactions (
    id         text        PRIMARY KEY,
    reference  text        NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

-- APPEND ONLY. No UPDATE, no DELETE, ever.
CREATE TABLE ledger_entries (
    id         text        PRIMARY KEY,
    txn_id     text        NOT NULL REFERENCES ledger_transactions (id),
    account_id text        NOT NULL REFERENCES accounts (id),
    direction  text        NOT NULL CHECK (direction IN ('debit', 'credit')),
    -- minor units. BIGINT, never numeric or float: $10.50 is 1050.
    -- always positive: direction carries the sign.
    amount     bigint      NOT NULL CHECK (amount > 0),
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX ledger_entries_account_id_idx ON ledger_entries (account_id);
CREATE INDEX ledger_entries_txn_id_idx     ON ledger_entries (txn_id);
