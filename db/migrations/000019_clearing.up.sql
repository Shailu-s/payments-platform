-- What we owe the card network for cleared purchases. A settlement account, so
-- no balance row: every clearing line credits it, and a row would serialise them.
INSERT INTO accounts (id, currency, type)
VALUES ('acc_network_payable_usd', 'USD', 'settlement')
ON CONFLICT (id) DO NOTHING;

-- Each clearing file as received. The hash is the deduplication key for the
-- file: the same bytes imported twice are one file.
CREATE TABLE clearing_files (
    id          text        PRIMARY KEY,
    file_date   date        NOT NULL,
    sha256      text        NOT NULL UNIQUE CHECK (sha256 ~ '^[0-9a-f]{64}$'),
    raw         bytea       NOT NULL,
    line_count  integer     NOT NULL CHECK (line_count >= 0),
    imported_at timestamptz NOT NULL DEFAULT now()
);

-- One row per network clearing line. clearing_id is the deduplication key for
-- lines, across files and reruns: a line is posted at most once.
CREATE TABLE clearing_records (
    clearing_id            text        PRIMARY KEY,
    file_id                text        NOT NULL REFERENCES clearing_files (id),
    auth_id                text        NULL,
    card_id                text        NOT NULL,
    kind                   text        NOT NULL CHECK (kind IN ('purchase', 'refund')),
    amount                 bigint      NOT NULL CHECK (amount > 0),
    currency               text        NOT NULL,
    merchant_id            text        NOT NULL,
    cleared_at             timestamptz NOT NULL,
    status                 text        NOT NULL CHECK (status IN ('posted', 'deferred', 'rejected')),
    charged_from_held      bigint      NOT NULL DEFAULT 0 CHECK (charged_from_held >= 0),
    charged_from_available bigint      NOT NULL DEFAULT 0 CHECK (charged_from_available >= 0),
    released_amount        bigint      NOT NULL DEFAULT 0 CHECK (released_amount >= 0),
    -- What the network cleared but the account could not pay: exposure, never an overdraft.
    shortfall              bigint      NOT NULL DEFAULT 0 CHECK (shortfall >= 0),
    -- Comma-separated reasons a person should look at this line.
    flag                   text        NULL,
    -- Chosen before the ledger write, which comes last in the transaction.
    ledger_txn_id          text        NULL REFERENCES ledger_transactions (id) DEFERRABLE INITIALLY DEFERRED,
    created_at             timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX clearing_records_auth_idx ON clearing_records (auth_id);
CREATE INDEX clearing_records_file_idx ON clearing_records (file_id, cleared_at, clearing_id);
CREATE INDEX clearing_records_flag_idx ON clearing_records (flag) WHERE flag IS NOT NULL;

ALTER TABLE card_authorizations
    ADD COLUMN cleared_amount bigint      NOT NULL DEFAULT 0 CHECK (cleared_amount >= 0),
    ADD COLUMN cleared_at     timestamptz NULL;
