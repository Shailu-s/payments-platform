-- Our cash leg toward the network: paying a day's net debits the payable and
-- credits this. A settlement account, so no balance row.
INSERT INTO accounts (id, currency, type)
VALUES ('acc_network_cash_usd', 'USD', 'settlement')
ON CONFLICT (id) DO NOTHING;

-- One row per day we paid the network. The primary key is the guarantee: a
-- second settlement of the same day finds this row and moves no money.
CREATE TABLE network_settlements (
    settlement_date date        PRIMARY KEY,
    purchases_total bigint      NOT NULL CHECK (purchases_total >= 0),
    refunds_total   bigint      NOT NULL CHECK (refunds_total >= 0),
    -- Negative: the network owes us.
    net_amount      bigint      NOT NULL CHECK (net_amount = purchases_total - refunds_total),
    record_count    integer     NOT NULL CHECK (record_count >= 0),
    -- NULL when the net is zero: nothing to pay, no ledger transaction.
    ledger_txn_id   text        NULL REFERENCES ledger_transactions (id) DEFERRABLE INITIALLY DEFERRED,
    created_at      timestamptz NOT NULL DEFAULT now()
);

-- One three-way reconciliation of a settlement date: the network's statement
-- as received and what our records said in one snapshot.
CREATE TABLE card_recon_runs (
    id                  text        PRIMARY KEY CHECK (length(btrim(id)) > 0 AND length(id) <= 255),
    settlement_date     date        NOT NULL,
    statement           bytea       NOT NULL,
    statement_sha256    text        NOT NULL CHECK (statement_sha256 ~ '^[0-9a-f]{64}$'),
    captured_at         timestamptz NOT NULL,
    hold_expiry_days    integer     NOT NULL CHECK (hold_expiry_days >= 1),
    record_count        integer     NOT NULL CHECK (record_count >= 0),
    authorization_count integer     NOT NULL CHECK (authorization_count >= 0),
    counts              jsonb       NOT NULL,
    preparation_ms      bigint      NOT NULL CHECK (preparation_ms >= 0),
    created_at          timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX card_recon_runs_created_idx ON card_recon_runs (created_at DESC, id DESC);

-- MATCHED is counted on the run, not stored: a clean day writes no findings.
CREATE TABLE card_recon_findings (
    run_id      text    NOT NULL REFERENCES card_recon_runs (id),
    ordinal     integer NOT NULL CHECK (ordinal >= 0),
    class       text    NOT NULL CHECK (class IN (
        'CLEARED_NOT_AUTHORIZED', 'AUTHORIZED_NOT_CLEARED', 'AMOUNT_MISMATCH', 'DUPLICATE_CLEARING',
        'LATE_CLEARING', 'REFUND_EXCEPTION', 'FUNDING_SHORTFALL', 'HOLD_AFTER_CLEARING',
        'SETTLEMENT_MISMATCH')),
    auth_id     text    NULL,
    clearing_id text    NULL,
    card_id     text    NULL,
    amount      bigint  NOT NULL,
    evidence    jsonb   NOT NULL,
    PRIMARY KEY (run_id, ordinal)
);

CREATE INDEX card_recon_findings_class_idx ON card_recon_findings (run_id, class, ordinal);

-- The V1 reconciliation trigger function: evidence is never edited or removed.
CREATE TRIGGER card_recon_runs_immutable BEFORE UPDATE OR DELETE ON card_recon_runs
    FOR EACH ROW EXECUTE FUNCTION reject_reconciliation_mutation();
CREATE TRIGGER card_recon_findings_immutable BEFORE UPDATE OR DELETE ON card_recon_findings
    FOR EACH ROW EXECUTE FUNCTION reject_reconciliation_mutation();

-- Reconciliation finds a cleared chain's incremental rows, and the holds that
-- expired on a date. Both indexes are partial: an initial authorization, which
-- is what the hot path inserts, enters neither.
CREATE INDEX card_authorizations_original_idx ON card_authorizations (original_auth_id)
    WHERE original_auth_id IS NOT NULL;
CREATE INDEX card_authorizations_expired_idx ON card_authorizations (expired_at)
    WHERE expired_at IS NOT NULL;
