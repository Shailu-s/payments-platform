CREATE TABLE reconciliation_runs (
    id text PRIMARY KEY CHECK (length(btrim(id)) > 0 AND length(id) <= 255),
    report_sha256 text NOT NULL CHECK (report_sha256 ~ '^[0-9a-f]{64}$'),
    provider_captured_at timestamptz NOT NULL,
    internal_captured_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    internal_rows integer NOT NULL CHECK (internal_rows >= 0),
    external_rows integer NOT NULL CHECK (external_rows >= 0),
    counts jsonb NOT NULL,
    preparation_ms bigint NOT NULL CHECK (preparation_ms >= 0),
    provider_report bytea NOT NULL,
    internal_snapshot jsonb NOT NULL
);

CREATE INDEX reconciliation_runs_created_idx ON reconciliation_runs (created_at DESC, id DESC);

CREATE TABLE reconciliation_findings (
    run_id text NOT NULL REFERENCES reconciliation_runs (id),
    ordinal integer NOT NULL CHECK (ordinal >= 0),
    classification text NOT NULL CHECK (classification IN
        ('MATCHED', 'AMOUNT_MISMATCH', 'STATUS_MISMATCH', 'MISSING_EXTERNAL', 'MISSING_INTERNAL', 'DUPLICATE_EXTERNAL')),
    client_reference text NOT NULL,
    provider_ref text NOT NULL,
    evidence jsonb NOT NULL,
    PRIMARY KEY (run_id, ordinal)
);

CREATE INDEX reconciliation_findings_class_idx ON reconciliation_findings (run_id, classification, ordinal);
CREATE INDEX reconciliation_findings_client_idx ON reconciliation_findings (run_id, client_reference, ordinal);

CREATE FUNCTION reject_reconciliation_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'reconciliation evidence is append-only';
END;
$$;

CREATE TRIGGER reconciliation_runs_immutable BEFORE UPDATE OR DELETE ON reconciliation_runs
    FOR EACH ROW EXECUTE FUNCTION reject_reconciliation_mutation();
CREATE TRIGGER reconciliation_findings_immutable BEFORE UPDATE OR DELETE ON reconciliation_findings
    FOR EACH ROW EXECUTE FUNCTION reject_reconciliation_mutation();
