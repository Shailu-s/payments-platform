-- Match the keyset cursor's tuple and ordering, including timestamp ties.
CREATE INDEX transfers_created_at_id_idx ON transfers (created_at DESC, id DESC);

-- Superseded: every query that used it is served better by the composite above.
DROP INDEX IF EXISTS transfers_created_at_idx;
