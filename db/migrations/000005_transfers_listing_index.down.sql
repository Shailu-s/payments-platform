CREATE INDEX IF NOT EXISTS transfers_created_at_idx ON transfers (created_at);
DROP INDEX IF EXISTS transfers_created_at_id_idx;
