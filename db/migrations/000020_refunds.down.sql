DROP INDEX clearing_records_deferred_idx;
-- Fails while two dates share the same bytes: remove one of the files first.
ALTER TABLE clearing_files
    DROP CONSTRAINT clearing_files_date_sha256_key,
    ADD CONSTRAINT clearing_files_sha256_key UNIQUE (sha256);
ALTER TABLE clearing_records DROP COLUMN credited_amount;
ALTER TABLE card_authorizations DROP COLUMN refunded_amount;
