-- What refund lines have given back against this authorization's cleared amount.
ALTER TABLE card_authorizations
    ADD COLUMN refunded_amount bigint NOT NULL DEFAULT 0 CHECK (refunded_amount >= 0);

-- What a refund line credited to the account.
ALTER TABLE clearing_records
    ADD COLUMN credited_amount bigint NOT NULL DEFAULT 0 CHECK (credited_amount >= 0);

-- The same bytes on different dates are different files: a header-only file
-- is byte-identical every day it is sent.
ALTER TABLE clearing_files
    DROP CONSTRAINT clearing_files_sha256_key,
    ADD CONSTRAINT clearing_files_date_sha256_key UNIQUE (file_date, sha256);

-- Refund lines imported before refunds were posted, for PostDeferred to find
-- without scanning every record. New imports defer nothing, so it stays small.
CREATE INDEX clearing_records_deferred_idx ON clearing_records (clearing_id) WHERE status = 'deferred';
