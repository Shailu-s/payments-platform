-- Serialise same-key inserts across processes; a prior lookup cannot close the race.
-- Exclude transfers without client keys from the index.
CREATE UNIQUE INDEX transfers_idempotency_key_idx
    ON transfers (idempotency_key)
    WHERE idempotency_key IS NOT NULL;

-- Distinguish a retry from key reuse with different payment terms.
ALTER TABLE transfers ADD COLUMN request_fingerprint text;
