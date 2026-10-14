-- Transfer creation inserts the transfer, already linked to its ledger
-- transaction, before that transaction exists: the idempotency key must be
-- claimed first and the balance lock taken last. The link is checked at commit.
ALTER TABLE transfers
    ALTER CONSTRAINT transfers_ledger_txn_id_fkey DEFERRABLE INITIALLY DEFERRED;
