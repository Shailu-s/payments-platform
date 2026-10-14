ALTER TABLE transfers
    ALTER CONSTRAINT transfers_ledger_txn_id_fkey NOT DEFERRABLE;
