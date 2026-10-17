ALTER TABLE card_authorizations
    DROP COLUMN cleared_at,
    DROP COLUMN cleared_amount;
DROP TABLE clearing_records;
DROP TABLE clearing_files;
-- The foreign key prevents removal while ledger history references this account.
DELETE FROM accounts WHERE id = 'acc_network_payable_usd';
