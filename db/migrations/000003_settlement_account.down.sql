-- The foreign key prevents removal while ledger history references this account.
DELETE FROM accounts WHERE id = 'acc_settlement_usd';
