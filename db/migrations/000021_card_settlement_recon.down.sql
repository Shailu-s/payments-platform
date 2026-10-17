DROP INDEX card_authorizations_expired_idx;
DROP INDEX card_authorizations_original_idx;
DROP TABLE card_recon_findings;
DROP TABLE card_recon_runs;
DROP TABLE network_settlements;
-- The foreign key prevents removal while ledger history references this account.
DELETE FROM accounts WHERE id = 'acc_network_cash_usd';
