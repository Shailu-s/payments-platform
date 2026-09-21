-- Only removable while nothing references it. A ledger entry against this
-- account means real money passed through it, and the foreign key will refuse
-- rather than orphan the history.
DELETE FROM accounts WHERE id = 'acc_settlement_usd';
