-- One stable account holds reserved funds until provider confirmation.
-- The destination is credited by a later settlement transaction.
INSERT INTO accounts (id, currency, type)
VALUES ('acc_settlement_usd', 'USD', 'settlement')
ON CONFLICT (id) DO NOTHING;
