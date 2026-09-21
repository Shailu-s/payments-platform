-- The platform settlement account: money accepted from a caller but not yet
-- confirmed as delivered by the provider.
--
-- One fixed row, created by migration rather than per request. At commit time
-- the money is ours and earmarked, so every transfer credits this same account;
-- the destination is credited by a second ledger transaction in phase 4 when
-- the provider confirms. Reconciliation in phase 7 compares against this
-- account, which needs it to be stable and singular.
INSERT INTO accounts (id, currency, type)
VALUES ('acc_settlement_usd', 'USD', 'settlement')
ON CONFLICT (id) DO NOTHING;
