-- The spendable balance as one row per account, so a spend reads and locks O(1)
-- state instead of summing all history. Entries remain the source of truth;
-- ledger.Record is the only writer and ledger.VerifyBalances checks for drift.
CREATE TABLE account_balances (
    account_id text   PRIMARY KEY REFERENCES accounts (id),
    balance    bigint NOT NULL DEFAULT 0,
    -- The overspend guard: a debit that would go below zero fails in the database.
    CONSTRAINT account_balances_non_negative CHECK (balance >= 0)
);

-- Settlement accounts get no row: every payment touches them, so a stored
-- balance would serialise the whole system on one row.
CREATE FUNCTION create_account_balance() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.type <> 'settlement' THEN
        INSERT INTO account_balances (account_id) VALUES (NEW.id);
    END IF;
    RETURN NULL;
END;
$$;

CREATE TRIGGER accounts_create_balance AFTER INSERT ON accounts
    FOR EACH ROW EXECUTE FUNCTION create_account_balance();

-- An account already overdrawn violates the CHECK and fails this migration;
-- it must be investigated, not clamped to zero.
INSERT INTO account_balances (account_id, balance)
SELECT a.id, COALESCE(SUM(CASE WHEN e.direction = 'credit' THEN e.amount ELSE -e.amount END), 0)
FROM accounts a
LEFT JOIN ledger_entries e ON e.account_id = a.id
WHERE a.type <> 'settlement'
GROUP BY a.id;
