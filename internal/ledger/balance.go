package ledger

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// Querier lets balance reads share the caller's transaction.
type Querier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Balance derives a balance from signed entries; it does not check account existence.
// It is the source of truth that stored balances are verified against.
func Balance(ctx context.Context, db Querier, accountID string) (int64, error) {
	// SUM over no entries is NULL; an unused account must return zero.
	const q = `
		SELECT COALESCE(SUM(CASE WHEN direction = 'credit' THEN amount ELSE -amount END), 0)
		FROM ledger_entries
		WHERE account_id = $1`

	var balance int64
	if err := db.QueryRow(ctx, q, accountID).Scan(&balance); err != nil {
		return 0, fmt.Errorf("derive balance for %s: %w", accountID, err)
	}
	return balance, nil
}

// StoredBalance reads the maintained balance. tracked is false for accounts
// without a row (settlement accounts), whose balance is only derivable.
func StoredBalance(ctx context.Context, db Querier, accountID string) (balance int64, tracked bool, err error) {
	const q = `SELECT balance FROM account_balances WHERE account_id = $1`
	err = db.QueryRow(ctx, q, accountID).Scan(&balance)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("stored balance for %s: %w", accountID, err)
	}
	return balance, true, nil
}

// Drift is an account whose stored balance disagrees with its entries.
type Drift struct {
	AccountID string
	Stored    int64
	Derived   int64
	// Missing means a non-settlement account has no balance row, so nothing
	// guards it against overspending.
	Missing bool
}

// RowsQuerier runs multi-row queries.
type RowsQuerier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// VerifyBalances compares every stored balance with the sum of its entries in
// one pass and returns the accounts that disagree.
func VerifyBalances(ctx context.Context, db RowsQuerier) ([]Drift, error) {
	const q = `
		SELECT a.id, COALESCE(b.balance, 0), COALESCE(d.derived, 0), b.account_id IS NULL
		FROM accounts a
		LEFT JOIN account_balances b ON b.account_id = a.id
		LEFT JOIN (
			SELECT account_id, SUM(CASE WHEN direction = 'credit' THEN amount ELSE -amount END) AS derived
			FROM ledger_entries
			GROUP BY account_id
		) d ON d.account_id = a.id
		WHERE a.type <> 'settlement'
		  AND (b.account_id IS NULL OR b.balance <> COALESCE(d.derived, 0))
		ORDER BY a.id`

	rows, err := db.Query(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("verify balances: %w", err)
	}
	drifts, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (Drift, error) {
		var d Drift
		err := row.Scan(&d.AccountID, &d.Stored, &d.Derived, &d.Missing)
		return d, err
	})
	if err != nil {
		return nil, fmt.Errorf("verify balances: %w", err)
	}
	return drifts, nil
}
