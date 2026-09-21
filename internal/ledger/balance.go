package ledger

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// Querier is anything that can run a query: a pool, a connection, or a transaction.
// Balance must be callable inside a transaction — Phase 3 reads a balance while holding
// a row lock, and a read on a different connection sees a different snapshot — so this
// cannot take a *pgxpool.Pool.
type Querier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Balance derives an account's balance from its entries. There is no stored balance
// column: a stored number can disagree with the entries, a derived one cannot.
// Amounts are always positive and direction carries the sign.
func Balance(ctx context.Context, db Querier, accountID string) (int64, error) {
	// COALESCE because SUM over zero rows is NULL, not 0 — and an account that exists
	// but has never been used is a normal state that must return 0, not an error.
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
