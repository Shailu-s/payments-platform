package ledger

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// Querier lets Balance share the spending transaction and its account lock.
type Querier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Balance derives a balance from signed entries; it does not check account existence.
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
