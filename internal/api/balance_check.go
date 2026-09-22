package api

import (
	"context"
	"errors"
	"fmt"

	"github.com/Shailu-s/payments-platform/internal/ledger"
	"github.com/jackc/pgx/v5"
)

// checkBalance refuses the transfer unless the source account can cover it.
//
// It runs INSIDE the caller's transaction, not in the handler before it. A
// check before BEGIN reads a balance that a concurrent transaction is about to
// invalidate, which is the bug this replaces. This is also why ledger.Balance
// takes a Querier rather than a pool — phase 1's interface choice, made before
// it was needed.
//
// The row lock is what makes it correct. Without it, two concurrent transfers
// both read $1,000, both conclude $700 is affordable, and the account ends at
// -$400. With it, the second transaction waits until the first commits and then
// reads a balance that already includes it.
//
// SERIALIZABLE was implemented and measured as the alternative and lost: on a
// hot account it dropped 27% of valid payments to exhausted retries. The
// numbers and the reasoning are in PLAN.md, phase 3; TestLockStrategyComparison
// re-runs the comparison. Note that this transaction runs at Read Committed, so
// nothing raises 40001 and no retry loop is needed — raising the isolation
// level later would mean reintroducing one.
func (s *Server) checkBalance(ctx context.Context, tx pgx.Tx, accountID string, amount int64) error {
	// Lock the ACCOUNTS row, not the ledger entries. You cannot lock rows that
	// do not exist yet, and the entries about to be written are exactly those
	// rows; the account is the thing being contended.
	const lock = `SELECT id FROM accounts WHERE id = $1 FOR UPDATE`
	var locked string
	if err := tx.QueryRow(ctx, lock, accountID).Scan(&locked); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("lock account %s: %w", accountID, pgx.ErrNoRows)
		}
		return fmt.Errorf("lock account %s: %w", accountID, err)
	}

	balance, err := ledger.Balance(ctx, tx, accountID)
	if err != nil {
		return err
	}
	if balance < amount {
		return ErrInsufficientFunds
	}
	return nil
}
