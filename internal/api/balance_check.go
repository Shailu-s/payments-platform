package api

import (
	"context"
	"errors"
	"fmt"

	"github.com/Shailu-s/payments-platform/internal/ledger"
	"github.com/jackc/pgx/v5"
)

// Read the balance under the source account lock in the spending transaction.
// At Read Committed, the next writer waits and then sees the committed debit.
func (s *Server) checkBalance(ctx context.Context, tx pgx.Tx, accountID string, amount int64) error {
	// Lock the account: future ledger entries do not exist yet and cannot be locked.
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
