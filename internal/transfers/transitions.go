package transfers

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/Shailu-s/payments-platform/internal/ledger"
	"github.com/jackc/pgx/v5"
)

// SettlementAccountID is where money waits between leaving the source and
// reaching the destination. Created by migration 000003.
const SettlementAccountID = "acc_settlement_usd"

// Beginner lets status changes and ledger writes share a transaction.
type Beginner interface {
	Begin(ctx context.Context) (pgx.Tx, error)
}

// Settle credits the destination with a new ledger transaction and marks the
// transfer settled atomically. The status guard makes repeated calls harmless.
func Settle(ctx context.Context, db Beginner, transferID string) error {
	tx, err := db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin settle %s: %w", transferID, err)
	}
	defer tx.Rollback(ctx)

	// Only processing or unresolved transfers may produce a terminal ledger effect.
	const claim = `
		UPDATE transfers
		SET status = 'settled', updated_at = now()
		WHERE id = $1 AND status IN ('processing', 'unresolved')
		RETURNING source_account, destination_account, amount`

	var source, destination string
	var amount int64
	err = tx.QueryRow(ctx, claim, transferID).Scan(&source, &destination, &amount)
	if errors.Is(err, pgx.ErrNoRows) {
		// No eligible transition, including duplicates; acknowledge without another effect.
		slog.InfoContext(ctx, "settle ignored, transfer is already terminal",
			"transfer_id", transferID)
		return nil
	}
	if err != nil {
		return fmt.Errorf("claim settle %s: %w", transferID, err)
	}

	if _, err := ledger.Record(ctx, tx, "settlement "+transferID, []ledger.Entry{
		{AccountID: SettlementAccountID, Direction: ledger.DirectionDebit, Amount: amount},
		{AccountID: destination, Direction: ledger.DirectionCredit, Amount: amount},
	}); err != nil {
		return fmt.Errorf("settle %s: %w", transferID, err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit settle %s: %w", transferID, err)
	}

	slog.InfoContext(ctx, "transfer settled, destination credited",
		"transfer_id", transferID, "destination", destination, "amount", amount)
	return nil
}

// Fail atomically marks failure and refunds the source with new ledger entries.
func Fail(ctx context.Context, db Beginner, transferID, reason string) error {
	tx, err := db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin fail %s: %w", transferID, err)
	}
	defer tx.Rollback(ctx)

	const claim = `
		UPDATE transfers
		SET status = 'failed', last_error = $2, updated_at = now()
		WHERE id = $1 AND status IN ('processing', 'unresolved')
		RETURNING source_account, amount`

	var source string
	var amount int64
	err = tx.QueryRow(ctx, claim, transferID, truncate(reason, 500)).Scan(&source, &amount)
	if errors.Is(err, pgx.ErrNoRows) {
		slog.InfoContext(ctx, "fail ignored, transfer is already terminal",
			"transfer_id", transferID)
		return nil
	}
	if err != nil {
		return fmt.Errorf("claim fail %s: %w", transferID, err)
	}

	if _, err := ledger.Record(ctx, tx, "reversal "+transferID, []ledger.Entry{
		{AccountID: SettlementAccountID, Direction: ledger.DirectionDebit, Amount: amount},
		{AccountID: source, Direction: ledger.DirectionCredit, Amount: amount},
	}); err != nil {
		return fmt.Errorf("reverse %s: %w", transferID, err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit fail %s: %w", transferID, err)
	}

	slog.InfoContext(ctx, "transfer failed, money returned to source",
		"transfer_id", transferID, "source", source, "amount", amount, "reason", reason)
	return nil
}

func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max]
}
