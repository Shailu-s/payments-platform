package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/Shailu-s/payments-platform/internal/ledger"
	"github.com/Shailu-s/payments-platform/internal/provider"
	"github.com/Shailu-s/payments-platform/internal/transfers"
	"github.com/jackc/pgx/v5"
)

// Settle credits the destination and marks the transfer settled.
//
// This is the moment phase 2 promised. Creating a transfer debited the source
// and credited SETTLEMENT, not the destination, because at that instant the
// money was ours and the destination had nothing — and the ledger is
// append-only, so recording otherwise would have made a permanent record of
// something untrue. This is the confirmation, and it moves the money the rest
// of the way with a SECOND ledger transaction. The first is never touched.
//
// One transfer, two ledger transactions. That is why transfers and
// ledger_transactions were separated in phase 2: one real-world payment
// produces several accounting facts over its life.
//
// Called by the webhook handler and by Resolve, which are the same event
// arriving through two different doors. Both must be safe to call repeatedly:
// the guard on status is what makes a redelivered webhook harmless, and that is
// guarantee 4.
func Settle(ctx context.Context, db DB, transferID string) error {
	tx, err := db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin settle %s: %w", transferID, err)
	}
	defer tx.Rollback(ctx)

	// Claim the transition. A transfer that is not processing has already been
	// settled, failed, or is parked — and in every one of those cases writing
	// another ledger transaction would move money a second time.
	const claim = `
		UPDATE transfers
		SET status = 'settled', updated_at = now()
		WHERE id = $1 AND status IN ('processing', 'unresolved')
		RETURNING source_account, destination_account, amount`

	var source, destination string
	var amount int64
	err = tx.QueryRow(ctx, claim, transferID).Scan(&source, &destination, &amount)
	if errors.Is(err, pgx.ErrNoRows) {
		// Already terminal. This is the duplicate-webhook path and it is not
		// an error: the caller should answer 2xx and move on.
		slog.InfoContext(ctx, "settle ignored, transfer is already terminal",
			"transfer_id", transferID)
		return nil
	}
	if err != nil {
		return fmt.Errorf("claim settle %s: %w", transferID, err)
	}

	// Settlement releases what it was holding; the destination receives it.
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

// Fail marks a transfer failed and returns the money to its source, without
// touching what was already written.
func Fail(ctx context.Context, db DB, transferID, reason string) error {
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

// settleFromLookup records the reference the lookup gave us, then settles. A
// transfer parked as unresolved may never have had a provider_ref stored.
func (w *Worker) settleFromLookup(ctx context.Context, t transfers.Transfer, p provider.Payment) error {
	const storeRef = `
		UPDATE transfers SET provider_ref = COALESCE(provider_ref, $1), updated_at = now()
		WHERE id = $2`
	if _, err := w.db.Exec(ctx, storeRef, p.ProviderRef, t.ID); err != nil {
		return fmt.Errorf("store provider ref for %s: %w", t.ID, err)
	}

	slog.InfoContext(ctx, "unresolved transfer settled by lookup",
		"transfer_id", t.ID, "provider_ref", p.ProviderRef)
	return Settle(ctx, w.db, t.ID)
}
