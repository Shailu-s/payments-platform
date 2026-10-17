package clearing

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// NetworkPayableAccountID is what we owe the network for cleared purchases.
const NetworkPayableAccountID = "acc_network_payable_usd"

const EventPosted = "card.clearing.posted"

var errPurchaseNotPosted = errors.New("purchase lines are not posted yet")

// postLine records one line in one transaction, and reports whether it
// recorded it: false means the clearing_id was already recorded.
//
// The record is inserted first: its primary key makes a second import of the
// same line wait for the first and then skip, so no line is posted twice.
func postLine(ctx context.Context, db DB, fileID string, l Line, cfg Config) (bool, error) {
	tx, err := db.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback(ctx)

	status := StatusPosted
	if l.Kind == KindRefund {
		// Phase 14 posts refunds; until then they are kept, not applied.
		status = StatusDeferred
	}
	var authID *string
	if l.AuthID != "" {
		authID = &l.AuthID
	}
	batch := &pgx.Batch{}
	// A tap holding an account's balance lock delays the line; bounded.
	batch.Queue(`SET LOCAL lock_timeout = '1s'`)
	var claimed bool
	batch.Queue(`
		INSERT INTO clearing_records (clearing_id, file_id, auth_id, card_id, kind, amount,
			currency, merchant_id, cleared_at, status)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		ON CONFLICT (clearing_id) DO NOTHING`,
		l.ClearingID, fileID, authID, l.CardID, l.Kind, l.Amount, l.Currency, l.MerchantID, l.ClearedAt, status,
	).Exec(func(tag pgconn.CommandTag) error {
		claimed = tag.RowsAffected() == 1
		return nil
	})
	if err := tx.SendBatch(ctx, batch).Close(); err != nil {
		return false, fmt.Errorf("record: %w", err)
	}
	if !claimed {
		return false, nil
	}
	if l.Kind != KindRefund {
		return false, errPurchaseNotPosted
	}
	return true, commit(ctx, tx)
}

func commit(ctx context.Context, tx pgx.Tx) error {
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}
