package clearing

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/Shailu-s/payments-platform/internal/authorizations"
	"github.com/Shailu-s/payments-platform/internal/cards"
	"github.com/Shailu-s/payments-platform/internal/ledger"
	"github.com/Shailu-s/payments-platform/internal/outbox"
	"github.com/jackc/pgx/v5"
)

// RefundEvent is the outbox record of a posted refund line.
type RefundEvent struct {
	ClearingID string  `json:"clearing_id"`
	AuthID     *string `json:"auth_id"`
	CardID     string  `json:"card_id"`
	AccountID  string  `json:"account_id"`
	Amount     int64   `json:"amount"`
	Credited   int64   `json:"credited"`
	Flag       *string `json:"flag"`
}

// postRefund credits a refund line to its card's account as a new ledger
// transaction; the purchase it refunds is never edited. h is the locked
// authorization the line names, nil if none.
//
// The network has already sent the money, so a refund is credited whenever
// the card is ours: one with no cleared purchase to match, or one taking
// refunds past what the purchase cleared, is flagged rather than refused.
// Neither the holds nor the daily spend change: a refund is not a release.
func postRefund(ctx context.Context, tx pgx.Tx, l Line, h *hold) error {
	card, err := cards.Get(ctx, tx, l.CardID)
	if errors.Is(err, cards.ErrNotFound) {
		if _, err := tx.Exec(ctx, `UPDATE clearing_records SET status = $2, flag = $3 WHERE clearing_id = $1`,
			l.ClearingID, StatusRejected, FlagUnknownCard); err != nil {
			return fmt.Errorf("reject %s: %w", l.ClearingID, err)
		}
		return nil
	}
	if err != nil {
		return err
	}

	var flags []string
	matched := h != nil && h.cleared > 0
	switch {
	case !matched:
		flags = append(flags, FlagUnmatchedRefund)
	case l.Amount > h.cleared-h.refunded:
		flags = append(flags, FlagRefundExceedsPurchase)
	}
	if matched && h.cardID != l.CardID {
		flags = append(flags, FlagCardMismatch)
	}
	var flag *string
	if len(flags) > 0 {
		joined := strings.Join(flags, ",")
		flag = &joined
	}
	txnID := ledger.NewTxnID()

	batch := &pgx.Batch{}
	if matched {
		batch.Queue(`
			UPDATE card_authorizations SET refunded_amount = refunded_amount + $2, updated_at = now()
			WHERE auth_id = $1`, h.authID, l.Amount)
	}
	batch.Queue(`
		UPDATE clearing_records SET status = $2, credited_amount = $3, flag = $4, ledger_txn_id = $5
		WHERE clearing_id = $1`, l.ClearingID, StatusPosted, l.Amount, flag, txnID)
	var authID *string
	if l.AuthID != "" {
		authID = &l.AuthID
	}
	payload, err := json.Marshal(RefundEvent{
		ClearingID: l.ClearingID, AuthID: authID, CardID: l.CardID, AccountID: card.AccountID,
		Amount: l.Amount, Credited: l.Amount, Flag: flag,
	})
	if err != nil {
		return fmt.Errorf("encode event for %s: %w", l.ClearingID, err)
	}
	aggregate := l.ClearingID
	if h != nil {
		aggregate = h.authID
	}
	outbox.Queue(batch, outbox.Event{
		ID: newID("evt"), AggregateID: aggregate, EventType: EventRefundPosted,
		Topic: authorizations.Topic, Payload: payload,
	})
	if err := tx.SendBatch(ctx, batch).Close(); err != nil {
		return fmt.Errorf("post refund %s: %w", l.ClearingID, err)
	}
	return ledger.RecordWithID(ctx, tx, txnID, "refund "+l.ClearingID, []ledger.Entry{
		{AccountID: NetworkPayableAccountID, Direction: ledger.DirectionDebit, Amount: l.Amount},
		{AccountID: card.AccountID, Direction: ledger.DirectionCredit, Amount: l.Amount},
	})
}
