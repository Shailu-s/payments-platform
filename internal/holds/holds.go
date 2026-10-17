// Package holds gives back money that card authorizations set aside: on a
// reversal from the network, or when an approved hold never clears. Every way
// a hold shrinks goes through Release, so a hold is released at most once in
// total, whatever arrives twice or at the same time.
package holds

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/Shailu-s/payments-platform/internal/authorizations"
	"github.com/Shailu-s/payments-platform/internal/ledger"
	"github.com/Shailu-s/payments-platform/internal/outbox"
	"github.com/jackc/pgx/v5"
)

const (
	ReasonReversal = "reversal"
	ReasonExpiry   = "expiry"

	EventReversed = "card.authorization.reversed"
	EventExpired  = "card.authorization.expired"
)

// Event is the outbox record of a release.
type Event struct {
	AuthID     string `json:"auth_id"`
	CardID     string `json:"card_id"`
	Released   int64  `json:"released"`
	HeldAmount int64  `json:"held_amount"`
	Reason     string `json:"reason"`
	MessageID  string `json:"message_id,omitempty"`
}

// Release gives back up to amount of authID's hold, moving it from the held
// account to the funding account, and returns how much it released. It never
// releases more than the row still holds, so a second release of the same
// money releases 0 and writes nothing. messageID is the network message that
// asked for it, if any.
//
// The row lock is the exactly-once guarantee: held_amount is read and reduced
// under it, and every releaser takes it first. The held account's balance
// CHECK is the backstop. Locks follow the authorization path's order: the
// authorization row, the card's daily spend, then the balances, last.
func Release(ctx context.Context, tx pgx.Tx, authID string, amount int64, reason, messageID string) (int64, error) {
	if reason != ReasonReversal && reason != ReasonExpiry {
		return 0, fmt.Errorf("release %s: unknown reason %q", authID, reason)
	}
	const lock = `
		SELECT card_id, account_id, held_account_id, held_amount,
			(created_at AT TIME ZONE 'UTC')::date
		FROM card_authorizations WHERE auth_id = $1 FOR UPDATE`
	var cardID string
	var accountID, heldAccountID *string
	var held int64
	var day time.Time
	err := tx.QueryRow(ctx, lock, authID).Scan(&cardID, &accountID, &heldAccountID, &held, &day)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, authorizations.ErrNotFound
	}
	if err != nil {
		return 0, fmt.Errorf("lock authorization %s: %w", authID, err)
	}

	n := min(amount, held)
	if n <= 0 {
		return 0, nil
	}
	if accountID == nil || heldAccountID == nil {
		return 0, fmt.Errorf("release %s: holds %d with no card accounts", authID, held)
	}

	eventType := EventReversed
	if reason == ReasonExpiry {
		eventType = EventExpired
	}
	payload, err := json.Marshal(Event{
		AuthID: authID, CardID: cardID, Released: n, HeldAmount: held - n,
		Reason: reason, MessageID: messageID,
	})
	if err != nil {
		return 0, fmt.Errorf("encode release of %s: %w", authID, err)
	}

	batch := &pgx.Batch{}
	batch.Queue(`
		UPDATE card_authorizations
		SET held_amount = held_amount - $2, released_amount = released_amount + $2,
			expired_at = CASE WHEN $3 THEN now() ELSE expired_at END, updated_at = now()
		WHERE auth_id = $1`, authID, n, reason == ReasonExpiry)
	// Released money no longer counts against the day it was spent on. A
	// stand-in may have counted more than it held: never below zero.
	batch.Queue(`
		UPDATE card_daily_spend SET amount = greatest(amount - $3, 0)
		WHERE card_id = $1 AND day = $2`, cardID, day, n)
	outbox.Queue(batch, outbox.Event{
		ID:          newID("evt"),
		AggregateID: authID,
		EventType:   eventType,
		Topic:       authorizations.Topic,
		Payload:     payload,
	})
	if err := tx.SendBatch(ctx, batch).Close(); err != nil {
		return 0, fmt.Errorf("release %s: %w", authID, err)
	}

	reference := "release " + authID + " " + reason
	if messageID != "" {
		reference += " " + messageID
	}
	if err := ledger.RecordWithID(ctx, tx, ledger.NewTxnID(), reference, []ledger.Entry{
		{AccountID: *heldAccountID, Direction: ledger.DirectionDebit, Amount: n},
		{AccountID: *accountID, Direction: ledger.DirectionCredit, Amount: n},
	}); err != nil {
		return 0, err
	}
	return n, nil
}

func newID(prefix string) string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return prefix + "_" + hex.EncodeToString(b[:])
}
