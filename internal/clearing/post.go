package clearing

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/bits"
	"strings"
	"time"

	"github.com/Shailu-s/payments-platform/internal/authorizations"
	"github.com/Shailu-s/payments-platform/internal/cards"
	"github.com/Shailu-s/payments-platform/internal/holds"
	"github.com/Shailu-s/payments-platform/internal/ledger"
	"github.com/Shailu-s/payments-platform/internal/outbox"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// NetworkPayableAccountID is what we owe the network for cleared purchases.
const NetworkPayableAccountID = "acc_network_payable_usd"

const (
	EventPosted       = "card.clearing.posted"
	EventRefundPosted = "card.refund.posted"
)

// Flags: reasons a person should look at a line. A line can have several.
const (
	FlagOverTolerance        = "over_tolerance"
	FlagClearedAfterExpiry   = "cleared_after_expiry"
	FlagClearedAfterReversal = "cleared_after_reversal"
	FlagDuplicateClearing    = "duplicate_clearing"
	FlagUnauthorizedClearing = "unauthorized_clearing"
	FlagUnknownCard          = "unknown_card"
	FlagShortfall            = "shortfall"
	// FlagNothingHeld: approved, never released or cleared, yet holding
	// nothing, as a stand-in the account could not cover leaves it.
	FlagNothingHeld = "nothing_held"
	// FlagCardMismatch: the line's card is not the authorization's. The
	// authorization's accounts are charged: it is the hold being cleared.
	FlagCardMismatch = "card_mismatch"
	// FlagUnmatchedRefund: a refund with no cleared purchase to match, by an
	// empty or unknown auth_id or one never cleared. It is still credited.
	FlagUnmatchedRefund = "unmatched_refund"
	// FlagRefundExceedsPurchase: refunds now total more than the purchase
	// cleared. Still credited: the network has sent the money.
	FlagRefundExceedsPurchase = "refund_exceeds_purchase"
)

// Event is the outbox record of a posted clearing line.
type Event struct {
	ClearingID           string  `json:"clearing_id"`
	AuthID               *string `json:"auth_id"`
	CardID               string  `json:"card_id"`
	Amount               int64   `json:"amount"`
	ChargedFromHeld      int64   `json:"charged_from_held"`
	ChargedFromAvailable int64   `json:"charged_from_available"`
	Released             int64   `json:"released"`
	Shortfall            int64   `json:"shortfall"`
	Flag                 *string `json:"flag"`
}

// hold is the authorization row a purchase line clears, read under its lock.
type hold struct {
	authID         string
	cardID         string
	accountID      *string
	heldAccountID  *string
	approved       bool
	approvedAmount int64
	held           int64
	released       int64
	cleared        int64
	refunded       int64
	expired        bool
	// original is set on an incremental: the initial authorization holding its money.
	original *string
	// day is the UTC day its spend was counted against.
	day time.Time
}

// posting is what one line does to the money.
type posting struct {
	status        string
	accountID     string
	heldAccountID string
	fromHeld      int64
	release       int64
	wantAvailable int64
	fromAvailable int64
	shortfall     int64
	flags         []string
	ledgerTxnID   *string
	holder        *hold
}

// postLine records one line and its effect in one transaction, and reports
// whether it recorded it: false means the clearing_id was already recorded.
//
// The record is inserted first: its primary key makes a second import of the
// same line wait for the first and then skip, so no line is posted twice.
// Purchases and refunds take locks in the authorization path's order (auth_id
// lock, the authorization row, the card's daily spend, the balances), so
// clearing cannot deadlock with a tap, a reversal or the expirer; the ledger
// write, which takes the balance locks, comes last.
func postLine(ctx context.Context, db DB, fileID string, l Line, cfg Config) (bool, error) {
	tx, err := db.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback(ctx)

	var authID *string
	if l.AuthID != "" {
		authID = &l.AuthID
	}
	// One round trip claims the line and locks the authorization it names. A
	// line already recorded took the hold's locks for nothing and rolls back.
	batch := &pgx.Batch{}
	// A tap holding an account's balance lock delays the line; bounded.
	batch.Queue(`SET LOCAL lock_timeout = '1s'`)
	var claimed bool
	batch.Queue(`
		INSERT INTO clearing_records (clearing_id, file_id, auth_id, card_id, kind, amount,
			currency, merchant_id, cleared_at, status)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		ON CONFLICT (clearing_id) DO NOTHING`,
		l.ClearingID, fileID, authID, l.CardID, l.Kind, l.Amount, l.Currency, l.MerchantID, l.ClearedAt, StatusPosted,
	).Exec(func(tag pgconn.CommandTag) error {
		claimed = tag.RowsAffected() == 1
		return nil
	})
	var h *hold
	if l.AuthID != "" {
		holds.QueueLockAuthID(batch, l.AuthID)
		queueLockHold(batch, l.AuthID, &h)
	}
	if err := tx.SendBatch(ctx, batch).Close(); err != nil {
		return false, fmt.Errorf("record: %w", err)
	}
	if !claimed {
		return false, nil
	}
	// The contract clears a chain on its initial auth_id; an incremental one
	// is resolved to the row holding the money, in the reversal path's order.
	if h != nil && h.original != nil {
		batch := &pgx.Batch{}
		queueLockHold(batch, *h.original, &h)
		if err := tx.SendBatch(ctx, batch).Close(); err != nil {
			return false, err
		}
	}

	if l.Kind == KindRefund {
		if err := postRefund(ctx, tx, l, h); err != nil {
			return false, err
		}
		return true, commit(ctx, tx)
	}

	p, err := plan(ctx, tx, l, h, cfg)
	if err != nil {
		return false, err
	}
	if err := apply(ctx, tx, l, p); err != nil {
		return false, err
	}
	return true, commit(ctx, tx)
}

func commit(ctx context.Context, tx pgx.Tx) error {
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

// plan decides what a purchase line does to h, the locked hold it clears (nil
// if none). F is the line's amount and H what the hold still holds.
func plan(ctx context.Context, tx pgx.Tx, l Line, h *hold, cfg Config) (posting, error) {
	p := posting{status: StatusPosted}
	switch {
	case h != nil && h.accountID != nil && h.heldAccountID != nil:
		p.accountID, p.heldAccountID = *h.accountID, *h.heldAccountID
		if h.cardID != l.CardID {
			p.flags = append(p.flags, FlagCardMismatch)
		}
	default:
		// Never authorized, or authorized on a card we did not know: the
		// line's card is all there is to charge.
		card, err := cards.Get(ctx, tx, l.CardID)
		if errors.Is(err, cards.ErrNotFound) {
			p.status, p.flags = StatusRejected, append(p.flags, FlagUnknownCard)
			return p, nil
		}
		if err != nil {
			return p, err
		}
		p.accountID, p.heldAccountID = card.AccountID, card.HeldAccountID
	}

	if h == nil {
		p.flags = append(p.flags, FlagUnauthorizedClearing)
		p.wantAvailable = l.Amount
		return p, nil
	}
	p.holder = h
	if h.cleared > 0 {
		p.flags = append(p.flags, FlagDuplicateClearing)
	}
	switch {
	case !h.approved:
		// A stand-in whose advice never reached us: the network approved it.
		p.flags = append(p.flags, FlagUnauthorizedClearing)
	case h.held == 0 && h.cleared > 0:
	case h.held == 0 && h.expired:
		p.flags = append(p.flags, FlagClearedAfterExpiry)
	case h.held == 0 && h.released > 0:
		p.flags = append(p.flags, FlagClearedAfterReversal)
	case h.held == 0:
		p.flags = append(p.flags, FlagNothingHeld)
	}
	p.fromHeld = min(l.Amount, h.held)
	p.release = h.held - p.fromHeld
	p.wantAvailable = l.Amount - p.fromHeld
	if h.held > 0 && p.wantAvailable > 0 && exceeds(p.wantAvailable, h.approvedAmount, cfg.OverTolerancePct) {
		p.flags = append(p.flags, FlagOverTolerance)
	}
	return p, nil
}

// queueLockHold adds the read and lock of authID's row to batch, setting *into
// when the batch runs; it stays nil if there is no such row.
func queueLockHold(batch *pgx.Batch, authID string, into **hold) {
	const q = `
		SELECT auth_id, card_id, account_id, held_account_id,
			CASE WHEN kind = 'incremental' THEN original_auth_id END,
			decision = 'approved', approved_amount, held_amount, released_amount, cleared_amount,
			refunded_amount, expired_at IS NOT NULL, (created_at AT TIME ZONE 'UTC')::date
		FROM card_authorizations WHERE auth_id = $1 FOR UPDATE`
	batch.Queue(q, authID).QueryRow(func(row pgx.Row) error {
		var h hold
		err := row.Scan(&h.authID, &h.cardID, &h.accountID, &h.heldAccountID, &h.original,
			&h.approved, &h.approvedAmount, &h.held, &h.released, &h.cleared, &h.refunded,
			&h.expired, &h.day)
		if errors.Is(err, pgx.ErrNoRows) {
			*into = nil
			return nil
		}
		if err != nil {
			return fmt.Errorf("lock authorization %s: %w", authID, err)
		}
		*into = &h
		return nil
	})
}

// exceeds reports whether over is more than pct percent of authorized,
// computed in 128 bits so no amount can overflow it.
func exceeds(over, authorized, pct int64) bool {
	lhsHi, lhsLo := bits.Mul64(uint64(over), 100)
	rhsHi, rhsLo := bits.Mul64(uint64(authorized), uint64(pct))
	return lhsHi > rhsHi || (lhsHi == rhsHi && lhsLo > rhsLo)
}

// apply writes p: the balance locks and the shortfall they decide, the daily
// spend, the authorization and the record, the event, and the ledger last.
func apply(ctx context.Context, tx pgx.Tx, l Line, p posting) error {
	h := p.holder
	if p.status == StatusPosted {
		if p.wantAvailable > 0 {
			batch := &pgx.Batch{}
			// Every balance row the ledger write will touch, in its id order,
			// so the balance read stays true until commit.
			ids := []string{p.accountID}
			if p.fromHeld > 0 {
				ids = append(ids, p.heldAccountID)
			}
			batch.Queue(`
				SELECT account_id, balance FROM account_balances
				WHERE account_id = ANY($1) ORDER BY account_id FOR UPDATE`, ids).
				Query(func(rows pgx.Rows) error {
					for rows.Next() {
						var id string
						var balance int64
						if err := rows.Scan(&id, &balance); err != nil {
							return err
						}
						if id == p.accountID {
							p.fromAvailable = min(p.wantAvailable, balance)
						}
					}
					return rows.Err()
				})
			if err := tx.SendBatch(ctx, batch).Close(); err != nil {
				return fmt.Errorf("lock balances for %s: %w", l.ClearingID, err)
			}
		}
		p.shortfall = p.wantAvailable - p.fromAvailable
		if p.shortfall > 0 {
			p.flags = append(p.flags, FlagShortfall)
		}
		if p.fromHeld+p.fromAvailable > 0 {
			id := ledger.NewTxnID()
			p.ledgerTxnID = &id
		}
	}

	var flag *string
	if len(p.flags) > 0 {
		joined := strings.Join(p.flags, ",")
		flag = &joined
	}
	batch := &pgx.Batch{}
	if p.release > 0 {
		// Money given back no longer counts against the day it was held on.
		// Only a partial clear releases, and it reads no balance: this daily
		// spend lock is still taken before the ledger's balance locks.
		batch.Queue(`
			UPDATE card_daily_spend SET amount = greatest(amount - $3, 0)
			WHERE card_id = $1 AND day = $2`, h.cardID, h.day, p.release)
	}
	if h != nil && p.status == StatusPosted {
		batch.Queue(`
			UPDATE card_authorizations
			SET held_amount = held_amount - $2, released_amount = released_amount + $3,
				cleared_amount = cleared_amount + $4, cleared_at = $5, updated_at = now()
			WHERE auth_id = $1`, h.authID, p.fromHeld+p.release, p.release, l.Amount, l.ClearedAt)
	}
	batch.Queue(`
		UPDATE clearing_records
		SET status = $2, charged_from_held = $3, charged_from_available = $4, released_amount = $5,
			shortfall = $6, flag = $7, ledger_txn_id = $8
		WHERE clearing_id = $1`,
		l.ClearingID, p.status, p.fromHeld, p.fromAvailable, p.release, p.shortfall, flag, p.ledgerTxnID)
	if p.status == StatusPosted {
		var authID *string
		if l.AuthID != "" {
			authID = &l.AuthID
		}
		payload, err := json.Marshal(Event{
			ClearingID: l.ClearingID, AuthID: authID, CardID: l.CardID, Amount: l.Amount,
			ChargedFromHeld: p.fromHeld, ChargedFromAvailable: p.fromAvailable,
			Released: p.release, Shortfall: p.shortfall, Flag: flag,
		})
		if err != nil {
			return fmt.Errorf("encode event for %s: %w", l.ClearingID, err)
		}
		aggregate := l.ClearingID
		if h != nil {
			aggregate = h.authID
		}
		outbox.Queue(batch, outbox.Event{
			ID: newID("evt"), AggregateID: aggregate, EventType: EventPosted,
			Topic: authorizations.Topic, Payload: payload,
		})
	}
	if err := tx.SendBatch(ctx, batch).Close(); err != nil {
		return fmt.Errorf("post %s: %w", l.ClearingID, err)
	}

	if p.ledgerTxnID == nil {
		return nil
	}
	return ledger.RecordWithID(ctx, tx, *p.ledgerTxnID, "clearing "+l.ClearingID, entries(p))
}

// entries is the line's single balanced ledger transaction: the held part and
// the available part are both owed to the network; the rest of the hold goes
// back to the account.
func entries(p posting) []ledger.Entry {
	var e []ledger.Entry
	if taken := p.fromHeld + p.release; taken > 0 {
		e = append(e, ledger.Entry{AccountID: p.heldAccountID, Direction: ledger.DirectionDebit, Amount: taken})
	}
	if p.release > 0 {
		e = append(e, ledger.Entry{AccountID: p.accountID, Direction: ledger.DirectionCredit, Amount: p.release})
	}
	if p.fromAvailable > 0 {
		e = append(e, ledger.Entry{AccountID: p.accountID, Direction: ledger.DirectionDebit, Amount: p.fromAvailable})
	}
	return append(e, ledger.Entry{
		AccountID: NetworkPayableAccountID, Direction: ledger.DirectionCredit, Amount: p.fromHeld + p.fromAvailable,
	})
}
