// Package authorizations stores card authorizations: the network's question
// "may this card spend this much here?", the answer we gave, and the hold that
// an approval placed. The network's auth_id is the primary key, so one auth_id
// has one row, one answer and at most one hold.
package authorizations

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/Shailu-s/payments-platform/internal/cards"
	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

const (
	DecisionApproved = "approved"
	DecisionDeclined = "declined"

	KindInitial     = "initial"
	KindIncremental = "incremental"

	Currency = "USD"

	ReasonCardNotFound            = "card_not_found"
	ReasonCardInactive            = "card_inactive"
	ReasonInvalidCurrency         = "invalid_currency"
	ReasonExceedsTransactionLimit = "exceeds_transaction_limit"
	ReasonMerchantBlocked         = "merchant_blocked"
	ReasonIncrementalNotSupported = "incremental_not_supported"
	ReasonDailyLimitExceeded      = "daily_limit_exceeded"
	ReasonInsufficientFunds       = "insufficient_funds"

	EventApproved = "card.authorization.approved"
	EventDeclined = "card.authorization.declined"
	EventStandIn  = "card.authorization.stand_in"
	Topic         = "cards"
)

var (
	ErrNotFound = errors.New("authorization not found")
	// ErrDuplicate means another request already owns the auth_id.
	ErrDuplicate = errors.New("auth_id already authorized")
)

// Request is what the network asks, in our terms.
type Request struct {
	AuthID         string
	CardID         string
	Amount         int64
	Currency       string
	MerchantID     string
	MerchantName   string
	MCC            string
	Kind           string
	OriginalAuthID string
}

type Authorization struct {
	AuthID             string
	CardID             string
	AccountID          *string
	HeldAccountID      *string
	Amount             int64
	Currency           string
	MerchantID         string
	MerchantName       string
	MCC                string
	Kind               string
	OriginalAuthID     *string
	Decision           string
	ApprovedAmount     int64
	DeclineReason      *string
	StandIn            bool
	HoldShortfall      int64
	LedgerTxnID        *string
	RequestFingerprint string
	CreatedAt          time.Time
	UpdatedAt          time.Time
}

// Querier lets reads and single statements share the caller's transaction.
type Querier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// RuleDecline returns why the card's own rules refuse r, or "" if they allow
// it. It moves no money and reads nothing: card is nil for an unknown card.
// Funds and the daily limit are decided by the database, not here.
func RuleDecline(card *cards.Card, r Request) string {
	switch {
	case card == nil:
		return ReasonCardNotFound
	case card.Status != cards.StatusActive:
		return ReasonCardInactive
	case r.Currency != Currency:
		return ReasonInvalidCurrency
	case r.Amount > card.PerTransactionLimit:
		return ReasonExceedsTransactionLimit
	case slices.Contains(card.BlockedMCCs, r.MCC):
		return ReasonMerchantBlocked
	case r.Kind == KindIncremental:
		return ReasonIncrementalNotSupported
	}
	return ""
}

// New is the row for r: approved in full, or declined with reason.
func New(card *cards.Card, r Request, reason string) Authorization {
	a := Authorization{
		AuthID:             r.AuthID,
		CardID:             r.CardID,
		Amount:             r.Amount,
		Currency:           r.Currency,
		MerchantID:         r.MerchantID,
		MerchantName:       r.MerchantName,
		MCC:                r.MCC,
		Kind:               r.Kind,
		Decision:           DecisionApproved,
		ApprovedAmount:     r.Amount,
		RequestFingerprint: Fingerprint(r),
	}
	if card != nil {
		a.AccountID, a.HeldAccountID = &card.AccountID, &card.HeldAccountID
	}
	if r.OriginalAuthID != "" {
		a.OriginalAuthID = &r.OriginalAuthID
	}
	if reason != "" {
		a.Decision, a.ApprovedAmount, a.DeclineReason = DecisionDeclined, 0, &reason
	}
	return a
}

// Fingerprint identifies the request's content, so a duplicate auth_id with a
// different body can be told apart from a retry.
func Fingerprint(r Request) string {
	sum := sha256.Sum256([]byte(strings.Join([]string{
		r.CardID, strconv.FormatInt(r.Amount, 10), r.Currency,
		r.MerchantID, r.MerchantName, r.MCC, r.Kind, r.OriginalAuthID,
	}, "|")))
	return hex.EncodeToString(sum[:])
}

const columns = `auth_id, card_id, account_id, held_account_id, amount, currency,
	merchant_id, merchant_name, mcc, kind, original_auth_id, decision, approved_amount,
	decline_reason, stand_in, hold_shortfall, ledger_txn_id, request_fingerprint,
	created_at, updated_at`

const insertSQL = `
	INSERT INTO card_authorizations (auth_id, card_id, account_id, held_account_id, amount,
		currency, merchant_id, merchant_name, mcc, kind, original_auth_id, decision,
		approved_amount, decline_reason, stand_in, hold_shortfall, ledger_txn_id,
		request_fingerprint)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)`

func insertArgs(a Authorization) []any {
	return []any{a.AuthID, a.CardID, a.AccountID, a.HeldAccountID, a.Amount, a.Currency,
		a.MerchantID, a.MerchantName, a.MCC, a.Kind, a.OriginalAuthID, a.Decision,
		a.ApprovedAmount, a.DeclineReason, a.StandIn, a.HoldShortfall, a.LedgerTxnID,
		a.RequestFingerprint}
}

// QueueInsert adds the insert of *a to batch and fills its timestamps when the
// batch runs. The primary key picks one winner among concurrent requests for
// one auth_id; the others wait for it to commit and fail with ErrDuplicate.
func QueueInsert(batch *pgx.Batch, a *Authorization) {
	batch.Queue(insertSQL+` RETURNING created_at, updated_at`, insertArgs(*a)...).
		QueryRow(func(row pgx.Row) error {
			err := row.Scan(&a.CreatedAt, &a.UpdatedAt)
			if isUniqueViolation(err) {
				return ErrDuplicate
			}
			if err != nil {
				return fmt.Errorf("insert authorization %s: %w", a.AuthID, err)
			}
			return nil
		})
}

// InsertIfAbsent inserts a and its outbox event in one statement, unless the
// auth_id already has a row. It reports whether it inserted. A conflicting
// insert still in flight is waited for, so a false answer means a committed
// row exists.
func InsertIfAbsent(ctx context.Context, db Querier, a Authorization, eventID, eventType string) (Authorization, bool, error) {
	payload, err := EventPayload(a)
	if err != nil {
		return Authorization{}, false, err
	}
	q := `
		WITH inserted AS (` + insertSQL + `
			ON CONFLICT (auth_id) DO NOTHING
			RETURNING ` + columns + `
		), event AS (
			INSERT INTO outbox_events (id, aggregate_id, event_type, topic, payload)
			SELECT $19::text, auth_id, $20::text, $21::text, $22::jsonb FROM inserted
		)
		SELECT ` + columns + ` FROM inserted`
	args := append(insertArgs(a), eventID, eventType, Topic, payload)
	stored, err := scan(db.QueryRow(ctx, q, args...))
	if errors.Is(err, pgx.ErrNoRows) {
		return Authorization{}, false, nil
	}
	if err != nil {
		return Authorization{}, false, fmt.Errorf("insert authorization %s if absent: %w", a.AuthID, err)
	}
	return stored, true, nil
}

func Get(ctx context.Context, db Querier, authID string) (Authorization, error) {
	return get(ctx, db, `SELECT `+columns+` FROM card_authorizations WHERE auth_id = $1`, authID)
}

// GetForUpdate reads and locks the row, so an advice and a late authorization
// cannot change it at the same time.
func GetForUpdate(ctx context.Context, tx pgx.Tx, authID string) (Authorization, error) {
	return get(ctx, tx, `SELECT `+columns+` FROM card_authorizations WHERE auth_id = $1 FOR UPDATE`, authID)
}

func get(ctx context.Context, db Querier, q, authID string) (Authorization, error) {
	a, err := scan(db.QueryRow(ctx, q, authID))
	if errors.Is(err, pgx.ErrNoRows) {
		return Authorization{}, ErrNotFound
	}
	if err != nil {
		return Authorization{}, fmt.Errorf("get authorization %s: %w", authID, err)
	}
	return a, nil
}

// ErrDailyLimit means the approval would take the card past its daily limit.
var ErrDailyLimit = errors.New("daily limit exceeded")

// QueueDailySpend adds amount to the card's spend for today (UTC) in one
// conditional statement, failing with ErrDailyLimit instead of passing limit.
// The row lock it takes serialises taps on one card until commit.
func QueueDailySpend(batch *pgx.Batch, cardID string, amount, limit int64) {
	const q = `
		INSERT INTO card_daily_spend (card_id, day, amount)
		VALUES ($1, (now() AT TIME ZONE 'UTC')::date, $2)
		ON CONFLICT (card_id, day) DO UPDATE
			SET amount = card_daily_spend.amount + EXCLUDED.amount
			WHERE card_daily_spend.amount + EXCLUDED.amount <= $3
		RETURNING amount`
	batch.Queue(q, cardID, amount, limit).QueryRow(func(row pgx.Row) error {
		var total int64
		err := row.Scan(&total)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrDailyLimit
		}
		if err != nil {
			return fmt.Errorf("daily spend for %s: %w", cardID, err)
		}
		// The first tap of the day inserts without the WHERE: check it here.
		if total > limit {
			return ErrDailyLimit
		}
		return nil
	})
}

// Event is the outbox record of an authorization's decision.
type Event struct {
	AuthID         string  `json:"auth_id"`
	CardID         string  `json:"card_id"`
	Amount         int64   `json:"amount"`
	ApprovedAmount int64   `json:"approved_amount"`
	Decision       string  `json:"decision"`
	Reason         *string `json:"reason"`
	StandIn        bool    `json:"stand_in"`
	HoldShortfall  int64   `json:"hold_shortfall"`
}

func EventPayload(a Authorization) ([]byte, error) {
	payload, err := json.Marshal(Event{
		AuthID: a.AuthID, CardID: a.CardID, Amount: a.Amount, ApprovedAmount: a.ApprovedAmount,
		Decision: a.Decision, Reason: a.DeclineReason, StandIn: a.StandIn, HoldShortfall: a.HoldShortfall,
	})
	if err != nil {
		return nil, fmt.Errorf("encode event for authorization %s: %w", a.AuthID, err)
	}
	return payload, nil
}

// DecisionEvent is the event type for a's decision.
func DecisionEvent(a Authorization) string {
	if a.Decision == DecisionApproved {
		return EventApproved
	}
	return EventDeclined
}

type scannable interface {
	Scan(dest ...any) error
}

func scan(row scannable) (Authorization, error) {
	var a Authorization
	err := row.Scan(&a.AuthID, &a.CardID, &a.AccountID, &a.HeldAccountID, &a.Amount, &a.Currency,
		&a.MerchantID, &a.MerchantName, &a.MCC, &a.Kind, &a.OriginalAuthID, &a.Decision,
		&a.ApprovedAmount, &a.DeclineReason, &a.StandIn, &a.HoldShortfall, &a.LedgerTxnID,
		&a.RequestFingerprint, &a.CreatedAt, &a.UpdatedAt)
	return a, err
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == pgerrcode.UniqueViolation
}
