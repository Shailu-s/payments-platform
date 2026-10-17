package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/Shailu-s/payments-platform/internal/authorizations"
	"github.com/Shailu-s/payments-platform/internal/cardnetwork"
	"github.com/Shailu-s/payments-platform/internal/cards"
	"github.com/Shailu-s/payments-platform/internal/ledger"
	"github.com/Shailu-s/payments-platform/internal/outbox"
	"github.com/jackc/pgx/v5"
)

// The network waits 2 s per attempt and retries once. Answering 503 well inside
// that leaves time for the retry; an answer after it is discarded anyway.
const networkDeadline = time.Second

type networkMerchant struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	MCC  string `json:"mcc"`
}

// Unknown fields are accepted: the network, not us, owns this message format.
type networkAuthorization struct {
	AuthID         string          `json:"auth_id"`
	CardID         string          `json:"card_id"`
	Amount         int64           `json:"amount"`
	Currency       string          `json:"currency"`
	Merchant       networkMerchant `json:"merchant"`
	Kind           string          `json:"kind"`
	OriginalAuthID string          `json:"original_auth_id"`
}

type networkAnswer struct {
	AuthID         string `json:"auth_id"`
	Decision       string `json:"decision"`
	ApprovedAmount int64  `json:"approved_amount"`
	DeclineReason  string `json:"decline_reason,omitempty"`
}

func toNetworkAnswer(a authorizations.Authorization) networkAnswer {
	answer := networkAnswer{AuthID: a.AuthID, Decision: a.Decision, ApprovedAmount: a.ApprovedAmount}
	if a.DeclineReason != nil {
		answer.DeclineReason = *a.DeclineReason
	}
	return answer
}

// readNetworkRequest returns the body of a request the network signed, or
// answers it 401 and returns false. Nothing is parsed before the signature
// is checked.
func (s *Server) readNetworkRequest(w http.ResponseWriter, r *http.Request) ([]byte, bool) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 64<<10))
	if err != nil {
		var maxErr *http.MaxBytesError
		status := http.StatusBadRequest
		if errors.As(err, &maxErr) {
			status = http.StatusRequestEntityTooLarge
		}
		writeError(w, status, CodeInvalidRequest, "could not read network request body")
		return nil, false
	}
	if !cardnetwork.VerifyRequest(s.networkSecret, r.Header, body, time.Now()) {
		writeError(w, http.StatusUnauthorized, CodeUnauthorized, "invalid network signature or timestamp")
		return nil, false
	}
	return body, true
}

// handleNetworkAuthorization answers "may this card spend this much here?".
// Every answer is the stored row, so the same auth_id always gets the same
// answer. A non-200 means no answer: the network retries once with the same
// auth_id, then decides alone and tells us with an advice.
func (s *Server) handleNetworkAuthorization(w http.ResponseWriter, r *http.Request) {
	body, ok := s.readNetworkRequest(w, r)
	if !ok {
		return
	}
	var msg networkAuthorization
	if err := json.Unmarshal(body, &msg); err != nil {
		writeError(w, http.StatusBadRequest, CodeInvalidRequest, "authorization body is not valid json")
		return
	}
	if text, ok := validateNetworkAuthorization(msg); !ok {
		writeError(w, http.StatusBadRequest, CodeInvalidRequest, text)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), networkDeadline)
	defer cancel()

	a, err := s.authorize(ctx, authorizations.Request{
		AuthID:         msg.AuthID,
		CardID:         msg.CardID,
		Amount:         msg.Amount,
		Currency:       msg.Currency,
		MerchantID:     msg.Merchant.ID,
		MerchantName:   msg.Merchant.Name,
		MCC:            msg.Merchant.MCC,
		Kind:           msg.Kind,
		OriginalAuthID: msg.OriginalAuthID,
	})
	if err != nil {
		writeNetworkError(ctx, w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toNetworkAnswer(a))
}

// writeNetworkError answers 503 to anything a retry can fix: nothing was
// stored for this attempt, so the network's retry starts afresh.
func writeNetworkError(ctx context.Context, w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, ErrAccountBusy) || isLockTimeout(err):
		writeError(w, http.StatusServiceUnavailable, CodeAccountBusy, "the account is busy, retry")
	case errors.Is(err, context.DeadlineExceeded) || ctx.Err() != nil:
		writeError(w, http.StatusServiceUnavailable, CodeRequestTimeout, "no answer within the deadline, retry")
	case errors.Is(err, authorizations.ErrNotFound):
		writeError(w, http.StatusServiceUnavailable, CodeIdempotencyInFlight,
			"this auth_id is still being decided, retry")
	default:
		writeInternalError(w, r, err)
	}
}

func validateNetworkAuthorization(msg networkAuthorization) (string, bool) {
	switch {
	case msg.AuthID == "":
		return "auth_id is required", false
	case msg.CardID == "":
		return "card_id is required", false
	case msg.Amount <= 0:
		return "amount must be positive minor units", false
	case msg.Kind != authorizations.KindInitial && msg.Kind != authorizations.KindIncremental:
		return "kind must be initial or incremental", false
	}
	return "", true
}

// authorize decides a tap and returns the stored row.
func (s *Server) authorize(ctx context.Context, req authorizations.Request) (authorizations.Authorization, error) {
	// Read outside the transaction, so the gate below is taken before a
	// connection is. Reading it inside without a lock would be no stronger: a
	// freeze could still commit between the read and the hold.
	var card *cards.Card
	found, err := cards.Get(ctx, s.db, req.CardID)
	switch {
	case err == nil:
		card = &found
	case !errors.Is(err, cards.ErrNotFound):
		return authorizations.Authorization{}, err
	}

	reason := authorizations.RuleDecline(card, req)
	a := authorizations.New(card, req, reason)
	if reason == "" {
		// A hot card waits here, not in the pool.
		release, err := s.gate.acquire(ctx, card.AccountID)
		if err != nil {
			return authorizations.Authorization{}, fmt.Errorf("account %s: %w", card.AccountID, err)
		}
		defer release()
		txnID := ledger.NewTxnID()
		a.LedgerTxnID = &txnID
	}

	err = s.insertAuthorization(ctx, &a, card)
	switch {
	case err == nil:
		return a, nil
	case errors.Is(err, authorizations.ErrDuplicate):
		return s.replayAuthorization(ctx, a)
	case errors.Is(err, authorizations.ErrDailyLimit):
		return s.declineAuthorization(ctx, a, authorizations.ReasonDailyLimitExceeded)
	case errors.Is(err, ledger.ErrInsufficientFunds):
		// The CHECK aborted the transaction, row and all: the answer is
		// stored by a second one.
		return s.declineAuthorization(ctx, a, authorizations.ReasonInsufficientFunds)
	}
	return authorizations.Authorization{}, err
}

// insertAuthorization writes the authorization, its outbox event and, if
// approved, the daily spend and the hold, in one transaction.
//
// The order mirrors insertTransfer. The row goes first, so the primary key
// picks one winner among duplicates before anything else happens. Then the
// card's daily counter, then the hold, whose balance update takes the lock
// that serialises spending from the account: each lock is taken as late as
// possible and all are held only until commit.
func (s *Server) insertAuthorization(ctx context.Context, a *authorizations.Authorization, card *cards.Card) error {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin authorization %s: %w", a.AuthID, err)
	}
	defer tx.Rollback(ctx)

	payload, err := authorizations.EventPayload(*a)
	if err != nil {
		return err
	}
	approved := a.Decision == authorizations.DecisionApproved

	batch := &pgx.Batch{}
	authorizations.QueueInsert(batch, a)
	// After the insert, like transfers: a duplicate may wait for the original
	// to finish; every wait after that is bounded.
	batch.Queue(setSpendingLockTimeout)
	// The approved event is written before the daily check: an approval that
	// fails it rolls back, event included.
	outbox.Queue(batch, outbox.Event{
		ID:          newID("evt"),
		AggregateID: a.AuthID,
		EventType:   authorizations.DecisionEvent(*a),
		Topic:       authorizations.Topic,
		Payload:     payload,
	})
	if approved {
		authorizations.QueueDailySpend(batch, a.CardID, a.Amount, card.DailyLimit)
	}
	if err := tx.SendBatch(ctx, batch).Close(); err != nil {
		return err
	}

	// Last: an overdraft fails here with ErrInsufficientFunds and rolls back
	// the row, the event and the daily spend with it.
	if approved {
		if err := ledger.RecordWithID(ctx, tx, *a.LedgerTxnID, "authorization "+a.AuthID, []ledger.Entry{
			{AccountID: card.AccountID, Direction: ledger.DirectionDebit, Amount: a.Amount},
			{AccountID: card.HeldAccountID, Direction: ledger.DirectionCredit, Amount: a.Amount},
		}); err != nil {
			return err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit authorization %s: %w", a.AuthID, err)
	}
	return nil
}

// declineAuthorization stores a decline for an approval that failed in the
// database, after its transaction rolled back. A concurrent duplicate may
// have stored a row meanwhile; that row is the answer for both.
func (s *Server) declineAuthorization(ctx context.Context, a authorizations.Authorization, reason string) (authorizations.Authorization, error) {
	a.Decision, a.ApprovedAmount, a.DeclineReason, a.LedgerTxnID = authorizations.DecisionDeclined, 0, &reason, nil
	stored, inserted, err := authorizations.InsertIfAbsent(ctx, s.db, a, newID("evt"), authorizations.EventDeclined)
	if err != nil {
		return authorizations.Authorization{}, err
	}
	if inserted {
		return stored, nil
	}
	return s.replayAuthorization(ctx, a)
}

// replayAuthorization answers an auth_id that already has a row, with that row.
func (s *Server) replayAuthorization(ctx context.Context, a authorizations.Authorization) (authorizations.Authorization, error) {
	stored, err := s.awaitAuthorization(ctx, a.AuthID)
	if err != nil {
		return authorizations.Authorization{}, err
	}
	// Still the stored answer: a second answer for one auth_id is worse than
	// a stale one. The network promises a retry carries the same body.
	if stored.RequestFingerprint != a.RequestFingerprint {
		slog.WarnContext(ctx, "authorization replayed with a different body",
			"auth_id", a.AuthID, "card_id", a.CardID)
	}
	return stored, nil
}

// awaitAuthorization reads an auth_id's row, retrying briefly: the duplicate
// key error can arrive before the winner's row is visible.
func (s *Server) awaitAuthorization(ctx context.Context, authID string) (authorizations.Authorization, error) {
	const attempts = 10
	const gap = 20 * time.Millisecond

	var err error
	for i := 0; i < attempts; i++ {
		var a authorizations.Authorization
		a, err = authorizations.Get(ctx, s.db, authID)
		if err == nil {
			return a, nil
		}
		if !errors.Is(err, authorizations.ErrNotFound) {
			return authorizations.Authorization{}, err
		}
		select {
		case <-ctx.Done():
			return authorizations.Authorization{}, ctx.Err()
		case <-time.After(gap):
		}
	}
	return authorizations.Authorization{}, err
}

// An advice is not a question: the network already told the merchant yes.
type networkAdvice struct {
	AdviceID string          `json:"advice_id"`
	AuthID   string          `json:"auth_id"`
	CardID   string          `json:"card_id"`
	Amount   int64           `json:"amount"`
	Merchant networkMerchant `json:"merchant"`
	Decision string          `json:"decision"`
	Reason   string          `json:"reason"`
}

// handleNetworkAdvice honours a stand-in approval. Any non-2xx is redelivered
// by the network with backoff, so a failure here is retried, not lost.
func (s *Server) handleNetworkAdvice(w http.ResponseWriter, r *http.Request) {
	body, ok := s.readNetworkRequest(w, r)
	if !ok {
		return
	}
	var msg networkAdvice
	if err := json.Unmarshal(body, &msg); err != nil {
		writeError(w, http.StatusBadRequest, CodeInvalidRequest, "advice body is not valid json")
		return
	}
	switch {
	case msg.AdviceID == "" || msg.AuthID == "" || msg.CardID == "":
		writeError(w, http.StatusBadRequest, CodeInvalidRequest, "advice_id, auth_id and card_id are required")
		return
	case msg.Amount <= 0:
		writeError(w, http.StatusBadRequest, CodeInvalidRequest, "amount must be positive minor units")
		return
	case msg.Decision != authorizations.DecisionApproved:
		writeError(w, http.StatusBadRequest, CodeInvalidRequest, "only approved advices are sent")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), networkDeadline)
	defer cancel()

	status, err := s.applyAdvice(ctx, msg)
	if err != nil {
		writeNetworkError(ctx, w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": status, "advice_id": msg.AdviceID})
}

// applyAdvice makes our record agree with the network's: the authorization is
// approved and held, whatever we answered, if anything. The advice id and its
// effect commit together, so a redelivered advice changes nothing.
func (s *Server) applyAdvice(ctx context.Context, msg networkAdvice) (string, error) {
	var card *cards.Card
	found, err := cards.Get(ctx, s.db, msg.CardID)
	switch {
	case err == nil:
		card = &found
	case !errors.Is(err, cards.ErrNotFound):
		return "", err
	}

	tx, err := s.db.Begin(ctx)
	if err != nil {
		return "", fmt.Errorf("begin advice %s: %w", msg.AdviceID, err)
	}
	defer tx.Rollback(ctx)

	first, err := authorizations.RecordAdvice(ctx, tx, msg.AdviceID, msg.AuthID)
	if err != nil {
		return "", err
	}
	if !first {
		return "already_processed", nil
	}
	if _, err := tx.Exec(ctx, setSpendingLockTimeout); err != nil {
		return "", fmt.Errorf("advice %s: %w", msg.AdviceID, err)
	}

	claim := authorizations.New(card, authorizations.Request{
		AuthID:       msg.AuthID,
		CardID:       msg.CardID,
		Amount:       msg.Amount,
		Currency:     authorizations.Currency,
		MerchantID:   msg.Merchant.ID,
		MerchantName: msg.Merchant.Name,
		MCC:          msg.Merchant.MCC,
		Kind:         authorizations.KindInitial,
	}, "")
	claim.StandIn = true
	stored, inserted, err := authorizations.ClaimForAdvice(ctx, tx, claim)
	if err != nil {
		return "", err
	}

	var a authorizations.Authorization
	if !inserted && stored.Decision == authorizations.DecisionApproved {
		// Our approval reached the network too late; the hold is already there.
		a, err = authorizations.MarkStandIn(ctx, tx, stored.AuthID)
	} else {
		if card != nil {
			stored.AccountID, stored.HeldAccountID = &card.AccountID, &card.HeldAccountID
		}
		if err = s.placeStandInHold(ctx, tx, &stored, card); err == nil {
			a, err = authorizations.ApproveStandIn(ctx, tx, stored)
		}
	}
	if err != nil {
		return "", err
	}

	payload, err := authorizations.EventPayload(a)
	if err != nil {
		return "", err
	}
	if err := outbox.Insert(ctx, tx, outbox.Event{
		ID:          newID("evt"),
		AggregateID: a.AuthID,
		EventType:   authorizations.EventStandIn,
		Topic:       authorizations.Topic,
		Payload:     payload,
	}); err != nil {
		return "", err
	}
	if err := tx.Commit(ctx); err != nil {
		return "", fmt.Errorf("commit advice %s: %w", msg.AdviceID, err)
	}
	return "processed", nil
}

// placeStandInHold holds as much of a.Amount as the funding account has and
// records the rest as a.HoldShortfall. Card status and limits do not apply:
// the merchant was already told yes. It never overdraws and never invents
// money; a shortfall is credit exposure for reconciliation to surface.
func (s *Server) placeStandInHold(ctx context.Context, tx pgx.Tx, a *authorizations.Authorization, card *cards.Card) error {
	a.HoldShortfall, a.LedgerTxnID = a.Amount, nil
	if card == nil {
		return nil
	}
	if err := authorizations.AddDailySpend(ctx, tx, card.ID, a.Amount); err != nil {
		return err
	}

	// Both balance rows, in id order: the order ledger.RecordWithID locks
	// them in, so this cannot deadlock with a concurrent tap on the account.
	const lock = `
		SELECT account_id, balance FROM account_balances
		WHERE account_id = ANY($1) ORDER BY account_id FOR UPDATE`
	rows, err := tx.Query(ctx, lock, []string{card.AccountID, card.HeldAccountID})
	if err != nil {
		return fmt.Errorf("lock balances for %s: %w", a.AuthID, err)
	}
	var available int64
	for rows.Next() {
		var id string
		var balance int64
		if err := rows.Scan(&id, &balance); err != nil {
			rows.Close()
			return fmt.Errorf("lock balances for %s: %w", a.AuthID, err)
		}
		if id == card.AccountID {
			available = balance
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return fmt.Errorf("lock balances for %s: %w", a.AuthID, err)
	}

	held := min(a.Amount, available)
	a.HoldShortfall = a.Amount - held
	if held == 0 {
		return nil
	}
	txnID := ledger.NewTxnID()
	a.LedgerTxnID = &txnID
	return ledger.RecordWithID(ctx, tx, txnID, "authorization "+a.AuthID, []ledger.Entry{
		{AccountID: card.AccountID, Direction: ledger.DirectionDebit, Amount: held},
		{AccountID: card.HeldAccountID, Direction: ledger.DirectionCredit, Amount: held},
	})
}
