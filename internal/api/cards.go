package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/Shailu-s/payments-platform/internal/cards"
	"github.com/jackc/pgx/v5"
)

type issueCardRequest struct {
	AccountID           string   `json:"account_id"`
	PerTransactionLimit int64    `json:"per_transaction_limit"`
	DailyLimit          int64    `json:"daily_limit"`
	BlockedMCCs         []string `json:"blocked_mccs"`
}

var mccPattern = regexp.MustCompile(`^[0-9]{4}$`)

type cardResponse struct {
	ID                  string    `json:"id"`
	AccountID           string    `json:"account_id"`
	HeldAccountID       string    `json:"held_account_id"`
	Last4               string    `json:"last4"`
	Status              string    `json:"status"`
	PerTransactionLimit int64     `json:"per_transaction_limit"`
	DailyLimit          int64     `json:"daily_limit"`
	BlockedMCCs         []string  `json:"blocked_mccs"`
	CreatedAt           time.Time `json:"created_at"`
	UpdatedAt           time.Time `json:"updated_at"`
}

func toCardResponse(c cards.Card) cardResponse {
	return cardResponse{
		ID:                  c.ID,
		AccountID:           c.AccountID,
		HeldAccountID:       c.HeldAccountID,
		Last4:               c.Last4,
		Status:              c.Status,
		PerTransactionLimit: c.PerTransactionLimit,
		DailyLimit:          c.DailyLimit,
		BlockedMCCs:         c.BlockedMCCs,
		CreatedAt:           c.CreatedAt,
		UpdatedAt:           c.UpdatedAt,
	}
}

// handleIssueCard issues a card on a funding account. A retry with the same
// Idempotency-Key answers with the card already issued.
func (s *Server) handleIssueCard(w http.ResponseWriter, r *http.Request) {
	key, ok := APIKeyFrom(r.Context())
	if !ok {
		// Unreachable behind the auth middleware.
		writeError(w, http.StatusUnauthorized, CodeUnauthorized, "no authenticated api key")
		return
	}

	idempotencyKey, msg, ok := idempotencyKeyFrom(r)
	if !ok {
		writeError(w, http.StatusBadRequest, CodeInvalidRequest, msg)
		return
	}

	var req issueCardRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	if msg, ok := validateIssueCardRequest(&req); !ok {
		writeError(w, http.StatusBadRequest, CodeInvalidRequest, msg)
		return
	}

	msg, err := s.validateFundingAccount(r.Context(), req.AccountID)
	if err != nil {
		writeInternalError(w, r, err)
		return
	}
	if msg != "" {
		writeError(w, http.StatusBadRequest, CodeInvalidRequest, msg)
		return
	}

	fingerprint := fingerprintCardRequest(key.ID, req)
	card, err := cards.Issue(r.Context(), s.db, cards.Card{
		ID:                  newID("card"),
		AccountID:           req.AccountID,
		PerTransactionLimit: req.PerTransactionLimit,
		DailyLimit:          req.DailyLimit,
		BlockedMCCs:         req.BlockedMCCs,
		APIKeyID:            key.ID,
		IdempotencyKey:      idempotencyKey,
		RequestFingerprint:  fingerprint,
	})
	if errors.Is(err, cards.ErrDuplicateKey) {
		s.replayCard(w, r, idempotencyKey, fingerprint)
		return
	}
	if err != nil {
		writeInternalError(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, toCardResponse(card))
}

// replayCard answers a request whose idempotency key was already used.
func (s *Server) replayCard(w http.ResponseWriter, r *http.Request, idempotencyKey, fingerprint string) {
	existing, err := s.awaitCard(r.Context(), idempotencyKey)
	if errors.Is(err, cards.ErrNotFound) {
		writeError(w, http.StatusConflict, CodeIdempotencyInFlight,
			"a request with this Idempotency-Key is still being processed, retry shortly")
		return
	}
	if err != nil {
		writeInternalError(w, r, err)
		return
	}
	if existing.RequestFingerprint != fingerprint {
		writeError(w, http.StatusUnprocessableEntity, CodeIdempotencyKeyReused,
			"this Idempotency-Key was already used for a different request: "+
				"use a new key for a new card")
		return
	}
	writeJSON(w, http.StatusCreated, toCardResponse(existing))
}

// awaitCard reads the card owning a key, retrying briefly: the duplicate key
// error can arrive before the winner's row is visible.
func (s *Server) awaitCard(ctx context.Context, idempotencyKey string) (cards.Card, error) {
	const attempts = 10
	const gap = 20 * time.Millisecond

	var err error
	for i := 0; i < attempts; i++ {
		var card cards.Card
		card, err = cards.FindByIdempotencyKey(ctx, s.db, idempotencyKey)
		if err == nil {
			return card, nil
		}
		if !errors.Is(err, cards.ErrNotFound) {
			return cards.Card{}, err
		}
		select {
		case <-ctx.Done():
			return cards.Card{}, ctx.Err()
		case <-time.After(gap):
		}
	}
	return cards.Card{}, err
}

func validateIssueCardRequest(req *issueCardRequest) (string, bool) {
	req.AccountID = strings.TrimSpace(req.AccountID)

	// Sorted and deduplicated, so the fingerprint and the stored card do not
	// depend on the order the client listed them in.
	req.BlockedMCCs = slices.Compact(slices.Sorted(slices.Values(req.BlockedMCCs)))
	for _, mcc := range req.BlockedMCCs {
		if !mccPattern.MatchString(mcc) {
			return fmt.Sprintf("blocked_mccs must hold 4-digit merchant category codes, got %q", mcc), false
		}
	}

	switch {
	case req.AccountID == "":
		return "account_id is required", false
	case req.PerTransactionLimit <= 0:
		return "per_transaction_limit must be a positive integer in minor units, for example 50000 for $500.00", false
	case req.DailyLimit < req.PerTransactionLimit:
		return "daily_limit must be at least per_transaction_limit", false
	}
	return "", true
}

// validateFundingAccount returns why an account cannot fund a card, or "" if
// it can, in one round trip. A held account cannot fund a card: its money
// already belongs to another card's authorizations.
func (s *Server) validateFundingAccount(ctx context.Context, accountID string) (string, error) {
	const q = `
		SELECT a.currency, a.type,
			EXISTS (SELECT 1 FROM card_accounts c WHERE c.held_account_id = a.id)
		FROM accounts a WHERE a.id = $1`

	var currency, accountType string
	var held bool
	err := s.db.QueryRow(ctx, q, accountID).Scan(&currency, &accountType, &held)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		// 400, not 404: the account is a field in the body, not the resource.
		return "no such account: " + accountID, nil
	case err != nil:
		return "", fmt.Errorf("load funding account %s: %w", accountID, err)
	case currency != supportedCurrency:
		return fmt.Sprintf("account %s holds %s, cards support %s only", accountID, currency, supportedCurrency), nil
	case accountType == "settlement":
		// It has no stored balance, so nothing would stop a card overspending it.
		return fmt.Sprintf("account %s is a settlement account and cannot fund a card", accountID), nil
	case held:
		return fmt.Sprintf("account %s is a card held account and cannot fund a card", accountID), nil
	}
	return "", nil
}

// fingerprintCardRequest tells a key reused for a different card from a retry.
func fingerprintCardRequest(apiKeyID string, req issueCardRequest) string {
	parts := []string{
		apiKeyID,
		req.AccountID,
		strconv.FormatInt(req.PerTransactionLimit, 10),
		strconv.FormatInt(req.DailyLimit, 10),
	}
	// Only when present, so a retry of a card issued before blocked_mccs
	// existed still matches its fingerprint.
	if len(req.BlockedMCCs) > 0 {
		parts = append(parts, strings.Join(req.BlockedMCCs, ","))
	}
	sum := sha256.Sum256([]byte(strings.Join(parts, "|")))
	return hex.EncodeToString(sum[:])
}

// handleCardTransition answers 200 with the card whether it changed or was
// already in the target state, so a retried freeze is not an error.
func (s *Server) handleCardTransition(action cards.Action) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		card, err := cards.Transition(r.Context(), s.db, id, action)
		switch {
		case errors.Is(err, cards.ErrNotFound):
			writeError(w, http.StatusNotFound, CodeNotFound, "no such card: "+id)
		case errors.Is(err, cards.ErrInvalidState):
			writeError(w, http.StatusConflict, CodeInvalidCardState,
				fmt.Sprintf("card %s is %s and cannot %s", id, card.Status, action.Name))
		case err != nil:
			writeInternalError(w, r, err)
		default:
			writeJSON(w, http.StatusOK, toCardResponse(card))
		}
	}
}

func (s *Server) handleGetCard(w http.ResponseWriter, r *http.Request) {
	card, err := cards.Get(r.Context(), s.db, r.PathValue("id"))
	if errors.Is(err, cards.ErrNotFound) {
		writeError(w, http.StatusNotFound, CodeNotFound, "no such card: "+r.PathValue("id"))
		return
	}
	if err != nil {
		writeInternalError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toCardResponse(card))
}
