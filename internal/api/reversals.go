package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/Shailu-s/payments-platform/internal/authorizations"
	"github.com/Shailu-s/payments-platform/internal/holds"
	"github.com/jackc/pgx/v5"
)

type networkReversal struct {
	ReversalID string `json:"reversal_id"`
	AuthID     string `json:"auth_id"`
	Amount     int64  `json:"amount"`
}

// handleNetworkReversal releases (part of) a hold the merchant cancelled. Like
// an advice, a non-2xx is redelivered with backoff, so a failure here is
// retried, not lost; a duplicate reversal_id is acknowledged and ignored.
func (s *Server) handleNetworkReversal(w http.ResponseWriter, r *http.Request) {
	body, ok := s.readNetworkRequest(w, r)
	if !ok {
		return
	}
	var msg networkReversal
	if err := json.Unmarshal(body, &msg); err != nil {
		writeError(w, http.StatusBadRequest, CodeInvalidRequest, "reversal body is not valid json")
		return
	}
	switch {
	case msg.ReversalID == "" || msg.AuthID == "":
		writeError(w, http.StatusBadRequest, CodeInvalidRequest, "reversal_id and auth_id are required")
		return
	case msg.Amount <= 0:
		writeError(w, http.StatusBadRequest, CodeInvalidRequest, "amount must be positive minor units")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), networkDeadline)
	defer cancel()

	status, err := s.applyReversal(ctx, msg)
	if err != nil {
		writeNetworkError(ctx, w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": status, "reversal_id": msg.ReversalID})
}

// applyReversal records the reversal id and its effect in one transaction:
// a release if the authorization holds money, otherwise a parked reversal
// that its approval applies, if it is ever approved. A declined row parks it
// too: a stand-in advice can still turn it into an approval.
func (s *Server) applyReversal(ctx context.Context, msg networkReversal) (string, error) {
	tx, err := s.hotDB().Begin(ctx)
	if err != nil {
		return "", fmt.Errorf("begin reversal %s: %w", msg.ReversalID, err)
	}
	defer tx.Rollback(ctx)

	if err := holds.LockAuthID(ctx, tx, msg.AuthID); err != nil {
		return "", err
	}
	first, err := authorizations.RecordMessage(ctx, tx, msg.ReversalID, authorizations.MessageReversal, msg.AuthID)
	if err != nil {
		return "", err
	}
	if !first {
		return "already_processed", nil
	}
	if _, err := tx.Exec(ctx, setSpendingLockTimeout); err != nil {
		return "", fmt.Errorf("reversal %s: %w", msg.ReversalID, err)
	}

	status := "released"
	a, err := authorizations.GetForUpdate(ctx, tx, msg.AuthID)
	switch {
	case errors.Is(err, authorizations.ErrNotFound) || (err == nil && a.Decision != authorizations.DecisionApproved):
		if err := holds.Park(ctx, tx, msg.ReversalID, msg.AuthID, msg.Amount); err != nil {
			return "", err
		}
		status = "pending"
	case err != nil:
		return "", err
	default:
		released, err := holds.Release(ctx, tx, holderOf(a), msg.Amount, holds.ReasonReversal, msg.ReversalID)
		if err != nil {
			return "", err
		}
		if released == 0 {
			status = "nothing_held"
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return "", fmt.Errorf("commit reversal %s: %w", msg.ReversalID, err)
	}
	return status, nil
}

// holderOf is the row holding a's money: an incremental chain keeps its
// whole hold on the initial authorization.
func holderOf(a authorizations.Authorization) string {
	if a.Kind == authorizations.KindIncremental && a.OriginalAuthID != nil {
		return *a.OriginalAuthID
	}
	return a.AuthID
}

// applyPendingReversals applies the parked reversals of an authorization just
// approved in tx. The caller holds the auth_id lock.
func (s *Server) applyPendingReversals(ctx context.Context, tx pgx.Tx, authID string) error {
	pending, err := holds.PendingFor(ctx, tx, authID)
	if err != nil {
		return err
	}
	return holds.ApplyPending(ctx, tx, authID, pending)
}
