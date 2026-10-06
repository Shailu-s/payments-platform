package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/Shailu-s/payments-platform/internal/transfers"
	"github.com/jackc/pgx/v5"
)

// Delivery is at least once and unordered; events may precede stored provider references.
// This endpoint does not yet verify signatures or enforce a replay window.
type providerEvent struct {
	EventID         string    `json:"event_id"`
	ProviderRef     string    `json:"provider_ref"`
	ClientReference string    `json:"client_reference"`
	Status          string    `json:"status"`
	Amount          int64     `json:"amount"`
	Currency        string    `json:"currency"`
	OccurredAt      time.Time `json:"occurred_at"`
	FailureReason   *string   `json:"failure_reason"`
}

func (s *Server) handleProviderWebhook(w http.ResponseWriter, r *http.Request) {
	var event providerEvent
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10)).Decode(&event); err != nil {
		// Malformed events cannot be fixed by redelivery.
		writeError(w, http.StatusBadRequest, CodeInvalidRequest, "event body is not valid json")
		return
	}
	if event.EventID == "" || event.ProviderRef == "" {
		writeError(w, http.StatusBadRequest, CodeInvalidRequest,
			"event_id and provider_ref are required")
		return
	}

	transferID, err := s.transferForEvent(r.Context(), event)
	if errors.Is(err, transfers.ErrNotFound) {
		// The transfer may become visible later; 4xx would stop provider retries.
		writeError(w, http.StatusServiceUnavailable, CodeNotFound,
			"no transfer for that provider_ref yet, retry shortly")
		return
	}
	if err != nil {
		writeInternalError(w, r, err)
		return
	}

	// The event ID and its financial effect commit together.
	// A failed attempt must leave the event available for redelivery.
	status, err := s.applyProviderEvent(r.Context(), event, transferID)
	if err != nil {
		writeInternalError(w, r, err)
		return
	}
	response := map[string]string{"status": status, "event_id": event.EventID}
	if status == "processed" {
		response["transfer_id"] = transferID
	}
	writeJSON(w, http.StatusOK, response)
}

func (s *Server) applyProviderEvent(ctx context.Context, event providerEvent, transferID string) (string, error) {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return "", fmt.Errorf("begin webhook %s: %w", event.EventID, err)
	}
	defer tx.Rollback(ctx)

	firstTime, err := recordEvent(ctx, tx, event)
	if err != nil {
		return "", err
	}
	if !firstTime {
		return "already_processed", nil
	}
	status := "processed"
	switch event.Status {
	case transfers.StatusSettled:
		err = transfers.Settle(ctx, tx, transferID)
	case transfers.StatusFailed:
		reason := "provider reported failed"
		if event.FailureReason != nil {
			reason = *event.FailureReason
		}
		err = transfers.Fail(ctx, tx, transferID, reason)
	default:
		// Redelivery cannot make an unsupported status actionable.
		status = "ignored"
	}
	if err != nil {
		return "", err
	}
	if err := tx.Commit(ctx); err != nil {
		return "", fmt.Errorf("commit webhook %s: %w", event.EventID, err)
	}
	return status, nil
}

func (s *Server) transferForEvent(ctx context.Context, event providerEvent) (string, error) {
	const byProviderRef = `SELECT id FROM transfers WHERE provider_ref = $1`

	var id string
	err := s.db.QueryRow(ctx, byProviderRef, event.ProviderRef).Scan(&id)
	if err == nil {
		return id, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return "", fmt.Errorf("find transfer by provider_ref: %w", err)
	}

	// The provider echoes our reference back, which rescues the race where the
	// event arrives before the worker stored the provider_ref.
	if event.ClientReference == "" {
		return "", transfers.ErrNotFound
	}
	const byID = `SELECT id FROM transfers WHERE id = $1`
	if err := s.db.QueryRow(ctx, byID, event.ClientReference).Scan(&id); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", transfers.ErrNotFound
		}
		return "", fmt.Errorf("find transfer by client reference: %w", err)
	}
	return id, nil
}

// Insert first: checking for an event before inserting would race concurrent deliveries.
func recordEvent(ctx context.Context, tx pgx.Tx, event providerEvent) (bool, error) {
	payload, err := json.Marshal(event)
	if err != nil {
		return false, fmt.Errorf("encoding event: %w", err)
	}

	const q = `
		INSERT INTO webhook_events (id, event_id, provider_ref, status, payload)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (event_id) DO NOTHING`

	tag, err := tx.Exec(ctx, q, newID("whe"), event.EventID, event.ProviderRef, event.Status, payload)
	if err != nil {
		return false, fmt.Errorf("record event %s: %w", event.EventID, err)
	}
	return tag.RowsAffected() == 1, nil
}
