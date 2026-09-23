package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/Shailu-s/payments-platform/internal/transfers"
	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// The provider tells us the outcome here. Per docs/mockbank-api.md this
// delivery is at least once and unordered, and an event can arrive before the
// worker has finished storing the provider reference it refers to.
//
// Phase 6 does the hard version: signatures, replay windows, out-of-order
// events, and references we have never heard of. This is the minimum that is
// correct.
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
		// Malformed: no amount of redelivery will fix it, so say so rather
		// than asking for it again forever.
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
		// We do not recognise this reference yet. Almost always the race the
		// contract warns about: the provider sent the outcome before our
		// worker finished storing the reference.
		//
		// 503 rather than 404, because 4xx tells the provider to stop retrying
		// a delivery we will want in a moment. Asking for it again is the
		// difference between a transfer that settles and one that is stuck.
		writeError(w, http.StatusServiceUnavailable, CodeNotFound,
			"no transfer for that provider_ref yet, retry shortly")
		return
	}
	if err != nil {
		writeInternalError(w, r, err)
		return
	}

	// Record the event before acting on it. The UNIQUE constraint on event_id
	// is what makes a redelivery harmless: the second insert fails, we skip the
	// financial effect, and answer 2xx anyway.
	firstTime, err := s.recordEvent(r.Context(), event)
	if err != nil {
		writeInternalError(w, r, err)
		return
	}
	if !firstTime {
		// Already processed. 2xx, not an error: a duplicate is expected, and
		// answering 4xx would stop redelivery of an event we may still need.
		writeJSON(w, http.StatusOK, map[string]string{
			"status": "already_processed", "event_id": event.EventID,
		})
		return
	}

	switch event.Status {
	case transfers.StatusSettled:
		if err := transfers.Settle(r.Context(), s.db, transferID); err != nil {
			writeInternalError(w, r, err)
			return
		}
	case transfers.StatusFailed:
		reason := "provider reported failed"
		if event.FailureReason != nil {
			reason = *event.FailureReason
		}
		if err := transfers.Fail(r.Context(), s.db, transferID, reason); err != nil {
			writeInternalError(w, r, err)
			return
		}
	default:
		// A status we do not act on. Recorded and acknowledged: asking for
		// redelivery would not change what it says.
		writeJSON(w, http.StatusOK, map[string]string{
			"status": "ignored", "event_id": event.EventID,
		})
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{
		"status": "processed", "event_id": event.EventID, "transfer_id": transferID,
	})
}

// transferForEvent finds the transfer an event is about, by the provider's
// reference or, failing that, by ours.
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

// recordEvent stores the event, reporting whether this is the first time we
// have seen it. Insert first and handle the rejection — the same shape as
// phase 3's idempotency, for the same reason.
func (s *Server) recordEvent(ctx context.Context, event providerEvent) (bool, error) {
	payload, err := json.Marshal(event)
	if err != nil {
		return false, fmt.Errorf("encoding event: %w", err)
	}

	const q = `
		INSERT INTO webhook_events (id, event_id, provider_ref, status, payload)
		VALUES ($1, $2, $3, $4, $5)`

	_, err = s.db.Exec(ctx, q, newID("whe"), event.EventID, event.ProviderRef, event.Status, payload)
	if err == nil {
		return true, nil
	}

	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == pgerrcode.UniqueViolation {
		return false, nil
	}
	return false, fmt.Errorf("record event %s: %w", event.EventID, err)
}
