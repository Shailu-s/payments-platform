package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"
)

type Server struct {
	store      *Store
	behaviour  Behaviour
	webhookURL string
	client     *http.Client

	// Tracked so shutdown can wait for in-flight webhook deliveries rather
	// than cutting them off mid-send.
	pending sync.WaitGroup
}

func NewServer(store *Store, behaviour Behaviour, webhookURL string) *Server {
	return &Server{
		store:      store,
		behaviour:  behaviour,
		webhookURL: webhookURL,
		client:     &http.Client{Timeout: 10 * time.Second},
	}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /transfers", s.handleSubmit)
	mux.HandleFunc("GET /transfers/{ref}", s.handleGetByProviderRef)
	mux.HandleFunc("GET /transfers", s.handleGetByClientReference)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})

	// Operator endpoints. Not part of the payment contract.
	mux.HandleFunc("POST /sandbox/reset", s.handleReset)
	mux.HandleFunc("GET /sandbox/transfers", s.handleList)

	return mux
}

type submitRequest struct {
	ClientReference string `json:"client_reference"`
	Amount          int64  `json:"amount"`
	Currency        string `json:"currency"`
	Source          string `json:"source"`
	Destination     string `json:"destination"`
}

func (s *Server) handleSubmit(w http.ResponseWriter, r *http.Request) {
	// Asked before the work, so a scenario can make this call time out from the
	// caller's point of view while the instruction is still accepted — which is
	// the failure the contract warns about.
	if delay := s.behaviour.SubmitDelay(); delay > 0 {
		time.Sleep(delay)
	}

	var req submitRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10)).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "body is not valid json")
		return
	}

	if msg, ok := validateSubmit(&req); !ok {
		writeError(w, http.StatusBadRequest, "invalid_request", msg)
		return
	}

	payment, duplicate, err := s.store.Submit(Payment{
		ClientReference: req.ClientReference,
		Amount:          req.Amount,
		Currency:        req.Currency,
		Source:          req.Source,
		Destination:     req.Destination,
	})
	if errors.Is(err, ErrReferenceConflict) {
		writeError(w, http.StatusConflict, "reference_conflict",
			"this client_reference was already used with different terms")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "could not record the instruction")
		return
	}

	// A duplicate returns the original and starts no second decision: the
	// money moves once.
	if !duplicate {
		s.scheduleOutcome(*payment)
	}

	slog.Info("instruction accepted",
		"provider_ref", payment.ProviderRef,
		"client_reference", payment.ClientReference,
		"amount", payment.Amount,
		"duplicate", duplicate)

	writeJSON(w, http.StatusAccepted, payment)
}

func validateSubmit(req *submitRequest) (string, bool) {
	req.ClientReference = strings.TrimSpace(req.ClientReference)
	req.Currency = strings.ToUpper(strings.TrimSpace(req.Currency))

	switch {
	case req.ClientReference == "":
		return "client_reference is required", false
	case len(req.ClientReference) > 255:
		return "client_reference must be at most 255 characters", false
	case req.Amount <= 0:
		return "amount must be a positive integer in minor units", false
	case req.Currency != "USD":
		return "currency must be USD", false
	case req.Source == "" || len(req.Source) > 255:
		return "source is required and must be at most 255 characters", false
	case req.Destination == "" || len(req.Destination) > 255:
		return "destination is required and must be at most 255 characters", false
	}
	return "", true
}

// scheduleOutcome decides the payment's fate and delivers the webhook, later
// and out of band. The caller's request has already returned by then, which is
// the entire point: a rail does not finish while you wait.
func (s *Server) scheduleOutcome(p Payment) {
	status, reason, after := s.behaviour.Outcome(p)

	s.pending.Add(1)
	go func() {
		defer s.pending.Done()
		time.Sleep(after)

		settled, changed, err := s.store.Settle(p.ProviderRef, status, reason)
		if err != nil || !changed {
			return
		}
		s.deliverWebhook(*settled)
	}()
}

type webhookEvent struct {
	EventID         string    `json:"event_id"`
	ProviderRef     string    `json:"provider_ref"`
	ClientReference string    `json:"client_reference"`
	Status          string    `json:"status"`
	Amount          int64     `json:"amount"`
	Currency        string    `json:"currency"`
	OccurredAt      time.Time `json:"occurred_at"`
	FailureReason   *string   `json:"failure_reason"`
}

// deliverWebhook posts the outcome, retrying on 5xx and transport failures with
// the backoff the contract promises. The same event_id is sent every time, so a
// receiver can tell a redelivery from a new event — and a receiver that cannot
// will double-count money, which is what guarantee 4 exists to prevent.
func (s *Server) deliverWebhook(p Payment) {
	if s.webhookURL == "" {
		return
	}

	event := webhookEvent{
		EventID:         newEventID(),
		ProviderRef:     p.ProviderRef,
		ClientReference: p.ClientReference,
		Status:          p.Status,
		Amount:          p.Amount,
		Currency:        p.Currency,
		OccurredAt:      time.Now().UTC(),
		FailureReason:   p.FailureReason,
	}
	body, err := json.Marshal(event)
	if err != nil {
		slog.Error("encoding webhook", "error", err)
		return
	}

	// Some behaviours deliver the same event more than once, to exercise the
	// receiver's idempotency.
	for attempt := 0; attempt < s.behaviour.WebhookAttempts(); attempt++ {
		s.deliverOnce(event, body)
	}
}

func (s *Server) deliverOnce(event webhookEvent, body []byte) {
	backoff := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second}

	for attempt := 0; ; attempt++ {
		req, err := http.NewRequest(http.MethodPost, s.webhookURL, bytes.NewReader(body))
		if err != nil {
			slog.Error("building webhook request", "error", err)
			return
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-MockBank-Event-Id", event.EventID)

		resp, err := s.client.Do(req)
		if err == nil {
			status := resp.StatusCode
			resp.Body.Close()

			switch {
			case status >= 200 && status < 300:
				slog.Info("webhook delivered",
					"event_id", event.EventID, "provider_ref", event.ProviderRef, "attempt", attempt+1)
				return
			case status >= 400 && status < 500:
				// Permanently rejected. Retrying a receiver that has refused
				// the event only wastes both sides' time.
				slog.Warn("webhook permanently rejected",
					"event_id", event.EventID, "status", status)
				return
			}
			slog.Warn("webhook failed, will retry",
				"event_id", event.EventID, "status", status, "attempt", attempt+1)
		} else {
			slog.Warn("webhook transport failure, will retry",
				"event_id", event.EventID, "error", err, "attempt", attempt+1)
		}

		if attempt >= len(backoff) {
			slog.Error("webhook given up",
				"event_id", event.EventID, "provider_ref", event.ProviderRef, "attempts", attempt+1)
			return
		}
		time.Sleep(backoff[attempt])
	}
}

func (s *Server) handleGetByProviderRef(w http.ResponseWriter, r *http.Request) {
	payment, err := s.store.ByProviderRef(r.PathValue("ref"))
	if errors.Is(err, ErrNotFound) {
		writeError(w, http.StatusNotFound, "not_found", "no payment with that provider_ref")
		return
	}
	writeJSON(w, http.StatusOK, payment)
}

// handleGetByClientReference is how a caller resolves a submit that timed out
// before it ever learned a provider_ref. Without it that state is unrecoverable.
func (s *Server) handleGetByClientReference(w http.ResponseWriter, r *http.Request) {
	ref := strings.TrimSpace(r.URL.Query().Get("client_reference"))
	if ref == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "client_reference is required")
		return
	}

	payment, err := s.store.ByClientReference(ref)
	if errors.Is(err, ErrNotFound) {
		writeError(w, http.StatusNotFound, "not_found", "no payment with that client_reference")
		return
	}
	writeJSON(w, http.StatusOK, payment)
}

func (s *Server) handleReset(w http.ResponseWriter, r *http.Request) {
	s.store.Reset()
	writeJSON(w, http.StatusOK, map[string]string{"status": "reset"})
}

func (s *Server) handleList(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"data": s.store.All()})
}

// WaitForPending blocks until scheduled outcomes and their webhooks have
// finished, so shutdown does not cut a delivery in half.
func (s *Server) WaitForPending(ctx context.Context) {
	done := make(chan struct{})
	go func() {
		s.pending.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		slog.Error("writing response", "error", err)
	}
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, map[string]any{
		"error": map[string]string{"code": code, "message": message},
	})
}

func newEventID() string {
	var b [6]byte
	_, _ = rand.Read(b[:])
	return "evt_" + hex.EncodeToString(b[:])
}
