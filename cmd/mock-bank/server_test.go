package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// These test MockBank against docs/mockbank-api.md — the promises it makes to a
// client, not its internals. A client is written against that document, so a
// promise MockBank quietly breaks is a bug in the integration nobody would see
// until the client mishandles the real thing.

func newTestBank(t *testing.T, behaviour Behaviour, webhookURL string) http.Handler {
	t.Helper()
	return NewServer(NewStore(), behaviour, webhookURL).Handler()
}

func submit(t *testing.T, h http.Handler, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest("POST", "/transfers", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func submitBody(clientRef string, amount int64) string {
	return fmt.Sprintf(
		`{"client_reference":%q,"amount":%d,"currency":"USD","source":"acc_a","destination":"acc_b"}`,
		clientRef, amount)
}

func decodePayment(t *testing.T, rec *httptest.ResponseRecorder) Payment {
	t.Helper()
	var p Payment
	if err := json.Unmarshal(rec.Body.Bytes(), &p); err != nil {
		t.Fatalf("decode payment: %v\nbody: %s", err, rec.Body.String())
	}
	return p
}

// The contract says 202, never 200: the instruction is accepted and the money
// has not moved.
func TestSubmitReturns202Processing(t *testing.T) {
	h := newTestBank(t, wellBehaved{settleDelay: time.Hour}, "")

	rec := submit(t, h, submitBody("tr_1", 50000))
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202: %s", rec.Code, rec.Body.String())
	}

	p := decodePayment(t, rec)
	if p.Status != StatusProcessing {
		t.Errorf("status = %q, want %q", p.Status, StatusProcessing)
	}
	if !strings.HasPrefix(p.ProviderRef, "mb_") {
		t.Errorf("provider_ref = %q, want an mb_ prefix", p.ProviderRef)
	}
	if p.SettledAt != nil {
		t.Error("settled_at is set on a payment that has not settled")
	}
}

// ⭐ The property the whole integration rests on: the same client_reference
// returns the same provider_ref and moves money once. This is what makes a
// caller's retry after a timeout safe.
func TestSameClientReferenceReturnsTheSamePayment(t *testing.T) {
	h := newTestBank(t, wellBehaved{settleDelay: time.Hour}, "")

	first := decodePayment(t, submit(t, h, submitBody("tr_same", 50000)))
	second := decodePayment(t, submit(t, h, submitBody("tr_same", 50000)))

	if first.ProviderRef != second.ProviderRef {
		t.Errorf("provider refs differ: %q then %q: the same instruction was "+
			"recorded as two payments", first.ProviderRef, second.ProviderRef)
	}

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/sandbox/transfers", nil))
	var listed struct {
		Data []Payment `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &listed); err != nil {
		t.Fatalf("decode list: %v", err)
	}
	if len(listed.Data) != 1 {
		t.Errorf("%d payments recorded, want 1", len(listed.Data))
	}
}

// Deduplication must survive concurrent submissions, because a retrying client
// can easily have two attempts in flight at once.
func TestConcurrentDuplicateSubmissionsRecordOnePayment(t *testing.T) {
	h := newTestBank(t, wellBehaved{settleDelay: time.Hour}, "")

	const attempts = 30
	refs := make([]string, attempts)

	var start sync.WaitGroup
	start.Add(1)
	var done sync.WaitGroup
	for i := 0; i < attempts; i++ {
		done.Add(1)
		go func(i int) {
			defer done.Done()
			start.Wait()
			refs[i] = decodePayment(t, submit(t, h, submitBody("tr_concurrent", 50000))).ProviderRef
		}(i)
	}
	start.Done()
	done.Wait()

	distinct := map[string]bool{}
	for _, ref := range refs {
		distinct[ref] = true
	}
	if len(distinct) != 1 {
		t.Errorf("%d distinct provider refs from %d concurrent duplicates, want 1",
			len(distinct), attempts)
	}
}

// Same reference, different terms is a caller bug, and answering with the
// original payment would hide it.
func TestReferenceReusedWithDifferentTermsIs409(t *testing.T) {
	h := newTestBank(t, wellBehaved{settleDelay: time.Hour}, "")

	if rec := submit(t, h, submitBody("tr_conflict", 50000)); rec.Code != http.StatusAccepted {
		t.Fatalf("first status = %d, want 202", rec.Code)
	}

	rec := submit(t, h, submitBody("tr_conflict", 80000))
	if rec.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409: %s", rec.Code, rec.Body.String())
	}
	if code := errorCode(t, rec); code != "reference_conflict" {
		t.Errorf("error code = %q, want reference_conflict", code)
	}
}

func TestSubmitRejectsBadRequests(t *testing.T) {
	h := newTestBank(t, wellBehaved{settleDelay: time.Hour}, "")

	tests := []struct{ name, body string }{
		{"not json", `nonsense`},
		{"missing client_reference", `{"amount":500,"currency":"USD","source":"a","destination":"b"}`},
		{"blank client_reference", `{"client_reference":"  ","amount":500,"currency":"USD","source":"a","destination":"b"}`},
		{"zero amount", `{"client_reference":"r","amount":0,"currency":"USD","source":"a","destination":"b"}`},
		{"negative amount", `{"client_reference":"r","amount":-5,"currency":"USD","source":"a","destination":"b"}`},
		{"fractional amount", `{"client_reference":"r","amount":500.75,"currency":"USD","source":"a","destination":"b"}`},
		{"wrong currency", `{"client_reference":"r","amount":500,"currency":"EUR","source":"a","destination":"b"}`},
		{"missing source", `{"client_reference":"r","amount":500,"currency":"USD","destination":"b"}`},
		{"oversized reference", fmt.Sprintf(`{"client_reference":%q,"amount":500,"currency":"USD","source":"a","destination":"b"}`, strings.Repeat("x", 256))},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rec := submit(t, h, tc.body)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400: %s", rec.Code, rec.Body.String())
			}
			if code := errorCode(t, rec); code != "invalid_request" {
				t.Errorf("error code = %q, want invalid_request", code)
			}
		})
	}
}

func TestGetByProviderRef(t *testing.T) {
	h := newTestBank(t, wellBehaved{settleDelay: time.Hour}, "")
	created := decodePayment(t, submit(t, h, submitBody("tr_get", 50000)))

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/transfers/"+created.ProviderRef, nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", rec.Code, rec.Body.String())
	}
	if got := decodePayment(t, rec); got.ProviderRef != created.ProviderRef {
		t.Errorf("provider_ref = %q, want %q", got.ProviderRef, created.ProviderRef)
	}

	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/transfers/mb_nonexistent", nil))
	if rec.Code != http.StatusNotFound {
		t.Errorf("unknown ref status = %d, want 404", rec.Code)
	}
}

// ⭐ How a caller resolves a submit that timed out before it ever learned a
// provider_ref. Without this endpoint that state is unrecoverable.
func TestGetByClientReferenceRescuesAnUnknownOutcome(t *testing.T) {
	h := newTestBank(t, wellBehaved{settleDelay: time.Hour}, "")

	// The caller submitted and never saw the response.
	created := decodePayment(t, submit(t, h, submitBody("tr_lost_response", 50000)))

	// All it has is its own reference.
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/transfers?client_reference=tr_lost_response", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", rec.Code, rec.Body.String())
	}
	if got := decodePayment(t, rec); got.ProviderRef != created.ProviderRef {
		t.Errorf("provider_ref = %q, want %q: the caller could not recover its payment",
			got.ProviderRef, created.ProviderRef)
	}

	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/transfers?client_reference=tr_never_sent", nil))
	if rec.Code != http.StatusNotFound {
		t.Errorf("unknown reference status = %d, want 404", rec.Code)
	}
}

// The webhook arrives later, on its own, with nobody watching.
func TestWebhookIsDeliveredAfterSettlement(t *testing.T) {
	var received atomic.Int64
	events := make(chan webhookEvent, 4)

	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var event webhookEvent
		json.NewDecoder(r.Body).Decode(&event)
		received.Add(1)
		events <- event
		w.WriteHeader(http.StatusOK)
	}))
	defer receiver.Close()

	h := newTestBank(t, wellBehaved{settleDelay: 10 * time.Millisecond}, receiver.URL)
	created := decodePayment(t, submit(t, h, submitBody("tr_webhook", 50000)))

	select {
	case event := <-events:
		if event.ProviderRef != created.ProviderRef {
			t.Errorf("event provider_ref = %q, want %q", event.ProviderRef, created.ProviderRef)
		}
		if event.Status != StatusSettled {
			t.Errorf("event status = %q, want %q", event.Status, StatusSettled)
		}
		if event.EventID == "" {
			t.Error("event has no event_id, so a receiver cannot deduplicate it")
		}
		if event.Amount != 50000 {
			t.Errorf("event amount = %d, want 50000", event.Amount)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no webhook arrived")
	}
}

// A receiver answering 5xx must see the delivery again; one answering 4xx must
// not. Both are promises in the contract.
func TestWebhookRetriesOn5xxAndStopsOn4xx(t *testing.T) {
	t.Run("5xx is retried", func(t *testing.T) {
		var attempts atomic.Int64
		done := make(chan struct{})
		receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if attempts.Add(1) == 1 {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			w.WriteHeader(http.StatusOK)
			select {
			case <-done:
			default:
				close(done)
			}
		}))
		defer receiver.Close()

		h := newTestBank(t, wellBehaved{settleDelay: time.Millisecond}, receiver.URL)
		submit(t, h, submitBody("tr_retry", 50000))

		select {
		case <-done:
			if got := attempts.Load(); got < 2 {
				t.Errorf("%d delivery attempts, want at least 2", got)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("delivery never succeeded after %d attempts", attempts.Load())
		}
	})

	t.Run("4xx is not retried", func(t *testing.T) {
		var attempts atomic.Int64
		receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			attempts.Add(1)
			w.WriteHeader(http.StatusBadRequest)
		}))
		defer receiver.Close()

		h := newTestBank(t, wellBehaved{settleDelay: time.Millisecond}, receiver.URL)
		submit(t, h, submitBody("tr_rejected", 50000))

		time.Sleep(2 * time.Second)
		if got := attempts.Load(); got != 1 {
			t.Errorf("%d delivery attempts after a 4xx, want exactly 1", got)
		}
	})
}

// A duplicate submission must not produce a second webhook: the money moved
// once, so the outcome is announced once.
func TestDuplicateSubmissionDoesNotSendASecondWebhook(t *testing.T) {
	var received atomic.Int64
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		received.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer receiver.Close()

	h := newTestBank(t, wellBehaved{settleDelay: 10 * time.Millisecond}, receiver.URL)
	submit(t, h, submitBody("tr_once", 50000))
	submit(t, h, submitBody("tr_once", 50000))
	submit(t, h, submitBody("tr_once", 50000))

	time.Sleep(1 * time.Second)
	if got := received.Load(); got != 1 {
		t.Errorf("%d webhooks for three submissions of one reference, want 1", got)
	}
}

func TestSandboxReset(t *testing.T) {
	h := newTestBank(t, wellBehaved{settleDelay: time.Hour}, "")
	submit(t, h, submitBody("tr_reset", 50000))

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("POST", "/sandbox/reset", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("reset status = %d, want 200", rec.Code)
	}

	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/sandbox/transfers", nil))
	var listed struct {
		Data []Payment `json:"data"`
	}
	json.Unmarshal(rec.Body.Bytes(), &listed)
	if len(listed.Data) != 0 {
		t.Errorf("%d payments survived a reset, want 0", len(listed.Data))
	}
}

func errorCode(t *testing.T, rec *httptest.ResponseRecorder) string {
	t.Helper()
	var body struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("response is not the house error shape: %v\nbody: %s", err, rec.Body.String())
	}
	return body.Error.Code
}
