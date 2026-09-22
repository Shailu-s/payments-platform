package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/Shailu-s/payments-platform/internal/ledger"
)

// sendWebhook posts a provider event. Unauthenticated, because the rail has no
// api key of ours — phase 6 authenticates it with a signature instead.
func sendWebhook(h http.Handler, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest("POST", "/v1/webhooks/mockbank", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func eventFor(eventID, providerRef, transferID, status string) string {
	return fmt.Sprintf(
		`{"event_id":%q,"provider_ref":%q,"client_reference":%q,"status":%q,
		  "amount":50000,"currency":"USD","occurred_at":"2026-09-22T10:00:00Z"}`,
		eventID, providerRef, transferID, status)
}

// sentTransfer creates a transfer that the worker has already submitted, so it
// is waiting for exactly the webhook these tests send.
func sentTransfer(t *testing.T, h http.Handler, apiKey, providerRef string) (id, source, destination string) {
	t.Helper()
	ctx := context.Background()

	src := createAccount(t, h, apiKey, "asset")
	dst := createAccount(t, h, apiKey, "liability")
	fund(t, src.ID, 1000000)

	created := createTransfer(t, h, apiKey, src.ID, dst.ID, 50000)

	if _, err := testPool.Exec(ctx,
		`UPDATE transfers SET provider_ref = $1 WHERE id = $2`, providerRef, created.ID); err != nil {
		t.Fatalf("set provider_ref: %v", err)
	}
	return created.ID, src.ID, dst.ID
}

func TestWebhookSettlesATransfer(t *testing.T) {
	h, apiKey := newTestServer(t)
	ctx := context.Background()

	id, _, destination := sentTransfer(t, h, apiKey, "mb_settle")

	rec := sendWebhook(h, eventFor("evt_1", "mb_settle", id, "settled"))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", rec.Code, rec.Body.String())
	}

	var got struct{ Status string }
	json.Unmarshal(rec.Body.Bytes(), &got)
	if got.Status != "processed" {
		t.Errorf("status = %q, want processed", got.Status)
	}

	var status string
	if err := testPool.QueryRow(ctx, `SELECT status FROM transfers WHERE id = $1`, id).Scan(&status); err != nil {
		t.Fatalf("read status: %v", err)
	}
	if status != "settled" {
		t.Errorf("transfer status = %q, want settled", status)
	}

	// This is the moment the destination is finally credited.
	balance, err := ledger.Balance(ctx, testPool, destination)
	if err != nil {
		t.Fatalf("balance: %v", err)
	}
	if balance != 50000 {
		t.Errorf("destination balance = %d, want 50000", balance)
	}
}

// ⭐ Guarantee 4: duplicate provider events never duplicate financial effects.
func TestDuplicateWebhookHasOneFinancialEffect(t *testing.T) {
	h, apiKey := newTestServer(t)
	ctx := context.Background()

	id, _, destination := sentTransfer(t, h, apiKey, "mb_dupe")
	event := eventFor("evt_same", "mb_dupe", id, "settled")

	for i := 0; i < 6; i++ {
		rec := sendWebhook(h, event)
		// Every delivery is acknowledged. Answering 4xx to a duplicate would
		// tell the provider to stop retrying an event we may still need.
		if rec.Code != http.StatusOK {
			t.Fatalf("delivery %d status = %d, want 200: %s", i, rec.Code, rec.Body.String())
		}
	}

	balance, err := ledger.Balance(ctx, testPool, destination)
	if err != nil {
		t.Fatalf("balance: %v", err)
	}
	if balance != 50000 {
		t.Errorf("destination balance = %d after six deliveries, want 50000: "+
			"the money moved %d times", balance, balance/50000)
	}

	var settlements int
	if err := testPool.QueryRow(ctx,
		`SELECT count(*) FROM ledger_transactions WHERE reference = $1`,
		"settlement "+id).Scan(&settlements); err != nil {
		t.Fatalf("count: %v", err)
	}
	if settlements != 1 {
		t.Errorf("%d settlement ledger transactions, want 1", settlements)
	}

	var events int
	if err := testPool.QueryRow(ctx,
		`SELECT count(*) FROM webhook_events WHERE event_id = 'evt_same'`).Scan(&events); err != nil {
		t.Fatalf("count events: %v", err)
	}
	if events != 1 {
		t.Errorf("%d rows for one event_id, want 1", events)
	}
}

// The same event delivered concurrently, which is what a provider retrying
// under load actually does.
func TestConcurrentDuplicateWebhooksHaveOneEffect(t *testing.T) {
	h, apiKey := newTestServer(t)
	ctx := context.Background()

	id, _, destination := sentTransfer(t, h, apiKey, "mb_concurrent")
	event := eventFor("evt_concurrent", "mb_concurrent", id, "settled")

	const deliveries = 20
	var accepted atomic.Int64

	var start sync.WaitGroup
	start.Add(1)
	var done sync.WaitGroup
	for i := 0; i < deliveries; i++ {
		done.Add(1)
		go func() {
			defer done.Done()
			start.Wait()
			if sendWebhook(h, event).Code == http.StatusOK {
				accepted.Add(1)
			}
		}()
	}
	start.Done()
	done.Wait()

	if got := accepted.Load(); got != deliveries {
		t.Errorf("%d of %d deliveries acknowledged, want all", got, deliveries)
	}

	balance, err := ledger.Balance(ctx, testPool, destination)
	if err != nil {
		t.Fatalf("balance: %v", err)
	}
	if balance != 50000 {
		t.Errorf("destination balance = %d after %d concurrent deliveries, want 50000",
			balance, deliveries)
	}
}

// A different event for an already-settled transfer. Deduplication does not
// catch this one — the status guard in Settle does, which is why there are two
// defences rather than one.
func TestNewEventForASettledTransferMovesNoMoney(t *testing.T) {
	h, apiKey := newTestServer(t)
	ctx := context.Background()

	id, _, destination := sentTransfer(t, h, apiKey, "mb_twice")

	if rec := sendWebhook(h, eventFor("evt_first", "mb_twice", id, "settled")); rec.Code != http.StatusOK {
		t.Fatalf("first delivery status = %d", rec.Code)
	}
	if rec := sendWebhook(h, eventFor("evt_second", "mb_twice", id, "settled")); rec.Code != http.StatusOK {
		t.Fatalf("second delivery status = %d", rec.Code)
	}

	balance, err := ledger.Balance(ctx, testPool, destination)
	if err != nil {
		t.Fatalf("balance: %v", err)
	}
	if balance != 50000 {
		t.Errorf("destination balance = %d, want 50000: a second event with a new "+
			"id must not credit the destination again", balance)
	}
}

// A failure event returns the money to the source with a new reversing
// transaction.
func TestWebhookFailureReversesTheTransfer(t *testing.T) {
	h, apiKey := newTestServer(t)
	ctx := context.Background()

	id, source, destination := sentTransfer(t, h, apiKey, "mb_failed")

	body := fmt.Sprintf(
		`{"event_id":"evt_fail","provider_ref":"mb_failed","client_reference":%q,
		  "status":"failed","amount":50000,"currency":"USD",
		  "occurred_at":"2026-09-22T10:00:00Z","failure_reason":"blocked account"}`, id)

	if rec := sendWebhook(h, body); rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", rec.Code, rec.Body.String())
	}

	var status string
	if err := testPool.QueryRow(ctx, `SELECT status FROM transfers WHERE id = $1`, id).Scan(&status); err != nil {
		t.Fatalf("read status: %v", err)
	}
	if status != "failed" {
		t.Errorf("status = %q, want failed", status)
	}

	// The money is back, and the destination never received it.
	sourceBalance, _ := ledger.Balance(ctx, testPool, source)
	if sourceBalance != 1000000 {
		t.Errorf("source balance = %d, want 1000000: the money should be returned", sourceBalance)
	}
	destinationBalance, _ := ledger.Balance(ctx, testPool, destination)
	if destinationBalance != 0 {
		t.Errorf("destination balance = %d, want 0", destinationBalance)
	}
}

// An unknown reference is 503, not 404. A 4xx tells the provider to stop
// retrying a delivery we will want once the worker has stored the reference.
func TestWebhookForAnUnknownReferenceAsksForRedelivery(t *testing.T) {
	h, _ := newTestServer(t)

	rec := sendWebhook(h, eventFor("evt_unknown", "mb_never_seen", "", "settled"))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503: a 4xx would stop the provider retrying "+
			"a delivery we may still need", rec.Code)
	}
}

// The race the contract warns about: the event arrives before the worker has
// stored the provider reference. The provider echoes our own reference back,
// which is enough to find the transfer.
func TestWebhookArrivingBeforeTheProviderRefIsStored(t *testing.T) {
	h, apiKey := newTestServer(t)
	ctx := context.Background()

	source := createAccount(t, h, apiKey, "asset")
	destination := createAccount(t, h, apiKey, "liability")
	fund(t, source.ID, 1000000)
	created := createTransfer(t, h, apiKey, source.ID, destination.ID, 50000)
	// Deliberately NOT setting provider_ref: the worker has not got there yet.

	rec := sendWebhook(h, eventFor("evt_early", "mb_not_stored_yet", created.ID, "settled"))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: the provider echoes our reference back, "+
			"which is enough to match the transfer: %s", rec.Code, rec.Body.String())
	}

	balance, err := ledger.Balance(ctx, testPool, destination.ID)
	if err != nil {
		t.Fatalf("balance: %v", err)
	}
	if balance != 50000 {
		t.Errorf("destination balance = %d, want 50000", balance)
	}
}

func TestWebhookRejectsMalformedEvents(t *testing.T) {
	h, _ := newTestServer(t)

	tests := []struct{ name, body string }{
		{"not json", `nonsense`},
		{"no event_id", `{"provider_ref":"mb_x","status":"settled"}`},
		{"no provider_ref", `{"event_id":"evt_x","status":"settled"}`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rec := sendWebhook(h, tc.body)
			if rec.Code != http.StatusBadRequest {
				t.Errorf("status = %d, want 400: redelivery cannot fix a malformed "+
					"event, so asking for it again is wrong", rec.Code)
			}
		})
	}
}
