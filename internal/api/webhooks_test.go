package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Shailu-s/payments-platform/internal/ledger"
	"github.com/Shailu-s/payments-platform/internal/webhooks"
)

const testWebhookSecret = "test-only-webhook-secret-never-use-in-production"

// Provider callbacks bypass customer API-key authentication.
func sendWebhook(h http.Handler, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest("POST", "/v1/webhooks/mockbank", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	timestamp := strconv.FormatInt(time.Now().Unix(), 10)
	req.Header.Set(webhooks.TimestampHeader, timestamp)
	req.Header.Set(webhooks.SignatureHeader, webhooks.Sign([]byte(testWebhookSecret), timestamp, []byte(body)))
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

	balance, err := ledger.Balance(ctx, testPool, destination)
	if err != nil {
		t.Fatalf("balance: %v", err)
	}
	if balance != 50000 {
		t.Errorf("destination balance = %d, want 50000", balance)
	}
}

func TestUnsignedWebhookCannotMoveMoney(t *testing.T) {
	h, apiKey := newTestServer(t)
	id, _, destination := sentTransfer(t, h, apiKey, "mb_unsigned")
	req := httptest.NewRequest("POST", "/v1/webhooks/mockbank", strings.NewReader(eventFor("evt_unsigned", "mb_unsigned", id, "settled")))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("unsigned webhook = %d, want 401", rec.Code)
	}
	if got := countRows(t, "webhook_events"); got != 0 {
		t.Errorf("unsigned webhook recorded %d events, want 0", got)
	}
	balance, err := ledger.Balance(context.Background(), testPool, destination)
	if err != nil {
		t.Fatal(err)
	}
	if balance != 0 {
		t.Errorf("unsigned webhook credited %d, want 0", balance)
	}
}

func TestWebhookRejectsInvalidSignaturesWithoutEffects(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*http.Request, string)
	}{
		{"missing timestamp", func(r *http.Request, _ string) { r.Header.Del(webhooks.TimestampHeader) }},
		{"missing signature", func(r *http.Request, _ string) { r.Header.Del(webhooks.SignatureHeader) }},
		{"wrong key", func(r *http.Request, body string) {
			r.Header.Set(webhooks.SignatureHeader, webhooks.Sign([]byte(strings.Repeat("x", 32)), r.Header.Get(webhooks.TimestampHeader), []byte(body)))
		}},
		{"altered timestamp", func(r *http.Request, _ string) {
			r.Header.Set(webhooks.TimestampHeader, strconv.FormatInt(time.Now().Unix()+60, 10))
		}},
		{"old timestamp", func(r *http.Request, body string) {
			timestamp := strconv.FormatInt(time.Now().Unix()-600, 10)
			r.Header.Set(webhooks.TimestampHeader, timestamp)
			r.Header.Set(webhooks.SignatureHeader, webhooks.Sign([]byte(testWebhookSecret), timestamp, []byte(body)))
		}},
		{"future timestamp", func(r *http.Request, body string) {
			timestamp := strconv.FormatInt(time.Now().Unix()+600, 10)
			r.Header.Set(webhooks.TimestampHeader, timestamp)
			r.Header.Set(webhooks.SignatureHeader, webhooks.Sign([]byte(testWebhookSecret), timestamp, []byte(body)))
		}},
		{"malformed signature", func(r *http.Request, _ string) { r.Header.Set(webhooks.SignatureHeader, "v1="+strings.Repeat("g", 64)) }},
		{"altered body whitespace", func(r *http.Request, body string) {
			r.Body = io.NopCloser(strings.NewReader(body + " "))
			r.ContentLength = int64(len(body) + 1)
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h, apiKey := newTestServer(t)
			id, _, destination := sentTransfer(t, h, apiKey, "mb_signature")
			body := eventFor("evt_signature", "mb_signature", id, "settled")
			req := httptest.NewRequest("POST", "/v1/webhooks/mockbank", strings.NewReader(body))
			timestamp := strconv.FormatInt(time.Now().Unix(), 10)
			req.Header.Set(webhooks.TimestampHeader, timestamp)
			req.Header.Set(webhooks.SignatureHeader, webhooks.Sign([]byte(testWebhookSecret), timestamp, []byte(body)))
			tc.mutate(req, body)
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			if rec.Code != http.StatusUnauthorized {
				t.Errorf("status = %d, want 401: %s", rec.Code, rec.Body.String())
			}
			if got := countRows(t, "webhook_events"); got != 0 {
				t.Errorf("invalid signature recorded %d events, want 0", got)
			}
			balance, err := ledger.Balance(context.Background(), testPool, destination)
			if err != nil {
				t.Fatal(err)
			}
			if balance != 0 {
				t.Errorf("invalid signature credited %d, want 0", balance)
			}
		})
	}
}

func TestWebhookAuthenticatesBeforeJSONOrDatabase(t *testing.T) {
	h := (&Server{webhookSecret: []byte(testWebhookSecret)}).Handler()
	for _, body := range []string{"not json", eventFor("evt_untrusted", "mb_untrusted", "tr_untrusted", "settled")} {
		req := httptest.NewRequest("POST", "/v1/webhooks/mockbank", strings.NewReader(body))
		req.Header.Set(webhooks.TimestampHeader, strconv.FormatInt(time.Now().Unix(), 10))
		req.Header.Set(webhooks.SignatureHeader, "v1="+strings.Repeat("0", 64))
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusUnauthorized {
			t.Errorf("untrusted request = %d, want 401 before parsing or touching the nil DB", rec.Code)
		}
	}
}

func TestWebhookBodyLimitStillAppliesWithValidSignature(t *testing.T) {
	h, _ := newTestServer(t)
	if rec := sendWebhook(h, strings.Repeat("x", (64<<10)+1)); rec.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("oversized signed body = %d, want 413", rec.Code)
	}
	if got := countRows(t, "webhook_events"); got != 0 {
		t.Errorf("oversized request recorded %d events, want 0", got)
	}
}

// Guarantee 4: duplicate provider events never duplicate financial effects.
func TestDuplicateWebhookHasOneFinancialEffect(t *testing.T) {
	h, apiKey := newTestServer(t)
	ctx := context.Background()

	id, _, destination := sentTransfer(t, h, apiKey, "mb_dupe")
	event := eventFor("evt_same", "mb_dupe", id, "settled")

	for i := 0; i < 6; i++ {
		rec := sendWebhook(h, event)
		// A duplicate is acknowledged, not treated as a delivery failure.
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

// A new event ID bypasses event deduplication; the terminal status guard must still protect money.
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

// The echoed client reference must resolve an event arriving before provider_ref is stored.
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

// Guarantee 4: a failed webhook attempt can retry without losing or duplicating its effect.
func TestWebhookRetriesAfterFailedTransaction(t *testing.T) {
	for _, failure := range []string{"ledger_write", "commit"} {
		for _, status := range []string{"settled", "failed"} {
			t.Run(failure+"/"+status, func(t *testing.T) {
				h, apiKey := newTestServer(t)
				ctx := context.Background()
				id, source, destination := sentTransfer(t, h, apiKey, "mb_retry")
				event := eventFor("evt_retry", "mb_retry", id, status)
				table := "ledger_entries"
				trigger := `CREATE TRIGGER fail_webhook_effect BEFORE INSERT ON ledger_entries
					FOR EACH ROW EXECUTE FUNCTION fail_webhook_effect();`
				if failure == "commit" {
					table = "webhook_events"
					trigger = `CREATE CONSTRAINT TRIGGER fail_webhook_effect AFTER INSERT ON webhook_events
						DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION fail_webhook_effect();`
				}
				if _, err := testPool.Exec(ctx, `
					CREATE FUNCTION fail_webhook_effect() RETURNS trigger LANGUAGE plpgsql AS $$
					BEGIN
						RAISE EXCEPTION 'injected webhook failure';
					END $$;`+trigger); err != nil {
					t.Fatal(err)
				}
				removeFailure := func() {
					if _, err := testPool.Exec(ctx, `DROP TRIGGER IF EXISTS fail_webhook_effect ON `+table+`;
						DROP FUNCTION IF EXISTS fail_webhook_effect();`); err != nil {
						t.Fatal(err)
					}
				}
				t.Cleanup(removeFailure)
				if rec := sendWebhook(h, event); rec.Code != http.StatusInternalServerError {
					t.Fatalf("failed attempt = %d, want 500: %s", rec.Code, rec.Body.String())
				}
				var events int
				if err := testPool.QueryRow(ctx, `SELECT count(*) FROM webhook_events WHERE event_id = 'evt_retry'`).Scan(&events); err != nil {
					t.Fatal(err)
				}
				if events != 0 {
					t.Errorf("failed attempt left %d dedupe events, want 0", events)
				}
				var storedStatus string
				if err := testPool.QueryRow(ctx, `SELECT status FROM transfers WHERE id = $1`, id).Scan(&storedStatus); err != nil {
					t.Fatal(err)
				}
				if storedStatus != "processing" {
					t.Errorf("status after failure = %q, want processing", storedStatus)
				}
				removeFailure()
				for i := 0; i < 2; i++ {
					if rec := sendWebhook(h, event); rec.Code != http.StatusOK {
						t.Fatalf("retry %d = %d, want 200: %s", i, rec.Code, rec.Body.String())
					}
				}
				if err := testPool.QueryRow(ctx, `SELECT status FROM transfers WHERE id = $1`, id).Scan(&storedStatus); err != nil {
					t.Fatal(err)
				}
				if storedStatus != status {
					t.Errorf("status after retry = %q, want %q", storedStatus, status)
				}
				account, want, reference := destination, int64(50000), "settlement "+id
				if status == "failed" {
					account, want, reference = source, 1000000, "reversal "+id
				}
				balance, err := ledger.Balance(ctx, testPool, account)
				if err != nil {
					t.Fatal(err)
				}
				if balance != want {
					t.Errorf("balance after retry = %d, want %d", balance, want)
				}
				var movements int
				if err := testPool.QueryRow(ctx, `SELECT count(*) FROM ledger_transactions WHERE reference = $1`, reference).Scan(&movements); err != nil {
					t.Fatal(err)
				}
				if movements != 1 {
					t.Errorf("financial effects after retries = %d, want 1", movements)
				}
			})
		}
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
