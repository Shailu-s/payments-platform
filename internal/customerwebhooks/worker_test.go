package customerwebhooks

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Shailu-s/payments-platform/internal/testdb"
	"github.com/Shailu-s/payments-platform/internal/webhooks"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	testSchema = "test_customer_webhooks"
	testSecret = "customer-test-key-never-use-in-production"
)

var testPool *pgxpool.Pool

func TestMain(m *testing.M) {
	pool, err := testdb.Connect(context.Background(), testSchema)
	if err != nil {
		fmt.Fprint(os.Stderr, testdb.ConnectionHint(err))
		os.Exit(1)
	}
	testPool = pool
	code := m.Run()
	pool.Close()
	os.Exit(code)
}

func resetDB(t *testing.T) {
	t.Helper()
	if err := testdb.TruncateAll(context.Background(), testPool, testSchema); err != nil {
		t.Fatal(err)
	}
}

func terminalRecord(id, status string) *kgo.Record {
	return &kgo.Record{
		Topic: "transfers", Key: []byte("tr_customer"),
		Value: []byte(fmt.Sprintf(`{"transfer_id":"tr_customer","status":%q,"amount":50000,"currency":"USD","source_account":"acc_src","destination_account":"acc_dst"}`, status)),
		Headers: []kgo.RecordHeader{
			{Key: "event_id", Value: []byte(id)},
			{Key: "event_type", Value: []byte("transfer." + status)},
		},
	}
}

func TestFinalAttemptLostAcknowledgementBecomesDead(t *testing.T) {
	resetDB(t)
	ctx := context.Background()
	var calls atomic.Int64
	receiver := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		rw.WriteHeader(http.StatusOK)
	}))
	defer receiver.Close()
	now := time.Now().UTC().Truncate(time.Microsecond)
	cfg := DefaultConfig()
	cfg.MaxAttempts = 1
	cfg.Now = func() time.Time { return now }
	w, err := New(testPool, receiver.URL, []byte(testSecret), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Handle(ctx, terminalRecord("evt_last", "settled")); err != nil {
		t.Fatal(err)
	}
	if _, err := testPool.Exec(ctx, `
		CREATE FUNCTION reject_delivery_success() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN
			IF NEW.status = 'delivered' THEN
				RAISE EXCEPTION 'injected completion failure';
			END IF;
			RETURN NEW;
		END $$;
		CREATE TRIGGER reject_delivery_success BEFORE UPDATE ON customer_webhook_deliveries
		FOR EACH ROW EXECUTE FUNCTION reject_delivery_success();`); err != nil {
		t.Fatal(err)
	}
	removeFailure := func() {
		if _, err := testPool.Exec(ctx, `DROP TRIGGER IF EXISTS reject_delivery_success ON customer_webhook_deliveries;
			DROP FUNCTION IF EXISTS reject_delivery_success();`); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(removeFailure)
	if _, err := w.RunOnce(ctx); err == nil {
		t.Fatal("expected failed delivery-state commit after customer acceptance")
	}
	removeFailure()
	now = now.Add(cfg.Lease)
	if n, err := w.RunOnce(ctx); err != nil || n != 0 {
		t.Fatalf("exhausted lease processed %d: %v", n, err)
	}
	job, err := w.Get(ctx, "evt_last")
	if err != nil || job.Status != StatusDead || job.LastError == nil {
		t.Errorf("expired final attempt = %+v, want recorded dead status: %v", job, err)
	}
	if calls.Load() != 1 {
		t.Errorf("exhausted job made %d requests, want 1", calls.Load())
	}
}

func TestDeliveryRetriesDurablyWithExponentialBackoff(t *testing.T) {
	resetDB(t)
	ctx := context.Background()
	var calls atomic.Int64
	bodies := make(chan string, 6)
	receiver := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		bodies <- string(body)
		if calls.Add(1) <= 5 {
			rw.WriteHeader(http.StatusServiceUnavailable)
		} else {
			rw.WriteHeader(http.StatusOK)
		}
	}))
	defer receiver.Close()
	now := time.Now().UTC().Truncate(time.Microsecond)
	cfg := DefaultConfig()
	cfg.Now = func() time.Time { return now }
	w, err := New(testPool, receiver.URL, []byte(testSecret), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Handle(ctx, terminalRecord("evt_retry", "settled")); err != nil {
		t.Fatal(err)
	}
	for attempt := 1; attempt <= 6; attempt++ {
		if n, err := w.RunOnce(ctx); err != nil || n != 1 {
			t.Fatalf("attempt %d processed %d: %v", attempt, n, err)
		}
		job, err := w.Get(ctx, "evt_retry")
		if err != nil {
			t.Fatal(err)
		}
		if attempt == 6 {
			if job.Status != StatusDelivered || job.AttemptCount != 6 {
				t.Errorf("after recovery = %+v, want delivered/6", job)
			}
			break
		}
		wantDelay := time.Duration(1<<(attempt-1)) * cfg.Backoff
		if job.Status != StatusPending || job.AttemptCount != attempt || !job.NextAttemptAt.Equal(now.Add(wantDelay)) {
			t.Fatalf("attempt %d job = %+v, want pending and retry in %s", attempt, job, wantDelay)
		}
		if n, err := w.RunOnce(ctx); err != nil || n != 0 {
			t.Fatalf("early retry processed %d jobs: %v", n, err)
		}
		now = job.NextAttemptAt
		w, err = New(testPool, receiver.URL, []byte(testSecret), cfg)
		if err != nil {
			t.Fatal(err)
		}
	}
	first := <-bodies
	for i := 1; i < 6; i++ {
		if body := <-bodies; body != first {
			t.Error("retry changed event ID or payload")
		}
	}
	if calls.Load() != 6 {
		t.Errorf("HTTP attempts = %d, want 6", calls.Load())
	}
}

func TestDeliveryIsSignedAndCompletedOnce(t *testing.T) {
	for _, status := range []string{"settled", "failed"} {
		t.Run(status, func(t *testing.T) {
			resetDB(t)
			ctx := context.Background()
			received := make(chan []byte, 2)
			receiver := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
				body, err := io.ReadAll(r.Body)
				if err != nil || r.Method != http.MethodPost || r.Header.Get(EventIDHeader) != "evt_signed" ||
					!webhooks.Verify([]byte(testSecret), r.Header.Get(TimestampHeader), r.Header.Get(SignatureHeader), body, time.Now()) {
					rw.WriteHeader(http.StatusUnauthorized)
					return
				}
				received <- body
				rw.WriteHeader(http.StatusNoContent)
			}))
			defer receiver.Close()
			w, err := New(testPool, receiver.URL, []byte(testSecret), DefaultConfig())
			if err != nil {
				t.Fatal(err)
			}
			record := terminalRecord("evt_signed", status)
			if err := w.Handle(ctx, record); err != nil {
				t.Fatal(err)
			}
			if n, err := w.RunOnce(ctx); err != nil || n != 1 {
				t.Fatalf("delivery processed %d jobs, want 1: %v", n, err)
			}
			select {
			case body := <-received:
				var envelope Envelope
				if err := json.Unmarshal(body, &envelope); err != nil || envelope.Data.Status != status || envelope.Data.Amount != 50000 {
					t.Errorf("customer payload = %+v: %v", envelope, err)
				}
			default:
				t.Fatal("customer received no correctly signed request")
			}
			job, err := w.Get(ctx, "evt_signed")
			if err != nil || job.Status != StatusDelivered || job.AttemptCount != 1 || job.DeliveredAt == nil {
				t.Fatalf("completed delivery = %+v: %v", job, err)
			}
			if err := w.Handle(ctx, record); err != nil {
				t.Fatal(err)
			}
			if n, err := w.RunOnce(ctx); err != nil || n != 0 {
				t.Fatalf("redelivery processed %d jobs, want 0: %v", n, err)
			}
			if len(received) != 0 {
				t.Error("completed event was sent a second time")
			}
		})
	}
}

func TestKafkaRedeliveryCreatesOneDurableJob(t *testing.T) {
	resetDB(t)
	ctx := context.Background()
	w, err := New(testPool, "http://127.0.0.1:1/webhooks", []byte(testSecret), DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	record := terminalRecord("evt_customer", "settled")
	for i := 0; i < 2; i++ {
		if err := w.Handle(ctx, record); err != nil {
			t.Fatal(err)
		}
	}
	created := terminalRecord("evt_created", "created")
	if err := w.Handle(ctx, created); err != nil {
		t.Fatal(err)
	}
	job, err := w.Get(ctx, "evt_customer")
	if err != nil {
		t.Fatal(err)
	}
	if job.Status != "pending" || job.AttemptCount != 0 {
		t.Errorf("new job status=%s attempts=%d, want pending/0", job.Status, job.AttemptCount)
	}
	var payload struct {
		EventID   string          `json:"event_id"`
		EventType string          `json:"event_type"`
		Data      json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(job.Payload, &payload); err != nil {
		t.Fatal(err)
	}
	if payload.EventID != "evt_customer" || payload.EventType != "transfer.settled" {
		t.Errorf("notification envelope = %+v", payload)
	}
	var jobs int
	if err := testPool.QueryRow(ctx, `SELECT count(*) FROM customer_webhook_deliveries`).Scan(&jobs); err != nil {
		t.Fatal(err)
	}
	if jobs != 1 {
		t.Errorf("redelivery created %d jobs, want 1", jobs)
	}
}
