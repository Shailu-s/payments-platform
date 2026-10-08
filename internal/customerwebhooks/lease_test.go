package customerwebhooks

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestStaleAttemptCannotOverwriteNewerSuccess(t *testing.T) {
	resetDB(t)
	ctx := context.Background()
	entered, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int64
	receiver := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, _ *http.Request) {
		if calls.Add(1) == 1 {
			close(entered)
			<-release
			rw.WriteHeader(http.StatusServiceUnavailable)
		} else {
			rw.WriteHeader(http.StatusOK)
		}
	}))
	defer receiver.Close()
	var clock atomic.Int64
	clock.Store(time.Now().UTC().Truncate(time.Microsecond).UnixNano())
	cfg := DefaultConfig()
	cfg.Now = func() time.Time { return time.Unix(0, clock.Load()).UTC() }
	w, err := New(testPool, receiver.URL, []byte(testSecret), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Handle(ctx, terminalRecord("evt_fenced", "settled")); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := w.RunOnce(ctx)
		done <- err
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		close(release)
		t.Fatal("first attempt did not start")
	}
	clock.Add(int64(cfg.Lease))
	if n, err := w.RunOnce(ctx); err != nil || n != 1 {
		close(release)
		t.Fatalf("new owner processed %d: %v", n, err)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	job, err := w.Get(ctx, "evt_fenced")
	if err != nil || job.Status != StatusDelivered || job.AttemptCount != 2 {
		t.Errorf("stale failure overwrote newer success: %+v, %v", job, err)
	}
}

func TestAcceptedNotificationCanReplayAfterStateSaveFailure(t *testing.T) {
	resetDB(t)
	ctx := context.Background()
	var calls, effects atomic.Int64
	var seen sync.Map
	bodies := make(chan string, 2)
	receiver := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		bodies <- string(body)
		calls.Add(1)
		if _, loaded := seen.LoadOrStore(r.Header.Get(EventIDHeader), true); !loaded {
			effects.Add(1)
		}
		rw.WriteHeader(http.StatusOK)
	}))
	defer receiver.Close()
	now := time.Now().UTC().Truncate(time.Microsecond)
	cfg := DefaultConfig()
	cfg.Now = func() time.Time { return now }
	w, err := New(testPool, receiver.URL, []byte(testSecret), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Handle(ctx, terminalRecord("evt_accepted", "settled")); err != nil {
		t.Fatal(err)
	}
	if _, err := testPool.Exec(ctx, `
		CREATE FUNCTION reject_ack() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN
			IF NEW.status = 'delivered' THEN
				RAISE EXCEPTION 'injected delivery-state failure';
			END IF;
			RETURN NEW;
		END $$;
		CREATE TRIGGER reject_ack BEFORE UPDATE ON customer_webhook_deliveries
		FOR EACH ROW EXECUTE FUNCTION reject_ack();`); err != nil {
		t.Fatal(err)
	}
	removeFailure := func() {
		if _, err := testPool.Exec(ctx, `DROP TRIGGER IF EXISTS reject_ack ON customer_webhook_deliveries;
			DROP FUNCTION IF EXISTS reject_ack();`); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(removeFailure)
	if _, err := w.RunOnce(ctx); err == nil {
		t.Fatal("expected delivery-state failure after customer accepted")
	}
	removeFailure()
	now = now.Add(cfg.Lease)
	w, err = New(testPool, receiver.URL, []byte(testSecret), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if n, err := w.RunOnce(ctx); err != nil || n != 1 {
		t.Fatalf("recovered delivery processed %d: %v", n, err)
	}
	job, err := w.Get(ctx, "evt_accepted")
	if err != nil || job.Status != StatusDelivered || job.AttemptCount != 2 {
		t.Errorf("recovered job = %+v: %v", job, err)
	}
	if calls.Load() != 2 || effects.Load() != 1 {
		t.Errorf("calls=%d effects=%d, want replay with customer-side dedupe", calls.Load(), effects.Load())
	}
	if first, second := <-bodies, <-bodies; first != second {
		t.Error("recovery changed event ID or payload")
	}
}
