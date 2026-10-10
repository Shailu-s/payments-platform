package customerwebhooks

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestFailedJobInsertReturnsErrorBeforeKafkaAcknowledgement(t *testing.T) {
	resetDB(t)
	ctx := context.Background()
	w, err := New(testPool, "http://127.0.0.1:1/webhooks", []byte(testSecret), DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := testPool.Exec(ctx, `
		CREATE FUNCTION reject_delivery_job() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN RAISE EXCEPTION 'injected job failure'; END $$;
		CREATE TRIGGER reject_delivery_job BEFORE INSERT ON customer_webhook_deliveries
		FOR EACH ROW EXECUTE FUNCTION reject_delivery_job();`); err != nil {
		t.Fatal(err)
	}
	removeFailure := func() {
		if _, err := testPool.Exec(ctx, `DROP TRIGGER IF EXISTS reject_delivery_job ON customer_webhook_deliveries;
			DROP FUNCTION IF EXISTS reject_delivery_job();`); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(removeFailure)
	record := terminalRecord("evt_job_failure", "settled")
	if err := w.Handle(ctx, record); err == nil {
		t.Fatal("handler reported success before persisting delivery work")
	}
	removeFailure()
	if err := w.Handle(ctx, record); err != nil {
		t.Fatal(err)
	}
	if job, err := w.Get(ctx, "evt_job_failure"); err != nil || job.Status != StatusPending {
		t.Errorf("job did not recover on record retry: %+v %v", job, err)
	}
}

func TestConfigurationRejectsUnsafeEndpointAndMissingKey(t *testing.T) {
	for _, endpoint := range []string{"", "file:///tmp/webhook", "https://user:password@example.invalid", "https://example.invalid/hook?token=secret", "https://example.invalid/#fragment"} {
		if _, err := New(testPool, endpoint, []byte(testSecret), DefaultConfig()); err == nil {
			t.Errorf("accepted endpoint that could leak credentials or use an unsupported scheme")
		}
	}
	if _, err := New(testPool, "https://example.invalid/hook", nil, DefaultConfig()); err == nil {
		t.Error("accepted missing signing key")
	}
}

func TestHTTPFailuresRetryOrBecomeDead(t *testing.T) {
	for _, code := range []int{302, 400, 401, 404, 408, 429, 500, 503} {
		t.Run(fmt.Sprint(code), func(t *testing.T) {
			resetDB(t)
			ctx := context.Background()
			var calls atomic.Int64
			receiver := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, _ *http.Request) {
				calls.Add(1)
				rw.Header().Set("Location", "/redirected")
				rw.WriteHeader(code)
			}))
			defer receiver.Close()
			w, err := New(testPool, receiver.URL, []byte(testSecret), DefaultConfig())
			if err != nil {
				t.Fatal(err)
			}
			if err := w.Handle(ctx, terminalRecord("evt_failure", "failed")); err != nil {
				t.Fatal(err)
			}
			if _, err := w.RunOnce(ctx); err != nil {
				t.Fatal(err)
			}
			job, err := w.Get(ctx, "evt_failure")
			want := StatusDead
			if code >= 500 || code == 408 || code == 429 {
				want = StatusPending
			}
			if err != nil || job.Status != want || job.LastHTTPStatus == nil || *job.LastHTTPStatus != code || job.LastError == nil {
				t.Errorf("HTTP %d job = %+v, want %s: %v", code, job, want, err)
			}
			if calls.Load() != 1 {
				t.Errorf("HTTP %d followed redirect or retried too soon: %d calls", code, calls.Load())
			}
		})
	}
}

func TestPermanentOutageStopsAtAttemptLimit(t *testing.T) {
	resetDB(t)
	ctx := context.Background()
	var calls atomic.Int64
	receiver := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		rw.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer receiver.Close()
	now := time.Now().UTC().Truncate(time.Microsecond)
	cfg := DefaultConfig()
	cfg.MaxAttempts = 3
	cfg.Now = func() time.Time { return now }
	w, err := New(testPool, receiver.URL, []byte(testSecret), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Handle(ctx, terminalRecord("evt_exhausted", "settled")); err != nil {
		t.Fatal(err)
	}
	for attempt := 1; attempt <= cfg.MaxAttempts; attempt++ {
		if n, err := w.RunOnce(ctx); err != nil || n != 1 {
			t.Fatalf("attempt %d processed %d: %v", attempt, n, err)
		}
		job, err := w.Get(ctx, "evt_exhausted")
		if err != nil {
			t.Fatal(err)
		}
		now = job.NextAttemptAt
	}
	job, err := w.Get(ctx, "evt_exhausted")
	if err != nil || job.Status != StatusDead || job.AttemptCount != cfg.MaxAttempts {
		t.Errorf("exhausted job = %+v: %v", job, err)
	}
	now = now.Add(time.Hour)
	if n, err := w.RunOnce(ctx); err != nil || n != 0 || calls.Load() != int64(cfg.MaxAttempts) {
		t.Errorf("dead job sent again: n=%d calls=%d err=%v", n, calls.Load(), err)
	}
}

func TestTimeoutKeepsDeliveryForRetry(t *testing.T) {
	resetDB(t)
	ctx := context.Background()
	receiver := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		<-r.Context().Done()
	}))
	defer receiver.Close()
	cfg := DefaultConfig()
	cfg.Timeout = 20 * time.Millisecond
	cfg.Lease = time.Second
	w, err := New(testPool, receiver.URL, []byte(testSecret), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Handle(ctx, terminalRecord("evt_timeout", "settled")); err != nil {
		t.Fatal(err)
	}
	if _, err := w.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	job, err := w.Get(ctx, "evt_timeout")
	if err != nil || job.Status != StatusPending || job.LastHTTPStatus != nil || job.LastError == nil {
		t.Errorf("timeout job = %+v: %v", job, err)
	}
}

func TestConcurrentWorkersClaimOneDelivery(t *testing.T) {
	resetDB(t)
	ctx := context.Background()
	var calls atomic.Int64
	receiver := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		time.Sleep(20 * time.Millisecond)
		rw.WriteHeader(http.StatusOK)
	}))
	defer receiver.Close()
	w, err := New(testPool, receiver.URL, []byte(testSecret), DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Handle(ctx, terminalRecord("evt_concurrent", "settled")); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := w.RunOnce(ctx); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if calls.Load() != 1 {
		t.Errorf("concurrent workers made %d HTTP calls, want 1", calls.Load())
	}
}

func TestMalformedTerminalEventIsDeadInsteadOfBlockingKafka(t *testing.T) {
	resetDB(t)
	ctx := context.Background()
	w, err := New(testPool, "http://127.0.0.1:1/webhooks", []byte(testSecret), DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	record := terminalRecord("evt_bad", "settled")
	record.Value = []byte("invalid json")
	if err := w.Handle(ctx, record); err != nil {
		t.Fatal(err)
	}
	job, err := w.Get(ctx, "evt_bad")
	if err != nil || job.Status != StatusDead || job.AttemptCount != 0 {
		t.Errorf("malformed event = %+v: %v", job, err)
	}
	if n, err := w.RunOnce(ctx); err != nil || n != 0 {
		t.Errorf("malformed event tried HTTP: %d, %v", n, err)
	}
}
