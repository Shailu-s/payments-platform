package api

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Shailu-s/payments-platform/internal/ratelimit"
	"github.com/Shailu-s/payments-platform/internal/reconciliation"
)

func TestMetricsReflectsDatabaseBacklogsAndLatestReconciliation(t *testing.T) {
	h, key := newTestServer(t)
	ctx := context.Background()
	id, _, _ := sentTransfer(t, h, key, "mb_metrics")
	if _, err := testPool.Exec(ctx, `INSERT INTO customer_webhook_deliveries (event_id,endpoint,payload,status) VALUES ('evt_metric_dead','http://127.0.0.1:1','{}','dead')`); err != nil {
		t.Fatal(err)
	}
	prefix := fmt.Sprintf("snapshot_at,%s\nprovider_ref,client_reference,amount,currency,status\n", time.Now().UTC().Format(time.RFC3339Nano))
	if _, err := reconciliation.Reconcile(ctx, testPool, "metrics_break", strings.NewReader(prefix)); err != nil {
		t.Fatal(err)
	}
	first := do(t, h, "GET", "/metrics", key, "")
	if first.Code != 200 {
		t.Fatalf("metrics=%d %s", first.Code, first.Body.String())
	}
	for _, line := range []string{`payments_outbox_pending 1`, `payments_transfers{status="processing"} 1`, `payments_customer_deliveries{status="dead"} 1`, `payments_reconciliation_exceptions{classification="MISSING_EXTERNAL"} 1`} {
		if !strings.Contains(first.Body.String(), line+"\n") {
			t.Fatalf("missing %q in %s", line, first.Body.String())
		}
	}
	clean := prefix + fmt.Sprintf("mb_metrics,%s,50000,USD,processing\n", id)
	latest, err := reconciliation.Reconcile(ctx, testPool, "metrics_clean", strings.NewReader(clean))
	if err != nil {
		t.Fatal(err)
	}
	second := do(t, h, "GET", "/metrics", key, "")
	if !strings.Contains(second.Body.String(), "payments_reconciliation_exceptions{classification=\"MISSING_EXTERNAL\"} 0\n") || !strings.Contains(second.Body.String(), fmt.Sprintf("payments_reconciliation_last_run_timestamp_seconds %d\n", latest.CreatedAt.Unix())) {
		t.Fatalf("metrics did not select latest run: %s", second.Body.String())
	}
	for _, private := range []string{key, id, "mb_metrics", "evt_metric_dead", "http://"} {
		if strings.Contains(second.Body.String(), private) {
			t.Fatal("metrics leaked a private identity or high-cardinality label")
		}
	}
}

func TestMetricsScrapesDoNotConsumePaymentQuota(t *testing.T) {
	_, key := newTestServer(t)
	s := NewServer(testPool, []byte(testWebhookSecret))
	s.limiter = ratelimit.New(testPool, 1, time.Hour)
	h := s.Handler()
	for i := 0; i < 3; i++ {
		if w := do(t, h, "GET", "/metrics", key, ""); w.Code != 200 {
			t.Fatalf("scrape=%d", w.Code)
		}
	}
	if w := do(t, h, "GET", "/v1/transfers", key, ""); w.Code != 200 {
		t.Fatalf("scrapes consumed payment quota: %d", w.Code)
	}
	if w := do(t, h, "GET", "/v1/transfers", key, ""); w.Code != 429 {
		t.Fatalf("payment quota not enforced: %d", w.Code)
	}
	if w := do(t, h, "GET", "/metrics", key, ""); w.Code != 200 {
		t.Fatalf("exhausted payment quota blocked monitoring: %d", w.Code)
	}
}

func TestMetricsDatabaseFailureDoesNotEmitZeroGauges(t *testing.T) {
	h, key := newTestServer(t)
	ctx := context.Background()
	if _, err := testPool.Exec(ctx, "ALTER TABLE customer_webhook_deliveries RENAME TO metrics_unavailable"); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if _, err := testPool.Exec(ctx, "ALTER TABLE metrics_unavailable RENAME TO customer_webhook_deliveries"); err != nil {
			t.Error(err)
		}
	}()
	response := do(t, h, "GET", "/metrics", key, "")
	if response.Code != 500 || strings.Contains(response.Body.String(), "payments_outbox_pending") || strings.HasPrefix(response.Header().Get("Content-Type"), "text/plain") {
		t.Fatalf("failed scrape emitted apparently healthy metrics: %d %s", response.Code, response.Body.String())
	}
}

func TestMetricsScrapeHasBoundedDatabaseDeadline(t *testing.T) {
	h, key := newTestServer(t)
	ctx := context.Background()
	tx, err := testPool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, "LOCK TABLE customer_webhook_deliveries IN ACCESS EXCLUSIVE MODE"); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	response := do(t, h, "GET", "/metrics", key, "")
	if elapsed := time.Since(started); response.Code != 500 || elapsed < time.Second || elapsed > 5*time.Second {
		t.Fatalf("blocked database did not respect scrape deadline: status=%d elapsed=%s", response.Code, elapsed)
	}
}

func TestMetricsCountsActualHTTPStatusUnderConcurrency(t *testing.T) {
	s := &Server{}
	h := s.withMetrics(withRecovery(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/panic":
			panic("test metrics recovery")
		case "/ignored-header":
			w.WriteHeader(200)
			w.WriteHeader(500)
		}
	})))
	for path, want := range map[string]int{"/implicit": 200, "/ignored-header": 200, "/panic": 500} {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest("GET", path, nil))
		if w.Code != want {
			t.Fatalf("status=%d want=%d", w.Code, want)
		}
	}
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/implicit", nil))
		}()
	}
	wg.Wait()
	if got := s.httpRequests[1].Load(); got != 102 {
		t.Fatalf("2xx count=%d want=102", got)
	}
	if got := s.httpRequests[4].Load(); got != 1 {
		t.Fatalf("5xx count=%d want=1", got)
	}
}

func TestMetricsRequiresAuthAndReportsBoundedHTTPCounts(t *testing.T) {
	h, key := newTestServer(t)
	if w := do(t, h, "GET", "/metrics", "", ""); w.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated metrics=%d", w.Code)
	}
	do(t, h, "GET", "/healthz", "", "")
	do(t, h, "GET", "/v1/transfers", "", "")
	first := do(t, h, "GET", "/metrics", key, "")
	if first.Code != http.StatusOK || first.Header().Get("Content-Type") != "text/plain; version=0.0.4; charset=utf-8" {
		t.Fatalf("metrics=%d content-type=%q body=%s", first.Code, first.Header().Get("Content-Type"), first.Body.String())
	}
	for _, line := range []string{
		`payments_http_requests_total{status_class="2xx"} 1`,
		`payments_http_requests_total{status_class="4xx"} 1`,
		`payments_outbox_pending 0`,
		`payments_reconciliation_last_run_timestamp_seconds 0`,
		`payments_transfers{status="processing"} 0`,
	} {
		if !strings.Contains(first.Body.String(), line+"\n") {
			t.Fatalf("missing %q in %s", line, first.Body.String())
		}
	}
	second := do(t, h, "GET", "/metrics", key, "")
	if first.Body.String() != second.Body.String() {
		t.Fatal("metrics scrapes changed application request counters")
	}
	if strings.Contains(first.Body.String(), key) || strings.Contains(first.Body.String(), "request_id") {
		t.Fatal("metrics exposed secrets or unbounded labels")
	}
}
