package api

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Shailu-s/payments-platform/internal/auth"
	"github.com/Shailu-s/payments-platform/internal/testdb"
)

type discardMetricsResponse struct{}

func (discardMetricsResponse) Header() http.Header         { return nil }
func (discardMetricsResponse) WriteHeader(int)             {}
func (discardMetricsResponse) Write(b []byte) (int, error) { return len(b), nil }

func BenchmarkHTTPMetricsObservation(b *testing.B) {
	for _, enabled := range []bool{false, true} {
		name := "baseline"
		if enabled {
			name = "observed"
		}
		b.Run(name, func(b *testing.B) {
			var h http.Handler = http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
			s := &Server{}
			if enabled {
				h = s.withMetrics(h)
			}
			r := httptest.NewRequest("GET", "/v1/transfers", nil)
			w := discardMetricsResponse{}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				h.ServeHTTP(w, r)
			}
			b.StopTimer()
			if enabled && s.httpRequests[1].Load() != uint64(b.N) {
				b.Fatal("request counter dropped observations")
			}
		})
	}
}

func BenchmarkMetricsScrape1000Records(b *testing.B) {
	ctx := context.Background()
	if err := testdb.TruncateAll(ctx, testPool, testSchema); err != nil {
		b.Fatal(err)
	}
	plaintext, key, err := auth.Generate("metrics benchmark")
	if err != nil {
		b.Fatal(err)
	}
	if err := auth.Insert(ctx, testPool, key); err != nil {
		b.Fatal(err)
	}
	if _, err := testPool.Exec(ctx, `INSERT INTO accounts (id,currency,type) VALUES ('acc_metrics_src','USD','asset'),('acc_metrics_dst','USD','liability');`); err != nil {
		b.Fatal(err)
	}
	if _, err := testPool.Exec(ctx, `INSERT INTO transfers (id,source_account,destination_account,amount,currency,status,api_key_id)
		SELECT 'tr_metrics_'||g,'acc_metrics_src','acc_metrics_dst',500,'USD',
		(CASE g%5 WHEN 0 THEN 'created' WHEN 1 THEN 'processing' WHEN 2 THEN 'unresolved' WHEN 3 THEN 'settled' ELSE 'failed' END),$1 FROM generate_series(1,1000) g`, key.ID); err != nil {
		b.Fatal(err)
	}
	if _, err := testPool.Exec(ctx, `INSERT INTO outbox_events (id,aggregate_id,event_type,topic,payload,published_at)
		SELECT 'evt_metric_'||g,'tr_metrics_'||g,'transfer.created','metrics_benchmark','{}',CASE WHEN g%2=0 THEN now() ELSE NULL END FROM generate_series(1,1000) g;
		INSERT INTO customer_webhook_deliveries (event_id,endpoint,payload,status,delivered_at)
		SELECT 'evt_metric_'||g,'http://127.0.0.1:1','{}',CASE g%3 WHEN 0 THEN 'delivered' WHEN 1 THEN 'pending' ELSE 'dead' END,
		CASE WHEN g%3=0 THEN now() ELSE NULL END FROM generate_series(1,1000) g;`); err != nil {
		b.Fatal(err)
	}
	h := NewServer(testPool, []byte(testWebhookSecret)).Handler()
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(io.Discard, nil)))
	defer slog.SetDefault(previousLogger)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r := httptest.NewRequest("GET", "/metrics", nil)
		r.Header.Set("Authorization", "Bearer "+plaintext)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != http.StatusOK {
			b.Fatalf("scrape=%d %s", w.Code, w.Body.String())
		}
	}
}
