package api

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

func (s *Server) withMetrics(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/metrics" {
			next.ServeHTTP(w, r)
			return
		}
		rec := &statusRecorder{ResponseWriter: w}
		next.ServeHTTP(rec, r)
		status := rec.status
		if status == 0 {
			status = http.StatusOK
		}
		if class := status/100 - 1; class >= 0 && class < len(s.httpRequests) {
			s.httpRequests[class].Add(1)
		}
	})
}

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := contextWithTimeout(r, 2*time.Second)
	defer cancel()
	s.withAuth(http.HandlerFunc(s.writeMetrics)).ServeHTTP(w, r.WithContext(ctx))
}

func (s *Server) writeMetrics(w http.ResponseWriter, r *http.Request) {
	var transfers, deliveries, exceptions []byte
	var pending, lastRun int64
	err := s.db.QueryRow(r.Context(), `SELECT
		(SELECT jsonb_object_agg(status,n) FROM (SELECT status,count(*) n FROM transfers GROUP BY status) t),
		(SELECT count(*) FROM outbox_events WHERE published_at IS NULL),
		(SELECT jsonb_object_agg(status,n) FROM (SELECT status,count(*) n FROM customer_webhook_deliveries GROUP BY status) d),
		(SELECT counts FROM reconciliation_runs ORDER BY created_at DESC,id DESC LIMIT 1),
		COALESCE((SELECT floor(extract(epoch FROM created_at))::bigint FROM reconciliation_runs ORDER BY created_at DESC,id DESC LIMIT 1),0)`).Scan(&transfers, &pending, &deliveries, &exceptions, &lastRun)
	if err != nil {
		writeInternalError(w, r, err)
		return
	}
	families := []struct {
		name   string
		raw    []byte
		labels []string
		label  string
	}{
		{"payments_transfers", transfers, []string{"created", "processing", "unresolved", "settled", "failed"}, "status"},
		{"payments_customer_deliveries", deliveries, []string{"pending", "delivered", "dead"}, "status"},
		{"payments_reconciliation_exceptions", exceptions, []string{"AMOUNT_MISMATCH", "STATUS_MISMATCH", "MISSING_EXTERNAL", "MISSING_INTERNAL", "DUPLICATE_EXTERNAL"}, "classification"},
	}
	var output strings.Builder
	output.WriteString("# TYPE payments_http_requests_total counter\n")
	for i := range s.httpRequests {
		fmt.Fprintf(&output, "payments_http_requests_total{status_class=\"%dxx\"} %d\n", i+1, s.httpRequests[i].Load())
	}
	fmt.Fprintf(&output, "# TYPE payments_outbox_pending gauge\npayments_outbox_pending %d\n", pending)
	fmt.Fprintf(&output, "# TYPE payments_reconciliation_last_run_timestamp_seconds gauge\npayments_reconciliation_last_run_timestamp_seconds %d\n", lastRun)
	for _, family := range families {
		counts := map[string]int64{}
		if len(family.raw) > 0 {
			if err := json.Unmarshal(family.raw, &counts); err != nil {
				writeInternalError(w, r, err)
				return
			}
		}
		fmt.Fprintf(&output, "# TYPE %s gauge\n", family.name)
		for _, label := range family.labels {
			fmt.Fprintf(&output, "%s{%s=%q} %d\n", family.name, family.label, label, counts[label])
		}
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	if _, err := fmt.Fprint(w, output.String()); err != nil {
		slog.ErrorContext(r.Context(), "write metrics response", "error", err)
	}
}
