package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Shailu-s/payments-platform/internal/reconciliation"
)

func TestSettlementReportRejectsPartialQueriesAndExportsEmptySnapshot(t *testing.T) {
	h := NewServer(NewStore(), wellBehaved{}, "", []byte(testWebhookSecret)).Handler()
	for _, path := range []string{"/settlements?date=2026-10-10", "/settlements?limit=1"} {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest("GET", path, nil))
		if w.Code != http.StatusBadRequest {
			t.Fatalf("partial query returned %d", w.Code)
		}
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "/settlements", nil))
	if report, err := reconciliation.ReadReport(w.Body); err != nil || len(report.Records) != 0 {
		t.Fatalf("empty snapshot=%+v err=%v", report, err)
	}
}

func TestSettlementReportExportsCompleteSnapshot(t *testing.T) {
	store := NewStore()
	for i, status := range []string{StatusProcessing, StatusSettled, StatusFailed} {
		p, _, err := store.Submit(Payment{ClientReference: []string{"tr_processing", "tr_settled", "tr_failed"}[i], Amount: 50000, Currency: "USD"})
		if err != nil {
			t.Fatal(err)
		}
		if status != StatusProcessing {
			if _, _, err := store.Settle(p.ProviderRef, status, nil); err != nil {
				t.Fatal(err)
			}
		}
	}
	h := NewServer(store, wellBehaved{}, "", []byte(testWebhookSecret)).Handler()
	before := time.Now().UTC()
	rw := httptest.NewRecorder()
	h.ServeHTTP(rw, httptest.NewRequest("GET", "/settlements", nil))
	if rw.Code != http.StatusOK || rw.Header().Get("Content-Type") != "text/csv; charset=utf-8" {
		t.Fatalf("status=%d content-type=%q body=%s", rw.Code, rw.Header().Get("Content-Type"), rw.Body.String())
	}
	report, err := reconciliation.ReadReport(rw.Body)
	if err != nil || len(report.Records) != 3 {
		t.Fatalf("report=%+v err=%v", report, err)
	}
	if report.CapturedAt.Before(before) || report.CapturedAt.After(time.Now().UTC()) {
		t.Fatal("capture timestamp is not from this snapshot")
	}
	statuses := map[string]bool{}
	for i, row := range report.Records {
		statuses[row.Status] = true
		if row.Amount != 50000 || (i > 0 && report.Records[i-1].ProviderRef >= row.ProviderRef) {
			t.Fatal("report lost amounts or provider-reference ordering")
		}
	}
	if len(statuses) != 3 {
		t.Fatalf("statuses=%v", statuses)
	}
}
