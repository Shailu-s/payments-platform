package reconciliation

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestFetchReportRejectsProviderFailure(t *testing.T) {
	bank := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/settlements" {
			t.Errorf("unexpected path=%s", r.URL.Path)
		}
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte("provider unavailable"))
	}))
	defer bank.Close()
	if _, err := FetchReport(context.Background(), bank.Client(), bank.URL); err == nil {
		t.Fatal("provider failure treated as a valid report")
	}
}
