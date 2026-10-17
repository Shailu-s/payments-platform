package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Shailu-s/payments-platform/internal/cardnetwork"
)

var testSecret = []byte("test-network-secret-0123456789abcdef")

// newSandbox starts a fake issuer that verifies signatures and approves everything,
// and the mock-network HTTP server in front of a network pointed at it.
func newSandbox(t *testing.T) (*httptest.Server, *cardnetwork.Network) {
	t.Helper()
	clock := cardnetwork.NewFakeClock(time.Date(2026, 10, 17, 10, 0, 0, 0, time.UTC))
	issuer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if !cardnetwork.VerifyRequest(testSecret, r.Header, body, clock.Now()) {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if r.URL.Path != cardnetwork.PathAuthorizations {
			w.WriteHeader(http.StatusOK)
			return
		}
		var a cardnetwork.Authorization
		_ = json.Unmarshal(body, &a)
		_ = json.NewEncoder(w).Encode(cardnetwork.AuthorizationAnswer{
			AuthID: a.AuthID, Decision: cardnetwork.DecisionApproved, ApprovedAmount: a.Amount})
	}))
	t.Cleanup(issuer.Close)

	network, err := cardnetwork.New(cardnetwork.Config{
		IssuerURL:    issuer.URL,
		Secret:       testSecret,
		AuthDeadline: 50 * time.Millisecond,
		StandInLimit: 10_000,
		Seed:         1,
		Clock:        clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(NewServer(network).Handler())
	t.Cleanup(srv.Close)
	return srv, network
}

func post(t *testing.T, url, body string) (int, []byte) {
	t.Helper()
	resp, err := http.Post(url, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, b
}

func get(t *testing.T, url string) (int, http.Header, []byte) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, resp.Header, b
}

func TestScenarioEndpointPlaysAndServesItsClearingFile(t *testing.T) {
	srv, network := newSandbox(t)

	status, body := post(t, srv.URL+"/sandbox/scenarios",
		`{"scenario":"clear_with_tip","card_id":"card_1","amount":5000,"seed":42}`)
	if status != http.StatusOK {
		t.Fatalf("status %d: %s", status, body)
	}
	var result cardnetwork.ScenarioResult
	if err := json.Unmarshal(body, &result); err != nil {
		t.Fatal(err)
	}
	if result.Seed != 42 || len(result.Authorizations) != 1 ||
		result.Authorizations[0].Decision != cardnetwork.DecisionApproved || len(result.ClearingDates) != 1 {
		t.Fatalf("result %s", body)
	}

	url := srv.URL + "/clearing?date=" + result.ClearingDates[0]
	status, header, first := get(t, url)
	if status != http.StatusOK || !strings.HasPrefix(header.Get("Content-Type"), "text/csv") {
		t.Fatalf("clearing status %d, type %q", status, header.Get("Content-Type"))
	}
	_, _, second := get(t, url)
	if !bytes.Equal(first, second) {
		t.Fatal("same date, different bytes")
	}
	want := result.Authorizations[0].AuthID + ",card_1,purchase,6000,USD,"
	if !strings.Contains(string(first), want) {
		t.Fatalf("clearing file lacks %q:\n%s", want, first)
	}

	dir := t.TempDir()
	paths, err := exportClearing(network, dir, result.ClearingDates)
	if err != nil || len(paths) != 1 || paths[0] != filepath.Join(dir, "clearing-2026-10-18.csv") {
		t.Fatalf("export: %v, %v", paths, err)
	}
	if onDisk, _ := os.ReadFile(paths[0]); !bytes.Equal(onDisk, first) {
		t.Fatal("exported file differs from GET /clearing")
	}
}

func TestSandboxRejectsBadInput(t *testing.T) {
	srv, _ := newSandbox(t)

	cases := []struct {
		body string
		want int
	}{
		{`not json`, http.StatusBadRequest},
		{`{"scenario":"nope","card_id":"card_1","amount":100}`, http.StatusBadRequest},
		{`{"scenario":"approve_and_clear","card_id":"","amount":100}`, http.StatusBadRequest},
		{`{"scenario":"approve_and_clear","card_id":"card_1","amount":0}`, http.StatusBadRequest},
		{`{"scenario":"approve_and_clear","card_id":"card_1","amount":100,"seed":5}`, http.StatusOK},
		{`{"scenario":"approve_and_clear","card_id":"card_1","amount":100,"seed":5}`, http.StatusConflict},
	}
	for _, tc := range cases {
		if status, body := post(t, srv.URL+"/sandbox/scenarios", tc.body); status != tc.want {
			t.Errorf("%s: status %d, want %d (%s)", tc.body, status, tc.want, body)
		}
	}
	for _, q := range []string{"", "?date=", "?date=2026/10/18"} {
		if status, _, _ := get(t, srv.URL+"/clearing"+q); status != http.StatusBadRequest {
			t.Errorf("clearing%s: status %d, want 400", q, status)
		}
	}
}

func TestJournalAndReset(t *testing.T) {
	srv, _ := newSandbox(t)
	if status, body := post(t, srv.URL+"/sandbox/scenarios",
		`{"scenario":"duplicate_auth","card_id":"card_1","amount":100,"seed":9}`); status != http.StatusOK {
		t.Fatalf("status %d: %s", status, body)
	}

	journalLen := func() int {
		_, _, body := get(t, srv.URL+"/sandbox/journal")
		var out struct {
			Data []cardnetwork.Entry `json:"data"`
		}
		if err := json.Unmarshal(body, &out); err != nil {
			t.Fatal(err)
		}
		return len(out.Data)
	}
	if got := journalLen(); got != 2 {
		t.Fatalf("want 2 journal entries (answer + duplicate), got %d", got)
	}
	if status, _ := post(t, srv.URL+"/sandbox/reset", ``); status != http.StatusOK {
		t.Fatal("reset failed")
	}
	if got := journalLen(); got != 0 {
		t.Fatalf("journal survived reset: %d entries", got)
	}
	// After a reset the same seed is free again.
	if status, body := post(t, srv.URL+"/sandbox/scenarios",
		`{"scenario":"duplicate_auth","card_id":"card_1","amount":100,"seed":9}`); status != http.StatusOK {
		t.Fatalf("status %d: %s", status, body)
	}
}
