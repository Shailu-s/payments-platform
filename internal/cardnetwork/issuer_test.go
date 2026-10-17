package cardnetwork

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

var (
	testSecret = []byte("test-network-secret-0123456789abcdef")
	testStart  = time.Date(2026, 10, 17, 10, 0, 0, 0, time.UTC)
)

const testDeadline = 50 * time.Millisecond

type received struct {
	Path   string
	Header http.Header
	Body   []byte
}

// fakeIssuer records every request and answers through handle, which by default
// approves every authorization in full and acknowledges everything else.
type fakeIssuer struct {
	mu     sync.Mutex
	got    []received
	handle func(w http.ResponseWriter, r *http.Request, body []byte)
	srv    *httptest.Server
}

func newFakeIssuer(t *testing.T, handle func(w http.ResponseWriter, r *http.Request, body []byte)) *fakeIssuer {
	t.Helper()
	if handle == nil {
		handle = approveAll
	}
	f := &fakeIssuer{handle: handle}
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		f.mu.Lock()
		f.got = append(f.got, received{Path: r.URL.Path, Header: r.Header.Clone(), Body: body})
		f.mu.Unlock()
		f.handle(w, r, body)
	}))
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakeIssuer) requests() []received {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]received(nil), f.got...)
}

func approveAll(w http.ResponseWriter, r *http.Request, body []byte) {
	if r.URL.Path != PathAuthorizations {
		w.WriteHeader(http.StatusOK)
		return
	}
	var a Authorization
	_ = json.Unmarshal(body, &a)
	writeAnswer(w, AuthorizationAnswer{AuthID: a.AuthID, Decision: DecisionApproved, ApprovedAmount: a.Amount})
}

func writeAnswer(w http.ResponseWriter, answer AuthorizationAnswer) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(answer)
}

// hang holds the request until the network gives up on it; no fixed sleep.
func hang(r *http.Request) {
	select {
	case <-r.Context().Done():
	case <-time.After(5 * time.Second):
	}
}

func newTestNetwork(t *testing.T, issuerURL string, clock Clock) *Network {
	t.Helper()
	n, err := New(Config{
		IssuerURL:    issuerURL,
		Secret:       testSecret,
		AuthDeadline: testDeadline,
		StandInLimit: 10_000,
		Clock:        clock,
	})
	if err != nil {
		t.Fatalf("new network: %v", err)
	}
	return n
}

func testAuth(id string, amount int64) Authorization {
	return Authorization{
		AuthID:   id,
		CardID:   "card_test",
		Amount:   amount,
		Currency: Currency,
		Merchant: Merchant{ID: "mer_coffee", Name: "Corner Coffee", MCC: "5814"},
		Kind:     KindInitial,
	}
}

func outcomes(entries []Entry, id string) []string {
	var out []string
	for _, e := range entries {
		if e.ID == id {
			out = append(out, e.Outcome)
		}
	}
	return out
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
