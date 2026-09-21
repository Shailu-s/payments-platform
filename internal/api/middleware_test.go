package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Shailu-s/payments-platform/internal/auth"
)

// The precondition for the API existing at all: no key, no money moved.
func TestEveryV1RouteRequiresAKey(t *testing.T) {
	h, _ := newTestServer(t)

	routes := []struct{ method, path string }{
		{"POST", "/v1/accounts"},
		{"GET", "/v1/accounts/acc_anything"},
		{"POST", "/v1/transfers"},
		{"GET", "/v1/transfers/tr_anything"},
		{"GET", "/v1/transfers"},
	}
	for _, rt := range routes {
		t.Run(rt.method+" "+rt.path, func(t *testing.T) {
			rec := do(t, h, rt.method, rt.path, "", `{}`)
			if rec.Code != http.StatusUnauthorized {
				t.Fatalf("status = %d, want 401", rec.Code)
			}
			if got := decodeError(t, rec).Code; got != CodeUnauthorized {
				t.Errorf("error code = %q, want %q", got, CodeUnauthorized)
			}
		})
	}
}

func TestAuthRejectsMalformedHeaders(t *testing.T) {
	h, key := newTestServer(t)

	tests := []struct{ name, header string }{
		{"empty", ""},
		{"no scheme", key},
		{"wrong scheme", "Basic " + key},
		{"bearer with no token", "Bearer"},
		{"bearer with blank token", "Bearer    "},
		{"unknown key", "Bearer pk_live_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", "/v1/transfers", nil)
			if tc.header != "" {
				req.Header.Set("Authorization", tc.header)
			}
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)

			if rec.Code != http.StatusUnauthorized {
				t.Fatalf("status = %d, want 401", rec.Code)
			}
		})
	}
}

// The scheme is case-insensitive per RFC 7235; the token is not.
func TestAuthAcceptsAnyCaseScheme(t *testing.T) {
	h, key := newTestServer(t)

	for _, scheme := range []string{"Bearer", "bearer", "BEARER", "BeArEr"} {
		t.Run(scheme, func(t *testing.T) {
			req := httptest.NewRequest("GET", "/v1/transfers", nil)
			req.Header.Set("Authorization", scheme+" "+key)
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)

			// 501 because the handler is 2.4 — but auth passed, which is the point.
			if rec.Code == http.StatusUnauthorized {
				t.Fatalf("scheme %q was rejected", scheme)
			}
		})
	}
}

// A revoked key is 401 like any other, but with its own code: "your key was
// revoked" and "that key never existed" are different problems for the caller.
func TestRevokedKeyIsDistinguishedFromInvalid(t *testing.T) {
	h, _ := newTestServer(t)
	ctx := context.Background()

	plaintext, key, err := auth.Generate("to be revoked")
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if err := auth.Insert(ctx, testPool, key); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	if err := auth.Revoke(ctx, testPool, key.ID); err != nil {
		t.Fatalf("Revoke: %v", err)
	}

	rec := do(t, h, "GET", "/v1/transfers", plaintext, "")
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", rec.Code)
	}
	if got := decodeError(t, rec).Code; got != CodeRevokedKey {
		t.Errorf("error code = %q, want %q", got, CodeRevokedKey)
	}
}

// The authenticated caller must reach the handler: every transfer records who
// asked for it, so a handler that cannot see the key cannot attribute one.
func TestAuthenticatedKeyReachesTheHandler(t *testing.T) {
	resetDB(t)
	ctx := context.Background()

	plaintext, key, err := auth.Generate("context test")
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if err := auth.Insert(ctx, testPool, key); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	var seen auth.Key
	var ok bool
	s := &Server{db: testPool}
	h := chain(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen, ok = APIKeyFrom(r.Context())
		w.WriteHeader(http.StatusOK)
	}), s.withAuth)

	req := httptest.NewRequest("GET", "/v1/transfers", nil)
	req.Header.Set("Authorization", "Bearer "+plaintext)
	h.ServeHTTP(httptest.NewRecorder(), req)

	if !ok {
		t.Fatal("handler could not see the authenticated key")
	}
	if seen.ID != key.ID {
		t.Errorf("handler saw key %q, want %q", seen.ID, key.ID)
	}
}

// Without a key in context a handler must be able to tell, rather than
// receiving a zero value it might attribute a transfer to.
func TestAPIKeyFromReportsAbsence(t *testing.T) {
	if _, ok := APIKeyFrom(context.Background()); ok {
		t.Error("APIKeyFrom reported a key on a context that has none")
	}
}

func TestEveryResponseCarriesARequestID(t *testing.T) {
	h, key := newTestServer(t)

	for _, path := range []string{"/healthz", "/v1/transfers", "/v1/nope"} {
		t.Run(path, func(t *testing.T) {
			rec := do(t, h, "GET", path, key, "")
			if rec.Header().Get("X-Request-Id") == "" {
				t.Error("response has no X-Request-Id header")
			}
		})
	}
}

// An inbound id is honoured so a trace survives a proxy, but only if it is safe
// to put in a log line.
func TestRequestIDIsHonouredOnlyWhenSafe(t *testing.T) {
	h, key := newTestServer(t)

	tests := []struct {
		name    string
		sent    string
		honoure bool
	}{
		{"plain", "abc123", true},
		{"with dashes", "trace-id_42", true},
		{"newline", "abc\ndef", false},
		{"spaces", "abc def", false},
		{"too long", strings.Repeat("a", 65), false},
		{"control characters", "abc\x00def", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", "/healthz", nil)
			req.Header.Set("Authorization", "Bearer "+key)
			req.Header.Set("X-Request-Id", tc.sent)
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)

			got := rec.Header().Get("X-Request-Id")
			if tc.honoure && got != tc.sent {
				t.Errorf("X-Request-Id = %q, want the inbound %q", got, tc.sent)
			}
			if !tc.honoure && got == tc.sent {
				t.Errorf("unsafe inbound id %q was echoed back", tc.sent)
			}
			if got == "" {
				t.Error("no request id was assigned")
			}
		})
	}
}

// A panic must become a 500, not a dropped connection. A caller who gets a
// closed socket cannot tell a failure from a success, which is the one
// ambiguity this system exists to avoid.
func TestPanicBecomesA500(t *testing.T) {
	h := chain(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		panic("handler exploded")
	}), withRequestID, withRecovery, withLogging)

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/boom", nil))

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rec.Code)
	}
	var body errorBody
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("panic response is not the house error shape: %v", err)
	}
	if body.Error.Code != CodeInternal {
		t.Errorf("error code = %q, want %q", body.Error.Code, CodeInternal)
	}
	if strings.Contains(rec.Body.String(), "handler exploded") {
		t.Error("the panic message leaked to the caller")
	}
}

// Liveness must not depend on the database: a probe that fails when Postgres is
// down tells an orchestrator to restart a process that is working fine.
func TestHealthzNeedsNoKeyAndNoDatabase(t *testing.T) {
	// A nil DB would panic if it were touched.
	h := (&Server{db: nil}).Handler()

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/healthz", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
}

func TestUnknownV1RouteIsJSONNotFound(t *testing.T) {
	h, key := newTestServer(t)

	rec := do(t, h, "GET", "/v1/nonsense", key, "")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Errorf("Content-Type = %q, want json", ct)
	}
	if got := decodeError(t, rec).Code; got != CodeNotFound {
		t.Errorf("error code = %q, want %q", got, CodeNotFound)
	}
}

// Declaring routes by method means the mux answers a wrong verb, not a handler.
func TestWrongMethodIsRejected(t *testing.T) {
	h, key := newTestServer(t)

	rec := do(t, h, "DELETE", "/v1/accounts", key, "")
	if rec.Code == http.StatusOK || rec.Code == http.StatusCreated {
		t.Fatalf("DELETE on a POST-only route returned %d", rec.Code)
	}
}

// The transfer endpoints are honestly unimplemented rather than faking success.
func TestTransferEndpointsAreNotImplementedYet(t *testing.T) {
	h, key := newTestServer(t)

	for _, rt := range []struct{ method, path, body string }{
		{"POST", "/v1/transfers", `{}`},
		{"GET", "/v1/transfers/tr_1", ""},
		{"GET", "/v1/transfers", ""},
	} {
		t.Run(rt.method+" "+rt.path, func(t *testing.T) {
			rec := do(t, h, rt.method, rt.path, key, rt.body)
			if rec.Code != http.StatusNotImplemented {
				t.Fatalf("status = %d, want 501", rec.Code)
			}
			if got := decodeError(t, rec).Code; got != CodeNotImplemented {
				t.Errorf("error code = %q, want %q", got, CodeNotImplemented)
			}
		})
	}
}
