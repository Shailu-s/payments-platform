package api

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/Shailu-s/payments-platform/internal/auth"
)

// The middleware chain, and the order is load-bearing:
//
//	request id  →  logging  →  auth  →  rate limit  →  handler
//
// Auth comes before the rate limit because the limit is per key, so there is
// nothing to count against until the caller is known. Logging wraps auth rather
// than the reverse, so a rejected request still appears in the log — a 401 you
// cannot see is a 401 you cannot debug.
type middleware func(http.Handler) http.Handler

func chain(h http.Handler, middlewares ...middleware) http.Handler {
	// Applied in reverse so the first argument is the outermost layer and the
	// list reads in request order.
	for i := len(middlewares) - 1; i >= 0; i-- {
		h = middlewares[i](h)
	}
	return h
}

// Context keys are an unexported type so no other package can collide with
// them, which is the documented reason not to use a bare string.
type contextKey int

const (
	requestIDKey contextKey = iota
	apiKeyKey
)

// RequestIDFrom returns the id assigned to this request, or "" outside one.
func RequestIDFrom(ctx context.Context) string {
	id, _ := ctx.Value(requestIDKey).(string)
	return id
}

// APIKeyFrom returns the authenticated caller. The second result is false on an
// unauthenticated route, so a handler cannot silently treat "nobody" as a
// caller and attribute a transfer to a zero-valued key.
func APIKeyFrom(ctx context.Context) (auth.Key, bool) {
	key, ok := ctx.Value(apiKeyKey).(auth.Key)
	return key, ok
}

// withRequestID gives every request an id, echoed in the response header and
// carried into every log line. It is what lets a caller quoting one line of
// output be traced through the system.
func withRequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Honour an inbound id so a trace survives a proxy, but bound its length
		// and character set: this value reaches logs, and a caller-supplied
		// string with newlines in it can forge log entries.
		id := r.Header.Get("X-Request-Id")
		if !isSafeRequestID(id) {
			id = newRequestID()
		}

		w.Header().Set("X-Request-Id", id)
		ctx := context.WithValue(r.Context(), requestIDKey, id)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// statusRecorder captures the status code, which http.ResponseWriter does not
// expose after the fact.
type statusRecorder struct {
	http.ResponseWriter
	status int
	bytes  int
}

func (s *statusRecorder) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}

func (s *statusRecorder) Write(b []byte) (int, error) {
	// A handler that writes without calling WriteHeader has implicitly sent 200.
	if s.status == 0 {
		s.status = http.StatusOK
	}
	n, err := s.ResponseWriter.Write(b)
	s.bytes += n
	return n, err
}

func withLogging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w}

		next.ServeHTTP(rec, r)

		if rec.status == 0 {
			rec.status = http.StatusOK
		}
		slog.InfoContext(r.Context(), "request",
			"request_id", RequestIDFrom(r.Context()),
			"method", r.Method,
			"path", r.URL.Path,
			"status", rec.status,
			"bytes", rec.bytes,
			"duration_ms", time.Since(start).Milliseconds(),
		)
	})
}

// withRecovery turns a panic into a 500 rather than a dropped connection. A
// payments API that silently closes the socket on a nil dereference gives the
// caller no way to tell "it failed" from "it may have worked" — which is the
// single most expensive ambiguity in this domain.
func withRecovery(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if v := recover(); v != nil {
				slog.ErrorContext(r.Context(), "panic serving request",
					"panic", v,
					"request_id", RequestIDFrom(r.Context()),
					"method", r.Method,
					"path", r.URL.Path,
				)
				writeError(w, http.StatusInternalServerError, CodeInternal,
					"something went wrong on our side")
			}
		}()
		next.ServeHTTP(w, r)
	})
}

// withAuth resolves `Authorization: Bearer <key>` to an api_keys row, or 401s.
// Nothing downstream runs without a caller: the rate limiter counts per key and
// every transfer records who asked for it.
func (s *Server) withAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		plaintext, ok := bearerToken(r.Header.Get("Authorization"))
		if !ok {
			writeError(w, http.StatusUnauthorized, CodeUnauthorized,
				"missing or malformed Authorization header, expected: Bearer <api key>")
			return
		}

		key, err := auth.Verify(r.Context(), s.db, plaintext)
		switch {
		case errors.Is(err, auth.ErrRevokedKey):
			// Distinguished from invalid in the body and the logs, because
			// "your key was revoked" and "that key never existed" are different
			// problems for the caller. Both are 401.
			writeError(w, http.StatusUnauthorized, CodeRevokedKey, "this api key has been revoked")
			return
		case errors.Is(err, auth.ErrInvalidKey):
			writeError(w, http.StatusUnauthorized, CodeUnauthorized, "invalid api key")
			return
		case err != nil:
			writeInternalError(w, r, err)
			return
		}

		ctx := context.WithValue(r.Context(), apiKeyKey, key)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// bearerToken parses an Authorization header. The scheme is case-insensitive
// per RFC 7235; the token is not.
func bearerToken(header string) (string, bool) {
	scheme, token, found := strings.Cut(header, " ")
	if !found || !strings.EqualFold(scheme, "bearer") {
		return "", false
	}
	token = strings.TrimSpace(token)
	if token == "" {
		return "", false
	}
	return token, true
}

func newRequestID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// isSafeRequestID accepts a bounded, printable ASCII id. An inbound value goes
// straight into log lines, so a newline in it would let a caller write their
// own entries.
func isSafeRequestID(id string) bool {
	if id == "" || len(id) > 64 {
		return false
	}
	for _, c := range id {
		isAllowed := c >= 'a' && c <= 'z' ||
			c >= 'A' && c <= 'Z' ||
			c >= '0' && c <= '9' ||
			c == '-' || c == '_'
		if !isAllowed {
			return false
		}
	}
	return true
}
