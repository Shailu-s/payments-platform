package api

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/Shailu-s/payments-platform/internal/auth"
)

// Logging wraps auth to capture rejected requests; auth precedes per-key limits.
type middleware func(http.Handler) http.Handler

func chain(h http.Handler, middlewares ...middleware) http.Handler {
	// First argument is outermost, matching request order.
	for i := len(middlewares) - 1; i >= 0; i-- {
		h = middlewares[i](h)
	}
	return h
}

// A private key type prevents collisions with other packages' context values.
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

// APIKeyFrom returns the authenticated caller and whether one is present.
func APIKeyFrom(ctx context.Context) (auth.Key, bool) {
	key, ok := ctx.Value(apiKeyKey).(auth.Key)
	return key, ok
}

func withRequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Preserve proxy correlation, but reject values that could forge log entries.
		id := r.Header.Get("X-Request-Id")
		if !isSafeRequestID(id) {
			id = newRequestID()
		}

		w.Header().Set("X-Request-Id", id)
		ctx := context.WithValue(r.Context(), requestIDKey, id)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

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

// Report panics without silently dropping the connection; this does not undo a commit.
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

// Per-key limits avoid sharing a quota between unrelated callers behind a NAT.
func (s *Server) withRateLimit(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.limiter == nil {
			next.ServeHTTP(w, r)
			return
		}

		key, ok := APIKeyFrom(r.Context())
		if !ok {
			// Fail closed if middleware ordering loses the caller.
			writeError(w, http.StatusUnauthorized, CodeUnauthorized, "no authenticated api key")
			return
		}

		decision, err := s.limiter.Allow(r.Context(), key.ID)
		if err != nil {
			writeInternalError(w, r, err)
			return
		}

		// Include quota on successes too, so callers can pace requests.
		w.Header().Set("X-RateLimit-Limit", strconv.Itoa(decision.Limit))
		w.Header().Set("X-RateLimit-Remaining", strconv.Itoa(decision.Remaining))
		w.Header().Set("X-RateLimit-Reset", strconv.FormatInt(decision.ResetAt.Unix(), 10))

		if !decision.Allowed {
			w.Header().Set("Retry-After", strconv.Itoa(int(decision.RetryAfter.Seconds())))
			writeError(w, http.StatusTooManyRequests, CodeRateLimited,
				"rate limit exceeded, retry after "+decision.RetryAfter.String())
			return
		}

		next.ServeHTTP(w, r)
	})
}

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

// Request IDs enter logs; accept only bounded alphanumeric, dash and underscore values.
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
