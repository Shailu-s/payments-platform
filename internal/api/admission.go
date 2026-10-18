package api

import (
	"errors"
	"net/http"
)

// ErrOverloaded means this process refused the request instead of queueing
// it. Nothing was written for it, so the same request can be retried.
var ErrOverloaded = errors.New("overloaded")

// Requests admitted per pool connection. A few per connection keep every
// connection busy while the next request parses and verifies; more only wait
// for one and answer after the caller has given up.
const admissionPerConn = 3

// withAdmission refuses a request at once when the process already has as
// many in flight as its pool can serve. It never waits: a queued request
// holds memory and a goroutine only to answer late.
func (s *Server) withAdmission(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.admit == nil {
			next.ServeHTTP(w, r)
			return
		}
		select {
		case s.admit <- struct{}{}:
		default:
			writeOverloaded(w)
			return
		}
		defer func() { <-s.admit }()
		next.ServeHTTP(w, r)
	})
}

func writeOverloaded(w http.ResponseWriter) {
	w.Header().Set("Retry-After", "1")
	writeError(w, http.StatusServiceUnavailable, CodeOverloaded,
		"the server is at capacity and recorded nothing for this request: retry it unchanged")
}
