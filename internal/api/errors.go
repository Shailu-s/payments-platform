package api

import (
	"encoding/json"
	"log/slog"
	"net/http"
)

// Every error the API returns has this shape. A machine-readable code is what
// makes an API integrable: a bare message forces callers to regex your prose,
// and then your prose can never change.
//
//	{ "error": { "code": "insufficient_funds", "message": "..." } }
type errorBody struct {
	Error errorDetail `json:"error"`
}

type errorDetail struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// Error codes. Stable strings: callers branch on these, so renaming one is a
// breaking change to the API.
const (
	CodeUnauthorized   = "unauthorized"
	CodeRevokedKey     = "revoked_api_key"
	CodeNotFound       = "not_found"
	CodeInvalidRequest = "invalid_request"
	CodeRateLimited    = "rate_limited"
	// The key is taken but the first request has not committed yet, so nothing
	// true can be said about the outcome.
	CodeIdempotencyInFlight = "idempotency_key_in_flight"
	// The key was used for a different request. A client bug, not a retry.
	CodeIdempotencyKeyReused = "idempotency_key_reused"
	// The source account cannot cover the transfer. A client error: they asked
	// to spend money that is not there.
	CodeInsufficientFunds = "insufficient_funds"
	CodeNotImplemented    = "not_implemented"
	CodeInternal          = "internal_error"
)

// writeJSON sends a value as JSON. An encoding failure here is already too late
// to report — the status line has been written — so it is logged rather than
// turned into a second, contradictory response.
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		slog.Error("writing response body", "error", err)
	}
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, errorBody{Error: errorDetail{Code: code, Message: message}})
}

// writeInternalError logs the real cause and tells the caller nothing about it.
// Internal detail in an error body is how database schemas and file paths leak.
func writeInternalError(w http.ResponseWriter, r *http.Request, err error) {
	slog.ErrorContext(r.Context(), "unhandled error",
		"error", err,
		"request_id", RequestIDFrom(r.Context()),
		"method", r.Method,
		"path", r.URL.Path,
	)
	writeError(w, http.StatusInternalServerError, CodeInternal, "something went wrong on our side")
}
