package api

import (
	"encoding/json"
	"log/slog"
	"net/http"
)

type errorBody struct {
	Error errorDetail `json:"error"`
}

type errorDetail struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// Stable error codes are an API contract; messages may change.
const (
	CodeUnauthorized   = "unauthorized"
	CodeRevokedKey     = "revoked_api_key"
	CodeNotFound       = "not_found"
	CodeInvalidRequest = "invalid_request"
	CodeRateLimited    = "rate_limited"
	// CodeIdempotencyInFlight means the owning transfer is not visible yet.
	CodeIdempotencyInFlight = "idempotency_key_in_flight"
	// The key was used for a different request. A client bug, not a retry.
	CodeIdempotencyKeyReused = "idempotency_key_reused"

	CodeInsufficientFunds = "insufficient_funds"
	CodeNotImplemented    = "not_implemented"
	CodeInternal          = "internal_error"
)

// Once headers are written, encoding errors can only be logged, not answered again.
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
