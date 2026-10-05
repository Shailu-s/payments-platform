package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/Shailu-s/payments-platform/internal/ledger"
	"github.com/jackc/pgx/v5"
)

// Currency support must be added explicitly, not accepted before the rail supports it.
const supportedCurrency = "USD"

var accountTypes = map[string]bool{"asset": true, "liability": true, "settlement": true}

type createAccountRequest struct {
	Currency string `json:"currency"`
	Type     string `json:"type"`
}

type accountResponse struct {
	ID        string    `json:"id"`
	Currency  string    `json:"currency"`
	Type      string    `json:"type"`
	Balance   int64     `json:"balance"`
	CreatedAt time.Time `json:"created_at"`
}

func (s *Server) handleCreateAccount(w http.ResponseWriter, r *http.Request) {
	var req createAccountRequest
	if !decodeJSON(w, r, &req) {
		return
	}

	req.Currency = strings.ToUpper(strings.TrimSpace(req.Currency))
	req.Type = strings.ToLower(strings.TrimSpace(req.Type))

	if req.Currency != supportedCurrency {
		writeError(w, http.StatusBadRequest, CodeInvalidRequest,
			fmt.Sprintf("currency must be %s in v1, got %q", supportedCurrency, req.Currency))
		return
	}
	if !accountTypes[req.Type] {
		writeError(w, http.StatusBadRequest, CodeInvalidRequest,
			fmt.Sprintf("type must be asset, liability or settlement, got %q", req.Type))
		return
	}

	id := newID("acc")
	const q = `INSERT INTO accounts (id, currency, type) VALUES ($1, $2, $3) RETURNING created_at`
	var createdAt time.Time
	if err := s.db.QueryRow(r.Context(), q, id, req.Currency, req.Type).Scan(&createdAt); err != nil {
		writeInternalError(w, r, fmt.Errorf("insert account: %w", err))
		return
	}

	writeJSON(w, http.StatusCreated, accountResponse{
		ID: id, Currency: req.Currency, Type: req.Type, Balance: 0, CreatedAt: createdAt,
	})
}

func (s *Server) handleGetAccount(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	// Balance returns zero for unknown IDs, so existence needs a separate lookup.
	const q = `SELECT currency, type, created_at FROM accounts WHERE id = $1`
	var acc accountResponse
	err := s.db.QueryRow(r.Context(), q, id).Scan(&acc.Currency, &acc.Type, &acc.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		writeError(w, http.StatusNotFound, CodeNotFound, "no such account: "+id)
		return
	}
	if err != nil {
		writeInternalError(w, r, fmt.Errorf("select account: %w", err))
		return
	}

	balance, err := ledger.Balance(r.Context(), s.db, id)
	if err != nil {
		writeInternalError(w, r, err)
		return
	}

	acc.ID = id
	acc.Balance = balance
	writeJSON(w, http.StatusOK, acc)
}

func decodeJSON(w http.ResponseWriter, r *http.Request, v any) bool {
	// Bound memory consumption from caller-controlled bodies.
	const maxBody = 64 << 10
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBody))

	// Reject misspelled fields rather than silently treating them as absent.
	dec.DisallowUnknownFields()

	if err := dec.Decode(v); err != nil {
		var maxErr *http.MaxBytesError
		switch {
		case errors.Is(err, io.EOF):
			writeError(w, http.StatusBadRequest, CodeInvalidRequest, "request body is empty")
		case errors.As(err, &maxErr):
			writeError(w, http.StatusRequestEntityTooLarge, CodeInvalidRequest, "request body is too large")
		default:
			writeError(w, http.StatusBadRequest, CodeInvalidRequest, "request body is not valid json: "+err.Error())
		}
		return false
	}

	// Decode accepts a prefix; require EOF to reject a second JSON value.
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		writeError(w, http.StatusBadRequest, CodeInvalidRequest, "request body must contain a single json object")
		return false
	}
	return true
}
