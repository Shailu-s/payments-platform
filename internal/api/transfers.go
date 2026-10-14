package api

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/Shailu-s/payments-platform/internal/ledger"
	"github.com/Shailu-s/payments-platform/internal/outbox"
	"github.com/Shailu-s/payments-platform/internal/transfers"
	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// ErrInsufficientFunds means the source account cannot cover the transfer.
// The database decides it: the debit fails the stored balance's CHECK.
var ErrInsufficientFunds = ledger.ErrInsufficientFunds

// Every transfer credits the settlement account first. The destination is
// credited by a second ledger transaction once the provider confirms.
const settlementAccountID = "acc_settlement_usd"

const (
	defaultPageSize = 25
	maxPageSize     = 100

	maxIdempotencyKeyLength = 255
)

type createTransferRequest struct {
	SourceAccount      string `json:"source_account"`
	DestinationAccount string `json:"destination_account"`
	Amount             int64  `json:"amount"`
	Currency           string `json:"currency"`
}

type transferResponse struct {
	ID                 string    `json:"id"`
	SourceAccount      string    `json:"source_account"`
	DestinationAccount string    `json:"destination_account"`
	Amount             int64     `json:"amount"`
	Currency           string    `json:"currency"`
	Status             string    `json:"status"`
	LedgerTxnID        *string   `json:"ledger_txn_id"`
	CreatedAt          time.Time `json:"created_at"`
	UpdatedAt          time.Time `json:"updated_at"`
}

type listTransfersResponse struct {
	Data       []transferResponse `json:"data"`
	NextCursor *string            `json:"next_cursor"`
}

func toResponse(t transfers.Transfer) transferResponse {
	return transferResponse{
		ID:                 t.ID,
		SourceAccount:      t.SourceAccount,
		DestinationAccount: t.DestinationAccount,
		Amount:             t.Amount,
		Currency:           t.Currency,
		Status:             t.Status,
		LedgerTxnID:        t.LedgerTxnID,
		CreatedAt:          t.CreatedAt,
		UpdatedAt:          t.UpdatedAt,
	}
}

// handleCreateTransfer accepts an instruction to move money. It answers 202
// "processing": the accounting is recorded, but nothing has reached the
// destination yet.
func (s *Server) handleCreateTransfer(w http.ResponseWriter, r *http.Request) {
	key, ok := APIKeyFrom(r.Context())
	if !ok {
		// Unreachable behind the auth middleware.
		writeError(w, http.StatusUnauthorized, CodeUnauthorized, "no authenticated api key")
		return
	}

	// Only the client can generate this: a retry and a genuine second payment
	// are byte for byte identical.
	idempotencyKey, msg, ok := idempotencyKeyFrom(r)
	if !ok {
		writeError(w, http.StatusBadRequest, CodeInvalidRequest, msg)
		return
	}

	var req createTransferRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	if msg, ok := validateTransferRequest(&req); !ok {
		writeError(w, http.StatusBadRequest, CodeInvalidRequest, msg)
		return
	}

	// Checked up front so a missing account is a 400, not a foreign key 500.
	if msg, code, ok := s.validateAccounts(r.Context(), req); !ok {
		if code == CodeInternal {
			writeInternalError(w, r, errors.New(msg))
			return
		}
		writeError(w, http.StatusBadRequest, code, msg)
		return
	}

	transfer, status, err := s.createTransfer(r.Context(), req, key.ID, idempotencyKey)
	switch {
	case errors.Is(err, ErrInsufficientFunds):
		writeError(w, http.StatusUnprocessableEntity, CodeInsufficientFunds,
			"the source account does not have enough money for this transfer")
		return
	case errors.Is(err, ErrAccountBusy) || isLockTimeout(err):
		w.Header().Set("Retry-After", "1")
		writeError(w, http.StatusServiceUnavailable, CodeAccountBusy,
			"the source account is busy with other payments: retry with the same "+
				"Idempotency-Key, which cannot pay twice")
		return
	case errors.Is(err, context.DeadlineExceeded):
		w.Header().Set("Retry-After", "1")
		writeError(w, http.StatusServiceUnavailable, CodeRequestTimeout,
			"the request timed out before the payment was answered: retry with the "+
				"same Idempotency-Key to get its outcome")
		return
	}
	if err != nil {
		writeInternalError(w, r, err)
		return
	}

	switch status {
	case http.StatusConflict:
		writeError(w, http.StatusConflict, CodeIdempotencyInFlight,
			"a request with this Idempotency-Key is still being processed, retry shortly")
	case http.StatusUnprocessableEntity:
		writeError(w, http.StatusUnprocessableEntity, CodeIdempotencyKeyReused,
			"this Idempotency-Key was already used for a different request: "+
				"use a new key for a new payment")
	default:
		writeJSON(w, http.StatusAccepted, toResponse(transfer))
	}
}

// createTransfer inserts the transfer, or answers a retry of one already made.
func (s *Server) createTransfer(ctx context.Context, req createTransferRequest, apiKeyID, idempotencyKey string) (transfers.Transfer, int, error) {
	fingerprint := fingerprintRequest(apiKeyID, req)

	transfer, err := s.insertTransfer(ctx, req, apiKeyID, idempotencyKey, fingerprint)
	if err == nil {
		return transfer, http.StatusAccepted, nil
	}
	if !errors.Is(err, transfers.ErrDuplicateKey) {
		return transfers.Transfer{}, http.StatusInternalServerError, err
	}

	// The key is taken, so this is a retry: answer with the original outcome,
	// which is what the client is missing. A 409 would leave it no wiser.
	return s.replayTransfer(ctx, idempotencyKey, fingerprint)
}

// insertTransfer writes the transfer, its outbox event and its ledger entries
// in one transaction: none of them may exist without the others.
//
// The order is load-bearing. The transfer goes first, so the unique index on
// the idempotency key picks one winner before anything else happens, and a
// retry of a payment that spent the last of the funds replays it instead of
// being refused. The ledger write goes last, because its balance update takes
// the row lock that serialises spending from the account; everything before
// it runs without blocking other payments from the same account.
func (s *Server) insertTransfer(ctx context.Context, req createTransferRequest, apiKeyID, idempotencyKey, fingerprint string) (transfers.Transfer, error) {
	// Before taking a pool connection: a hot account waits here, not in the pool.
	release, err := s.gate.acquire(ctx, req.SourceAccount)
	if err != nil {
		return transfers.Transfer{}, fmt.Errorf("source %s: %w", req.SourceAccount, err)
	}
	defer release()

	tx, err := s.db.Begin(ctx)
	if err != nil {
		return transfers.Transfer{}, fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback(ctx)

	// Chosen up front so the transfer row can be linked when it is inserted;
	// the deferred foreign key is checked at commit.
	ledgerTxnID := ledger.NewTxnID()
	transfer, err := transfers.Insert(ctx, tx, transfers.Transfer{
		ID:                 newID("tr"),
		SourceAccount:      req.SourceAccount,
		DestinationAccount: req.DestinationAccount,
		Amount:             req.Amount,
		Currency:           req.Currency,
		Status:             transfers.StatusProcessing,
		LedgerTxnID:        &ledgerTxnID,
		APIKeyID:           apiKeyID,
		IdempotencyKey:     &idempotencyKey,
		RequestFingerprint: &fingerprint,
	})
	if err != nil {
		return transfers.Transfer{}, err
	}

	// Written to the outbox on tx rather than published to Kafka: a publish
	// after the commit is lost if the process dies in between.
	payload, err := json.Marshal(TransferEvent{
		TransferID:         transfer.ID,
		Amount:             transfer.Amount,
		Currency:           transfer.Currency,
		SourceAccount:      transfer.SourceAccount,
		DestinationAccount: transfer.DestinationAccount,
	})
	if err != nil {
		return transfers.Transfer{}, fmt.Errorf("marshal transfer event %s: %w", transfer.ID, err)
	}
	// The lock timeout starts after the key is claimed: a duplicate may wait
	// for the original to finish, and the original's own waits are bounded.
	// From here on, a lock held too long means the account is busy.
	batch := &pgx.Batch{}
	batch.Queue(setSpendingLockTimeout)
	outbox.Queue(batch, outbox.Event{
		ID:          newID("evt"),
		AggregateID: transfer.ID,
		EventType:   "transfer.created",
		Topic:       "transfers",
		Payload:     payload,
	})
	if err := tx.SendBatch(ctx, batch).Close(); err != nil {
		return transfers.Transfer{}, fmt.Errorf("insert outbox event for %s: %w", transfer.ID, err)
	}

	// Settlement, not the destination: the provider has not paid anyone yet.
	// An overdraft fails here with ErrInsufficientFunds and rolls back the rest.
	if err := ledger.RecordWithID(ctx, tx, ledgerTxnID, "transfer "+transfer.ID, []ledger.Entry{
		{AccountID: req.SourceAccount, Direction: ledger.DirectionDebit, Amount: req.Amount},
		{AccountID: settlementAccountID, Direction: ledger.DirectionCredit, Amount: req.Amount},
	}); err != nil {
		return transfers.Transfer{}, err
	}

	if err := tx.Commit(ctx); err != nil {
		return transfers.Transfer{}, fmt.Errorf("commit transfer %s: %w", transfer.ID, err)
	}
	return transfer, nil
}

// Shorter than any client timeout: waiting longer for one account's lock only
// holds a pool connection that other accounts need.
const setSpendingLockTimeout = `SET LOCAL lock_timeout = '250ms'`

func isLockTimeout(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == pgerrcode.LockNotAvailable
}

// replayTransfer answers a request whose idempotency key was already used.
func (s *Server) replayTransfer(ctx context.Context, idempotencyKey, fingerprint string) (transfers.Transfer, int, error) {
	existing, err := s.awaitTransfer(ctx, idempotencyKey)
	if errors.Is(err, transfers.ErrNotFound) {
		// The first request has not committed yet.
		return transfers.Transfer{}, http.StatusConflict, nil
	}
	if err != nil {
		return transfers.Transfer{}, http.StatusInternalServerError, err
	}

	// Same key, different request: a client bug, not a retry.
	if existing.RequestFingerprint == nil || *existing.RequestFingerprint != fingerprint {
		return transfers.Transfer{}, http.StatusUnprocessableEntity, nil
	}

	return existing, http.StatusAccepted, nil
}

// awaitTransfer reads the transfer owning a key, retrying briefly: the
// duplicate key error can arrive before the winner's row is visible.
func (s *Server) awaitTransfer(ctx context.Context, idempotencyKey string) (transfers.Transfer, error) {
	const attempts = 10
	const gap = 20 * time.Millisecond

	var err error
	for i := 0; i < attempts; i++ {
		var transfer transfers.Transfer
		transfer, err = transfers.FindByIdempotencyKey(ctx, s.db, idempotencyKey)
		if err == nil {
			return transfer, nil
		}
		if !errors.Is(err, transfers.ErrNotFound) {
			return transfers.Transfer{}, err
		}
		select {
		case <-ctx.Done():
			return transfers.Transfer{}, ctx.Err()
		case <-time.After(gap):
		}
	}
	return transfers.Transfer{}, err
}

// fingerprintRequest hashes the request, so a key reused for a different
// payment is told apart from a retry. The api key is part of it, so callers
// cannot collide on, or probe, each other's keys.
func fingerprintRequest(apiKeyID string, req createTransferRequest) string {
	sum := sha256.Sum256([]byte(strings.Join([]string{
		apiKeyID,
		req.SourceAccount,
		req.DestinationAccount,
		strconv.FormatInt(req.Amount, 10),
		req.Currency,
	}, "|")))
	return hex.EncodeToString(sum[:])
}

// idempotencyKeyFrom reads the Idempotency-Key header. It is required: an
// optional key is forgotten exactly when it matters.
func idempotencyKeyFrom(r *http.Request) (key, message string, ok bool) {
	key = strings.TrimSpace(r.Header.Get("Idempotency-Key"))

	switch {
	case key == "":
		return "", "Idempotency-Key header is required: send a unique value per " +
			"payment so a retry cannot create a second one", false
	case len(key) > maxIdempotencyKeyLength:
		return "", fmt.Sprintf("Idempotency-Key must be at most %d characters, got %d",
			maxIdempotencyKeyLength, len(key)), false
	}
	return key, "", true
}

func validateTransferRequest(req *createTransferRequest) (string, bool) {
	req.SourceAccount = strings.TrimSpace(req.SourceAccount)
	req.DestinationAccount = strings.TrimSpace(req.DestinationAccount)
	req.Currency = strings.ToUpper(strings.TrimSpace(req.Currency))

	switch {
	case req.SourceAccount == "":
		return "source_account is required", false
	case req.DestinationAccount == "":
		return "destination_account is required", false
	case req.SourceAccount == req.DestinationAccount:
		return "source_account and destination_account must differ", false
	case req.Amount <= 0:
		return "amount must be a positive integer in minor units, for example 50000 for $500.00", false
	case req.Currency == "":
		return "currency is required", false
	case req.Currency != supportedCurrency:
		return fmt.Sprintf("currency must be %s in v1, got %q", supportedCurrency, req.Currency), false
	}
	return "", true
}

// validateAccounts confirms both accounts exist and match the requested
// currency, and that the source can be spent from, in one round trip.
func (s *Server) validateAccounts(ctx context.Context, req createTransferRequest) (message, code string, ok bool) {
	const q = `SELECT id, currency, type FROM accounts WHERE id = ANY($1)`

	rows, err := s.db.Query(ctx, q, []string{req.SourceAccount, req.DestinationAccount})
	if err != nil {
		return fmt.Sprintf("load accounts: %v", err), CodeInternal, false
	}
	defer rows.Close()

	currencies := map[string]string{}
	types := map[string]string{}
	for rows.Next() {
		var id, currency, accountType string
		if err := rows.Scan(&id, &currency, &accountType); err != nil {
			return fmt.Sprintf("scan account: %v", err), CodeInternal, false
		}
		currencies[id] = currency
		types[id] = accountType
	}
	if err := rows.Err(); err != nil {
		return fmt.Sprintf("load accounts: %v", err), CodeInternal, false
	}

	for _, id := range []string{req.SourceAccount, req.DestinationAccount} {
		currency, found := currencies[id]
		if !found {
			// 400, not 404: the account is a field in the body, not the
			// resource being addressed.
			return "no such account: " + id, CodeInvalidRequest, false
		}
		if currency != req.Currency {
			return fmt.Sprintf("account %s holds %s, not %s", id, currency, req.Currency),
				CodeInvalidRequest, false
		}
	}
	// A settlement account has no stored balance, so nothing would stop it
	// from being overspent.
	if types[req.SourceAccount] == "settlement" {
		return fmt.Sprintf("account %s is a settlement account and cannot be a transfer source",
			req.SourceAccount), CodeInvalidRequest, false
	}
	return "", "", true
}

func (s *Server) handleGetTransfer(w http.ResponseWriter, r *http.Request) {
	transfer, err := transfers.Get(r.Context(), s.db, r.PathValue("id"))
	if errors.Is(err, transfers.ErrNotFound) {
		writeError(w, http.StatusNotFound, CodeNotFound, "no such transfer: "+r.PathValue("id"))
		return
	}
	if err != nil {
		writeInternalError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toResponse(transfer))
}

func (s *Server) handleListTransfers(w http.ResponseWriter, r *http.Request) {
	limit := defaultPageSize
	if raw := r.URL.Query().Get("limit"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 1 || parsed > maxPageSize {
			writeError(w, http.StatusBadRequest, CodeInvalidRequest,
				fmt.Sprintf("limit must be between 1 and %d", maxPageSize))
			return
		}
		limit = parsed
	}

	var (
		cursorCreatedAt *time.Time
		cursorID        string
	)
	if raw := r.URL.Query().Get("cursor"); raw != "" {
		createdAt, id, err := decodeCursor(raw)
		if err != nil {
			writeError(w, http.StatusBadRequest, CodeInvalidRequest, "cursor is not valid")
			return
		}
		cursorCreatedAt, cursorID = &createdAt, id
	}

	// One extra row tells us whether there is a next page.
	page, err := transfers.List(r.Context(), s.db, limit+1, cursorCreatedAt, cursorID)
	if err != nil {
		writeInternalError(w, r, err)
		return
	}

	var nextCursor *string
	if len(page) > limit {
		page = page[:limit]
		last := page[len(page)-1]
		encoded := encodeCursor(last.CreatedAt, last.ID)
		nextCursor = &encoded
	}

	// An empty array, never null.
	data := make([]transferResponse, 0, len(page))
	for _, t := range page {
		data = append(data, toResponse(t))
	}
	writeJSON(w, http.StatusOK, listTransfersResponse{Data: data, NextCursor: nextCursor})
}

// Keyset cursor: OFFSET skips or repeats rows as transfers arrive, and
// created_at alone is not unique.
func encodeCursor(createdAt time.Time, id string) string {
	return base64.RawURLEncoding.EncodeToString(
		[]byte(createdAt.UTC().Format(time.RFC3339Nano) + "|" + id))
}

func decodeCursor(encoded string) (time.Time, string, error) {
	raw, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		return time.Time{}, "", err
	}
	timestamp, id, found := strings.Cut(string(raw), "|")
	if !found {
		return time.Time{}, "", errors.New("cursor is malformed")
	}
	createdAt, err := time.Parse(time.RFC3339Nano, timestamp)
	if err != nil {
		return time.Time{}, "", err
	}
	return createdAt, id, nil
}
