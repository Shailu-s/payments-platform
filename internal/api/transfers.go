package api

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/Shailu-s/payments-platform/internal/ledger"
	"github.com/Shailu-s/payments-platform/internal/transfers"
)

// The platform settlement account, created by migration 000003. Every transfer
// credits this one account: at commit time the money is ours and earmarked, and
// the destination is credited by a second ledger transaction in phase 4 when
// the provider confirms.
const settlementAccountID = "acc_settlement_usd"

const (
	defaultPageSize = 25
	maxPageSize     = 100

	// Bounded because the value is stored on every transfer and appears in log
	// lines. A uuid is 36 characters; this leaves room for a client's own
	// scheme without letting one send a megabyte.
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

// handleCreateTransfer accepts an instruction to move money.
//
// It returns 202, not 201, and status "processing", not "settled": we have
// accepted the instruction and recorded the accounting, but no money has
// reached the destination. Saying otherwise would be a lie the ledger makes
// permanent.
//
// Deliberately absent: any check that the source account holds enough money.
// That is a read-then-write race and it is phase 3's entire subject. Recording
// the hole here is better than writing the broken version.
func (s *Server) handleCreateTransfer(w http.ResponseWriter, r *http.Request) {
	key, ok := APIKeyFrom(r.Context())
	if !ok {
		// Unreachable behind the auth middleware. Checked anyway, because the
		// alternative is attributing a transfer to a zero-valued key.
		writeError(w, http.StatusUnauthorized, CodeUnauthorized, "no authenticated api key")
		return
	}

	// The client generates this, and it has to: the server cannot tell a retry
	// from a genuine second payment, because the two are byte for byte
	// identical. The difference exists only in the caller's knowledge that it
	// is sending the same instruction again.
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

	// Both accounts must exist and agree on currency before anything is
	// written. The ledger's foreign keys would catch a missing account, but as
	// a 500 rather than the 400 it actually is.
	if msg, code, ok := s.validateAccounts(r.Context(), req); !ok {
		if code == CodeInternal {
			writeInternalError(w, r, errors.New(msg))
			return
		}
		writeError(w, http.StatusBadRequest, code, msg)
		return
	}

	transfer, status, err := s.createTransfer(r.Context(), req, key.ID, idempotencyKey)
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
		// 202 on the first request and on every replay alike: we accepted the
		// instruction, and the money has not moved.
		writeJSON(w, http.StatusAccepted, toResponse(transfer))
	}
}

// createTransfer writes the transfer and its ledger entries in ONE database
// transaction.
//
// The constraint that forces it: a transfer row without its ledger entries is
// an instruction with no accounting behind it, and ledger entries without their
// transfer are money moved for no recorded reason. Neither is a state the
// system may be observed in, so both land or neither does.
//
// Note what makes this possible: ledger.Record takes a Beginner and
// transfers.Insert takes a Querier, so both accept the pgx.Tx started here.
// Had phase 1 typed those as *pgxpool.Pool, this handler could not be atomic.
func (s *Server) createTransfer(ctx context.Context, req createTransferRequest, apiKeyID, idempotencyKey string) (transfers.Transfer, int, error) {
	fingerprint := fingerprintRequest(apiKeyID, req)

	transfer, err := s.insertTransfer(ctx, req, apiKeyID, idempotencyKey, fingerprint)
	if err == nil {
		return transfer, http.StatusAccepted, nil
	}
	if !errors.Is(err, transfers.ErrDuplicateKey) {
		return transfers.Transfer{}, http.StatusInternalServerError, err
	}

	// The unique index refused this insert, which means another request already
	// owns the key: this is a retry. The client retried because it never
	// learned the outcome of the first attempt, so it must now be told that
	// outcome — not that it conflicted. A 409 leaves it exactly as ignorant as
	// it was, and its only safe move would be to retry harder.
	return s.replayTransfer(ctx, idempotencyKey, fingerprint)
}

// insertTransfer writes the transfer and its ledger entries in ONE database
// transaction.
//
// The constraint that forces it: a transfer row without its ledger entries is
// an instruction with no accounting behind it, and ledger entries without their
// transfer are money moved for no recorded reason. Neither is a state the
// system may be observed in.
//
// The insert comes FIRST, before any check for an existing transfer. That order
// is the whole fix. Reading first and inserting if nothing came back leaves a
// window in which every concurrent request reads the same nothing — measured at
// 99 transfers from 100 requests. Inserting first makes the database the
// arbiter: it blocks the second writer until the first commits, then refuses
// it. Application code cannot produce that blocking, because the two requests
// may be in different processes.
func (s *Server) insertTransfer(ctx context.Context, req createTransferRequest, apiKeyID, idempotencyKey, fingerprint string) (transfers.Transfer, error) {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return transfers.Transfer{}, fmt.Errorf("begin: %w", err)
	}
	// Harmless after a successful commit, and it is what releases the
	// connection on every early return below.
	defer tx.Rollback(ctx)

	transfer, err := transfers.Insert(ctx, tx, transfers.Transfer{
		ID:                 newID("tr"),
		SourceAccount:      req.SourceAccount,
		DestinationAccount: req.DestinationAccount,
		Amount:             req.Amount,
		Currency:           req.Currency,
		// processing, not created: the instruction is accepted and the
		// accounting is written, so there is no moment where it sits idle.
		Status:             transfers.StatusProcessing,
		APIKeyID:           apiKeyID,
		IdempotencyKey:     &idempotencyKey,
		RequestFingerprint: &fingerprint,
	})
	if err != nil {
		return transfers.Transfer{}, err
	}

	// Source debited, settlement credited. NOT the destination: at this moment
	// the money is ours and earmarked, and the ledger is append-only, so
	// crediting the destination now would write a permanent lie.
	ledgerTxnID, err := ledger.Record(ctx, tx, "transfer "+transfer.ID, []ledger.Entry{
		{AccountID: req.SourceAccount, Direction: ledger.DirectionDebit, Amount: req.Amount},
		{AccountID: settlementAccountID, Direction: ledger.DirectionCredit, Amount: req.Amount},
	})
	if err != nil {
		return transfers.Transfer{}, err
	}

	if err := transfers.SetLedgerTxn(ctx, tx, transfer.ID, ledgerTxnID); err != nil {
		return transfers.Transfer{}, err
	}
	transfer.LedgerTxnID = &ledgerTxnID

	if err := tx.Commit(ctx); err != nil {
		return transfers.Transfer{}, fmt.Errorf("commit transfer %s: %w", transfer.ID, err)
	}
	return transfer, nil
}

// replayTransfer answers a request whose idempotency key was already used.
func (s *Server) replayTransfer(ctx context.Context, idempotencyKey, fingerprint string) (transfers.Transfer, int, error) {
	existing, err := s.awaitTransfer(ctx, idempotencyKey)
	if errors.Is(err, transfers.ErrNotFound) {
		// The key is taken but its row is not visible, so the first request is
		// still in flight and has not committed. Nothing true can be said about
		// the outcome yet, so say exactly that and let the client retry.
		return transfers.Transfer{}, http.StatusConflict, nil
	}
	if err != nil {
		return transfers.Transfer{}, http.StatusInternalServerError, err
	}

	// Same key, different request. This is a client bug, not a retry, and it
	// must be told so: returning the original transfer would let a caller
	// believe a payment happened that never did.
	if existing.RequestFingerprint == nil || *existing.RequestFingerprint != fingerprint {
		return transfers.Transfer{}, http.StatusUnprocessableEntity, nil
	}

	return existing, http.StatusAccepted, nil
}

// awaitTransfer reads the transfer owning a key, retrying briefly.
//
// A duplicate key error can arrive before the winning transaction has
// committed: the index entry is taken while the row is still invisible to
// everyone else. Retrying gives the winner a moment to finish, which turns most
// of these into a correct replay rather than a 409 the client has to handle.
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

// fingerprintRequest hashes what the caller asked for, so a key reused with
// different terms can be told apart from a genuine retry.
//
// The api key is included: two callers must not be able to collide on each
// other's idempotency keys, and without it one caller could learn another's
// transfer by guessing a key.
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

// idempotencyKeyFrom reads and validates the Idempotency-Key header.
//
// Required, not optional. An optional key is forgotten exactly when it matters,
// and the cost of forgetting it is a duplicate payment. Stripe makes it
// optional for backward compatibility with clients that predate it; this API
// has no such history.
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
		// An account paying itself nets to nothing, and the ledger would still
		// balance, so the invariant never notices.
		return "source_account and destination_account must differ", false
	case req.Amount <= 0:
		// Minor units. The type is int64 for the reason in the ledger: a float
		// cannot represent most decimal fractions exactly.
		return "amount must be a positive integer in minor units, for example 50000 for $500.00", false
	case req.Currency == "":
		return "currency is required", false
	case req.Currency != supportedCurrency:
		return fmt.Sprintf("currency must be %s in v1, got %q", supportedCurrency, req.Currency), false
	}
	return "", true
}

// validateAccounts confirms both accounts exist and match the requested
// currency, in one round trip.
func (s *Server) validateAccounts(ctx context.Context, req createTransferRequest) (message, code string, ok bool) {
	const q = `SELECT id, currency FROM accounts WHERE id = ANY($1)`

	rows, err := s.db.Query(ctx, q, []string{req.SourceAccount, req.DestinationAccount})
	if err != nil {
		return fmt.Sprintf("load accounts: %v", err), CodeInternal, false
	}
	defer rows.Close()

	currencies := map[string]string{}
	for rows.Next() {
		var id, currency string
		if err := rows.Scan(&id, &currency); err != nil {
			return fmt.Sprintf("scan account: %v", err), CodeInternal, false
		}
		currencies[id] = currency
	}
	if err := rows.Err(); err != nil {
		return fmt.Sprintf("load accounts: %v", err), CodeInternal, false
	}

	for _, id := range []string{req.SourceAccount, req.DestinationAccount} {
		currency, found := currencies[id]
		if !found {
			// invalid_request, not not_found: the account is a field in the
			// request body rather than the resource being addressed, so the
			// request is malformed. A 404 here would claim POST /v1/transfers
			// does not exist.
			return "no such account: " + id, CodeInvalidRequest, false
		}
		if currency != req.Currency {
			return fmt.Sprintf("account %s holds %s, not %s", id, currency, req.Currency),
				CodeInvalidRequest, false
		}
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

	// One more than asked for, so the presence of a next page is known without
	// a second count query.
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

	// An empty array, never null: a caller iterating the response should not
	// have to special-case the last page.
	data := make([]transferResponse, 0, len(page))
	for _, t := range page {
		data = append(data, toResponse(t))
	}
	writeJSON(w, http.StatusOK, listTransfersResponse{Data: data, NextCursor: nextCursor})
}

// Cursors are keyset, not OFFSET: with new transfers arriving constantly an
// offset page skips or repeats rows. The pair is needed because created_at
// alone is not unique.
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
