// Package transfers holds the business intent of moving money: who paid whom,
// how much, and what state that instruction is in.
//
// It sits on top of the ledger rather than replacing it. The ledger records the
// accounting fact; a transfer records what a caller asked for. One transfer
// produces several ledger transactions over its life — the movement now, the
// settlement in phase 4, perhaps a reversal or a fee — which is why the two are
// separate tables.
package transfers

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Status values. Mutable, unlike a ledger entry, and that distinction is
// coherent: this is business state, not an accounting record. The append-only
// rule applies to ledger_entries.
const (
	StatusCreated    = "created"
	StatusProcessing = "processing"
	StatusSettled    = "settled"
	StatusFailed     = "failed"
	// StatusUnresolved means a call to the rail timed out and we genuinely do
	// not know whether the money moved. Not a failure and not a retry.
	StatusUnresolved = "unresolved"
)

var (
	ErrNotFound = errors.New("transfer not found")
	// ErrDuplicateKey means the unique index on idempotency_key refused this
	// insert: another request already owns the key. It is not a failure, it is
	// the signal that this request is a retry.
	ErrDuplicateKey = errors.New("idempotency key already used")
)

type Transfer struct {
	ID                 string
	SourceAccount      string
	DestinationAccount string
	Amount             int64
	Currency           string
	Status             string
	LedgerTxnID        *string
	APIKeyID           string
	// Supplied by the client so a retry can be recognised as one. Nullable
	// because transfers created by anything other than the API — a reversal in
	// phase 4, say — have no client instruction behind them.
	IdempotencyKey *string
	// A hash of the request that created this transfer. Same key with a
	// different fingerprint is a client bug rather than a retry.
	RequestFingerprint *string
	// The rail's own identifier, once it has accepted the instruction. Nil
	// until then, and nil forever on a transfer the rail never saw.
	ProviderRef *string
	// How many times a worker has sent this to the rail. For observability and
	// for giving up, not for deciding what to do next.
	AttemptCount int
	// When this transfer next becomes claimable. Nil means never, which is how
	// an unresolved transfer stops being retried.
	NextAttemptAt *time.Time
	// The last thing the rail said. Kept for an operator to read, not parsed.
	LastError *string
	CreatedAt time.Time
	UpdatedAt time.Time
}

// Querier is satisfied by a pool, a connection and a transaction alike, so
// Insert can run inside the caller's transaction. That is the whole point:
// POST /transfers writes the transfer and its ledger entries atomically.
type Querier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

const columns = `id, source_account, destination_account, amount, currency,
	status, ledger_txn_id, api_key_id, idempotency_key, request_fingerprint,
	provider_ref, attempt_count, next_attempt_at, last_error,
	created_at, updated_at`

// Insert writes a transfer row and returns it as stored.
func Insert(ctx context.Context, db Querier, t Transfer) (Transfer, error) {
	const q = `
		INSERT INTO transfers (id, source_account, destination_account, amount,
			currency, status, ledger_txn_id, api_key_id, idempotency_key,
			request_fingerprint)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		RETURNING ` + columns

	row := db.QueryRow(ctx, q, t.ID, t.SourceAccount, t.DestinationAccount, t.Amount,
		t.Currency, t.Status, t.LedgerTxnID, t.APIKeyID, t.IdempotencyKey,
		t.RequestFingerprint)

	stored, err := scan(row)
	if IsDuplicateKey(err) {
		// The unique index fired: another request holds this key. Returned
		// unwrapped so the caller can branch on it without unwrapping first.
		return Transfer{}, ErrDuplicateKey
	}
	if err != nil {
		return Transfer{}, fmt.Errorf("insert transfer %s: %w", t.ID, err)
	}
	return stored, nil
}

// IsDuplicateKey reports whether an error is Postgres 23505, unique_violation.
//
// Branching on the SQLSTATE rather than on a string match of the message: the
// message is localised and can change between versions, the code cannot.
func IsDuplicateKey(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == pgerrcode.UniqueViolation
}

// SetLedgerTxn links a transfer to the ledger transaction that recorded its
// movement. Called inside the same transaction as the insert, so a transfer is
// never visible without its accounting.
func SetLedgerTxn(ctx context.Context, db Querier, transferID, ledgerTxnID string) error {
	const q = `
		UPDATE transfers
		SET ledger_txn_id = $1, updated_at = now()
		WHERE id = $2
		RETURNING id`

	var id string
	if err := db.QueryRow(ctx, q, ledgerTxnID, transferID).Scan(&id); err != nil {
		return fmt.Errorf("link transfer %s to ledger txn %s: %w", transferID, ledgerTxnID, err)
	}
	return nil
}

// FindByIdempotencyKey returns the transfer a client's key already created, if
// there is one.
//
// On its own this is NOT enough to make POST /transfers idempotent: a caller
// that reads here and inserts afterwards has a window between the two in which
// a concurrent request reads the same nothing. Phase 3.2 adds the unique
// constraint that closes it.
func FindByIdempotencyKey(ctx context.Context, db Querier, key string) (Transfer, error) {
	const q = `SELECT ` + columns + ` FROM transfers WHERE idempotency_key = $1`

	t, err := scan(db.QueryRow(ctx, q, key))
	if errors.Is(err, pgx.ErrNoRows) {
		return Transfer{}, ErrNotFound
	}
	if err != nil {
		return Transfer{}, fmt.Errorf("find transfer by idempotency key: %w", err)
	}
	return t, nil
}

func Get(ctx context.Context, db Querier, id string) (Transfer, error) {
	const q = `SELECT ` + columns + ` FROM transfers WHERE id = $1`

	t, err := scan(db.QueryRow(ctx, q, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return Transfer{}, ErrNotFound
	}
	if err != nil {
		return Transfer{}, fmt.Errorf("get transfer %s: %w", id, err)
	}
	return t, nil
}

// List returns transfers newest first. The cursor is the id of the last row of
// the previous page, paired with its created_at: ordering by a timestamp alone
// is not stable when two rows share one, and an OFFSET would skip or repeat
// rows as new transfers arrive.
func List(ctx context.Context, db Querier, limit int, cursorCreatedAt *time.Time, cursorID string) ([]Transfer, error) {
	var (
		rows pgx.Rows
		err  error
	)
	if cursorCreatedAt == nil {
		const q = `SELECT ` + columns + ` FROM transfers ORDER BY created_at DESC, id DESC LIMIT $1`
		rows, err = db.Query(ctx, q, limit)
	} else {
		const q = `
			SELECT ` + columns + ` FROM transfers
			WHERE (created_at, id) < ($1, $2)
			ORDER BY created_at DESC, id DESC
			LIMIT $3`
		rows, err = db.Query(ctx, q, *cursorCreatedAt, cursorID, limit)
	}
	if err != nil {
		return nil, fmt.Errorf("list transfers: %w", err)
	}
	defer rows.Close()

	var out []Transfer
	for rows.Next() {
		t, err := scan(rows)
		if err != nil {
			return nil, fmt.Errorf("list transfers: %w", err)
		}
		out = append(out, t)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list transfers: %w", err)
	}
	return out, nil
}

// scannable is satisfied by both pgx.Row and pgx.Rows, so one scan function
// serves a single-row query and a loop over many.
type scannable interface {
	Scan(dest ...any) error
}

func scan(row scannable) (Transfer, error) {
	var t Transfer
	err := row.Scan(&t.ID, &t.SourceAccount, &t.DestinationAccount, &t.Amount,
		&t.Currency, &t.Status, &t.LedgerTxnID, &t.APIKeyID, &t.IdempotencyKey,
		&t.RequestFingerprint, &t.ProviderRef, &t.AttemptCount, &t.NextAttemptAt,
		&t.LastError, &t.CreatedAt, &t.UpdatedAt)
	return t, err
}
