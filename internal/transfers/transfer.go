// Package transfers stores payment instructions and their lifecycle.
// One transfer can produce multiple immutable ledger transactions.
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

// Transfer statuses are mutable; ledger entries are not.
const (
	StatusCreated    = "created"
	StatusProcessing = "processing"
	StatusSettled    = "settled"
	StatusFailed     = "failed"
	// StatusUnresolved means the provider outcome is unknown, not failed.
	StatusUnresolved = "unresolved"
)

var (
	ErrNotFound = errors.New("transfer not found")
	// ErrDuplicateKey means another transfer owns the idempotency key.
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
	// IdempotencyKey is nullable for transfers created outside the API.
	IdempotencyKey *string
	// RequestFingerprint distinguishes key reuse with different terms from a retry.
	RequestFingerprint *string
	// ProviderRef is nil until acceptance is recorded, including unknown outcomes.
	ProviderRef *string
	// AttemptCount counts claims, not confirmed provider submissions.
	AttemptCount int
	// NextAttemptAt is the retry deadline. Nil processing transfers without a
	// provider reference use the consumer or wait for the poller's HeadStart.
	NextAttemptAt *time.Time
	// LastError is diagnostic text, not machine-readable state.
	LastError *string
	CreatedAt time.Time
	UpdatedAt time.Time
}

// Querier allows store operations to share the caller's transaction.
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
		return Transfer{}, ErrDuplicateKey
	}
	if err != nil {
		return Transfer{}, fmt.Errorf("insert transfer %s: %w", t.ID, err)
	}
	return stored, nil
}

// IsDuplicateKey checks SQLSTATE 23505 rather than a localised error message.
func IsDuplicateKey(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == pgerrcode.UniqueViolation
}

// SetLedgerTxn links the initial movement; call it in the transfer's transaction.
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

// FindByIdempotencyKey returns the transfer owning a key.
// This lookup alone cannot prevent concurrent inserts; the unique index does.
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

// List pages newest first using (created_at, id); timestamps alone are not unique
// and OFFSET can skip or repeat rows as transfers arrive.
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
