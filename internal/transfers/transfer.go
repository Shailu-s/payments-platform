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

	"github.com/jackc/pgx/v5"
)

// Status values. Mutable, unlike a ledger entry, and that distinction is
// coherent: this is business state, not an accounting record. The append-only
// rule applies to ledger_entries.
const (
	StatusCreated    = "created"
	StatusProcessing = "processing"
	StatusSettled    = "settled"
	StatusFailed     = "failed"
)

var ErrNotFound = errors.New("transfer not found")

type Transfer struct {
	ID                 string
	SourceAccount      string
	DestinationAccount string
	Amount             int64
	Currency           string
	Status             string
	LedgerTxnID        *string
	APIKeyID           string
	CreatedAt          time.Time
	UpdatedAt          time.Time
}

// Querier is satisfied by a pool, a connection and a transaction alike, so
// Insert can run inside the caller's transaction. That is the whole point:
// POST /transfers writes the transfer and its ledger entries atomically.
type Querier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

const columns = `id, source_account, destination_account, amount, currency,
	status, ledger_txn_id, api_key_id, created_at, updated_at`

// Insert writes a transfer row and returns it as stored.
func Insert(ctx context.Context, db Querier, t Transfer) (Transfer, error) {
	const q = `
		INSERT INTO transfers (id, source_account, destination_account, amount,
			currency, status, ledger_txn_id, api_key_id)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		RETURNING ` + columns

	row := db.QueryRow(ctx, q, t.ID, t.SourceAccount, t.DestinationAccount, t.Amount,
		t.Currency, t.Status, t.LedgerTxnID, t.APIKeyID)

	stored, err := scan(row)
	if err != nil {
		return Transfer{}, fmt.Errorf("insert transfer %s: %w", t.ID, err)
	}
	return stored, nil
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
		&t.Currency, &t.Status, &t.LedgerTxnID, &t.APIKeyID, &t.CreatedAt, &t.UpdatedAt)
	return t, err
}
