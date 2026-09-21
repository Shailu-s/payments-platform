package ledger

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// Directions. Stored as text and constrained by a CHECK in the schema, so an
// invalid direction cannot reach the table even if this package is bypassed.
const (
	DirectionDebit  = "debit"
	DirectionCredit = "credit"
)

// Beginner is anything that can start a transaction: a pool or a connection.
// Record needs this rather than a Querier because the whole point of the
// function is that its writes are atomic.
type Beginner interface {
	Begin(ctx context.Context) (pgx.Tx, error)
}

// Errors callers are expected to branch on. Wrapped with %w at the return
// site, so errors.Is works through the context added by each message.
var (
	ErrTooFewEntries     = errors.New("a transaction needs at least two entries")
	ErrNonPositiveAmount = errors.New("entry amounts must be greater than zero")
	ErrInvalidDirection  = errors.New("entry direction must be debit or credit")
	ErrUnbalanced        = errors.New("debits do not equal credits")
)

// Entry is one side of a movement. Amount is in minor units and is always
// positive; Direction carries the sign.
type Entry struct {
	AccountID string
	Direction string
	Amount    int64
}

// Record writes one balanced ledger transaction and its entries atomically.
//
// The constraint that forces the database transaction: a movement with its
// debit written and its credit not written is money destroyed. There is no
// valid intermediate state, so either all of it lands or none of it does.
//
// Deliberately absent: any check that the source account has enough money.
// That is a read-then-write race, and it is solved properly in phase 3.
func Record(ctx context.Context, db Beginner, reference string, entries []Entry) (string, error) {
	// Validate before opening a transaction. An unbalanced set is a caller bug,
	// not a database concern, and there is no reason to hold a connection to
	// discover it.
	if len(entries) < 2 {
		return "", fmt.Errorf("record %q: got %d: %w", reference, len(entries), ErrTooFewEntries)
	}

	var debits, credits int64
	for i, e := range entries {
		if e.Amount <= 0 {
			return "", fmt.Errorf("record %q: entry %d amount %d: %w", reference, i, e.Amount, ErrNonPositiveAmount)
		}
		switch e.Direction {
		case DirectionDebit:
			debits += e.Amount
		case DirectionCredit:
			credits += e.Amount
		default:
			return "", fmt.Errorf("record %q: entry %d direction %q: %w", reference, i, e.Direction, ErrInvalidDirection)
		}
	}
	if debits != credits {
		return "", fmt.Errorf("record %q: debits %d, credits %d: %w", reference, debits, credits, ErrUnbalanced)
	}

	tx, err := db.Begin(ctx)
	if err != nil {
		return "", fmt.Errorf("record %q: begin: %w", reference, err)
	}
	// Harmless after a successful commit, and it is what releases the
	// connection on every early return below.
	defer tx.Rollback(ctx)

	txnID := newID("ltx")
	const insertTxn = `INSERT INTO ledger_transactions (id, reference) VALUES ($1, $2)`
	if _, err := tx.Exec(ctx, insertTxn, txnID, reference); err != nil {
		return "", fmt.Errorf("record %q: insert transaction: %w", reference, err)
	}

	const insertEntry = `
		INSERT INTO ledger_entries (id, txn_id, account_id, direction, amount)
		VALUES ($1, $2, $3, $4, $5)`
	batch := &pgx.Batch{}
	for _, e := range entries {
		batch.Queue(insertEntry, newID("le"), txnID, e.AccountID, e.Direction, e.Amount)
	}
	if err := tx.SendBatch(ctx, batch).Close(); err != nil {
		return "", fmt.Errorf("record %q: insert entries: %w", reference, err)
	}

	if err := tx.Commit(ctx); err != nil {
		return "", fmt.Errorf("record %q: commit: %w", reference, err)
	}
	return txnID, nil
}
