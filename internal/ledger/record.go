package ledger

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Ledger directions are also constrained by the database.
const (
	DirectionDebit  = "debit"
	DirectionCredit = "credit"
)

// Beginner supports atomic ledger writes: a pool, or the caller's pgx.Tx.
type Beginner interface {
	Begin(ctx context.Context) (pgx.Tx, error)
}

var (
	ErrTooFewEntries     = errors.New("a transaction needs at least two entries")
	ErrNonPositiveAmount = errors.New("entry amounts must be greater than zero")
	ErrInvalidDirection  = errors.New("entry direction must be debit or credit")
	ErrUnbalanced        = errors.New("debits do not equal credits")
	ErrAmountOverflow    = errors.New("ledger total exceeds int64 minor units")
	// ErrInsufficientFunds means a debit would take a stored balance below zero.
	ErrInsufficientFunds = errors.New("insufficient funds")
)

// Entry is one side of a movement. Amount is in minor units and is always
// positive; Direction carries the sign.
type Entry struct {
	AccountID string
	Direction string
	Amount    int64
}

// Record writes a balanced transaction and its entries atomically, and applies
// them to the stored balances. A debit that would overdraw an account fails with
// ErrInsufficientFunds and writes nothing.
func Record(ctx context.Context, db Beginner, reference string, entries []Entry) (string, error) {
	txnID := NewTxnID()
	if err := RecordWithID(ctx, db, txnID, reference, entries); err != nil {
		return "", err
	}
	return txnID, nil
}

// NewTxnID returns an id for RecordWithID, so a caller can reference the
// transaction before it is written.
func NewTxnID() string {
	return newID("ltx")
}

// RecordWithID is Record with a caller-chosen transaction id.
//
// The balance updates are queued last: the balance row lock is what serialises
// spending from an account, so it is taken as late as possible and held only
// until the caller commits.
//
// Given the caller's pgx.Tx it writes in that transaction without a savepoint,
// so a failed write leaves the caller's transaction aborted: roll it back.
func RecordWithID(ctx context.Context, db Beginner, txnID, reference string, entries []Entry) error {
	// Reject invalid entries before reserving a database connection.
	if len(entries) < 2 {
		return fmt.Errorf("record %q: got %d: %w", reference, len(entries), ErrTooFewEntries)
	}

	var debits, credits int64
	for i, e := range entries {
		if e.Amount <= 0 {
			return fmt.Errorf("record %q: entry %d amount %d: %w", reference, i, e.Amount, ErrNonPositiveAmount)
		}
		var total *int64
		switch e.Direction {
		case DirectionDebit:
			total = &debits
		case DirectionCredit:
			total = &credits
		default:
			return fmt.Errorf("record %q: entry %d direction %q: %w", reference, i, e.Direction, ErrInvalidDirection)
		}
		if e.Amount > math.MaxInt64-*total {
			return fmt.Errorf("record %q: entry %d: %w", reference, i, ErrAmountOverflow)
		}
		*total += e.Amount
	}
	if debits != credits {
		return fmt.Errorf("record %q: debits %d, credits %d: %w", reference, debits, credits, ErrUnbalanced)
	}

	// A savepoint would add SAVEPOINT and RELEASE round trips, the second while
	// the balance lock is held, and every caller rolls back on error anyway.
	if tx, ok := db.(pgx.Tx); ok {
		return write(ctx, tx, txnID, reference, entries)
	}

	tx, err := db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("record %q: begin: %w", reference, err)
	}
	// Release the transaction on every error path; harmless after commit.
	defer tx.Rollback(ctx)
	if err := write(ctx, tx, txnID, reference, entries); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("record %q: commit: %w", reference, err)
	}
	return nil
}

// write sends the transaction, its entries and the balance updates as one batch.
func write(ctx context.Context, tx pgx.Tx, txnID, reference string, entries []Entry) error {
	const insertTxn = `INSERT INTO ledger_transactions (id, reference) VALUES ($1, $2)`
	const insertEntry = `
		INSERT INTO ledger_entries (id, txn_id, account_id, direction, amount)
		VALUES ($1, $2, $3, $4, $5)`
	// Settlement accounts have no row; matching nothing is expected.
	const applyDelta = `UPDATE account_balances SET balance = balance + $2 WHERE account_id = $1`

	batch := &pgx.Batch{}
	batch.Queue(insertTxn, txnID, reference)
	for _, e := range entries {
		batch.Queue(insertEntry, newID("le"), txnID, e.AccountID, e.Direction, e.Amount)
	}
	for _, d := range netDeltas(entries) {
		batch.Queue(applyDelta, d.accountID, d.delta)
	}
	if err := tx.SendBatch(ctx, batch).Close(); err != nil {
		if isOverdraft(err) {
			return fmt.Errorf("record %q: %w: %w", reference, ErrInsufficientFunds, err)
		}
		return fmt.Errorf("record %q: write: %w", reference, err)
	}
	return nil
}

type balanceDelta struct {
	accountID string
	delta     int64
}

// netDeltas sums each account's signed movement and orders accounts by id.
// A fixed lock order means two transactions touching the same accounts in
// opposite directions queue behind each other instead of deadlocking.
func netDeltas(entries []Entry) []balanceDelta {
	sums := map[string]int64{}
	for _, e := range entries {
		if e.Direction == DirectionCredit {
			sums[e.AccountID] += e.Amount
		} else {
			sums[e.AccountID] -= e.Amount
		}
	}
	deltas := make([]balanceDelta, 0, len(sums))
	for id, delta := range sums {
		// No change, no lock.
		if delta != 0 {
			deltas = append(deltas, balanceDelta{accountID: id, delta: delta})
		}
	}
	slices.SortFunc(deltas, func(a, b balanceDelta) int { return strings.Compare(a.accountID, b.accountID) })
	return deltas
}

func isOverdraft(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) &&
		pgErr.Code == pgerrcode.CheckViolation &&
		pgErr.ConstraintName == "account_balances_non_negative"
}
