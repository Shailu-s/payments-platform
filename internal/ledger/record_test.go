package ledger

import (
	"context"
	"errors"
	"testing"
)

// Guarantee 1: the ledger always balances — every transaction's debits equal
// its credits.
func TestRecordMovesMoneyAndBalances(t *testing.T) {
	resetDB(t)
	ctx := context.Background()
	createAccount(t, "acc_a")
	createAccount(t, "acc_b")

	txnID, err := Record(ctx, testPool, "vendor payout", []Entry{
		{AccountID: "acc_a", Direction: DirectionDebit, Amount: 50000},
		{AccountID: "acc_b", Direction: DirectionCredit, Amount: 50000},
	})
	if err != nil {
		t.Fatalf("Record: %v", err)
	}
	if txnID == "" {
		t.Fatal("Record returned an empty transaction id")
	}

	balanceA, err := Balance(ctx, testPool, "acc_a")
	if err != nil {
		t.Fatalf("Balance(acc_a): %v", err)
	}
	if balanceA != -50000 {
		t.Errorf("Balance(acc_a) = %d, want -50000", balanceA)
	}

	balanceB, err := Balance(ctx, testPool, "acc_b")
	if err != nil {
		t.Fatalf("Balance(acc_b): %v", err)
	}
	if balanceB != 50000 {
		t.Errorf("Balance(acc_b) = %d, want 50000", balanceB)
	}

	// The invariant itself: the transaction's entries sum to zero. Asserting
	// the two balances is not the same claim — this one holds for any number
	// of entries on either side.
	var sum int64
	if err := testPool.QueryRow(ctx, `
		SELECT COALESCE(SUM(CASE WHEN direction = 'credit' THEN amount ELSE -amount END), 0)
		FROM ledger_entries WHERE txn_id = $1`, txnID).Scan(&sum); err != nil {
		t.Fatalf("sum entries: %v", err)
	}
	if sum != 0 {
		t.Errorf("entries of %s sum to %d, want 0: the books do not balance", txnID, sum)
	}
}

// An unbalanced write must be refused AND must leave nothing behind. The second
// half is the real assertion: an error return means nothing if half a
// transaction was already committed.
func TestRecordRejectsUnbalancedAndWritesNothing(t *testing.T) {
	resetDB(t)
	ctx := context.Background()
	createAccount(t, "acc_a")
	createAccount(t, "acc_b")

	_, err := Record(ctx, testPool, "bad write", []Entry{
		{AccountID: "acc_a", Direction: DirectionDebit, Amount: 500},
		{AccountID: "acc_b", Direction: DirectionCredit, Amount: 400},
	})
	if !errors.Is(err, ErrUnbalanced) {
		t.Fatalf("Record error = %v, want ErrUnbalanced", err)
	}

	txns, entries := countRows(t)
	if txns != 0 || entries != 0 {
		t.Errorf("after a rejected write: %d transactions and %d entries, want 0 and 0", txns, entries)
	}
}

// Validation that happens before BEGIN. Table-driven because each case is the
// same shape and the list is what documents the contract.
func TestRecordRejectsInvalidEntries(t *testing.T) {
	resetDB(t)
	ctx := context.Background()
	createAccount(t, "acc_a")
	createAccount(t, "acc_b")

	tests := []struct {
		name    string
		entries []Entry
		want    error
	}{
		{"single entry", []Entry{
			{AccountID: "acc_a", Direction: DirectionDebit, Amount: 500},
		}, ErrTooFewEntries},
		{"no entries", nil, ErrTooFewEntries},
		{"zero amount", []Entry{
			{AccountID: "acc_a", Direction: DirectionDebit, Amount: 0},
			{AccountID: "acc_b", Direction: DirectionCredit, Amount: 0},
		}, ErrNonPositiveAmount},
		{"negative amount", []Entry{
			{AccountID: "acc_a", Direction: DirectionDebit, Amount: -500},
			{AccountID: "acc_b", Direction: DirectionCredit, Amount: -500},
		}, ErrNonPositiveAmount},
		{"unknown direction", []Entry{
			{AccountID: "acc_a", Direction: "transfer", Amount: 500},
			{AccountID: "acc_b", Direction: DirectionCredit, Amount: 500},
		}, ErrInvalidDirection},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Record(ctx, testPool, tc.name, tc.entries)
			if !errors.Is(err, tc.want) {
				t.Fatalf("Record error = %v, want %v", err, tc.want)
			}
			if txns, entries := countRows(t); txns != 0 || entries != 0 {
				t.Errorf("after rejection: %d transactions and %d entries, want 0 and 0", txns, entries)
			}
		})
	}
}

// A failure the database raises mid-transaction, after BEGIN and after the
// transaction row is already inserted. Validation cannot catch this one, so
// only the rollback prevents an orphaned ledger transaction.
func TestRecordRollsBackWhenDatabaseRejectsAnEntry(t *testing.T) {
	resetDB(t)
	ctx := context.Background()
	createAccount(t, "acc_a")

	_, err := Record(ctx, testPool, "unknown account", []Entry{
		{AccountID: "acc_a", Direction: DirectionDebit, Amount: 500},
		{AccountID: "acc_does_not_exist", Direction: DirectionCredit, Amount: 500},
	})
	if err == nil {
		t.Fatal("Record succeeded with an unknown account, want a foreign key error")
	}

	txns, entries := countRows(t)
	if txns != 0 || entries != 0 {
		t.Errorf("after a mid-transaction failure: %d transactions and %d entries, want 0 and 0", txns, entries)
	}
}

// Nothing assumes a transaction has exactly two sides.
func TestRecordAcceptsMoreThanTwoEntries(t *testing.T) {
	resetDB(t)
	ctx := context.Background()
	createAccount(t, "acc_a")
	createAccount(t, "acc_b")

	txnID, err := Record(ctx, testPool, "split payment", []Entry{
		{AccountID: "acc_a", Direction: DirectionDebit, Amount: 300},
		{AccountID: "acc_a", Direction: DirectionDebit, Amount: 200},
		{AccountID: "acc_b", Direction: DirectionCredit, Amount: 500},
	})
	if err != nil {
		t.Fatalf("Record: %v", err)
	}

	var sum int64
	if err := testPool.QueryRow(ctx, `
		SELECT COALESCE(SUM(CASE WHEN direction = 'credit' THEN amount ELSE -amount END), 0)
		FROM ledger_entries WHERE txn_id = $1`, txnID).Scan(&sum); err != nil {
		t.Fatalf("sum entries: %v", err)
	}
	if sum != 0 {
		t.Errorf("entries sum to %d, want 0", sum)
	}
}
