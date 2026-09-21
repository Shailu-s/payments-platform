package ledger

import (
	"context"
	"testing"
)

// An account that exists but has never been used is a normal state. SUM over
// zero rows is NULL, not 0, so this is the COALESCE path — the most likely bug
// in the query.
func TestBalanceOfUnusedAccountIsZero(t *testing.T) {
	resetDB(t)
	ctx := context.Background()
	createAccount(t, "acc_empty")

	balance, err := Balance(ctx, testPool, "acc_empty")
	if err != nil {
		t.Fatalf("Balance: %v", err)
	}
	if balance != 0 {
		t.Errorf("Balance(acc_empty) = %d, want 0", balance)
	}
}

// Balance sums entries; it does not check that the account exists. Worth
// pinning down, because it means the API layer cannot use Balance to decide
// whether an account id is real.
func TestBalanceOfUnknownAccountIsZero(t *testing.T) {
	resetDB(t)
	ctx := context.Background()

	balance, err := Balance(ctx, testPool, "acc_never_created")
	if err != nil {
		t.Fatalf("Balance: %v", err)
	}
	if balance != 0 {
		t.Errorf("Balance(acc_never_created) = %d, want 0", balance)
	}
}

// Balance must be callable inside a transaction: phase 3 reads a balance while
// holding a row lock, and a read on a different connection sees a different
// snapshot and defeats the lock.
func TestBalanceRunsInsideATransaction(t *testing.T) {
	resetDB(t)
	ctx := context.Background()
	createAccount(t, "acc_a")
	createAccount(t, "acc_b")

	if _, err := Record(ctx, testPool, "seed", []Entry{
		{AccountID: "acc_a", Direction: DirectionDebit, Amount: 700},
		{AccountID: "acc_b", Direction: DirectionCredit, Amount: 700},
	}); err != nil {
		t.Fatalf("Record: %v", err)
	}

	tx, err := testPool.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	defer tx.Rollback(ctx)

	balance, err := Balance(ctx, tx, "acc_b")
	if err != nil {
		t.Fatalf("Balance inside transaction: %v", err)
	}
	if balance != 700 {
		t.Errorf("Balance(acc_b) inside transaction = %d, want 700", balance)
	}
}

// Many movements across several transactions accumulate correctly.
func TestBalanceAccumulatesAcrossTransactions(t *testing.T) {
	resetDB(t)
	ctx := context.Background()
	createAccount(t, "acc_a")
	createAccount(t, "acc_b")

	for _, amount := range []int64{1000, 250, 75} {
		if _, err := Record(ctx, testPool, "movement", []Entry{
			{AccountID: "acc_a", Direction: DirectionDebit, Amount: amount},
			{AccountID: "acc_b", Direction: DirectionCredit, Amount: amount},
		}); err != nil {
			t.Fatalf("Record %d: %v", amount, err)
		}
	}

	balanceA, err := Balance(ctx, testPool, "acc_a")
	if err != nil {
		t.Fatalf("Balance(acc_a): %v", err)
	}
	balanceB, err := Balance(ctx, testPool, "acc_b")
	if err != nil {
		t.Fatalf("Balance(acc_b): %v", err)
	}
	if balanceA != -1325 {
		t.Errorf("Balance(acc_a) = %d, want -1325", balanceA)
	}
	if balanceB != 1325 {
		t.Errorf("Balance(acc_b) = %d, want 1325", balanceB)
	}
	if balanceA+balanceB != 0 {
		t.Errorf("balances sum to %d, want 0", balanceA+balanceB)
	}
}
