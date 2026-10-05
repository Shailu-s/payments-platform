package ledger

import (
	"context"
	"testing"
)

// SUM over no entries is NULL; COALESCE must return zero.
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

// A zero balance cannot establish that an account exists.
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

// Read uncommitted entries on the caller's transaction, not another pool connection.
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
