package ledger

import (
	"context"
	"errors"
	"math"
	"testing"
)

func TestRecordRejectsOverflowingTotals(t *testing.T) {
	for _, direction := range []string{DirectionDebit, DirectionCredit} {
		t.Run(direction, func(t *testing.T) {
			resetDB(t)
			ctx := context.Background()
			createAccount(t, "acc_overflow_a")
			createAccount(t, "acc_overflow_b")
			other := DirectionCredit
			if direction == DirectionCredit {
				other = DirectionDebit
			}
			tx, err := testPool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback(ctx)
			id, err := Record(ctx, tx, "overflow must not look balanced", []Entry{
				{AccountID: "acc_overflow_a", Direction: direction, Amount: math.MaxInt64},
				{AccountID: "acc_overflow_a", Direction: direction, Amount: math.MaxInt64},
				{AccountID: "acc_overflow_a", Direction: direction, Amount: 10},
				{AccountID: "acc_overflow_b", Direction: other, Amount: 8},
			})
			if err == nil {
				var balanced bool
				if scanErr := tx.QueryRow(ctx, `SELECT sum(CASE WHEN direction='credit' THEN amount ELSE -amount END)=0 FROM ledger_entries WHERE txn_id=$1`, id).Scan(&balanced); scanErr != nil {
					t.Fatal(scanErr)
				}
				t.Fatalf("overflowing totals were accepted: database says balanced=%t", balanced)
			}
			if !errors.Is(err, ErrAmountOverflow) {
				t.Fatalf("overflow error=%v", err)
			}
			var written int
			if err := tx.QueryRow(ctx, "SELECT count(*) FROM ledger_transactions").Scan(&written); err != nil || written != 0 {
				t.Fatalf("overflow left partial history: count=%d err=%v", written, err)
			}
		})
	}
}

func TestRecordAcceptsMaximumRepresentableTotal(t *testing.T) {
	resetDB(t)
	ctx := context.Background()
	createAccount(t, "acc_max_a")
	createAccount(t, "acc_max_b")
	if _, err := Record(ctx, testPool, "maximum total", []Entry{
		{AccountID: "acc_max_a", Direction: DirectionDebit, Amount: math.MaxInt64 - 1},
		{AccountID: "acc_max_a", Direction: DirectionDebit, Amount: 1},
		{AccountID: "acc_max_b", Direction: DirectionCredit, Amount: math.MaxInt64},
	}); err != nil {
		t.Fatal(err)
	}
	if balance, err := Balance(ctx, testPool, "acc_max_b"); err != nil || balance != math.MaxInt64 {
		t.Fatalf("maximum balance=%d err=%v", balance, err)
	}
}
