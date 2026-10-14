package ledger

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestLedgerHistoryRejectsUpdatesAndDeletes(t *testing.T) {
	resetDB(t)
	ctx := context.Background()
	createSettlementAccount(t, "acc_immutable_source")
	createAccount(t, "acc_immutable_destination")
	id, err := Record(ctx, testPool, "immutable payout", []Entry{
		{AccountID: "acc_immutable_source", Direction: DirectionDebit, Amount: 50000},
		{AccountID: "acc_immutable_destination", Direction: DirectionCredit, Amount: 50000},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, query := range []string{
		"UPDATE ledger_entries SET amount = amount + 1 WHERE txn_id = $1",
		"DELETE FROM ledger_entries WHERE txn_id = $1",
		"UPDATE ledger_transactions SET reference = 'rewritten' WHERE id = $1",
		"DELETE FROM ledger_transactions WHERE id = $1",
	} {
		t.Run(query, func(t *testing.T) {
			tx, err := testPool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback(ctx)
			_, err = tx.Exec(ctx, query, id)
			var databaseError *pgconn.PgError
			if !errors.As(err, &databaseError) || databaseError.Code != "P0001" || databaseError.Message != "ledger history is append-only" {
				t.Fatalf("mutation was not rejected by ledger guard: %v", err)
			}
		})
	}
	if txns, entries := countRows(t); txns != 1 || entries != 2 {
		t.Fatalf("history changed: %d transactions, %d entries", txns, entries)
	}
	balance, err := Balance(ctx, testPool, "acc_immutable_destination")
	if err != nil || balance != 50000 {
		t.Fatalf("balance=%d err=%v", balance, err)
	}
	if _, err := Record(ctx, testPool, "reversal "+id, []Entry{
		{AccountID: "acc_immutable_destination", Direction: DirectionDebit, Amount: 50000},
		{AccountID: "acc_immutable_source", Direction: DirectionCredit, Amount: 50000},
	}); err != nil {
		t.Fatalf("append-only correction failed: %v", err)
	}
	if txns, entries := countRows(t); txns != 2 || entries != 4 {
		t.Fatalf("correction erased history: %d transactions, %d entries", txns, entries)
	}
	balance, err = Balance(ctx, testPool, "acc_immutable_destination")
	if err != nil || balance != 0 {
		t.Fatalf("corrected balance=%d err=%v", balance, err)
	}
}
