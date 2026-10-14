package ledger

import (
	"context"
	"fmt"
	"testing"

	"github.com/Shailu-s/payments-platform/internal/testdb"
)

// Balance sums every entry an account has ever had, and the transfer path runs
// it while holding the account lock. Measure how that cost grows with history.
func BenchmarkBalanceByHistory(b *testing.B) {
	ctx := context.Background()
	for _, n := range []int{1_000, 10_000, 100_000, 1_000_000} {
		b.Run(fmt.Sprintf("entries=%d", n), func(b *testing.B) {
			if err := testdb.TruncateAll(ctx, testPool, testSchema); err != nil {
				b.Fatalf("reset: %v", err)
			}
			b.Cleanup(func() { _ = testdb.TruncateAll(ctx, testPool, testSchema) })

			// One balanced history transaction: n one-cent credits against one debit.
			const seed = `
				INSERT INTO accounts (id, currency, type) VALUES ('acc_history', 'USD', 'asset');
				INSERT INTO ledger_transactions (id, reference) VALUES ('ltx_history', 'history');
				INSERT INTO ledger_entries (id, txn_id, account_id, direction, amount)
				SELECT 'le_h_' || g, 'ltx_history', 'acc_history', 'credit', 1
				FROM generate_series(1, %d) g;
				INSERT INTO ledger_entries (id, txn_id, account_id, direction, amount)
				VALUES ('le_h_debit', 'ltx_history', 'acc_settlement_usd', 'debit', %d);
				ANALYZE ledger_entries;`
			if _, err := testPool.Exec(ctx, fmt.Sprintf(seed, n, n)); err != nil {
				b.Fatalf("seed history: %v", err)
			}

			b.ResetTimer()
			for b.Loop() {
				balance, err := Balance(ctx, testPool, "acc_history")
				if err != nil {
					b.Fatalf("balance: %v", err)
				}
				if balance != int64(n) {
					b.Fatalf("balance = %d, want %d", balance, n)
				}
			}
		})
	}
}
