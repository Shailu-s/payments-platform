package ledger

import (
	"context"
	"fmt"
	"testing"

	"github.com/Shailu-s/payments-platform/internal/testdb"
)

// Balance sums every entry an account has ever had. Measure how that cost
// grows with history, against the stored balance the spending path now uses.
func BenchmarkBalanceByHistory(b *testing.B) {
	benchmarkByHistory(b, func(ctx context.Context) (int64, error) {
		return Balance(ctx, testPool, "acc_history")
	})
}

func BenchmarkStoredBalanceByHistory(b *testing.B) {
	benchmarkByHistory(b, func(ctx context.Context) (int64, error) {
		balance, _, err := StoredBalance(ctx, testPool, "acc_history")
		return balance, err
	})
}

func benchmarkByHistory(b *testing.B, read func(context.Context) (int64, error)) {
	ctx := context.Background()
	for _, n := range []int{1_000, 10_000, 100_000, 1_000_000} {
		b.Run(fmt.Sprintf("entries=%d", n), func(b *testing.B) {
			if err := testdb.TruncateAll(ctx, testPool, testSchema); err != nil {
				b.Fatalf("reset: %v", err)
			}
			b.Cleanup(func() { _ = testdb.TruncateAll(ctx, testPool, testSchema) })

			// One balanced history transaction: n one-cent credits against one
			// debit. Raw SQL bypasses Record, so it applies the stored balance.
			const seed = `
				INSERT INTO accounts (id, currency, type) VALUES ('acc_history', 'USD', 'asset');
				INSERT INTO ledger_transactions (id, reference) VALUES ('ltx_history', 'history');
				INSERT INTO ledger_entries (id, txn_id, account_id, direction, amount)
				SELECT 'le_h_' || g, 'ltx_history', 'acc_history', 'credit', 1
				FROM generate_series(1, %d) g;
				INSERT INTO ledger_entries (id, txn_id, account_id, direction, amount)
				VALUES ('le_h_debit', 'ltx_history', 'acc_settlement_usd', 'debit', %d);
				UPDATE account_balances SET balance = %d WHERE account_id = 'acc_history';
				ANALYZE ledger_entries;`
			if _, err := testPool.Exec(ctx, fmt.Sprintf(seed, n, n, n)); err != nil {
				b.Fatalf("seed history: %v", err)
			}

			b.ResetTimer()
			for b.Loop() {
				balance, err := read(ctx)
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
