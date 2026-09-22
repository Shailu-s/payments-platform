package worker

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/Shailu-s/payments-platform/internal/testdb"
	"github.com/jackc/pgx/v5/pgxpool"
)

const testSchema = "test_worker"

var testPool *pgxpool.Pool

func TestMain(m *testing.M) {
	ctx := context.Background()

	pool, err := testdb.Connect(ctx, testSchema)
	if err != nil {
		fmt.Fprint(os.Stderr, testdb.ConnectionHint(err))
		if os.Getenv("LEDGER_TESTS") == "skip" {
			os.Exit(0)
		}
		os.Exit(1)
	}
	testPool = pool

	code := m.Run()
	pool.Close()
	os.Exit(code)
}

func resetDB(t testing.TB) {
	t.Helper()
	if err := testdb.TruncateAll(context.Background(), testPool, testSchema); err != nil {
		t.Fatalf("reset: %v", err)
	}
}

// seedTransfers creates n transfers waiting to be sent to the provider.
func seedTransfers(t testing.TB, n int) {
	t.Helper()
	ctx := context.Background()
	resetDB(t)

	if _, err := testPool.Exec(ctx, `
		INSERT INTO accounts (id, currency, type)
		VALUES ('acc_src', 'USD', 'asset'), ('acc_dst', 'USD', 'liability')
		ON CONFLICT (id) DO NOTHING`); err != nil {
		t.Fatalf("accounts: %v", err)
	}
	if _, err := testPool.Exec(ctx,
		`INSERT INTO api_keys (id, key_hash, prefix, name) VALUES ('ak_w', 'h', 'pk_live_w', 'worker test')`); err != nil {
		t.Fatalf("api key: %v", err)
	}
	if _, err := testPool.Exec(ctx, `
		INSERT INTO transfers (id, source_account, destination_account, amount,
			currency, status, api_key_id, idempotency_key)
		SELECT 'tr_w' || g, 'acc_src', 'acc_dst', 100, 'USD', 'processing', 'ak_w', 'k' || g
		FROM generate_series(1, $1) g`, n); err != nil {
		t.Fatalf("transfers: %v", err)
	}
}
