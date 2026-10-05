package ledger

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/Shailu-s/payments-platform/internal/testdb"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Separate schemas keep parallel packages from truncating each other's fixtures.
const testSchema = "test_ledger"

var testPool *pgxpool.Pool

func TestMain(m *testing.M) {
	ctx := context.Background()

	pool, err := testdb.Connect(ctx, testSchema)
	if err != nil {
		// Missing infrastructure must not look like a passing suite.
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

// Reset before and after each test to avoid fixture leakage on failures.
func resetDB(t *testing.T) {
	t.Helper()
	truncate := func() {
		if err := testdb.TruncateAll(context.Background(), testPool, testSchema); err != nil {
			t.Fatalf("reset: %v", err)
		}
	}
	truncate()
	t.Cleanup(truncate)
}

func createAccount(t *testing.T, id string) {
	t.Helper()
	if _, err := testPool.Exec(context.Background(),
		`INSERT INTO accounts (id, currency, type) VALUES ($1, 'USD', 'asset')`, id); err != nil {
		t.Fatalf("create account %s: %v", id, err)
	}
}

func countRows(t *testing.T) (txns, entries int) {
	t.Helper()
	ctx := context.Background()
	if err := testPool.QueryRow(ctx, `SELECT count(*) FROM ledger_transactions`).Scan(&txns); err != nil {
		t.Fatalf("count transactions: %v", err)
	}
	if err := testPool.QueryRow(ctx, `SELECT count(*) FROM ledger_entries`).Scan(&entries); err != nil {
		t.Fatalf("count entries: %v", err)
	}
	return txns, entries
}
