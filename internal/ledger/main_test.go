package ledger

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// These tests run against real Postgres. There is no in-memory substitute:
// SELECT FOR UPDATE and SERIALIZABLE are the point of phase 3, and a fake that
// does not implement them honestly would prove nothing.
const defaultTestDSN = "postgres://payments:payments@localhost:5433/payments?sslmode=disable"

var testPool *pgxpool.Pool

func TestMain(m *testing.M) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		dsn = defaultTestDSN
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err == nil {
		err = pool.Ping(ctx)
	}
	if err != nil {
		// Fail, do not skip. A suite that reports ok having run zero tests is a
		// green light CI would believe, and the whole argument of this project
		// is that the tests prove something. Skipping is opt-in and explicit.
		fmt.Fprintf(os.Stderr, "\nno database at %s: %v\n", dsn, err)
		fmt.Fprintf(os.Stderr, "run `make up && make migrate-up` first, or set "+
			"LEDGER_TESTS=skip to skip these deliberately\n\n")
		if os.Getenv("LEDGER_TESTS") == "skip" {
			fmt.Fprintf(os.Stderr, "LEDGER_TESTS=skip set: skipping\n")
			os.Exit(0)
		}
		os.Exit(1)
	}
	testPool = pool

	code := m.Run()
	pool.Close()
	os.Exit(code)
}

// resetDB empties every table. Registered with t.Cleanup as well as run up
// front, because cross-test leakage turns one real failure into several fake ones.
// Every new table must be added here: a stale list leaks rows silently.
func resetDB(t *testing.T) {
	t.Helper()
	truncate := func() {
		if _, err := testPool.Exec(context.Background(),
			`TRUNCATE transfers, api_keys, ledger_entries, ledger_transactions, accounts`); err != nil {
			t.Fatalf("truncate: %v", err)
		}
		// Truncating accounts removes the settlement account that migration
		// 000003 creates, and every transfer credits it. Restored here rather
		// than in each test, so a forgotten setup cannot make a test pass for
		// the wrong reason.
		if _, err := testPool.Exec(context.Background(),
			`INSERT INTO accounts (id, currency, type) VALUES ('acc_settlement_usd', 'USD', 'settlement')
			 ON CONFLICT (id) DO NOTHING`); err != nil {
			t.Fatalf("restore settlement account: %v", err)
		}
	}
	truncate()
	t.Cleanup(truncate)
}

// createAccount inserts an account directly. Phase 1 has no account package yet;
// the ledger only needs the row to exist for the foreign key.
func createAccount(t *testing.T, id string) {
	t.Helper()
	if _, err := testPool.Exec(context.Background(),
		`INSERT INTO accounts (id, currency, type) VALUES ($1, 'USD', 'asset')`, id); err != nil {
		t.Fatalf("create account %s: %v", id, err)
	}
}

// countRows returns the number of ledger transactions and entries in the database.
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
