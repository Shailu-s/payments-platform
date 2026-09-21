package ledger

import (
	"context"
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
		// Skip rather than fail: a machine without the container running should
		// report "no database", not a wall of confusing assertion failures.
		println("skipping ledger tests, no database at " + dsn + ": " + err.Error())
		println("run `make up && make migrate-up` first")
		os.Exit(0)
	}
	testPool = pool

	code := m.Run()
	pool.Close()
	os.Exit(code)
}

// resetDB empties the three tables. Registered with t.Cleanup as well as run up
// front, because cross-test leakage turns one real failure into several fake ones.
func resetDB(t *testing.T) {
	t.Helper()
	truncate := func() {
		if _, err := testPool.Exec(context.Background(),
			`TRUNCATE ledger_entries, ledger_transactions, accounts`); err != nil {
			t.Fatalf("truncate: %v", err)
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
