package auth

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

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
		// Fail rather than skip: a suite reporting ok having run nothing is a
		// green light CI would believe.
		fmt.Fprintf(os.Stderr, "\nno database at %s: %v\n", dsn, err)
		fmt.Fprintf(os.Stderr, "run `make up && make migrate-up` first, or set "+
			"LEDGER_TESTS=skip to skip these deliberately\n\n")
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

// mustGenerate mints and stores a key, returning the plaintext that will never
// be recoverable again.
func mustGenerate(t *testing.T, name string) (string, Key) {
	t.Helper()
	plaintext, key, err := Generate(name)
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if err := Insert(context.Background(), testPool, key); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	return plaintext, key
}
