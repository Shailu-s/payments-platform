package auth

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/Shailu-s/payments-platform/internal/testdb"
	"github.com/jackc/pgx/v5/pgxpool"
)

const testSchema = "test_auth"

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
