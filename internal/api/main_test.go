package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/Shailu-s/payments-platform/internal/auth"
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

// newTestServer returns the full handler stack and a live api key.
func newTestServer(t *testing.T) (http.Handler, string) {
	t.Helper()
	resetDB(t)

	plaintext, key, err := auth.Generate("test")
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if err := auth.Insert(context.Background(), testPool, key); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	return (&Server{db: testPool}).Handler(), plaintext
}

// do sends a request through the whole middleware chain.
func do(t *testing.T, h http.Handler, method, path, key, body string) *httptest.ResponseRecorder {
	t.Helper()
	var reader *strings.Reader
	if body == "" {
		reader = strings.NewReader("")
	} else {
		reader = strings.NewReader(body)
	}

	req := httptest.NewRequest(method, path, reader)
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

// decodeError reads the house error shape out of a response.
func decodeError(t *testing.T, rec *httptest.ResponseRecorder) errorDetail {
	t.Helper()
	var body errorBody
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("response is not the house error shape: %v\nbody: %s", err, rec.Body.String())
	}
	return body.Error
}
