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
	"github.com/Shailu-s/payments-platform/internal/testdb"
	"github.com/jackc/pgx/v5/pgxpool"
)

const testSchema = "test_api"

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
