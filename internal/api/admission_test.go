package api

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Shailu-s/payments-platform/internal/testdb"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// smallPool gives a test its own pool, so it can starve connections without
// affecting the shared one.
func smallPool(t *testing.T, maxConns int32) *pgxpool.Pool {
	t.Helper()
	config, err := pgxpool.ParseConfig(testdb.DSN())
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	config.MaxConns = maxConns
	config.ConnConfig.RuntimeParams["search_path"] = testSchema
	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		t.Fatalf("small pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// holdBalanceLock locks an account's balance row, the lock spending waits on,
// in a transaction the caller rolls back.
func holdBalanceLock(t *testing.T, accountID string) pgx.Tx {
	t.Helper()
	ctx := context.Background()
	tx, err := testPool.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if _, err := tx.Exec(ctx,
		`SELECT balance FROM account_balances WHERE account_id = $1 FOR UPDATE`, accountID); err != nil {
		tx.Rollback(ctx)
		t.Fatalf("lock balance of %s: %v", accountID, err)
	}
	return tx
}

type timedResponse struct {
	rec     *httptest.ResponseRecorder
	elapsed time.Duration
}

// postTransfer bounds the client wait so a starved server fails the test
// instead of hanging it.
func postTransfer(h http.Handler, apiKey, idempotencyKey, body string) timedResponse {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req := httptest.NewRequestWithContext(ctx, "POST", "/v1/transfers", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+apiKey)
	req.Header.Set("Idempotency-Key", idempotencyKey)
	rec := httptest.NewRecorder()
	start := time.Now()
	h.ServeHTTP(rec, req)
	return timedResponse{rec: rec, elapsed: time.Since(start)}
}

// One account whose spending lock is stuck must not take the pool from the
// others: its own requests fail fast and a different account still pays.
func TestHotAccountDoesNotStarveOtherAccounts(t *testing.T) {
	h, apiKey := newTestServer(t)
	ctx := context.Background()

	hot := createAccount(t, h, apiKey, "asset")
	other := createAccount(t, h, apiKey, "asset")
	destination := createAccount(t, h, apiKey, "liability")
	fund(t, hot.ID, 1000000)
	fund(t, other.ID, 1000000)

	pool := smallPool(t, 4)
	handler := newServerOn(pool).Handler()

	// Stands in for a slow spender: holds the hot account's lock until the end.
	holder := holdBalanceLock(t, hot.ID)
	defer holder.Rollback(ctx)

	const hotRequests = 20
	hotBody := fmt.Sprintf(`{"source_account":%q,"destination_account":%q,"amount":100,"currency":"USD"}`,
		hot.ID, destination.ID)
	hotResults := make([]timedResponse, hotRequests)
	var wg sync.WaitGroup
	for i := range hotRequests {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			hotResults[i] = postTransfer(handler, apiKey, fmt.Sprintf("hot-%d", i), hotBody)
		}(i)
	}

	// Let the hot requests take what they can before the other account arrives.
	time.Sleep(50 * time.Millisecond)
	otherBody := fmt.Sprintf(`{"source_account":%q,"destination_account":%q,"amount":100,"currency":"USD"}`,
		other.ID, destination.ID)
	got := postTransfer(handler, apiKey, "other-account", otherBody)
	if got.rec.Code != http.StatusAccepted {
		t.Errorf("other account got %d after %v, want 202: %s",
			got.rec.Code, got.elapsed, got.rec.Body.String())
	}
	if got.elapsed > time.Second {
		t.Errorf("other account waited %v behind the hot one, want under 1s", got.elapsed)
	}
	t.Logf("other account answered %d in %v", got.rec.Code, got.elapsed)

	wg.Wait()
	for i, r := range hotResults {
		if r.rec.Code != http.StatusServiceUnavailable {
			t.Errorf("hot request %d got %d after %v, want 503: %s",
				i, r.rec.Code, r.elapsed, r.rec.Body.String())
			continue
		}
		if code := decodeError(t, r.rec).Code; code != CodeAccountBusy {
			t.Errorf("hot request %d error code = %q, want account_busy", i, code)
		}
		if r.rec.Header().Get("Retry-After") == "" {
			t.Errorf("hot request %d has no Retry-After header", i)
		}
		if r.elapsed > time.Second {
			t.Errorf("hot request %d took %v to be refused, want under 1s", i, r.elapsed)
		}
	}
}

// A spend that cannot get the balance lock in time is refused fast, writes
// nothing, and can be retried with the same key once the account is free.
func TestLockTimeoutReturnsAccountBusy(t *testing.T) {
	h, apiKey := newTestServer(t)
	ctx := context.Background()

	source := createAccount(t, h, apiKey, "asset")
	destination := createAccount(t, h, apiKey, "liability")
	fund(t, source.ID, 100000)

	holder := holdBalanceLock(t, source.ID)
	defer holder.Rollback(ctx)

	body := fmt.Sprintf(`{"source_account":%q,"destination_account":%q,"amount":500,"currency":"USD"}`,
		source.ID, destination.ID)
	got := postTransfer(h, apiKey, "busy-then-free", body)
	if got.rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d after %v, want 503: %s", got.rec.Code, got.elapsed, got.rec.Body.String())
	}
	if code := decodeError(t, got.rec).Code; code != CodeAccountBusy {
		t.Errorf("error code = %q, want %q", code, CodeAccountBusy)
	}
	if got.rec.Header().Get("Retry-After") != "1" {
		t.Errorf("Retry-After = %q, want 1", got.rec.Header().Get("Retry-After"))
	}
	if got.elapsed < 250*time.Millisecond || got.elapsed > time.Second {
		t.Errorf("refused after %v, want the 250ms lock timeout", got.elapsed)
	}
	if n := countRows(t, "transfers"); n != 0 {
		t.Errorf("a refused transfer left %d transfer rows, want 0", n)
	}
	if n := countRows(t, "outbox_events"); n != 0 {
		t.Errorf("a refused transfer left %d outbox events, want 0", n)
	}

	if err := holder.Rollback(ctx); err != nil {
		t.Fatalf("release lock: %v", err)
	}
	retry := postTransfer(h, apiKey, "busy-then-free", body)
	if retry.rec.Code != http.StatusAccepted {
		t.Fatalf("retry with the same key = %d, want 202: %s", retry.rec.Code, retry.rec.Body.String())
	}
}
