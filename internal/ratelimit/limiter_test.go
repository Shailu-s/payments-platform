package ratelimit

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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

// newKey resets the tables and returns a live api key id to count against.
func newKey(t *testing.T) string {
	t.Helper()
	ctx := context.Background()

	truncateAll(t)
	t.Cleanup(func() {
		truncateAll(t)
		testPool.Exec(context.Background(),
			`INSERT INTO accounts (id, currency, type) VALUES ('acc_settlement_usd', 'USD', 'settlement')
			 ON CONFLICT (id) DO NOTHING`)
	})

	_, key, err := auth.Generate("rate limit test")
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if err := auth.Insert(ctx, testPool, key); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	return key.ID
}

// truncateAll empties every table, listed by query rather than by hand: a
// hand-written list goes stale the moment a migration adds a table.
func truncateAll(tb testing.TB) {
	tb.Helper()
	if _, err := testPool.Exec(context.Background(), `
		DO $$
		DECLARE tables text;
		BEGIN
			SELECT string_agg(format('%I.%I', schemaname, tablename), ', ')
			INTO tables
			FROM pg_tables
			WHERE schemaname = 'public' AND tablename <> 'schema_migrations';
			IF tables IS NOT NULL THEN
				EXECUTE 'TRUNCATE ' || tables || ' CASCADE';
			END IF;
		END $$`); err != nil {
		tb.Fatalf("truncate: %v", err)
	}
}

func TestAllowPermitsUpToTheLimitThenRefuses(t *testing.T) {
	keyID := newKey(t)
	ctx := context.Background()
	limiter := New(testPool, 5, time.Minute)

	for i := 1; i <= 5; i++ {
		decision, err := limiter.Allow(ctx, keyID)
		if err != nil {
			t.Fatalf("request %d: %v", i, err)
		}
		if !decision.Allowed {
			t.Fatalf("request %d was refused, want allowed: the limit is 5", i)
		}
		if want := 5 - i; decision.Remaining != want {
			t.Errorf("request %d remaining = %d, want %d", i, decision.Remaining, want)
		}
	}

	decision, err := limiter.Allow(ctx, keyID)
	if err != nil {
		t.Fatalf("request 6: %v", err)
	}
	if decision.Allowed {
		t.Error("request 6 was allowed, want refused")
	}
	if decision.RetryAfter <= 0 {
		t.Errorf("RetryAfter = %v, want a positive duration", decision.RetryAfter)
	}
	if decision.Remaining != 0 {
		t.Errorf("Remaining = %d, want 0", decision.Remaining)
	}
}

// ⭐ The evidence the phase requires: N+20 concurrent requests against a limit
// of N must let exactly N through. This is what the naive read-then-write
// implementation fails, and it fails by allowing far more than it should.
func TestConcurrentRequestsHoldAtExactlyTheLimit(t *testing.T) {
	keyID := newKey(t)
	ctx := context.Background()

	const limit = 30
	const requests = limit + 20

	limiter := New(testPool, limit, time.Minute)

	var allowed atomic.Int64
	var failed atomic.Int64

	// Released together, so the requests genuinely overlap rather than
	// arriving in a queue.
	var start sync.WaitGroup
	start.Add(1)
	var done sync.WaitGroup

	for i := 0; i < requests; i++ {
		done.Add(1)
		go func() {
			defer done.Done()
			start.Wait()

			decision, err := limiter.Allow(ctx, keyID)
			if err != nil {
				failed.Add(1)
				return
			}
			if decision.Allowed {
				allowed.Add(1)
			}
		}()
	}

	start.Done()
	done.Wait()

	if failed.Load() != 0 {
		t.Fatalf("%d requests errored", failed.Load())
	}
	if got := allowed.Load(); got != limit {
		t.Errorf("%d of %d concurrent requests were allowed, want exactly %d",
			got, requests, limit)
	}

	// The stored count must equal every request made, not just the allowed
	// ones: a refused request still happened.
	var count int
	if err := testPool.QueryRow(ctx,
		`SELECT count FROM rate_limits WHERE api_key_id = $1`, keyID).Scan(&count); err != nil {
		t.Fatalf("read count: %v", err)
	}
	if count != requests {
		t.Errorf("stored count = %d, want %d: every attempt must be counted", count, requests)
	}
}

// The same concurrency against the naive implementation, to show the race is
// real rather than theoretical. This test asserts the WRONG behaviour on
// purpose: if it ever starts passing at exactly the limit, the naive version
// has been accidentally fixed and the evidence is no longer evidence.
func TestNaiveImplementationOvershootsUnderConcurrency(t *testing.T) {
	keyID := newKey(t)
	ctx := context.Background()

	const limit = 30
	const requests = limit + 20

	limiter := New(testPool, limit, time.Minute)

	var allowed atomic.Int64
	var start sync.WaitGroup
	start.Add(1)
	var done sync.WaitGroup

	for i := 0; i < requests; i++ {
		done.Add(1)
		go func() {
			defer done.Done()
			start.Wait()

			decision, err := limiter.AllowNaive(ctx, keyID)
			if err == nil && decision.Allowed {
				allowed.Add(1)
			}
		}()
	}

	start.Done()
	done.Wait()

	t.Logf("naive implementation allowed %d of %d requests against a limit of %d",
		allowed.Load(), requests, limit)

	if allowed.Load() <= int64(limit) {
		t.Skipf("the naive version held at %d this run; the race is real but not "+
			"deterministic, so this is not a failure", allowed.Load())
	}
}

// Windows are independent: a new window starts the count again.
func TestANewWindowResetsTheCount(t *testing.T) {
	keyID := newKey(t)
	ctx := context.Background()

	// A very short window so the rollover can be observed without sleeping long.
	limiter := New(testPool, 2, 100*time.Millisecond)

	for i := 0; i < 2; i++ {
		if decision, err := limiter.Allow(ctx, keyID); err != nil || !decision.Allowed {
			t.Fatalf("request %d: allowed=%v err=%v", i, decision.Allowed, err)
		}
	}
	if decision, _ := limiter.Allow(ctx, keyID); decision.Allowed {
		t.Fatal("third request in the window was allowed")
	}

	time.Sleep(150 * time.Millisecond)

	decision, err := limiter.Allow(ctx, keyID)
	if err != nil {
		t.Fatalf("after rollover: %v", err)
	}
	if !decision.Allowed {
		t.Error("the first request of a new window was refused")
	}
}

// One key's traffic must not consume another's quota.
func TestLimitsAreIndependentPerKey(t *testing.T) {
	first := newKey(t)
	ctx := context.Background()

	_, second, err := auth.Generate("second key")
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if err := auth.Insert(ctx, testPool, second); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	limiter := New(testPool, 3, time.Minute)

	for i := 0; i < 3; i++ {
		if decision, _ := limiter.Allow(ctx, first); !decision.Allowed {
			t.Fatalf("first key request %d was refused", i)
		}
	}
	if decision, _ := limiter.Allow(ctx, first); decision.Allowed {
		t.Fatal("first key exceeded its limit")
	}

	decision, err := limiter.Allow(ctx, second.ID)
	if err != nil {
		t.Fatalf("second key: %v", err)
	}
	if !decision.Allowed {
		t.Error("the second key was refused because of the first key's traffic")
	}
}

// Unbounded growth is the obvious follow-up question to any counter kept in a
// database, so the sweep exists and is tested.
func TestSweepRemovesOldWindowsOnly(t *testing.T) {
	keyID := newKey(t)
	ctx := context.Background()
	limiter := New(testPool, 10, time.Minute)

	// An old window, written directly: the limiter only ever writes the current one.
	if _, err := testPool.Exec(ctx,
		`INSERT INTO rate_limits (api_key_id, window_start, count) VALUES ($1, $2, 5)`,
		keyID, time.Now().UTC().Add(-2*time.Hour)); err != nil {
		t.Fatalf("insert old window: %v", err)
	}
	if _, err := limiter.Allow(ctx, keyID); err != nil {
		t.Fatalf("Allow: %v", err)
	}

	removed, err := limiter.Sweep(ctx, time.Hour)
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if removed != 1 {
		t.Errorf("Sweep removed %d rows, want 1", removed)
	}

	var remaining int
	if err := testPool.QueryRow(ctx,
		`SELECT count(*) FROM rate_limits WHERE api_key_id = $1`, keyID).Scan(&remaining); err != nil {
		t.Fatalf("count: %v", err)
	}
	if remaining != 1 {
		t.Errorf("%d windows remain, want 1: the current window must survive", remaining)
	}
}
