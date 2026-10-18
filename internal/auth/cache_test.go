package auth

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// countingQuerier counts the queries that reach the database, optionally
// holding each one for delay so concurrent callers overlap.
type countingQuerier struct {
	db      Querier
	delay   time.Duration
	queries atomic.Int64
}

func (q *countingQuerier) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	q.queries.Add(1)
	time.Sleep(q.delay)
	return q.db.QueryRow(ctx, sql, args...)
}

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func newTestCache(db Querier) (*Cache, *fakeClock) {
	clock := &fakeClock{now: time.Date(2026, 10, 17, 10, 0, 0, 0, time.UTC)}
	c := NewCache(db, DefaultCacheTTL, DefaultCacheSize)
	c.now = clock.Now
	return c, clock
}

func TestAPIKeyCacheServesWithoutTheDatabase(t *testing.T) {
	resetDB(t)
	ctx := context.Background()
	plaintext, created := mustGenerate(t, "cached")
	db := &countingQuerier{db: testPool}
	c, _ := newTestCache(db)

	for range 100 {
		key, err := c.Verify(ctx, plaintext)
		if err != nil {
			t.Fatalf("Verify: %v", err)
		}
		if key.ID != created.ID {
			t.Fatalf("Verify returned %s, want %s", key.ID, created.ID)
		}
	}
	if n := db.queries.Load(); n != 1 {
		t.Errorf("100 verifications ran %d queries, want 1", n)
	}

	// Unknown keys are not cached: each one asks the database again.
	unknown, _, _ := Generate("never stored")
	for range 3 {
		if _, err := c.Verify(ctx, unknown); !errors.Is(err, ErrInvalidKey) {
			t.Fatalf("unknown key: err = %v, want ErrInvalidKey", err)
		}
	}
	if n := db.queries.Load(); n != 4 {
		t.Errorf("after 3 unknown keys, %d queries in total, want 4", n)
	}

	// A malformed key never reaches the database at all.
	if _, err := c.Verify(ctx, "pk_live_short"); !errors.Is(err, ErrInvalidKey) {
		t.Fatalf("malformed key: err = %v, want ErrInvalidKey", err)
	}
	if n := db.queries.Load(); n != 4 {
		t.Errorf("a malformed key reached the database: %d queries, want 4", n)
	}

	for _, el := range c.entries {
		if el.Value.(*cacheEntry).hash != created.Hash {
			t.Errorf("cache is keyed by %q, want the stored hash", el.Value.(*cacheEntry).hash)
		}
	}
}

func TestRevokedKeyIsEvictedLocally(t *testing.T) {
	resetDB(t)
	ctx := context.Background()
	plaintext, created := mustGenerate(t, "to revoke")
	c, _ := newTestCache(testPool)

	if _, err := c.Verify(ctx, plaintext); err != nil {
		t.Fatalf("Verify before revoke: %v", err)
	}
	if err := c.Revoke(ctx, testPool, created.ID); err != nil {
		t.Fatalf("Revoke: %v", err)
	}
	if _, err := c.Verify(ctx, plaintext); !errors.Is(err, ErrRevokedKey) {
		t.Errorf("Verify after a local revoke: err = %v, want ErrRevokedKey", err)
	}
}

func TestCachedKeyExpiresAfterTTL(t *testing.T) {
	resetDB(t)
	ctx := context.Background()
	plaintext, created := mustGenerate(t, "revoked elsewhere")
	db := &countingQuerier{db: testPool}
	c, clock := newTestCache(db)

	if _, err := c.Verify(ctx, plaintext); err != nil {
		t.Fatalf("Verify: %v", err)
	}
	// Another process revokes it: this one does not hear about it.
	if err := Revoke(ctx, testPool, created.ID); err != nil {
		t.Fatalf("Revoke: %v", err)
	}

	clock.Advance(DefaultCacheTTL - time.Second)
	if _, err := c.Verify(ctx, plaintext); err != nil {
		t.Errorf("inside the TTL the cached key is still accepted, got %v", err)
	}
	clock.Advance(time.Second)
	if _, err := c.Verify(ctx, plaintext); !errors.Is(err, ErrRevokedKey) {
		t.Errorf("at the TTL: err = %v, want ErrRevokedKey", err)
	}
	if n := db.queries.Load(); n != 2 {
		t.Errorf("%d queries, want 2: one to fill, one after expiry", n)
	}
}

func TestConcurrentMissesQueryOnce(t *testing.T) {
	resetDB(t)
	ctx := context.Background()
	plaintext, created := mustGenerate(t, "stampede")
	db := &countingQuerier{db: testPool, delay: 50 * time.Millisecond}
	c, _ := newTestCache(db)

	const callers = 50
	var wg sync.WaitGroup
	errs := make(chan error, callers)
	for range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			key, err := c.Verify(ctx, plaintext)
			if err == nil && key.ID != created.ID {
				err = errors.New("wrong key " + key.ID)
			}
			if err != nil {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("Verify: %v", err)
	}
	if n := db.queries.Load(); n != 1 {
		t.Errorf("%d concurrent misses ran %d queries, want 1", callers, n)
	}
}

func TestCacheStaysWithinItsSize(t *testing.T) {
	resetDB(t)
	ctx := context.Background()
	c := NewCache(testPool, DefaultCacheTTL, 3)
	for range 5 {
		plaintext, _ := mustGenerate(t, "bounded")
		if _, err := c.Verify(ctx, plaintext); err != nil {
			t.Fatalf("Verify: %v", err)
		}
	}
	if n := len(c.entries); n != 3 || c.order.Len() != 3 {
		t.Errorf("cache holds %d entries (%d in order), want 3", n, c.order.Len())
	}
}
