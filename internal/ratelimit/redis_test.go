package ratelimit

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

var testRedisURL string

func pingRedis(ctx context.Context, url string) error {
	opts, err := RedisOptions(url, 1)
	if err != nil {
		return err
	}
	client := redis.NewClient(opts)
	defer client.Close()
	return client.Ping(ctx).Err()
}

// newRedisClient is one API replica's client, with the production options.
func newRedisClient(tb testing.TB, poolSize int) *redis.Client {
	tb.Helper()
	opts, err := RedisOptions(testRedisURL, poolSize)
	if err != nil {
		tb.Fatal(err)
	}
	client := redis.NewClient(opts)
	tb.Cleanup(func() { _ = client.Close() })
	return client
}

// warm opens n connections up front, so a test measures counting rather than
// dialling under the 10 ms budget.
func warm(tb testing.TB, client *redis.Client, n int) {
	tb.Helper()
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			errs <- client.Do(ctx, "PING").Err()
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			tb.Fatalf("warm: %v", err)
		}
	}
}

// redisKeyID is a key id no other test or package uses, so tests sharing the
// Redis database never count against each other. Its windows are deleted
// afterwards.
func redisKeyID(tb testing.TB, client *redis.Client) string {
	tb.Helper()
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		tb.Fatal(err)
	}
	id := "ak_test_" + hex.EncodeToString(b[:])
	tb.Cleanup(func() {
		ctx := context.Background()
		keys, err := client.Keys(ctx, "rl:"+id+":*").Result()
		if err == nil && len(keys) > 0 {
			client.Del(ctx, keys...)
		}
	})
	return id
}

// Four replicas, each with its own client and connections, share one key's
// quota: the count lives in Redis, and INCR is atomic there.
func TestRedisLimitHoldsAcrossReplicas(t *testing.T) {
	allowed, refused, degraded := raceReplicas(t, func(l *RedisLimiter) func(context.Context, string) (Decision, error) {
		return l.Allow
	})
	if degraded != 0 {
		t.Fatalf("%d decisions failed open: Redis did not answer within %v", degraded, redisTimeout)
	}
	if allowed != 100 || refused != 100 {
		t.Errorf("allowed %d, refused %d across 4 replicas, want exactly 100 and 100", allowed, refused)
	}
}

// The read-then-write version lets every replica read the same count before
// any of them writes: more than the limit gets through.
func TestNaiveRedisOvershootsAcrossReplicas(t *testing.T) {
	allowed, _, _ := raceReplicas(t, func(l *RedisLimiter) func(context.Context, string) (Decision, error) {
		return l.allowNaive
	})
	t.Logf("naive Redis limiter allowed %d of 200 against a limit of 100", allowed)
	if allowed <= 100 {
		t.Skipf("the naive version held at %d this run; the race is real but not deterministic", allowed)
	}
}

// raceReplicas releases 200 requests at once on one key, 50 through each of
// four replicas with limit 100, and counts the outcomes.
func raceReplicas(t *testing.T, allow func(*RedisLimiter) func(context.Context, string) (Decision, error)) (allowed, refused, degraded int64) {
	t.Helper()
	const replicas, perReplica, limit = 4, 50, 100

	limiters := make([]*RedisLimiter, replicas)
	var keyID string
	for i := range limiters {
		client := newRedisClient(t, perReplica)
		warm(t, client, perReplica)
		if i == 0 {
			keyID = redisKeyID(t, client)
		}
		limiters[i] = NewRedis(client, limit, time.Minute)
	}

	var nAllowed, nRefused, nDegraded, nFailed atomic.Int64
	var start, done sync.WaitGroup
	start.Add(1)
	for _, l := range limiters {
		call := allow(l)
		for range perReplica {
			done.Add(1)
			go func() {
				defer done.Done()
				start.Wait()
				decision, err := call(context.Background(), keyID)
				switch {
				case err != nil:
					nFailed.Add(1)
				case decision.Degraded:
					nDegraded.Add(1)
				case decision.Allowed:
					nAllowed.Add(1)
				default:
					nRefused.Add(1)
				}
			}()
		}
	}
	start.Done()
	done.Wait()

	if n := nFailed.Load(); n != 0 {
		t.Fatalf("%d requests errored", n)
	}
	return nAllowed.Load(), nRefused.Load(), nDegraded.Load()
}

// The first request starts a TTL longer than the window and at most twice it;
// later requests leave it alone; the next window is a new key from 1.
func TestRedisWindowKeyExpires(t *testing.T) {
	ctx := context.Background()
	client := newRedisClient(t, 2)
	keyID := redisKeyID(t, client)

	const window = time.Minute
	l := NewRedis(client, 5, window)
	now := time.Date(2026, 10, 20, 12, 0, 30, 0, time.UTC)
	l.now = func() time.Time { return now }
	key := redisKey(keyID, windowStart(now, window))

	if d, err := l.Allow(ctx, keyID); err != nil || d.Remaining != 4 {
		t.Fatalf("first Allow: remaining %d, err %v", d.Remaining, err)
	}
	first, err := client.PTTL(ctx, key).Result()
	if err != nil {
		t.Fatal(err)
	}
	if first <= window || first > 2*window {
		t.Fatalf("TTL after the first request = %v, want in (%v, %v]", first, window, 2*window)
	}

	time.Sleep(20 * time.Millisecond)
	if d, err := l.Allow(ctx, keyID); err != nil || d.Remaining != 3 {
		t.Fatalf("second Allow: remaining %d, err %v", d.Remaining, err)
	}
	second, err := client.PTTL(ctx, key).Result()
	if err != nil {
		t.Fatal(err)
	}
	if second >= first {
		t.Errorf("TTL after the second request = %v, want below %v: a later request reset the expiry", second, first)
	}

	now = now.Add(window)
	d, err := l.Allow(ctx, keyID)
	if err != nil {
		t.Fatal(err)
	}
	if !d.Allowed || d.Remaining != 4 {
		t.Errorf("first request of the next window: allowed %v, remaining %d, want true and 4", d.Allowed, d.Remaining)
	}
	if want := windowStart(now, window).Add(window); !d.ResetAt.Equal(want) {
		t.Errorf("ResetAt = %v, want %v", d.ResetAt, want)
	}
}

// A refusal says how long until the window rolls over, never less than 1 s.
func TestRedisRefusalCarriesRetryAfter(t *testing.T) {
	ctx := context.Background()
	client := newRedisClient(t, 2)
	keyID := redisKeyID(t, client)

	l := NewRedis(client, 1, time.Minute)
	now := time.Date(2026, 10, 20, 12, 0, 59, 800_000_000, time.UTC)
	l.now = func() time.Time { return now }

	if d, err := l.Allow(ctx, keyID); err != nil || !d.Allowed {
		t.Fatalf("first request: allowed %v, err %v", d.Allowed, err)
	}
	d, err := l.Allow(ctx, keyID)
	if err != nil {
		t.Fatal(err)
	}
	if d.Allowed || d.Remaining != 0 || d.RetryAfter != time.Second {
		t.Errorf("second request: allowed %v, remaining %d, retry after %v; want false, 0, 1s",
			d.Allowed, d.Remaining, d.RetryAfter)
	}
}

// Redis is down: the request is allowed, marked degraded, and the check gives
// up at once instead of turning a Redis outage into a payments outage.
func TestRedisDownFailsOpen(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close() // nothing listens here now

	l := NewRedis(clientFor(t, addr), 100, time.Minute)
	start := time.Now()
	d, err := l.Allow(context.Background(), "ak_down")
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("Allow returned %v, want nil: an error answers 500", err)
	}
	if !d.Allowed || !d.Degraded || d.Limit != 100 {
		t.Errorf("decision = %+v, want allowed, degraded, limit 100", d)
	}
	if elapsed >= 50*time.Millisecond {
		t.Errorf("Allow took %v with Redis down, want under 50ms", elapsed)
	}
}

// Redis accepts the connection and never answers: the check gives up after
// its own deadline, not the caller's.
func TestRedisSlowFailsOpenWithinDeadline(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var conns []net.Conn
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			conns = append(conns, c) // held open, never read or answered
			mu.Unlock()
		}
	}()
	t.Cleanup(func() {
		ln.Close()
		mu.Lock()
		defer mu.Unlock()
		for _, c := range conns {
			c.Close()
		}
	})

	l := NewRedis(clientFor(t, ln.Addr().String()), 100, time.Minute)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	start := time.Now()
	d, err := l.Allow(ctx, "ak_slow")
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("Allow returned %v, want nil", err)
	}
	if !d.Allowed || !d.Degraded {
		t.Errorf("decision = %+v, want allowed and degraded", d)
	}
	if limit := redisTimeout + 20*time.Millisecond; elapsed > limit {
		t.Errorf("Allow took %v against a silent Redis, want at most %v", elapsed, limit)
	}
}

func clientFor(tb testing.TB, addr string) *redis.Client {
	tb.Helper()
	opts, err := RedisOptions("redis://"+addr+"/14", 4)
	if err != nil {
		tb.Fatal(err)
	}
	client := redis.NewClient(opts)
	tb.Cleanup(func() { _ = client.Close() })
	return client
}
