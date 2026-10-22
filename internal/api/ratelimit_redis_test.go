package api

import (
	"context"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Shailu-s/payments-platform/internal/auth"
	"github.com/Shailu-s/payments-platform/internal/ratelimit"
	"github.com/Shailu-s/payments-platform/internal/testredis"
	"github.com/redis/go-redis/v9"
)

// redisClientAt is a client with the production options; it needs no live
// Redis to be built.
func redisClientAt(t *testing.T, url string) *redis.Client {
	t.Helper()
	opts, err := ratelimit.RedisOptions(url, 4, 0)
	if err != nil {
		t.Fatal(err)
	}
	client := redis.NewClient(opts)
	t.Cleanup(func() { _ = client.Close() })
	return client
}

// The Redis backend answers callers exactly like the Postgres one; when Redis
// is unreachable the request is served, counted as failed open, and carries no
// invented quota.
func TestRateLimitHeadersAndRefusalWithRedis(t *testing.T) {
	ctx := context.Background()
	url, err := testredis.URL()
	if err == nil {
		err = testredis.Ping(ctx, url)
	}
	if err != nil {
		if os.Getenv("LEDGER_TESTS") == "skip" {
			t.Skip(testredis.ConnectionHint(err))
		}
		t.Fatal(testredis.ConnectionHint(err))
	}
	resetDB(t)

	plaintext, key, err := auth.Generate("redis rate limited")
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if err := auth.Insert(ctx, testPool, key); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	client := redisClientAt(t, url)
	t.Cleanup(func() {
		if keys, err := client.Keys(ctx, "rl:"+key.ID+":*").Result(); err == nil && len(keys) > 0 {
			client.Del(ctx, keys...)
		}
	})

	const limit = 3
	s := &Server{db: testPool, limiter: ratelimit.NewRedis(client, limit, time.Minute)}
	h := s.Handler()

	for i := 1; i <= limit; i++ {
		rec := do(t, h, "GET", "/v1/transfers", plaintext, "")
		if rec.Code != http.StatusOK {
			t.Fatalf("request %d status = %d, want 200 within a limit of %d", i, rec.Code, limit)
		}
		if got := rec.Header().Get("X-RateLimit-Limit"); got != "3" {
			t.Errorf("X-RateLimit-Limit = %q, want 3", got)
		}
		if want := strconv.Itoa(limit - i); rec.Header().Get("X-RateLimit-Remaining") != want {
			t.Errorf("request %d remaining = %q, want %q", i, rec.Header().Get("X-RateLimit-Remaining"), want)
		}
		if rec.Header().Get("X-RateLimit-Reset") == "" {
			t.Errorf("request %d carries no X-RateLimit-Reset", i)
		}
	}

	rec := do(t, h, "GET", "/v1/transfers", plaintext, "")
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429", rec.Code)
	}
	if got := decodeError(t, rec).Code; got != CodeRateLimited {
		t.Errorf("error code = %q, want %q", got, CodeRateLimited)
	}
	if retry, err := strconv.Atoi(rec.Header().Get("Retry-After")); err != nil || retry < 1 {
		t.Errorf("Retry-After = %q, want at least 1 second", rec.Header().Get("Retry-After"))
	}
	if n := s.rateLimitFailOpen.Load(); n != 0 {
		t.Errorf("fail-open count = %d with Redis up, want 0", n)
	}

	// Redis gone: nothing listens on this port.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	down := "redis://" + ln.Addr().String() + "/14"
	ln.Close()

	degraded := &Server{db: testPool, limiter: ratelimit.NewRedis(redisClientAt(t, down), limit, time.Minute)}
	h = degraded.Handler()
	rec = do(t, h, "GET", "/v1/transfers", plaintext, "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status with Redis down = %d, want 200: fail open", rec.Code)
	}
	if got := rec.Header().Get("X-RateLimit-Limit"); got != "3" {
		t.Errorf("X-RateLimit-Limit = %q, want 3", got)
	}
	for _, header := range []string{"X-RateLimit-Remaining", "X-RateLimit-Reset"} {
		if got := rec.Header().Get(header); got != "" {
			t.Errorf("%s = %q with Redis down, want absent: the quota is unknown", header, got)
		}
	}
	if n := degraded.rateLimitFailOpen.Load(); n != 1 {
		t.Errorf("fail-open count = %d, want 1", n)
	}
	metrics := do(t, h, "GET", "/metrics", plaintext, "")
	if !strings.Contains(metrics.Body.String(), "payments_ratelimit_failopen_total 1\n") {
		t.Errorf("metrics do not report the fail-open:\n%s", metrics.Body.String())
	}
}
