package ratelimit

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// Measure the hot-path write cost against a bare database round trip.
func BenchmarkAllow(b *testing.B) {
	keyID := benchKey(b)
	ctx := context.Background()
	// A limit high enough that the benchmark measures the write, not refusals.
	limiter := New(testPool, 1_000_000_000, time.Hour)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := limiter.Allow(ctx, keyID); err != nil {
			b.Fatalf("Allow: %v", err)
		}
	}
}

// Isolate database round-trip overhead from the limiter's increment cost.
func BenchmarkBaselineRoundTrip(b *testing.B) {
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var one int
		if err := testPool.QueryRow(ctx, `SELECT 1`).Scan(&one); err != nil {
			b.Fatalf("SELECT 1: %v", err)
		}
	}
}

func benchKey(b *testing.B) string {
	b.Helper()
	ctx := context.Background()

	truncateAll(b)
	_, key, err := generateBenchKey()
	if err != nil {
		b.Fatalf("generate: %v", err)
	}
	if _, err := testPool.Exec(ctx,
		`INSERT INTO api_keys (id, key_hash, prefix, name) VALUES ($1, $2, $3, $4)`,
		key.id, key.hash, key.prefix, "benchmark"); err != nil {
		b.Fatalf("insert key: %v", err)
	}
	return key.id
}

// BenchmarkRedisAllow is BenchmarkAllow against Redis, with the production
// client options and deadline, in the benchmark database (BENCH_REDIS_URL,
// default index 15). Calls that failed open are reported, not hidden.
func BenchmarkRedisAllow(b *testing.B) {
	client := benchRedis(b)
	keyID := redisKeyID(b, client)
	ctx := context.Background()
	limiter := NewRedis(client, 1_000_000_000, time.Hour)

	degraded := 0
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d, err := limiter.Allow(ctx, keyID)
		if err != nil {
			b.Fatalf("Allow: %v", err)
		}
		if d.Degraded {
			degraded++
		}
	}
	b.ReportMetric(float64(degraded), "failopen")
}

// BenchmarkRedisRoundTrip isolates the Redis round trip (PING) from the script.
func BenchmarkRedisRoundTrip(b *testing.B) {
	client := benchRedis(b)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := client.Ping(ctx).Err(); err != nil {
			b.Fatalf("PING: %v", err)
		}
	}
}

// benchRedis is a production-configured client on BENCH_REDIS_URL, never
// database 0. It closes after the benchmark's other cleanups.
func benchRedis(b *testing.B) *redis.Client {
	b.Helper()
	url := os.Getenv("BENCH_REDIS_URL")
	if url == "" {
		url = "redis://localhost:6379/15"
	}
	opts, err := RedisOptions(url, 4, 4)
	if err != nil {
		b.Fatal(err)
	}
	if opts.DB == 0 {
		b.Fatal("BENCH_REDIS_URL selects database 0; benchmarks use their own database")
	}
	client := redis.NewClient(opts)
	b.Cleanup(func() { _ = client.Close() })
	return client
}
