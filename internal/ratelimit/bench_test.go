package ratelimit

import (
	"context"
	"testing"
	"time"
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
