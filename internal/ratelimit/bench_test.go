package ratelimit

import (
	"context"
	"testing"
	"time"
)

// The measured cost of putting the limiter in Postgres, which is the real
// objection to this design: a database write on every single request, on the
// hot path. Redis exists precisely to avoid it.
//
//	go test ./internal/ratelimit/ -bench=. -benchtime=2000x -run=XXX
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

// The baseline to compare against: the cheapest possible round trip to the same
// database. The difference between the two is what the limiter itself costs, as
// opposed to the cost of talking to Postgres at all.
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
