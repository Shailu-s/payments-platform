// Package testredis points tests at a disposable Redis database.
package testredis

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/redis/go-redis/v9"
)

// DefaultURL is database 14, kept for tests; benchmarks use 15 and the
// application 0.
const DefaultURL = "redis://localhost:6379/14"

// URL is TEST_REDIS_URL or DefaultURL. It refuses database 0, which holds
// the application's counters.
func URL() (string, error) {
	url := os.Getenv("TEST_REDIS_URL")
	if url == "" {
		url = DefaultURL
	}
	opts, err := redis.ParseURL(url)
	if err != nil {
		return "", fmt.Errorf("TEST_REDIS_URL: %w", err)
	}
	if opts.DB == 0 {
		return "", fmt.Errorf("TEST_REDIS_URL selects database 0; tests use their own database (default %s)", DefaultURL)
	}
	return url, nil
}

// ConnectionHint is what a test prints when there is no Redis.
func ConnectionHint(err error) string {
	return fmt.Sprintf(`
These tests need Redis (%v).

Start it with:   docker compose -f docker/docker-compose.yml up -d --wait redis
Or point at one: TEST_REDIS_URL=redis://host:port/14
Or skip them:    LEDGER_TESTS=skip go test ./...
`, err)
}

// Ping checks that Redis answers at url.
func Ping(ctx context.Context, url string) error {
	opts, err := redis.ParseURL(url)
	if err != nil {
		return err
	}
	client := redis.NewClient(opts)
	defer client.Close()
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	return client.Ping(ctx).Err()
}
