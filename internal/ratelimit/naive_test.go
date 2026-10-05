package ratelimit

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// Deliberately incorrect read-then-write implementation for the concurrency comparison.
func (l *Limiter) allowNaive(ctx context.Context, apiKeyID string) (Decision, error) {
	windowStart := l.windowStart(nowUTC())

	var count int
	const read = `SELECT count FROM rate_limits WHERE api_key_id = $1 AND window_start = $2`
	err := l.db.QueryRow(ctx, read, apiKeyID, windowStart).Scan(&count)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return Decision{}, fmt.Errorf("naive rate limit read %s: %w", apiKeyID, err)
	}

	// Another request may change the count between this decision and the write.
	if count >= l.limit {
		return Decision{Allowed: false, Limit: l.limit}, nil
	}

	const write = `
		INSERT INTO rate_limits (api_key_id, window_start, count)
		VALUES ($1, $2, 1)
		ON CONFLICT (api_key_id, window_start)
		DO UPDATE SET count = rate_limits.count + 1`
	if _, err := l.db.Exec(ctx, write, apiKeyID, windowStart); err != nil {
		return Decision{}, fmt.Errorf("naive rate limit write %s: %w", apiKeyID, err)
	}

	return Decision{Allowed: true, Limit: l.limit, Remaining: l.limit - count - 1}, nil
}
