package ratelimit

import (
	"context"
	"errors"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// Deliberately incorrect GET-then-INCR version for the concurrency comparison.
func (l *RedisLimiter) allowNaive(ctx context.Context, apiKeyID string) (Decision, error) {
	now := l.now()
	key := redisKey(apiKeyID, windowStart(now, l.window))

	count, err := l.client.Get(ctx, key).Int()
	if err != nil && !errors.Is(err, redis.Nil) {
		return Decision{}, fmt.Errorf("naive redis read %s: %w", apiKeyID, err)
	}
	// Other replicas may count between this read and the write below.
	if count >= l.limit {
		return Decision{Allowed: false, Limit: l.limit}, nil
	}
	if err := l.client.Incr(ctx, key).Err(); err != nil {
		return Decision{}, fmt.Errorf("naive redis write %s: %w", apiKeyID, err)
	}
	return Decision{Allowed: true, Limit: l.limit, Remaining: l.limit - count - 1}, nil
}
