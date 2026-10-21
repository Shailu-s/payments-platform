package ratelimit

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/maintnotifications"
)

// RedisOptions parses url and sets the client up for a check on every
// request: socket reads and writes follow the caller's deadline, and a
// failed call is not retried (the next request tries a fresh connection).
func RedisOptions(url string, poolSize int) (*redis.Options, error) {
	opts, err := redis.ParseURL(url)
	if err != nil {
		return nil, fmt.Errorf("parse redis url: %w", err)
	}
	opts.PoolSize = poolSize
	opts.ContextTimeoutEnabled = true
	// Allow's own 10 ms deadline bounds the hot path (ContextTimeoutEnabled).
	// These only bound calls without a tighter deadline: the startup ping and
	// the handshake of a new connection, which took up to ~27 ms when 50 were
	// opened at once against Docker Desktop.
	opts.ReadTimeout = 100 * time.Millisecond
	opts.WriteTimeout = 100 * time.Millisecond
	opts.PoolTimeout = redisTimeout
	opts.DialTimeout = 100 * time.Millisecond
	opts.MaxRetries = -1
	// Only a managed Redis Enterprise sends these; skip the handshake round trip.
	opts.MaintNotificationsConfig = &maintnotifications.Config{Mode: maintnotifications.ModeDisabled}
	return opts, nil
}

// redisTimeout bounds every Allow call to Redis. A healthy local Redis answers
// in well under a millisecond; one that takes longer than this is treated as
// down.
const redisTimeout = 10 * time.Millisecond

// incrWindow counts one request and starts the key's expiry on the first, in
// one atomic step: a crash between a separate INCR and EXPIRE would leave a
// counter that never expires.
var incrWindow = redis.NewScript(`
local c = redis.call('INCR', KEYS[1])
if c == 1 then redis.call('PEXPIRE', KEYS[1], ARGV[1]) end
return c
`)

// RedisLimiter is the same fixed window as Limiter, counted in Redis so the
// check costs no database connection and writes no WAL. Every API replica
// shares the counter, so N replicas still allow the limit once, not N times.
type RedisLimiter struct {
	client *redis.Client
	limit  int
	window time.Duration
	ttlMS  int64
	now    func() time.Time
}

// NewRedis counts in client. Windows shorter than a second share a key, since
// keys are named by the window's start in unix seconds.
func NewRedis(client *redis.Client, limit int, window time.Duration) *RedisLimiter {
	return &RedisLimiter{
		client: client,
		limit:  limit,
		window: window,
		// Twice the window: the key outlives its window even when it was
		// created at the window's last instant, so a slow request can never
		// recreate an expired key and restart the count mid-window.
		ttlMS: (2 * window).Milliseconds(),
		now:   nowUTC,
	}
}

// Allow counts every request, including refusals, in one round trip.
// Replicas take the window from their own clocks: with skew, requests near a
// boundary may count into the adjacent window (the Postgres limiter does the
// same).
func (l *RedisLimiter) Allow(ctx context.Context, apiKeyID string) (Decision, error) {
	now := l.now()
	start := windowStart(now, l.window)
	resetAt := start.Add(l.window)

	count, err := incrWindow.Run(ctx, l.client, []string{redisKey(apiKeyID, start)}, l.ttlMS).Int64()
	if err != nil {
		return Decision{}, fmt.Errorf("rate limit %s: %w", apiKeyID, err)
	}
	return decide(int(count), l.limit, now, resetAt), nil
}

func redisKey(apiKeyID string, windowStart time.Time) string {
	return "rl:" + apiKeyID + ":" + strconv.FormatInt(windowStart.Unix(), 10)
}
