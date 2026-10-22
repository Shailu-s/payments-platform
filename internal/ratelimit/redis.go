package ratelimit

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/maintnotifications"
)

// RedisOptions parses url and sets the client up for a check on every
// request: socket reads and writes follow the caller's deadline, a failed
// call is not retried, and minIdle connections are opened up front and kept.
func RedisOptions(url string, poolSize, minIdle int) (*redis.Options, error) {
	opts, err := redis.ParseURL(url)
	if err != nil {
		return nil, fmt.Errorf("parse redis url: %w", err)
	}
	opts.PoolSize = poolSize
	// A warm pool: go-redis closes a connection whose call timed out, and a
	// new connection's handshake took up to ~27 ms under load. Started empty,
	// one slow moment cascaded into dial, timeout, close, dial. The pool
	// refills to minIdle in the background, off the request path.
	opts.MinIdleConns = minIdle
	opts.ContextTimeoutEnabled = true
	// Allow's own deadline bounds the hot path (ContextTimeoutEnabled).
	// These only bound calls without a tighter deadline: the startup ping and
	// the handshake of a new connection, which took up to ~27 ms when 50 were
	// opened at once against Docker Desktop.
	opts.ReadTimeout = 100 * time.Millisecond
	opts.WriteTimeout = 100 * time.Millisecond
	opts.PoolTimeout = redisTimeout
	opts.DialTimeout = 100 * time.Millisecond
	opts.MaxRetries = -1
	// One dial attempt: by default go-redis redials a refused connection five
	// times, 100 ms apart, so a stopped Redis would hold every check for the
	// whole deadline instead of failing it at once.
	opts.DialerRetries = 1
	// Only a managed Redis Enterprise sends these; skip the handshake round trip.
	opts.MaintNotificationsConfig = &maintnotifications.Config{Mode: maintnotifications.ModeDisabled}
	return opts, nil
}

// redisTimeout bounds every Allow call to Redis. It detects a broken Redis,
// not a busy one: at 10 ms, 2-6 % of checks failed open on a loaded laptop.
// The cost is up to 50 ms added to each rate-limited request while Redis
// accepts connections but does not answer (a refused connection fails at
// once). Card network routes are not rate limited, so taps never pay it.
const redisTimeout = 50 * time.Millisecond

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
	// timeout is redisTimeout; tests about counting, not latency, raise it.
	timeout time.Duration
	now     func() time.Time
	// Unix nanoseconds of the last fail-open warning.
	lastWarn atomic.Int64
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
		ttlMS:   (2 * window).Milliseconds(),
		timeout: redisTimeout,
		now:     nowUTC,
	}
}

// Allow counts every request, including refusals, in one round trip under its
// own redisTimeout. Any Redis error lets the request through as Degraded.
// Replicas take the window from their own clocks: with skew, requests near a
// boundary may count into the adjacent window (the Postgres limiter does the
// same).
func (l *RedisLimiter) Allow(ctx context.Context, apiKeyID string) (Decision, error) {
	now := l.now()
	start := windowStart(now, l.window)
	resetAt := start.Add(l.window)

	ctx, cancel := context.WithTimeout(ctx, l.timeout)
	defer cancel()
	count, err := incrWindow.Run(ctx, l.client, []string{redisKey(apiKeyID, start)}, l.ttlMS).Int64()
	if err != nil {
		// Fail open: while Redis is down or slow, quotas are not enforced
		// rather than every payment failing. Admission and the bounded pool
		// still protect the database.
		l.warn(err)
		return Decision{Allowed: true, Limit: l.limit, Degraded: true}, nil
	}
	return decide(int(count), l.limit, now, resetAt), nil
}

// warn logs at most once a second, so an outage does not log every request.
func (l *RedisLimiter) warn(err error) {
	now := time.Now().UnixNano()
	last := l.lastWarn.Load()
	if now-last < int64(time.Second) || !l.lastWarn.CompareAndSwap(last, now) {
		return
	}
	slog.Warn("rate limiter failing open: redis unavailable", "error", err)
}

func redisKey(apiKeyID string, windowStart time.Time) string {
	return "rl:" + apiKeyID + ":" + strconv.FormatInt(windowStart.Unix(), 10)
}
