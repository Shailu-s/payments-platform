// Package ratelimit caps how many requests one API key may make per window.
//
// The constraint that forces the design: an in-memory counter is wrong the
// instant there are two API instances. Each process allows the full quota, so
// the real limit is silently double what was configured — and nothing fails
// loudly to tell you. The limit must live in shared state, and the only shared
// state in V1 is Postgres.
package ratelimit

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type DB interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// Limiter is a fixed window: one row per key per window, counted from the start
// of each clock interval.
type Limiter struct {
	db     DB
	limit  int
	window time.Duration
}

func New(db DB, limit int, window time.Duration) *Limiter {
	return &Limiter{db: db, limit: limit, window: window}
}

// Decision is the outcome of one request against the limit.
type Decision struct {
	Allowed    bool
	Limit      int
	Remaining  int
	RetryAfter time.Duration // how long until the window rolls over
	ResetAt    time.Time
}

// Allow counts this request and reports whether it is within the limit.
//
// One statement. The increment and the read are the same operation, because
// they have to be: SELECT the count, decide, then UPDATE is a read-then-write
// race, and under concurrency every request reads the same value and the limit
// silently allows far more than it should.
//
// ON CONFLICT on the primary key is what serialises it. The database holds a
// row lock for the duration of the upsert, so concurrent requests queue rather
// than interleave. This is exactly the mechanism phase 3 uses for idempotency,
// and recognising it as the same race in different clothing is the point.
func (l *Limiter) Allow(ctx context.Context, apiKeyID string) (Decision, error) {
	windowStart := l.windowStart(time.Now())
	resetAt := windowStart.Add(l.window)

	const q = `
		INSERT INTO rate_limits (api_key_id, window_start, count)
		VALUES ($1, $2, 1)
		ON CONFLICT (api_key_id, window_start)
		DO UPDATE SET count = rate_limits.count + 1
		RETURNING count`

	var count int
	if err := l.db.QueryRow(ctx, q, apiKeyID, windowStart).Scan(&count); err != nil {
		return Decision{}, fmt.Errorf("rate limit %s: %w", apiKeyID, err)
	}

	decision := Decision{
		Allowed:   count <= l.limit,
		Limit:     l.limit,
		Remaining: max(l.limit-count, 0),
		ResetAt:   resetAt,
	}
	if !decision.Allowed {
		// Rounded up: a Retry-After of 0 invites an immediate retry that is
		// certain to be refused again.
		decision.RetryAfter = max(time.Until(resetAt).Round(time.Second), time.Second)
	}
	return decision, nil
}

// windowStart truncates to the current window boundary. Truncating in Go rather
// than with date_trunc keeps the window size configurable: date_trunc only
// knows about named units like 'minute'.
func (l *Limiter) windowStart(now time.Time) time.Time {
	return now.UTC().Truncate(l.window)
}

// Sweep deletes windows that have rolled over. Without it the table grows by
// one row per key per window forever, which is the obvious follow-up question
// to any counter kept in a database.
func (l *Limiter) Sweep(ctx context.Context, olderThan time.Duration) (int64, error) {
	const q = `DELETE FROM rate_limits WHERE window_start < $1`

	tag, err := l.db.Exec(ctx, q, time.Now().UTC().Add(-olderThan))
	if err != nil {
		return 0, fmt.Errorf("sweep rate limits: %w", err)
	}
	return tag.RowsAffected(), nil
}

// AllowNaive is the wrong implementation, kept because it is the evidence.
//
// It reads the count, decides, then writes — which is correct in a single
// thread and broken the moment two requests overlap: both read the same value,
// both conclude they are under the limit, and both write back the same
// increment. The test fires N+20 concurrent requests at a limit of N and
// watches this let far more than N through.
//
// Not called by the middleware. It exists so the failure can be demonstrated
// rather than asserted.
func (l *Limiter) AllowNaive(ctx context.Context, apiKeyID string) (Decision, error) {
	windowStart := l.windowStart(time.Now())

	var count int
	const read = `SELECT count FROM rate_limits WHERE api_key_id = $1 AND window_start = $2`
	err := l.db.QueryRow(ctx, read, apiKeyID, windowStart).Scan(&count)
	if err != nil && err != pgx.ErrNoRows {
		return Decision{}, fmt.Errorf("naive rate limit read %s: %w", apiKeyID, err)
	}

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
