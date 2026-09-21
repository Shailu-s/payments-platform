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
	"log/slog"
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
	windowStart := l.windowStart(nowUTC())
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

// nowUTC exists so tests can share one notion of the clock with Allow.
func nowUTC() time.Time { return time.Now().UTC() }

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

// RunSweeper deletes rolled-over windows on a ticker until ctx is cancelled.
//
// Without it the table grows by one row per key per window forever. Running it
// in the API process rather than as a scheduled job is a V1 shortcut: with
// several instances every one of them sweeps, which is wasteful but harmless
// because DELETE of an already-deleted row is a no-op. Phase 5 brings workers,
// and this moves there.
func (l *Limiter) RunSweeper(ctx context.Context, every, retain time.Duration) {
	ticker := time.NewTicker(every)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			removed, err := l.Sweep(ctx, retain)
			if err != nil {
				// Logged, not fatal: a failed sweep wastes disk, while a
				// crashed API refuses payments.
				slog.WarnContext(ctx, "rate limit sweep failed", "error", err)
				continue
			}
			if removed > 0 {
				slog.InfoContext(ctx, "swept rate limit windows", "removed", removed)
			}
		}
	}
}
