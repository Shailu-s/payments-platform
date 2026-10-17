// Command hold-expirer releases approved card holds that never cleared, after
// HOLD_EXPIRY_DAYS. Run any number of copies: each hold is claimed by one.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/Shailu-s/payments-platform/internal/config"
	"github.com/Shailu-s/payments-platform/internal/holds"
	"github.com/jackc/pgx/v5/pgxpool"
)

// batchSize bounds one pass, so a backlog is worked through in steps that each
// report progress and notice shutdown.
const batchSize = 500

type settings struct {
	dsn       string
	olderThan time.Duration
	// interval zero means one pass, then exit.
	interval time.Duration
}

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	s, err := readSettings()
	if err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}
	pool, err := pgxpool.New(ctx, s.dsn)
	if err == nil {
		err = pool.Ping(ctx)
	}
	if err != nil {
		slog.Error("no database", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	if err := run(ctx, pool, s); err != nil && !errors.Is(err, context.Canceled) {
		slog.Error("hold expiry failed", "error", err)
		os.Exit(1)
	}
}

func readSettings() (settings, error) {
	var env config.Env
	dsn := env.Require("DATABASE_URL")
	days := env.Require("HOLD_EXPIRY_DAYS")
	interval := env.Require("HOLD_EXPIRY_INTERVAL")
	if err := env.Err(); err != nil {
		return settings{}, err
	}
	n, err := strconv.Atoi(days)
	if err != nil || n < 1 {
		return settings{}, fmt.Errorf("HOLD_EXPIRY_DAYS must be a whole number of days, at least 1: %q", days)
	}
	every, err := time.ParseDuration(interval)
	if err != nil || every < 0 {
		return settings{}, fmt.Errorf("HOLD_EXPIRY_INTERVAL must be a duration such as 1m, or 0 for one pass: %q", interval)
	}
	return settings{dsn: dsn, olderThan: time.Duration(n) * 24 * time.Hour, interval: every}, nil
}

func run(ctx context.Context, pool *pgxpool.Pool, s settings) error {
	for {
		expired, err := pass(ctx, pool, s.olderThan)
		if err != nil {
			if s.interval == 0 {
				return err
			}
			slog.Error("hold expiry pass failed", "error", err, "expired", expired)
		} else {
			slog.Info("hold expiry pass", "expired", expired)
		}
		if s.interval == 0 {
			return nil
		}
		timer := time.NewTimer(s.interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

// pass expires batches until one comes back short: the backlog is empty.
func pass(ctx context.Context, pool *pgxpool.Pool, olderThan time.Duration) (int, error) {
	total := 0
	for {
		n, err := holds.ExpireBatch(ctx, pool, time.Now(), olderThan, batchSize)
		total += n
		if err != nil || n < batchSize {
			return total, err
		}
		if ctx.Err() != nil {
			return total, ctx.Err()
		}
	}
}
