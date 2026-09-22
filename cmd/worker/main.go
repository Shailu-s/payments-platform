// Command worker sends accepted transfers to the payment rail.
//
// A separate process from the API, deliberately. The API's job ends when the
// instruction is durably recorded; actually moving money through a slow,
// unreliable third party is work nobody's HTTP request should wait for.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Shailu-s/payments-platform/internal/provider"
	"github.com/Shailu-s/payments-platform/internal/worker"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	defaultDSN         = "postgres://payments:payments@localhost:5433/payments?sslmode=disable"
	defaultProviderURL = "http://localhost:8081"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))

	dsn := envOr("DATABASE_URL", defaultDSN)
	providerURL := envOr("PROVIDER_URL", defaultProviderURL)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool, err := pgxpool.New(ctx, dsn)
	if err == nil {
		err = pool.Ping(ctx)
	}
	if err != nil {
		slog.Error("no database", "dsn", dsn, "error", err)
		slog.Error("run `make up && make migrate-up` first")
		os.Exit(1)
	}
	defer pool.Close()

	w := worker.New(pool, provider.New(providerURL, provider.DefaultTimeout), worker.DefaultConfig())

	// Graceful shutdown from the start. Cancelling stops the worker taking new
	// work and lets the in-flight batch finish; a worker that cannot stop
	// cleanly makes every later test noisier, and phase 5 kills one on purpose.
	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, os.Interrupt, syscall.SIGTERM)

	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		w.Run(ctx)
	}()

	<-shutdown
	slog.Info("shutting down, finishing the batch in flight")
	cancel()

	select {
	case <-stopped:
		slog.Info("stopped cleanly")
	case <-time.After(30 * time.Second):
		slog.Error("shutdown timed out with work still in flight")
		os.Exit(1)
	}
}

func envOr(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}
