// Command worker polls for missed transfers and resolves unknown provider outcomes.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Shailu-s/payments-platform/internal/config"
	"github.com/Shailu-s/payments-platform/internal/provider"
	"github.com/Shailu-s/payments-platform/internal/worker"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))

	var env config.Env
	dsn := env.Require("DATABASE_URL")
	providerURL := env.Require("PROVIDER_URL")
	if err := env.Err(); err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool, err := pgxpool.New(ctx, dsn)
	if err == nil {
		err = pool.Ping(ctx)
	}
	if err != nil {
		slog.Error("no database", "error", err)
		slog.Error("run `make up && make migrate-up` first")
		os.Exit(1)
	}
	defer pool.Close()

	// Give the event consumer priority; polling still sends when Kafka is unavailable.
	cfg := worker.DefaultConfig()
	cfg.HeadStart = 30 * time.Second

	w := worker.New(pool, provider.New(providerURL, provider.DefaultTimeout), cfg)

	// Wait for the worker to exit so accepted-reference writes can finish.
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
