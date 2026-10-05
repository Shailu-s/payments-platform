// Command relay publishes durable outbox events to Kafka.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Shailu-s/payments-platform/internal/config"
	"github.com/Shailu-s/payments-platform/internal/relay"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))

	var env config.Env
	dsn := env.Require("DATABASE_URL")
	brokers := strings.Split(env.Require("KAFKA_BROKERS"), ",")
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

	// NewClient is lazy; fail startup on an unreachable broker. Runtime failures retry.
	client, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err == nil {
		pingCtx, cancelPing := context.WithTimeout(ctx, 5*time.Second)
		err = client.Ping(pingCtx)
		cancelPing()
	}
	if err != nil {
		slog.Error("no kafka", "brokers", brokers, "error", err)
		slog.Error("run `make up` first")
		os.Exit(1)
	}
	defer client.Close()

	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, os.Interrupt, syscall.SIGTERM)

	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		relay.Run(ctx, pool, client)
	}()

	<-shutdown
	slog.Info("shutting down, finishing the batch in flight")
	cancel()

	select {
	case <-stopped:
		slog.Info("stopped cleanly")
	case <-time.After(relay.BatchTimeout + 5*time.Second):
		slog.Error("shutdown timed out with a batch in flight")
		os.Exit(1)
	}
}
