// Command relay publishes outbox events to Kafka.
// A separate process from the API and the worker, so it can be stopped,
// restarted or killed on its own. While it is down nothing is lost: events wait
// in outbox_events and go out when it comes back.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Shailu-s/payments-platform/internal/relay"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	defaultDSN     = "postgres://payments:payments@localhost:5433/payments?sslmode=disable"
	defaultBrokers = "localhost:9092"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))

	dsn := envOr("DATABASE_URL", defaultDSN)
	brokers := strings.Split(envOr("KAFKA_BROKERS", defaultBrokers), ",")

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

	// NewClient does not connect, so Ping checks the broker is really there.
	// Failing at startup is for a developer who forgot `make up`. Once
	// running, a broker that goes away is retried, not fatal.
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

func envOr(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}
