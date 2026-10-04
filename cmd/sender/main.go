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
	"github.com/Shailu-s/payments-platform/internal/consumer"
	"github.com/Shailu-s/payments-platform/internal/provider"
	"github.com/Shailu-s/payments-platform/internal/worker"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	group = "transfer-sender"
	topic = "transfers"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))

	var env config.Env
	dsn := env.Require("DATABASE_URL")
	brokers := strings.Split(env.Require("KAFKA_BROKERS"), ",")
	providerURL := env.Require("PROVIDER_URL")
	if err := env.Err(); err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}

	pool, err := pgxpool.New(context.Background(), dsn)
	if err == nil {
		err = pool.Ping(context.Background())
	}
	if err != nil {
		slog.Error("no database", "error", err)
		slog.Error("run `make up && make migrate-up` first")
		os.Exit(1)
	}
	defer pool.Close()

	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.DisableAutoCommit(),
		kgo.OnPartitionsAssigned(logPartitions("partitions assigned")),
		kgo.OnPartitionsRevoked(logPartitions("partitions revoked")),
		kgo.OnPartitionsLost(logPartitions("partitions lost")),
	)
	if err == nil {
		pingCtx, cancelPing := context.WithTimeout(context.Background(), 5*time.Second)
		err = client.Ping(pingCtx)
		cancelPing()
	}
	if err != nil {
		slog.Error("no kafka", "brokers", brokers, "error", err)
		slog.Error("run `make up` first")
		os.Exit(1)
	}

	w := worker.New(pool, provider.New(providerURL, provider.DefaultTimeout), worker.DefaultConfig())

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	slog.Info("sending", "group", group, "topic", topic)
	consumer.Run(ctx, client, func(ctx context.Context, r *kgo.Record) error {
		// Ignore other event types without blocking this group's offset.
		if header(r, "event_type") != "transfer.created" {
			return nil
		}
		id := string(r.Key)

		// No claim is normal on redelivery or a poller race; errors retry the record.
		sent, err := w.SendByID(ctx, id)
		if err != nil {
			return err
		}
		slog.InfoContext(ctx, "event handled",
			"transfer_id", id, "sent", sent,
			"partition", r.Partition, "offset", r.Offset,
			"event_id", header(r, "event_id"))
		return nil
	})

	client.Close()
	slog.Info("stopped cleanly")
}

func logPartitions(msg string) func(context.Context, *kgo.Client, map[string][]int32) {
	return func(_ context.Context, _ *kgo.Client, p map[string][]int32) {
		if len(p) > 0 {
			slog.Info(msg, "partitions", p)
		}
	}
}

func header(r *kgo.Record, key string) string {
	for _, h := range r.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}
