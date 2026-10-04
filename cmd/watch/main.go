package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Shailu-s/payments-platform/internal/consumer"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	defaultBrokers = "localhost:9092"
	group          = "watch"
	topic          = "transfers"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	})))

	brokers := strings.Split(envOr("KAFKA_BROKERS", defaultBrokers), ",")

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
		pingctx, cancelPing := context.WithTimeout(context.Background(), 5*time.Second)
		err = client.Ping(pingctx)
		cancelPing()
	}

	if err != nil {
		slog.Error("no kafka", "brokers", brokers, "error", err)
		slog.Error("run make up first")
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)

	defer stop()

	slog.Info("watching", "group", group, "topic", topic)
	consumer.Run(ctx, client, func(_ context.Context, r *kgo.Record) error {
		slog.Info("event",
			"partition", r.Partition,
			"offset", r.Offset,
			"key", string(r.Key),
			"event_type", header(r, "event_type"),
			"event_id", header(r, "event_id"),
		)
		return nil
	})

	client.Close()
	slog.Info("Stopped cleanly")
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

func envOr(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}
