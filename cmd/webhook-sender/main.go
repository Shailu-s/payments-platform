package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/Shailu-s/payments-platform/internal/config"
	"github.com/Shailu-s/payments-platform/internal/consumer"
	"github.com/Shailu-s/payments-platform/internal/customerwebhooks"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	group = "customer-webhooks"
	topic = "transfers"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))
	var env config.Env
	dsn := env.Require("DATABASE_URL")
	brokers := strings.Split(env.Require("KAFKA_BROKERS"), ",")
	endpoint := env.Require("CUSTOMER_WEBHOOK_URL")
	secret := env.Require("CUSTOMER_WEBHOOK_SECRET")
	if err := env.Err(); err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}
	pool, err := pgxpool.New(context.Background(), dsn)
	if err == nil {
		err = pool.Ping(context.Background())
	}
	if err != nil {
		slog.Error("database unavailable; check DATABASE_URL and Postgres availability")
		os.Exit(1)
	}
	defer pool.Close()
	worker, err := customerwebhooks.New(pool, endpoint, []byte(secret), customerwebhooks.DefaultConfig())
	if err != nil {
		slog.Error("invalid customer webhook configuration", "error", err)
		os.Exit(1)
	}
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		slog.Error("create customer webhook consumer", "error", err)
		os.Exit(1)
	}
	defer client.Close()
	pingCtx, cancelPing := context.WithTimeout(context.Background(), 5*time.Second)
	err = client.Ping(pingCtx)
	cancelPing()
	if err != nil {
		slog.Warn("Kafka unavailable; saved customer deliveries will still be attempted")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	var workers sync.WaitGroup
	workers.Add(1)
	go func() {
		defer workers.Done()
		worker.Run(ctx)
	}()
	slog.Info("customer webhook consumer started", "group", group, "topic", topic)
	consumer.Run(ctx, client, worker.Handle)
	stop()
	workers.Wait()
	slog.Info("customer webhook sender stopped")
}
