// Command api serves the payments HTTP API.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Shailu-s/payments-platform/internal/api"
	"github.com/Shailu-s/payments-platform/internal/config"
	"github.com/Shailu-s/payments-platform/internal/ratelimit"
	"github.com/Shailu-s/payments-platform/internal/webhooks"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))

	var env config.Env
	dsn := env.Require("DATABASE_URL")
	addr := env.Require("API_ADDR")
	webhookSecret := env.Require("MOCKBANK_WEBHOOK_SECRET")
	networkSecret := env.Require("NETWORK_SECRET")
	backend := env.Require("RATE_LIMIT_BACKEND")
	var redisURL string
	if backend == "redis" {
		redisURL = env.Require("REDIS_URL")
	}
	if err := env.Err(); err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}
	if backend != "redis" && backend != "postgres" {
		slog.Error("RATE_LIMIT_BACKEND must be redis or postgres", "got", backend)
		os.Exit(1)
	}

	if len(webhookSecret) < webhooks.MinSecretBytes {
		slog.Error("MOCKBANK_WEBHOOK_SECRET must contain at least 32 bytes")
		os.Exit(1)
	}
	if len(networkSecret) < webhooks.MinSecretBytes {
		slog.Error("NETWORK_SECRET must contain at least 32 bytes")
		os.Exit(1)
	}

	ctx := context.Background()
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

	// Cancelled on shutdown, which stops the background sweeper.
	backgroundCtx, stopBackground := context.WithCancel(ctx)
	defer stopBackground()

	var opts []api.Option
	if backend == "redis" {
		// At most twice the admission limit (3 per pool connection):
		// admission bounds the money routes only, and reads are checked too.
		// The admission limit's worth stays open and warm.
		admission := 3 * int(pool.Config().MaxConns)
		redisOpts, err := ratelimit.RedisOptions(redisURL, 2*admission, admission)
		if err != nil {
			slog.Error("bad REDIS_URL", "error", err)
			os.Exit(1)
		}
		client := redis.NewClient(redisOpts)
		if err := client.Ping(ctx).Err(); err != nil {
			slog.Error("no redis", "error", err)
			slog.Error("run `docker compose -f docker/docker-compose.yml up -d --wait redis` first")
			os.Exit(1)
		}
		defer client.Close()
		opts = append(opts, api.WithRateLimiter(ratelimit.NewRedis(client, api.DefaultRateLimit, api.DefaultRateWindow)))
	}
	slog.Info("rate limiter", "backend", backend)

	apiServer := api.NewServer(pool, []byte(webhookSecret), []byte(networkSecret), opts...)
	apiServer.StartRateLimitSweeper(backgroundCtx)

	srv := &http.Server{
		Addr:    addr,
		Handler: apiServer.Handler(),
		// A client that opens a connection and sends nothing must not hold a
		// slot forever; these bound how long one can.
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	// Drain requests on shutdown to reduce ambiguous outcomes for callers.
	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, os.Interrupt, syscall.SIGTERM)

	go func() {
		slog.Info("api listening", "addr", addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("server stopped", "error", err)
			os.Exit(1)
		}
	}()

	<-shutdown
	slog.Info("shutting down, waiting for in-flight requests")
	stopBackground()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		slog.Error("shutdown timed out, some requests were cut off", "error", err)
		os.Exit(1)
	}
	slog.Info("stopped cleanly")
}
