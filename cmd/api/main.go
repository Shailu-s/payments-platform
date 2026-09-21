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
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	defaultDSN  = "postgres://payments:payments@localhost:5433/payments?sslmode=disable"
	defaultAddr = ":8080"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))

	dsn := envOr("DATABASE_URL", defaultDSN)
	addr := envOr("API_ADDR", defaultAddr)

	ctx := context.Background()
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

	srv := &http.Server{
		Addr:    addr,
		Handler: api.NewServer(pool).Handler(),
		// A client that opens a connection and sends nothing must not hold a
		// slot forever; these bound how long one can.
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	// Shut down gracefully: a payments API killed mid-request leaves the caller
	// unable to tell a failure from a success, which is the one ambiguity this
	// whole system exists to avoid.
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

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		slog.Error("shutdown timed out, some requests were cut off", "error", err)
		os.Exit(1)
	}
	slog.Info("stopped cleanly")
}

func envOr(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}
