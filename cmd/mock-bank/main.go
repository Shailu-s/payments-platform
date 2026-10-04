// Command mock-bank simulates a bank rail: it accepts payment instructions,
// answers immediately, and confirms the outcome later by webhook.
//
// It is a separate process on its own port with its own in-memory storage, and
// it has no access to the payments database. That separation is the point: a
// provider that can read our ledger is a function call wearing an HTTP costume,
// and none of the failures worth rehearsing — timeouts, duplicate events,
// settlement arriving before acknowledgement — are real unless the two sides
// genuinely do not share state.
//
// Its behaviour is specified in docs/mockbank-api.md, which was written before
// either side's code.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/Shailu-s/payments-platform/internal/config"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))

	var env config.Env
	addr := env.Require("MOCKBANK_ADDR")
	webhookURL := env.Require("WEBHOOK_URL")
	rawDelay := env.Require("SETTLE_DELAY")
	if err := env.Err(); err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}
	settleDelay, err := parseDuration(rawDelay)
	if err != nil {
		slog.Error("SETTLE_DELAY", "error", err)
		os.Exit(1)
	}

	server := NewServer(NewStore(), wellBehaved{settleDelay: settleDelay}, webhookURL)

	srv := &http.Server{
		Addr:              addr,
		Handler:           server.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		// Generous, because a scenario may deliberately delay a response past
		// the caller's deadline and the server must outlive that.
		WriteTimeout: 60 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, os.Interrupt, syscall.SIGTERM)

	go func() {
		slog.Info("mock-bank listening",
			"addr", addr, "webhook_url", webhookURL, "settle_delay", settleDelay.String())
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("server stopped", "error", err)
			os.Exit(1)
		}
	}()

	<-shutdown
	slog.Info("shutting down, waiting for scheduled outcomes and webhooks")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		slog.Error("shutdown timed out", "error", err)
	}
	// Outcomes are scheduled out of band, so stopping the listener is not
	// enough: a payment accepted a moment ago still owes its webhook.
	server.WaitForPending(ctx)
	slog.Info("stopped cleanly")
}

// parseDuration accepts a Go duration ("2s", "500ms") or a bare number of
// milliseconds, which is convenient in tests.
func parseDuration(v string) (time.Duration, error) {
	if d, err := time.ParseDuration(v); err == nil {
		return d, nil
	}
	if ms, err := strconv.Atoi(v); err == nil {
		return time.Duration(ms) * time.Millisecond, nil
	}
	return 0, fmt.Errorf("%q is not a duration: use 2s, 500ms, or a number of milliseconds", v)
}
