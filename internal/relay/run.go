package relay

import (
	"context"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	BatchSize    = 100
	PollInterval = 500 * time.Millisecond
	BatchTimeout = 10 * time.Second
)

// Run publishes outbox events until ctx is cancelled.
func Run(ctx context.Context, db Beginner, client *kgo.Client) {
	slog.InfoContext(ctx, "relay started",
		"batch_size", BatchSize, "poll_interval", PollInterval.String())

	for {
		if ctx.Err() != nil {
			slog.InfoContext(ctx, "relay stopped")
			return
		}

		// Finish the batch on shutdown to avoid republishing accepted records.
		batchCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), BatchTimeout)
		n, err := PublishBatch(batchCtx, db, client, BatchSize)
		cancel()

		if err != nil {
			slog.ErrorContext(ctx, "publish batch failed", "error", err)
		} else if n > 0 {
			slog.InfoContext(ctx, "published", "events", n)
		}
		if n == BatchSize {
			// A full batch means more are probably waiting; do not sleep.
			continue
		}

		select {
		case <-ctx.Done():
		case <-time.After(PollInterval):
		}
	}
}
