package consumer

import (
	"context"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

type Handler func(ctx context.Context, r *kgo.Record) error

const retryInterval = 1 * time.Second

func Run(ctx context.Context, client *kgo.Client, handler Handler) {
	for {
		fetches := client.PollFetches(ctx)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			return
		}

		fetches.EachError(func(topic string, partition int32, err error) {
			slog.ErrorContext(ctx, "error polling fetches", "topic", topic, "partition", partition, "error", err)
		})

		var handled []*kgo.Record
		for _, r := range fetches.Records() {
			if !handleUntilDone(ctx, handler, r) {
				break
			}
			handled = append(handled, r)
		}

		if len(handled) == 0 {
			continue
		}

		commitCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		err := client.CommitRecords(commitCtx, handled...)
		cancel()
		if err != nil {
			slog.ErrorContext(ctx, "error committing records", "error", err)
		}
	}
}

func handleUntilDone(ctx context.Context, handler Handler, r *kgo.Record) bool {
	for {
		err := handler(ctx, r)
		if err == nil {
			return true
		}
		slog.ErrorContext(ctx, "handled failed, retrying", "partition", r.Partition, "offset", r.Offset, "error", err)
		select {
		case <-ctx.Done():
			return false
		case <-time.After(retryInterval):
		}
	}
}
