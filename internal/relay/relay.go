package relay

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/twmb/franz-go/pkg/kgo"
)

type Beginner interface {
	Begin(ctx context.Context) (pgx.Tx, error)
}

func PublishBatch(ctx context.Context, db Beginner, client *kgo.Client, limit int) (int, error) {
	tx, err := db.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("relay: begin: %w", err)
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, `
			SELECT id, aggregate_id, event_type, topic, payload
			FROM outbox_events
			WHERE published_at IS NULL
			ORDER BY created_at, id
			LIMIT $1
			FOR UPDATE`, limit)
	if err != nil {
		return 0, fmt.Errorf("relay: select unpublished: %w", err)
	}
	defer rows.Close()

	var (
		records []*kgo.Record
		ids     []string
	)
	for rows.Next() {
		var (
			id, aggregateID, eventType, topic string
			payload                           []byte
		)
		if err := rows.Scan(&id, &aggregateID, &eventType, &topic, &payload); err != nil {
			return 0, fmt.Errorf("relay: scan: %w", err)
		}
		records = append(records, &kgo.Record{
			Topic: topic,
			Key:   []byte(aggregateID),
			Value: payload,
			Headers: []kgo.RecordHeader{
				{Key: "event_id", Value: []byte(id)},
				{Key: "event_type", Value: []byte(eventType)},
			},
		})
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("relay: read unpublished: %w", err)
	}
	if len(records) == 0 {
		return 0, nil
	}

	if err := client.ProduceSync(ctx, records...).FirstErr(); err != nil {
		return 0, fmt.Errorf("relay: produce %d events: %w", len(records), err)
	}

	if _, err := tx.Exec(ctx,
		`UPDATE outbox_events SET published_at = now() WHERE id = ANY($1)`, ids); err != nil {
		return 0, fmt.Errorf("relay: mark published: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("relay: commit: %w", err)
	}
	return len(ids), nil
}
