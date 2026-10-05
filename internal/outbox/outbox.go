package outbox

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgconn"
)

type Event struct {
	ID          string
	AggregateID string
	EventType   string
	Topic       string
	Payload     []byte
}

type Execer interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// Insert must use the business change's transaction so neither can commit alone.
func Insert(ctx context.Context, db Execer, event Event) error {
	_, err := db.Exec(ctx, "INSERT INTO outbox_events (id, aggregate_id, event_type, topic, payload) VALUES ($1, $2, $3, $4, $5)", event.ID, event.AggregateID, event.EventType, event.Topic, event.Payload)
	if err != nil {
		return fmt.Errorf("insert outbox event %s: %w", event.ID, err)
	}
	return nil
}
