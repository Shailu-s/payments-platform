package cards

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/Shailu-s/payments-platform/internal/outbox"
	"github.com/jackc/pgx/v5"
)

// Event is the audit record of one card change. It is written in the
// change's transaction, so neither can commit without the other.
type Event struct {
	CardID     string    `json:"card_id"`
	AccountID  string    `json:"account_id"`
	Status     string    `json:"status"`
	OccurredAt time.Time `json:"occurred_at"`
}

func insertEvent(ctx context.Context, tx pgx.Tx, eventType string, c Card) error {
	payload, err := json.Marshal(Event{
		CardID: c.ID, AccountID: c.AccountID, Status: c.Status, OccurredAt: c.UpdatedAt,
	})
	if err != nil {
		return fmt.Errorf("encode %s event for %s: %w", eventType, c.ID, err)
	}
	return outbox.Insert(ctx, tx, outbox.Event{
		ID:          newID("evt"),
		AggregateID: c.ID,
		EventType:   eventType,
		Topic:       "cards",
		Payload:     payload,
	})
}
