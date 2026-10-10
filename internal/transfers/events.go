package transfers

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/Shailu-s/payments-platform/internal/outbox"
	"github.com/jackc/pgx/v5"
)

type TerminalEvent struct {
	TransferID         string `json:"transfer_id"`
	Status             string `json:"status"`
	Amount             int64  `json:"amount"`
	Currency           string `json:"currency"`
	SourceAccount      string `json:"source_account"`
	DestinationAccount string `json:"destination_account"`
}

func insertTerminalEvent(ctx context.Context, tx pgx.Tx, event TerminalEvent) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("encode terminal event %s: %w", event.TransferID, err)
	}
	var id [16]byte
	_, _ = rand.Read(id[:])
	return outbox.Insert(ctx, tx, outbox.Event{
		ID:          "evt_" + hex.EncodeToString(id[:]),
		AggregateID: event.TransferID,
		EventType:   "transfer." + event.Status,
		Topic:       "transfers",
		Payload:     payload,
	})
}
