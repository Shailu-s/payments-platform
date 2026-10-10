package customerwebhooks

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/Shailu-s/payments-platform/internal/transfers"
	"github.com/twmb/franz-go/pkg/kgo"
)

type Delivery struct {
	EventID        string
	Endpoint       string
	Payload        []byte
	Status         string
	AttemptCount   int
	NextAttemptAt  time.Time
	LastHTTPStatus *int
	LastError      *string
	DeliveredAt    *time.Time
}

const columns = `event_id, endpoint, payload, status, attempt_count, next_attempt_at,
	last_http_status, last_error, delivered_at`

type Envelope struct {
	EventID   string                  `json:"event_id"`
	EventType string                  `json:"event_type"`
	Data      transfers.TerminalEvent `json:"data"`
}

func (w *Worker) Handle(ctx context.Context, record *kgo.Record) error {
	eventType := header(record, "event_type")
	if eventType != "transfer.settled" && eventType != "transfer.failed" {
		return nil
	}
	eventID := header(record, "event_id")
	var event transfers.TerminalEvent
	err := json.Unmarshal(record.Value, &event)
	status := StatusPending
	var reason *string
	payload := []byte(`{}`)
	if eventID == "" || err != nil || event.TransferID == "" || event.TransferID != string(record.Key) ||
		event.Status != strings.TrimPrefix(eventType, "transfer.") || event.Amount <= 0 || event.Currency != "USD" ||
		event.SourceAccount == "" || event.DestinationAccount == "" {
		if eventID == "" {
			eventID = fmt.Sprintf("kafka:%s:%d:%d", record.Topic, record.Partition, record.Offset)
		}
		status = StatusDead
		message := "invalid terminal event"
		reason = &message
	} else {
		payload, err = json.Marshal(Envelope{EventID: eventID, EventType: eventType, Data: event})
		if err != nil {
			return fmt.Errorf("encode customer delivery %s: %w", eventID, err)
		}
	}
	_, err = w.db.Exec(ctx, `
		INSERT INTO customer_webhook_deliveries (event_id, endpoint, payload, status, next_attempt_at, last_error)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (event_id) DO NOTHING`, eventID, w.endpoint, payload, status, w.cfg.Now(), reason)
	if err != nil {
		return fmt.Errorf("save customer delivery %s: %w", eventID, err)
	}
	return nil
}

func header(record *kgo.Record, name string) string {
	for _, h := range record.Headers {
		if h.Key == name {
			return string(h.Value)
		}
	}
	return ""
}

func (w *Worker) Get(ctx context.Context, eventID string) (Delivery, error) {
	d, err := scan(w.db.QueryRow(ctx, `SELECT `+columns+` FROM customer_webhook_deliveries WHERE event_id = $1`, eventID))
	if err != nil {
		return Delivery{}, fmt.Errorf("read customer delivery %s: %w", eventID, err)
	}
	return d, nil
}

func (w *Worker) claim(ctx context.Context) (Delivery, error) {
	now := w.cfg.Now()
	return scan(w.db.QueryRow(ctx, `
		UPDATE customer_webhook_deliveries
		SET attempt_count = attempt_count + 1, next_attempt_at = $2, updated_at = $1
		WHERE event_id = (
			SELECT event_id FROM customer_webhook_deliveries
			WHERE status = 'pending' AND next_attempt_at <= $1 AND attempt_count < $3
			ORDER BY next_attempt_at, created_at, event_id
			FOR UPDATE SKIP LOCKED LIMIT 1
		)
		RETURNING `+columns, now, now.Add(w.cfg.Lease), w.cfg.MaxAttempts))
}

type row interface {
	Scan(...any) error
}

func scan(r row) (Delivery, error) {
	var d Delivery
	err := r.Scan(&d.EventID, &d.Endpoint, &d.Payload, &d.Status, &d.AttemptCount,
		&d.NextAttemptAt, &d.LastHTTPStatus, &d.LastError, &d.DeliveredAt)
	return d, err
}
