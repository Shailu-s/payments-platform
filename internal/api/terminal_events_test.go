package api

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/Shailu-s/payments-platform/internal/ledger"
)

func TestTerminalOutboxFailureRollsBackFinancialEffect(t *testing.T) {
	for _, status := range []string{"settled", "failed"} {
		t.Run(status, func(t *testing.T) {
			ctx := context.Background()
			h, apiKey := newTestServer(t)
			id, source, destination := sentTransfer(t, h, apiKey, "mb_outbox_failure")
			event := eventFor("evt_outbox_failure", "mb_outbox_failure", id, status)
			if _, err := testPool.Exec(ctx, `
				CREATE FUNCTION reject_terminal_event() RETURNS trigger LANGUAGE plpgsql AS $$
				BEGIN
					RAISE EXCEPTION 'injected outbox insert failure';
				END $$;
				CREATE TRIGGER reject_terminal_event BEFORE INSERT ON outbox_events
				FOR EACH ROW EXECUTE FUNCTION reject_terminal_event();`); err != nil {
				t.Fatal(err)
			}
			removeFailure := func() {
				if _, err := testPool.Exec(ctx, `DROP TRIGGER IF EXISTS reject_terminal_event ON outbox_events;
					DROP FUNCTION IF EXISTS reject_terminal_event();`); err != nil {
					t.Fatal(err)
				}
			}
			t.Cleanup(removeFailure)
			if rec := sendWebhook(h, event); rec.Code != http.StatusInternalServerError {
				t.Fatalf("outbox insert failure = %d, want 500: %s", rec.Code, rec.Body.String())
			}
			var storedStatus string
			if err := testPool.QueryRow(ctx, `SELECT status FROM transfers WHERE id = $1`, id).Scan(&storedStatus); err != nil {
				t.Fatal(err)
			}
			if storedStatus != "processing" {
				t.Errorf("outbox failure committed transfer status %q", storedStatus)
			}
			if got := countRows(t, "webhook_events"); got != 0 {
				t.Errorf("outbox failure left %d provider events, want 0", got)
			}
			if got := countRows(t, "outbox_events"); got != 1 {
				t.Errorf("outbox failure left %d outbox rows, want only the creation event", got)
			}
			if got := countRows(t, "ledger_transactions"); got != 2 {
				t.Errorf("outbox failure left %d ledger transactions, want funding and creation only", got)
			}
			balance, err := ledger.Balance(ctx, testPool, destination)
			if err != nil || balance != 0 {
				t.Errorf("destination after rollback = %d, want 0: %v", balance, err)
			}
			balance, err = ledger.Balance(ctx, testPool, source)
			if err != nil || balance != 950000 {
				t.Errorf("source after rollback = %d, want 950000: %v", balance, err)
			}
			removeFailure()
			if rec := sendWebhook(h, event); rec.Code != http.StatusOK {
				t.Fatalf("retry after outbox recovery = %d: %s", rec.Code, rec.Body.String())
			}
			if got := countRows(t, "outbox_events"); got != 2 {
				t.Errorf("retry left %d outbox rows, want creation plus terminal", got)
			}
		})
	}
}

func TestTerminalWebhookWritesOnePublicOutboxEvent(t *testing.T) {
	for _, status := range []string{"settled", "failed"} {
		t.Run(status, func(t *testing.T) {
			ctx := context.Background()
			h, apiKey := newTestServer(t)
			id, source, destination := sentTransfer(t, h, apiKey, "mb_terminal")
			event := eventFor("evt_terminal", "mb_terminal", id, status)
			for i := 0; i < 2; i++ {
				if rec := sendWebhook(h, event); rec.Code != http.StatusOK {
					t.Fatalf("delivery %d = %d: %s", i, rec.Code, rec.Body.String())
				}
			}
			for _, laterStatus := range []string{"settled", "failed"} {
				if rec := sendWebhook(h, eventFor("evt_terminal_"+laterStatus, "mb_terminal", id, laterStatus)); rec.Code != http.StatusOK {
					t.Fatalf("new event for terminal transfer = %d: %s", rec.Code, rec.Body.String())
				}
			}
			eventType := "transfer." + status
			var count int
			if err := testPool.QueryRow(ctx, `SELECT count(*) FROM outbox_events WHERE aggregate_id = $1 AND event_type = $2`, id, eventType).Scan(&count); err != nil {
				t.Fatal(err)
			}
			if count != 1 {
				t.Fatalf("terminal event count = %d, want 1", count)
			}
			var (
				outboxID, topic string
				payload         []byte
				published       bool
			)
			if err := testPool.QueryRow(ctx, `
				SELECT id, topic, payload, published_at IS NOT NULL
				FROM outbox_events WHERE aggregate_id = $1 AND event_type = $2`, id, eventType,
			).Scan(&outboxID, &topic, &payload, &published); err != nil {
				t.Fatal(err)
			}
			if outboxID == "" || topic != "transfers" || published {
				t.Errorf("outbox id=%q topic=%q published=%t, want durable unpublished event on transfers", outboxID, topic, published)
			}
			var got struct {
				TransferID         string `json:"transfer_id"`
				Status             string `json:"status"`
				Amount             int64  `json:"amount"`
				Currency           string `json:"currency"`
				SourceAccount      string `json:"source_account"`
				DestinationAccount string `json:"destination_account"`
			}
			if err := json.Unmarshal(payload, &got); err != nil {
				t.Fatal(err)
			}
			if got.TransferID != id || got.Status != status || got.Amount != 50000 || got.Currency != "USD" || got.SourceAccount != source || got.DestinationAccount != destination {
				t.Errorf("public event payload = %+v", got)
			}
			var fields map[string]json.RawMessage
			if err := json.Unmarshal(payload, &fields); err != nil {
				t.Fatal(err)
			}
			if len(fields) != 6 {
				t.Errorf("payload has %d fields, want exactly six public fields", len(fields))
			}
			if got := countRows(t, "outbox_events"); got != 2 {
				t.Errorf("outbox has %d rows, want one created and one terminal event", got)
			}
		})
	}
}
