package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/Shailu-s/payments-platform/internal/ledger"
)

// A transfer commits with one public event; retries must not add another.
func TestTransferAndItsEventCommitTogether(t *testing.T) {
	ctx := context.Background()
	h, apiKey := newTestServer(t)

	source := createAccount(t, h, apiKey, "asset")
	destination := createAccount(t, h, apiKey, "liability")
	fund(t, source.ID, 1000000)

	body := fmt.Sprintf(
		`{"source_account":%q,"destination_account":%q,"amount":50000,"currency":"USD"}`,
		source.ID, destination.ID)

	rec := doWithKey(h, "POST", "/v1/transfers", apiKey, "outbox-1", body)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202: %s", rec.Code, rec.Body.String())
	}
	var created transferResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &created); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	var (
		eventType, topic string
		payload          []byte
		published        bool
	)
	if err := testPool.QueryRow(ctx, `
		SELECT event_type, topic, payload, published_at IS NOT NULL
		FROM outbox_events WHERE aggregate_id = $1`, created.ID,
	).Scan(&eventType, &topic, &payload, &published); err != nil {
		t.Fatalf("read outbox event for %s: %v", created.ID, err)
	}

	if eventType != "transfer.created" || topic != "transfers" {
		t.Errorf("event_type, topic = %q, %q, want transfer.created, transfers", eventType, topic)
	}
	if published {
		t.Errorf("published_at is set, want NULL: nothing has relayed it yet")
	}

	var event TransferEvent
	if err := json.Unmarshal(payload, &event); err != nil {
		t.Fatalf("payload is not a TransferEvent: %v\n%s", err, payload)
	}
	want := TransferEvent{
		TransferID:         created.ID,
		Amount:             50000,
		Currency:           "USD",
		SourceAccount:      source.ID,
		DestinationAccount: destination.ID,
	}
	if event != want {
		t.Errorf("payload = %+v, want %+v", event, want)
	}

	// Serialising the database row would leak private key and retry metadata.
	var fields map[string]any
	if err := json.Unmarshal(payload, &fields); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if len(fields) != 5 {
		t.Errorf("payload has %d fields, want exactly the 5 of TransferEvent: %s", len(fields), payload)
	}

	// A retry is answered from the existing transfer and never reaches
	// insertTransfer, so it cannot write a second event.
	replay := doWithKey(h, "POST", "/v1/transfers", apiKey, "outbox-1", body)
	if replay.Code != http.StatusAccepted {
		t.Fatalf("replay status = %d, want 202: %s", replay.Code, replay.Body.String())
	}
	if n := countRows(t, "outbox_events"); n != 1 {
		t.Errorf("after a replay there are %d outbox events, want 1", n)
	}
}

// Fail COMMIT with a deferred trigger after the outbox insert. Earlier failure
// cannot distinguish an insert on the caller's transaction from one on the pool.
func TestFailedCommitWritesNoEvent(t *testing.T) {
	ctx := context.Background()
	h, apiKey := newTestServer(t)

	source := createAccount(t, h, apiKey, "asset")
	destination := createAccount(t, h, apiKey, "liability")
	fund(t, source.ID, 1000000)

	if _, err := testPool.Exec(ctx, `
		CREATE FUNCTION fail_at_commit() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN
			RAISE EXCEPTION 'injected commit failure';
		END $$;
		CREATE CONSTRAINT TRIGGER fail_at_commit
			AFTER INSERT ON transfers
			DEFERRABLE INITIALLY DEFERRED
			FOR EACH ROW EXECUTE FUNCTION fail_at_commit();`); err != nil {
		t.Fatalf("install commit failure: %v", err)
	}
	t.Cleanup(func() {
		if _, err := testPool.Exec(context.Background(), `
			DROP TRIGGER IF EXISTS fail_at_commit ON transfers;
			DROP FUNCTION IF EXISTS fail_at_commit();`); err != nil {
			t.Errorf("remove commit failure: %v", err)
		}
	})

	body := fmt.Sprintf(
		`{"source_account":%q,"destination_account":%q,"amount":50000,"currency":"USD"}`,
		source.ID, destination.ID)

	rec := doWithKey(h, "POST", "/v1/transfers", apiKey, "outbox-commit-fails", body)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500 from the failed commit: %s", rec.Code, rec.Body.String())
	}

	if n := countRows(t, "transfers"); n != 0 {
		t.Errorf("%d transfers exist after a failed commit, want 0", n)
	}
	if n := countRows(t, "outbox_events"); n != 0 {
		t.Errorf("%d outbox events exist after a failed commit, want 0: "+
			"an event for a transfer that does not exist", n)
	}

	balance, err := ledger.Balance(ctx, testPool, source.ID)
	if err != nil {
		t.Fatalf("balance: %v", err)
	}
	if balance != 1000000 {
		t.Errorf("source balance = %d, want 1000000: the debit should have rolled back", balance)
	}
}

func countRows(t *testing.T, table string) int {
	t.Helper()
	var n int
	if err := testPool.QueryRow(context.Background(),
		"SELECT count(*) FROM "+table).Scan(&n); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	return n
}
