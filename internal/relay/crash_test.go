package relay

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Shailu-s/payments-platform/internal/outbox"
	"github.com/twmb/franz-go/pkg/kadm"
)

// ⭐ At-least-once: a relay that dies after Kafka has the event, but before
// its COMMIT, publishes the event again. Nothing is lost; something is
// duplicated, and that duplicate is why the consumer must dedupe on event_id.
//
// The failure is injected at COMMIT, the latest point it can happen: the
// produce has succeeded and the UPDATE has run, so this is the widest version
// of the gap. A deferred constraint trigger runs at COMMIT rather than at the
// UPDATE, raises, and Postgres rolls the transaction back — the same trick as
// TestFailedCommitWritesNoEvent in internal/api.
func TestRelayRepublishesAfterFailedCommit(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	resetDB(t)

	topic := fmt.Sprintf("relay-test-%d", time.Now().UnixNano())
	if _, err := kadm.NewClient(kafka).CreateTopic(ctx, 3, 1, nil, topic); err != nil {
		t.Fatalf("create topic %s: %v", topic, err)
	}

	if err := outbox.Insert(ctx, testPool, outbox.Event{
		ID: "evt_crash", AggregateID: "tr_crash", EventType: "transfer.created",
		Topic: topic, Payload: []byte(`{"seq":1}`),
	}); err != nil {
		t.Fatal(err)
	}

	if _, err := testPool.Exec(ctx, `
              CREATE FUNCTION fail_at_commit() RETURNS trigger LANGUAGE plpgsql AS $$
              BEGIN
                      RAISE EXCEPTION 'injected commit failure';
              END $$;
              CREATE CONSTRAINT TRIGGER fail_at_commit
                      AFTER UPDATE ON outbox_events
                      DEFERRABLE INITIALLY DEFERRED
                      FOR EACH ROW EXECUTE FUNCTION fail_at_commit();`); err != nil {
		t.Fatalf("install commit failure: %v", err)
	}
	removeFailure := func() {
		if _, err := testPool.Exec(context.Background(), `
                      DROP TRIGGER IF EXISTS fail_at_commit ON outbox_events;
                      DROP FUNCTION IF EXISTS fail_at_commit();`); err != nil {
			t.Errorf("remove commit failure: %v", err)
		}
	}
	t.Cleanup(removeFailure)

	// The crash: Kafka takes the event, then the COMMIT that marks it fails.
	if _, err := PublishBatch(ctx, testPool, kafka, 10); err == nil {
		t.Fatal("publish succeeded, want the injected commit failure")
	}

	if got := readRecords(t, ctx, topic, 1); header(got[0], "event_id") != "evt_crash" {
		t.Fatalf("event_id = %q, want evt_crash: Kafka should hold the event despite the failed commit",
			header(got[0], "event_id"))
	}

	var published bool
	if err := testPool.QueryRow(ctx,
		`SELECT published_at IS NOT NULL FROM outbox_events WHERE id = 'evt_crash'`).Scan(&published); err !=
		nil {
		t.Fatalf("read evt_crash: %v", err)
	}
	if published {
		t.Fatal("evt_crash is marked published, want unpublished: the commit that marked it failed")
	}

	// The restart: the next batch finds the row still unpublished and sends it
	// again.
	removeFailure()
	n, err := PublishBatch(ctx, testPool, kafka, 10)
	if err != nil {
		t.Fatalf("publish after recovery: %v", err)
	}
	if n != 1 {
		t.Fatalf("republished %d events, want 1", n)
	}

	got := readRecords(t, ctx, topic, 2)
	for i, r := range got {
		if id := header(r, "event_id"); id != "evt_crash" {
			t.Errorf("record %d event_id = %q, want evt_crash", i, id)
		}
	}
	if got[0].Partition != got[1].Partition {
		t.Errorf("the copies landed on partitions %d and %d, want one", got[0].Partition, got[1].Partition)
	}
	t.Logf("Kafka holds %d copies of evt_crash on partition %d, offsets %d and %d",
		len(got), got[0].Partition, got[0].Offset, got[1].Offset)
}
