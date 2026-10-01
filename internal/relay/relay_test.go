package relay

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/Shailu-s/payments-platform/internal/outbox"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// ⭐ The relay moves outbox rows into Kafka, keeps one transfer's events in
// order, and marks a row published only after Kafka has it.
//
// Two events for the same transfer are the point. With one event, "in order"
// and "same partition" would be true by accident.
func TestRelayPublishesInOrderAndMarksDelivered(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	resetDB(t)

	// A fresh topic per run, so records from an earlier run cannot be
	// mistaken for this one's.
	topic := fmt.Sprintf("relay-test-%d", time.Now().UnixNano())
	if _, err := kadm.NewClient(kafka).CreateTopic(ctx, 3, 1, nil, topic); err != nil {
		t.Fatalf("create topic %s: %v", topic, err)
	}

	events := []outbox.Event{
		{ID: "evt_1", AggregateID: "tr_123", EventType: "transfer.created", Topic: topic, Payload: []byte(`{"seq":1}`)},
		{ID: "evt_2", AggregateID: "tr_123", EventType: "transfer.settled", Topic: topic, Payload: []byte(`{"seq":2}`)},
	}
	// Inserted on the pool, so each is its own transaction — as in real life,
	// where settled is written long after created committed.
	for _, e := range events {
		if err := outbox.Insert(ctx, testPool, e); err != nil {
			t.Fatal(err)
		}
	}

	n, err := PublishBatch(ctx, testPool, kafka, 10)
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	if n != 2 {
		t.Fatalf("published %d events, want 2", n)
	}

	got := readRecords(t, ctx, topic, 2)

	for i, r := range got {
		want := events[i]
		if string(r.Key) != want.AggregateID {
			t.Errorf("record %d key = %q, want %q", i, r.Key, want.AggregateID)
		}
		if id := header(r, "event_id"); id != want.ID {
			t.Errorf("record %d event_id = %q, want %q: created must arrive before settled", i, id, want.ID)
		}
		if typ := header(r, "event_type"); typ != want.EventType {
			t.Errorf("record %d event_type = %q, want %q", i, typ, want.EventType)
		}
		// jsonb stores parsed JSON and prints it back reformatted, so the
		// bytes differ from what was inserted. Compare the meaning.
		var body struct {
			Seq int `json:"seq"`
		}
		if err := json.Unmarshal(r.Value, &body); err != nil || body.Seq != i+1 {
			t.Errorf("record %d value = %s, want seq %d", i, r.Value, i+1)
		}
	}
	if got[0].Partition != got[1].Partition {
		t.Errorf("partitions = %d and %d, want one: same key, same partition",
			got[0].Partition, got[1].Partition)
	}

	var unpublished int
	if err := testPool.QueryRow(ctx,
		`SELECT count(*) FROM outbox_events WHERE published_at IS NULL`).Scan(&unpublished); err != nil {
		t.Fatalf("count unpublished: %v", err)
	}
	if unpublished != 0 {
		t.Errorf("%d events still unpublished after a successful batch, want 0", unpublished)
	}

	n, err = PublishBatch(ctx, testPool, kafka, 10)
	if err != nil {
		t.Fatalf("second publish: %v", err)
	}
	if n != 0 {
		t.Errorf("second batch published %d events, want 0: published rows were sent again", n)
	}
}

// readRecords reads topic from the start until it has n records, or fails when
// ctx expires. It is a consumer with no group: it reads every partition and
// remembers nothing, which is all a test needs. The real consumer is 5.4.
func readRecords(t *testing.T, ctx context.Context, topic string, n int) []*kgo.Record {
	t.Helper()
	c, err := kgo.NewClient(
		kgo.SeedBrokers(brokers),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatalf("consumer: %v", err)
	}
	defer c.Close()

	var got []*kgo.Record
	for len(got) < n {
		fetches := c.PollFetches(ctx)
		if err := fetches.Err(); err != nil {
			t.Fatalf("read %s: %v (have %d of %d records)", topic, err, len(got), n)
		}
		fetches.EachRecord(func(r *kgo.Record) { got = append(got, r) })
	}
	return got
}

func header(r *kgo.Record, key string) string {
	for _, h := range r.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}
