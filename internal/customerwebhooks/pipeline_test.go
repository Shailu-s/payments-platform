package customerwebhooks

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Shailu-s/payments-platform/internal/api"
	"github.com/Shailu-s/payments-platform/internal/auth"
	"github.com/Shailu-s/payments-platform/internal/consumer"
	"github.com/Shailu-s/payments-platform/internal/ledger"
	"github.com/Shailu-s/payments-platform/internal/relay"
	"github.com/Shailu-s/payments-platform/internal/transfers"
	"github.com/Shailu-s/payments-platform/internal/webhooks"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestTerminalEventTravelsThroughKafkaAndCustomerRetry(t *testing.T) {
	resetDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	producer, err := kgo.NewClient(kgo.SeedBrokers("localhost:9092"))
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	admin := kadm.NewClient(producer)
	topic := fmt.Sprintf("customer_pipeline_%d", time.Now().UnixNano())
	group := topic + "_group"
	response, err := admin.CreateTopics(ctx, 1, 1, nil, topic)
	if err != nil || response[topic].Err != nil {
		t.Fatalf("create isolated topic: %v, %+v", err, response)
	}
	if _, err := testPool.Exec(ctx, `INSERT INTO accounts (id, currency, type) VALUES ('acc_src', 'USD', 'asset'), ('acc_dst', 'USD', 'liability')`); err != nil {
		t.Fatal(err)
	}
	if _, err := ledger.Record(ctx, testPool, "funding", []ledger.Entry{
		{AccountID: transfers.SettlementAccountID, Direction: ledger.DirectionDebit, Amount: 1000},
		{AccountID: "acc_src", Direction: ledger.DirectionCredit, Amount: 1000},
	}); err != nil {
		t.Fatal(err)
	}
	plaintext, key, err := auth.Generate("customer pipeline")
	if err != nil {
		t.Fatal(err)
	}
	if err := auth.Insert(ctx, testPool, key); err != nil {
		t.Fatal(err)
	}
	handler := api.NewServer(testPool, []byte(testSecret)).Handler()
	req := httptest.NewRequest("POST", "/v1/transfers", strings.NewReader(`{"source_account":"acc_src","destination_account":"acc_dst","amount":500,"currency":"USD"}`))
	req.Header.Set("Authorization", "Bearer "+plaintext)
	req.Header.Set("Idempotency-Key", "pipeline")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("create transfer: %d %s", rec.Code, rec.Body.String())
	}
	var transfer struct{ ID string }
	if err := json.Unmarshal(rec.Body.Bytes(), &transfer); err != nil {
		t.Fatal(err)
	}
	if err := transfers.Settle(ctx, testPool, transfer.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := testPool.Exec(ctx, `UPDATE outbox_events SET topic = $1`, topic); err != nil {
		t.Fatal(err)
	}
	if n, err := relay.PublishBatch(ctx, testPool, producer, 10); err != nil || n != 2 {
		t.Fatalf("relay published %d, want created and settled: %v", n, err)
	}
	var calls atomic.Int64
	received := make(chan Envelope, 2)
	receiver := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if !webhooks.Verify([]byte(testSecret), r.Header.Get(TimestampHeader), r.Header.Get(SignatureHeader), body, time.Now()) {
			rw.WriteHeader(http.StatusUnauthorized)
			return
		}
		var envelope Envelope
		json.Unmarshal(body, &envelope)
		received <- envelope
		if calls.Add(1) == 1 {
			rw.WriteHeader(http.StatusServiceUnavailable)
		} else {
			rw.WriteHeader(http.StatusOK)
		}
	}))
	defer receiver.Close()
	now := time.Now().UTC().Truncate(time.Microsecond)
	cfg := DefaultConfig()
	cfg.Now = func() time.Time { return now }
	worker, err := New(testPool, receiver.URL, []byte(testSecret), cfg)
	if err != nil {
		t.Fatal(err)
	}
	client, err := kgo.NewClient(kgo.SeedBrokers("localhost:9092"), kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), kgo.DisableAutoCommit())
	if err != nil {
		t.Fatal(err)
	}
	consumeCtx, stop := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		consumer.Run(consumeCtx, client, worker.Handle)
	}()
	t.Cleanup(func() {
		stop()
		client.Close()
		<-done
	})
	for {
		offsets, err := admin.FetchOffsets(ctx, group)
		if err == nil {
			if offset, ok := offsets.Lookup(topic, 0); ok && offset.Err == nil && offset.At == 2 {
				break
			}
		}
		select {
		case <-ctx.Done():
			t.Fatal("customer consumer did not durably ingest and commit both records")
		case <-time.After(20 * time.Millisecond):
		}
	}
	stop()
	client.Close()
	<-done
	if n, err := worker.RunOnce(ctx); err != nil || n != 1 {
		t.Fatalf("first customer attempt: %d %v", n, err)
	}
	first := <-received
	now = now.Add(cfg.Backoff)
	if n, err := worker.RunOnce(ctx); err != nil || n != 1 {
		t.Fatalf("customer recovery attempt: %d %v", n, err)
	}
	second := <-received
	if first.EventID != second.EventID || first.EventType != "transfer.settled" || first.Data.TransferID != transfer.ID {
		t.Errorf("customer event changed across retry: %+v %+v", first, second)
	}
	job, err := worker.Get(ctx, first.EventID)
	if err != nil || job.Status != StatusDelivered || job.AttemptCount != 2 {
		t.Errorf("pipeline delivery = %+v: %v", job, err)
	}
	balance, err := ledger.Balance(ctx, testPool, "acc_dst")
	if err != nil || balance != 500 {
		t.Errorf("notification retries altered money: balance %d: %v", balance, err)
	}
	var movements int
	if err := testPool.QueryRow(ctx, `SELECT count(*) FROM ledger_transactions`).Scan(&movements); err != nil || movements != 3 {
		t.Errorf("notification retry added ledger movements: %d %v", movements, err)
	}
}
