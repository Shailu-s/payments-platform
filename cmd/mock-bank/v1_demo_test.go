package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Shailu-s/payments-platform/internal/api"
	"github.com/Shailu-s/payments-platform/internal/auth"
	"github.com/Shailu-s/payments-platform/internal/consumer"
	"github.com/Shailu-s/payments-platform/internal/customerwebhooks"
	"github.com/Shailu-s/payments-platform/internal/ledger"
	"github.com/Shailu-s/payments-platform/internal/provider"
	"github.com/Shailu-s/payments-platform/internal/reconciliation"
	"github.com/Shailu-s/payments-platform/internal/relay"
	"github.com/Shailu-s/payments-platform/internal/testdb"
	"github.com/Shailu-s/payments-platform/internal/transfers"
	"github.com/Shailu-s/payments-platform/internal/webhooks"
	"github.com/Shailu-s/payments-platform/internal/worker"
	"github.com/jackc/pgx/v5"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

const demoCustomerSecret = "v1-demo-customer-key-not-for-production"

type duplicateDemoBank struct{}

func (duplicateDemoBank) SubmitDelay() time.Duration { return 0 }
func (duplicateDemoBank) Outcome(Payment) (string, *string, time.Duration) {
	return StatusSettled, nil, 0
}
func (duplicateDemoBank) WebhookAttempts() int { return 2 }

type withheldAcknowledgement struct {
	http.ResponseWriter
	ctx      context.Context
	accepted chan struct{}
}

func (w *withheldAcknowledgement) WriteHeader(code int) {
	if code == http.StatusAccepted {
		select {
		case w.accepted <- struct{}{}:
		default:
		}
		<-w.ctx.Done()
	}
	w.ResponseWriter.WriteHeader(code)
}

func awaitDemo(t *testing.T, ctx context.Context, ready func() (bool, error)) {
	t.Helper()
	for {
		ok, err := ready()
		if err != nil {
			t.Fatal(err)
		}
		if ok {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func TestV1PaymentSurvivesTimeoutAfterSuccess(t *testing.T) {
	for _, timedOut := range []bool{false, true} {
		name := "normal"
		if timedOut {
			name = "timeout_after_success"
		}
		t.Run(name, func(t *testing.T) { runV1Demo(t, timedOut) })
	}
}

func runV1Demo(t *testing.T, timedOut bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	var nonce [12]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		t.Fatal(err)
	}
	name := "test_v1_" + hex.EncodeToString(nonce[:])
	pool, err := testdb.Connect(ctx, name)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := pool.Exec(context.Background(), "DROP SCHEMA "+pgx.Identifier{name}.Sanitize()+" CASCADE"); err != nil {
			t.Error(err)
		}
		pool.Close()
	})
	plaintext, key, err := auth.Generate("isolated V1 demonstration")
	if err != nil {
		t.Fatal(err)
	}
	if err := auth.Insert(ctx, pool, key); err != nil {
		t.Fatal(err)
	}
	callbackGate := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(callbackGate) }) }
	defer release()
	callbackBodies := make(chan []byte, 2)
	apiHandler := api.NewServer(pool, []byte(testWebhookSecret), nil).Handler()
	platform := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/webhooks/mockbank" {
			body, err := io.ReadAll(io.LimitReader(r.Body, 64<<10))
			if err != nil {
				http.Error(w, "bad callback", 400)
				return
			}
			select {
			case callbackBodies <- body:
			case <-ctx.Done():
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			select {
			case <-callbackGate:
			case <-ctx.Done():
				w.WriteHeader(503)
				return
			}
			r.Body = io.NopCloser(bytes.NewReader(body))
		}
		apiHandler.ServeHTTP(w, r)
	}))
	defer platform.Close()
	request := func(method, path, body string) []byte {
		t.Helper()
		r, err := http.NewRequestWithContext(ctx, method, platform.URL+path, strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		r.Header.Set("Authorization", "Bearer "+plaintext)
		r.Header.Set("Content-Type", "application/json")
		if path == "/v1/transfers" {
			r.Header.Set("Idempotency-Key", name)
		}
		resp, err := platform.Client().Do(r)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		raw, err := io.ReadAll(resp.Body)
		if err != nil || resp.StatusCode >= 300 {
			t.Fatalf("%s %s: %d %s err=%v", method, path, resp.StatusCode, raw, err)
		}
		return raw
	}
	createAccount := func(kind string) string {
		raw := request("POST", "/v1/accounts", fmt.Sprintf(`{"currency":"USD","type":%q}`, kind))
		var account struct{ ID string }
		if err := json.Unmarshal(raw, &account); err != nil || account.ID == "" {
			t.Fatalf("create account: %v", err)
		}
		return account.ID
	}
	source, destination := createAccount("asset"), createAccount("liability")
	if _, err := ledger.Record(ctx, pool, "demo funding", []ledger.Entry{
		{AccountID: transfers.SettlementAccountID, Direction: ledger.DirectionDebit, Amount: 1000},
		{AccountID: source, Direction: ledger.DirectionCredit, Amount: 1000},
	}); err != nil {
		t.Fatal(err)
	}
	raw := request("POST", "/v1/transfers", fmt.Sprintf(`{"source_account":%q,"destination_account":%q,"amount":500,"currency":"USD"}`, source, destination))
	var payment struct {
		ID     string
		Status string
	}
	if err := json.Unmarshal(raw, &payment); err != nil || payment.Status != "processing" {
		t.Fatalf("create transfer: %s %v", raw, err)
	}
	store := NewStore()
	bankServer := NewServer(store, duplicateDemoBank{}, platform.URL+"/v1/webhooks/mockbank", []byte(testWebhookSecret))
	bankHandler := bankServer.Handler()
	accepted := make(chan struct{}, 1)
	var submissions atomic.Int64
	bank := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "POST" && r.URL.Path == "/transfers" {
			submissions.Add(1)
			if timedOut {
				w = &withheldAcknowledgement{ResponseWriter: w, ctx: r.Context(), accepted: accepted}
			}
		}
		bankHandler.ServeHTTP(w, r)
	}))
	defer bank.Close()
	timeout := provider.DefaultTimeout
	if timedOut {
		timeout = 500 * time.Millisecond
	}
	sender := worker.New(pool, provider.New(bank.URL, timeout), worker.DefaultConfig())
	producer, err := kgo.NewClient(kgo.SeedBrokers("localhost:9092"))
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	topic := "v1_demo_" + hex.EncodeToString(nonce[:])
	admin := kadm.NewClient(producer)
	created, err := admin.CreateTopics(ctx, 1, 1, nil, topic)
	if err != nil || created[topic].Err != nil {
		t.Fatalf("create isolated Kafka topic: %v %+v", err, created)
	}
	publish := func(want int) {
		t.Helper()
		if _, err := pool.Exec(ctx, "UPDATE outbox_events SET topic=$1 WHERE published_at IS NULL", topic); err != nil {
			t.Fatal(err)
		}
		if n, err := relay.PublishBatch(ctx, pool, producer, 10); err != nil || n != want {
			t.Fatalf("published=%d want=%d err=%v", n, want, err)
		}
	}
	startConsumer := func(group string, handler consumer.Handler) {
		t.Helper()
		client, err := kgo.NewClient(kgo.SeedBrokers("localhost:9092"), kgo.ConsumerGroup(group), kgo.ConsumeTopics(topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), kgo.DisableAutoCommit())
		if err != nil {
			t.Fatal(err)
		}
		consumeCtx, stop := context.WithCancel(ctx)
		done := make(chan struct{})
		go func() { defer close(done); consumer.Run(consumeCtx, client, handler) }()
		t.Cleanup(func() { stop(); client.Close(); <-done })
	}
	var customerCalls atomic.Int64
	customerBodies := make(chan []byte, 2)
	customer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(io.LimitReader(r.Body, 64<<10))
		if err != nil || !webhooks.Verify([]byte(demoCustomerSecret), r.Header.Get(customerwebhooks.TimestampHeader), r.Header.Get(customerwebhooks.SignatureHeader), body, time.Now()) {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		select {
		case customerBodies <- body:
		case <-ctx.Done():
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		if customerCalls.Add(1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
		} else {
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer customer.Close()
	defer func() { cancel(); release() }()
	deliveryConfig := customerwebhooks.DefaultConfig()
	deliveryConfig.Backoff = 10 * time.Millisecond
	deliveryConfig.BatchSize = 1
	deliveries, err := customerwebhooks.New(pool, customer.URL, []byte(demoCustomerSecret), deliveryConfig)
	if err != nil {
		t.Fatal(err)
	}
	type sendResult struct {
		claimed bool
		err     error
	}
	sent := make(chan sendResult, 1)
	startConsumer(topic+"_sender", func(ctx context.Context, r *kgo.Record) error {
		for _, h := range r.Headers {
			if h.Key == "event_type" && string(h.Value) == "transfer.created" {
				claimed, err := sender.SendByID(ctx, string(r.Key))
				select {
				case sent <- sendResult{claimed, err}:
				default:
				}
				return err
			}
		}
		return nil
	})
	startConsumer(topic+"_customer", deliveries.Handle)
	publish(1)
	select {
	case result := <-sent:
		if result.err != nil || !result.claimed {
			t.Fatalf("initial sender result: %+v", result)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if timedOut {
		select {
		case <-accepted:
		case <-ctx.Done():
			t.Fatal("provider never accepted before timeout")
		}
	}
	stored, err := transfers.Get(ctx, pool, payment.ID)
	wantState := transfers.StatusProcessing
	if timedOut {
		wantState = transfers.StatusUnresolved
	}
	if err != nil || stored.Status != wantState {
		t.Fatalf("pre-callback status=%s want=%s err=%v", stored.Status, wantState, err)
	}
	for i := 0; i < 3; i++ {
		if claimed, err := sender.SendByID(ctx, payment.ID); err != nil || claimed {
			t.Fatalf("redelivery resubmitted payment: %t %v", claimed, err)
		}
		if n, err := sender.RunOnce(ctx); err != nil || n != 0 {
			t.Fatalf("poller resubmitted payment: %d %v", n, err)
		}
	}
	if submissions.Load() != 1 || len(store.All()) != 1 {
		t.Fatal("unknown outcome triggered a second bank submission")
	}
	for account, want := range map[string]int64{source: 500, destination: 0} {
		if balance, err := ledger.Balance(ctx, pool, account); err != nil || balance != want {
			t.Fatalf("pre-callback balance=%d want=%d err=%v", balance, want, err)
		}
	}
	var firstCallback []byte
	select {
	case firstCallback = <-callbackBodies:
	case <-ctx.Done():
		t.Fatal("bank did not settle and send its callback")
	}
	report, err := reconciliation.FetchReport(ctx, bank.Client(), bank.URL)
	if err != nil || len(report.Records) != 1 || report.Records[0].Status != "settled" {
		t.Fatalf("bank did not actually settle: %+v %v", report, err)
	}
	before, err := reconciliation.Reconcile(ctx, pool, "before_callback", bytes.NewReader(report.Raw))
	if err != nil || before.Counts[reconciliation.StatusMismatch] != 1 {
		t.Fatalf("unknown outcome not detected: %+v %v", before, err)
	}
	release()
	bankServer.WaitForPending(ctx)
	if ctx.Err() != nil {
		t.Fatal(ctx.Err())
	}
	select {
	case second := <-callbackBodies:
		if !bytes.Equal(firstCallback, second) {
			t.Fatal("duplicate provider event changed")
		}
	case <-ctx.Done():
		t.Fatal("second callback did not arrive")
	}
	stored, err = transfers.Get(ctx, pool, payment.ID)
	if err != nil || stored.Status != transfers.StatusSettled {
		t.Fatalf("callback did not settle: %+v %v", stored, err)
	}
	var providerEvents int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM webhook_events").Scan(&providerEvents); err != nil || providerEvents != 1 {
		t.Fatalf("duplicate webhook recorded multiple effects: %d %v", providerEvents, err)
	}
	publish(1)
	var terminalEvent string
	if err := pool.QueryRow(ctx, "SELECT id FROM outbox_events WHERE event_type='transfer.settled'").Scan(&terminalEvent); err != nil {
		t.Fatal(err)
	}
	awaitDemo(t, ctx, func() (bool, error) {
		job, err := deliveries.Get(ctx, terminalEvent)
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return err == nil && job.Status == customerwebhooks.StatusPending, err
	})
	if n, err := deliveries.RunOnce(ctx); err != nil || n != 1 {
		t.Fatalf("first customer attempt: %d %v", n, err)
	}
	awaitDemo(t, ctx, func() (bool, error) { n, err := deliveries.RunOnce(ctx); return n == 1, err })
	firstCustomer, secondCustomer := <-customerBodies, <-customerBodies
	if !bytes.Equal(firstCustomer, secondCustomer) || customerCalls.Load() != 2 {
		t.Fatal("customer retry changed the event or sent extra requests")
	}
	var notification customerwebhooks.Envelope
	if err := json.Unmarshal(firstCustomer, &notification); err != nil || notification.EventID != terminalEvent || notification.EventType != "transfer.settled" || notification.Data.TransferID != payment.ID || notification.Data.Amount != 500 || notification.Data.Currency != "USD" {
		t.Fatalf("customer received the wrong signed payment event: %+v %v", notification, err)
	}
	job, err := deliveries.Get(ctx, terminalEvent)
	if err != nil || job.Status != customerwebhooks.StatusDelivered || job.AttemptCount != 2 {
		t.Fatalf("customer job=%+v err=%v", job, err)
	}
	for account, want := range map[string]int64{source: 500, destination: 500} {
		if balance, err := ledger.Balance(ctx, pool, account); err != nil || balance != want {
			t.Fatalf("final balance=%d want=%d err=%v", balance, want, err)
		}
	}
	var movements, entries, unbalanced int
	if err := pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM ledger_transactions),(SELECT count(*) FROM ledger_entries),
		(SELECT count(*) FROM (SELECT txn_id FROM ledger_entries GROUP BY txn_id HAVING sum(CASE WHEN direction='credit' THEN amount ELSE -amount END)<>0) breaks)`).Scan(&movements, &entries, &unbalanced); err != nil || movements != 3 || entries != 6 || unbalanced != 0 {
		t.Fatalf("audit history: movements=%d entries=%d unbalanced=%d err=%v", movements, entries, unbalanced, err)
	}
	report, err = reconciliation.FetchReport(ctx, bank.Client(), bank.URL)
	if err != nil {
		t.Fatal(err)
	}
	after, err := reconciliation.Reconcile(ctx, pool, "after_callback", bytes.NewReader(report.Raw))
	if err != nil || after.Counts[reconciliation.Matched] != 1 || after.Counts[reconciliation.StatusMismatch] != 0 {
		t.Fatalf("final reconciliation=%+v err=%v", after, err)
	}
	if submissions.Load() != 1 {
		t.Fatal("settlement or customer retry resubmitted payment")
	}
	t.Logf("$5 transfer: pre-callback=%s, provider submissions=1, duplicate callbacks=2/one effect, signed customer attempts=2/delivered, ledger=3 balanced transactions/6 entries, final reconciliation=MATCHED", wantState)
}
