package consumer_test

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/Shailu-s/payments-platform/internal/api"
	"github.com/Shailu-s/payments-platform/internal/auth"
	"github.com/Shailu-s/payments-platform/internal/consumer"
	"github.com/Shailu-s/payments-platform/internal/ledger"
	"github.com/Shailu-s/payments-platform/internal/provider"
	"github.com/Shailu-s/payments-platform/internal/relay"
	"github.com/Shailu-s/payments-platform/internal/testdb"
	"github.com/Shailu-s/payments-platform/internal/transfers"
	"github.com/Shailu-s/payments-platform/internal/webhooks"
	"github.com/Shailu-s/payments-platform/internal/worker"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	crashBrokers       = "localhost:9092"
	crashWebhookSecret = "test-only-webhook-secret-never-use-in-production"
)

// Guarantee 7: SIGKILL before offset commit redelivers an accepted payment without sending it twice.
func TestSenderSIGKILLRedeliversWithoutDuplicatePayment(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	f := newCrashFixture(t, 5)
	client, err := kgo.NewClient(kgo.SeedBrokers(crashBrokers))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if err := client.Ping(ctx); err != nil {
		t.Fatalf("Kafka unavailable; run make up: %v", err)
	}
	admin := kadm.NewClient(client)
	topic := "sender-kill-" + f.schema
	if _, err := admin.CreateTopic(ctx, 1, 1, nil, topic); err != nil {
		t.Fatal(err)
	}
	if _, err := f.pool.Exec(ctx, `UPDATE outbox_events SET topic = $1`, topic); err != nil {
		t.Fatal(err)
	}
	if n, err := relay.PublishBatch(ctx, f.pool, client, 10); err != nil || n != len(f.ids) {
		t.Fatalf("relay published %d, want %d: %v", n, len(f.ids), err)
	}
	group := topic
	first, lines := startSenderProcess(t, ctx, f, topic, group, true)
	before := readDelivery(t, ctx, lines)
	if before.Offset != 0 || !before.Sent {
		t.Fatalf("first delivery = %+v, want offset 0 sent=true", before)
	}
	assertCommittedOffset(t, ctx, admin, group, topic, -1)
	stored, err := transfers.Get(ctx, f.pool, before.ID)
	if err != nil || stored.ProviderRef == nil || stored.Status != transfers.StatusProcessing {
		t.Fatalf("acceptance was not durable and awaiting settlement before kill: %+v, %v", stored, err)
	}
	ledgerBefore := ledgerSnapshot(t, ctx, f.pool, before.ID)
	if err := first.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	err = first.Wait()
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.Sys().(syscall.WaitStatus).Signal() != syscall.SIGKILL {
		t.Fatalf("sender exit = %v, want SIGKILL", err)
	}
	t.Logf("SIGKILL pid=%d after offset %d was sent; no committed offset", first.Process.Pid, before.Offset)

	second, lines := startSenderProcess(t, ctx, f, topic, group, false)
	redelivered := readDelivery(t, ctx, lines)
	if redelivered.ID != before.ID || redelivered.Offset != before.Offset || redelivered.Partition != before.Partition || redelivered.EventID != before.EventID {
		t.Fatalf("redelivery = %+v, want the same record as %+v", redelivered, before)
	}
	if redelivered.Sent {
		t.Errorf("redelivered transfer %s was submitted again", before.ID)
	}
	if got := ledgerSnapshot(t, ctx, f.pool, before.ID); got != ledgerBefore {
		t.Errorf("redelivery changed ledger entries: before=%s after=%s", ledgerBefore, got)
	}
	for i := 1; i < len(f.ids); i++ {
		if got := readDelivery(t, ctx, lines); !got.Sent {
			t.Errorf("new record was not sent: %+v", got)
		}
	}
	waitForCommit(t, ctx, admin, group, topic, int64(len(f.ids)))
	if err := second.Process.Signal(os.Interrupt); err != nil {
		t.Fatal(err)
	}
	if err := second.Wait(); err != nil {
		t.Fatalf("restarted sender: %v", err)
	}
	f.settleAndAssertOnce(t, ctx)
	if t.Failed() {
		return
	}
	t.Logf("redelivered event=%s partition=%d offset=%d sent=%t; %d transfers settled, %d provider submissions, zero duplicate effects", redelivered.EventID, redelivered.Partition, redelivered.Offset, redelivered.Sent, len(f.ids), f.submissionCount())
}

type delivery struct {
	ID        string `json:"id"`
	EventID   string `json:"event_id"`
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
	Sent      bool   `json:"sent"`
}

func TestPollerSettlesWithKafkaUnavailable(t *testing.T) {
	if os.Getenv("KAFKA_DOWN_TEST") != "1" {
		t.Skip("opt-in experiment: stop Kafka first and set KAFKA_DOWN_TEST=1")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	client, err := kgo.NewClient(kgo.SeedBrokers(crashBrokers))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	pingCtx, stopPing := context.WithTimeout(ctx, time.Second)
	err = client.Ping(pingCtx)
	stopPing()
	if err == nil {
		t.Fatal("Kafka is reachable; this experiment requires a stopped broker")
	}
	started := time.Now()
	f := newCrashFixture(t, 1)
	cfg := worker.DefaultConfig()
	cfg.HeadStart = 30 * time.Second
	w := worker.New(f.pool, provider.New(f.rail.URL, provider.DefaultTimeout), cfg)
	for {
		claimed, err := w.RunOnce(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if claimed > 0 {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("poller did not rescue the transfer before the deadline")
		case <-time.After(cfg.PollInterval):
		}
	}
	elapsed := time.Since(started)
	if elapsed < cfg.HeadStart {
		t.Errorf("poller sent after %s, before its %s head start", elapsed, cfg.HeadStart)
	}
	f.settleAndAssertOnce(t, ctx)
	var unpublished int
	if err := f.pool.QueryRow(ctx, `SELECT count(*) FROM outbox_events WHERE published_at IS NULL`).Scan(&unpublished); err != nil {
		t.Fatal(err)
	}
	if unpublished != 2 {
		t.Errorf("unpublished events = %d, want creation and settlement while Kafka is down", unpublished)
	}
	t.Logf("Kafka unavailable: transfer settled via poller in %s, one provider submission, event retained in outbox", elapsed.Round(time.Millisecond))
}

func TestSenderProcess(t *testing.T) {
	if os.Getenv("PAYMENTS_SENDER_HELPER") != "1" {
		t.Skip("subprocess helper")
	}
	cfg, err := pgxpool.ParseConfig(testdb.DSN())
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = os.Getenv("PAYMENTS_TEST_SCHEMA")
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	client, err := kgo.NewClient(
		kgo.SeedBrokers(crashBrokers),
		kgo.ConsumerGroup(os.Getenv("PAYMENTS_TEST_GROUP")),
		kgo.ConsumeTopics(os.Getenv("PAYMENTS_TEST_TOPIC")),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
		kgo.SessionTimeout(6*time.Second),
		kgo.HeartbeatInterval(time.Second),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	w := worker.New(pool, provider.New(os.Getenv("PAYMENTS_TEST_PROVIDER"), provider.DefaultTimeout), worker.DefaultConfig())
	encoder := json.NewEncoder(os.Stdout)
	consumer.Run(ctx, client, func(ctx context.Context, record *kgo.Record) error {
		sent, err := w.SendByID(ctx, string(record.Key))
		if err != nil {
			return err
		}
		if err := encoder.Encode(delivery{ID: string(record.Key), EventID: eventHeader(record, "event_id"), Partition: record.Partition, Offset: record.Offset, Sent: sent}); err != nil {
			return err
		}
		if os.Getenv("PAYMENTS_TEST_PAUSE") == "true" {
			<-ctx.Done()
			return ctx.Err()
		}
		return nil
	})
}

func eventHeader(record *kgo.Record, name string) string {
	for _, header := range record.Headers {
		if header.Key == name {
			return string(header.Value)
		}
	}
	return ""
}

func startSenderProcess(t *testing.T, ctx context.Context, f *crashFixture, topic, group string, pause bool) (*exec.Cmd, <-chan string) {
	t.Helper()
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(ctx, binary, "-test.run=^TestSenderProcess$")
	cmd.Env = append(os.Environ(),
		"PAYMENTS_SENDER_HELPER=1", "PAYMENTS_TEST_SCHEMA="+f.schema,
		"PAYMENTS_TEST_TOPIC="+topic, "PAYMENTS_TEST_GROUP="+group,
		"PAYMENTS_TEST_PROVIDER="+f.rail.URL, fmt.Sprintf("PAYMENTS_TEST_PAUSE=%t", pause))
	cmd.Stderr = os.Stderr
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if cmd.ProcessState == nil {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	})
	lines := make(chan string, 16)
	go func() {
		defer close(lines)
		scan := bufio.NewScanner(stdout)
		for scan.Scan() {
			select {
			case lines <- scan.Text():
			case <-ctx.Done():
				return
			}
		}
	}()
	return cmd, lines
}

func readDelivery(t *testing.T, ctx context.Context, lines <-chan string) delivery {
	t.Helper()
	for {
		select {
		case line, ok := <-lines:
			if !ok {
				t.Fatal("sender exited before reporting delivery")
			}
			var got delivery
			if json.Unmarshal([]byte(line), &got) == nil && got.ID != "" {
				return got
			}
		case <-ctx.Done():
			t.Fatalf("waiting for sender delivery: %v", ctx.Err())
		}
	}
}

func assertCommittedOffset(t *testing.T, ctx context.Context, admin *kadm.Client, group, topic string, want int64) {
	t.Helper()
	offsets, err := admin.FetchOffsets(ctx, group)
	if err != nil {
		t.Fatal(err)
	}
	got := int64(-1)
	if offset, ok := offsets.Lookup(topic, 0); ok {
		if offset.Err != nil {
			t.Fatal(offset.Err)
		}
		got = offset.At
	}
	if got != want {
		t.Fatalf("committed offset = %d, want %d", got, want)
	}
}

func waitForCommit(t *testing.T, ctx context.Context, admin *kadm.Client, group, topic string, want int64) {
	t.Helper()
	for ctx.Err() == nil {
		offsets, err := admin.FetchOffsets(ctx, group)
		if err != nil {
			t.Fatal(err)
		}
		if offset, ok := offsets.Lookup(topic, 0); ok && offset.Err == nil && offset.At == want {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("offset did not reach %d: %v", want, ctx.Err())
}

func ledgerSnapshot(t *testing.T, ctx context.Context, pool *pgxpool.Pool, id string) string {
	t.Helper()
	var snapshot string
	if err := pool.QueryRow(ctx, `
		SELECT COALESCE(jsonb_agg(to_jsonb(e) ORDER BY e.id)::text, '[]')
		FROM ledger_entries e JOIN ledger_transactions tx ON tx.id = e.txn_id
		WHERE tx.reference IN ('transfer ' || $1, 'settlement ' || $1)`, id).Scan(&snapshot); err != nil {
		t.Fatal(err)
	}
	return snapshot
}

type crashFixture struct {
	pool        *pgxpool.Pool
	schema      string
	ids         []string
	rail        *httptest.Server
	handler     http.Handler
	mu          sync.Mutex
	submissions map[string]int
}

func newCrashFixture(t *testing.T, count int) *crashFixture {
	t.Helper()
	ctx := context.Background()
	f := &crashFixture{schema: fmt.Sprintf("test_sender_%d", time.Now().UnixNano()), submissions: map[string]int{}}
	var err error
	f.pool, err = testdb.Connect(ctx, f.schema)
	if err != nil {
		t.Fatalf("%s", testdb.ConnectionHint(err))
	}
	t.Cleanup(f.pool.Close)
	if _, err := f.pool.Exec(ctx, `INSERT INTO accounts (id, currency, type) VALUES ('acc_src', 'USD', 'asset'), ('acc_dst', 'USD', 'liability')`); err != nil {
		t.Fatal(err)
	}
	if _, err := ledger.Record(ctx, f.pool, "funding", []ledger.Entry{
		{AccountID: transfers.SettlementAccountID, Direction: ledger.DirectionDebit, Amount: int64(count) * 2000},
		{AccountID: "acc_src", Direction: ledger.DirectionCredit, Amount: int64(count) * 2000},
	}); err != nil {
		t.Fatal(err)
	}
	plaintext, key, err := auth.Generate("sender crash test")
	if err != nil {
		t.Fatal(err)
	}
	if err := auth.Insert(ctx, f.pool, key); err != nil {
		t.Fatal(err)
	}
	f.handler = api.NewServer(f.pool, []byte(crashWebhookSecret)).Handler()
	for i := 0; i < count; i++ {
		req := httptest.NewRequest("POST", "/v1/transfers", strings.NewReader(`{"source_account":"acc_src","destination_account":"acc_dst","amount":1000,"currency":"USD"}`))
		req.Header.Set("Authorization", "Bearer "+plaintext)
		req.Header.Set("Idempotency-Key", fmt.Sprintf("kill-%d", i))
		rec := httptest.NewRecorder()
		f.handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusAccepted {
			t.Fatalf("create transfer: %d %s", rec.Code, rec.Body.String())
		}
		var response struct{ ID string }
		if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil || response.ID == "" {
			t.Fatalf("create response: %s, %v", rec.Body.String(), err)
		}
		f.ids = append(f.ids, response.ID)
	}
	f.rail = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payment provider.SubmitRequest
		if r.Method != "POST" || r.URL.Path != "/transfers" || json.NewDecoder(r.Body).Decode(&payment) != nil {
			http.Error(w, "bad submission", http.StatusBadRequest)
			return
		}
		f.mu.Lock()
		f.submissions[payment.ClientReference]++
		f.mu.Unlock()
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(provider.Payment{ProviderRef: "mb_" + payment.ClientReference, ClientReference: payment.ClientReference, Status: provider.StatusProcessing, Amount: payment.Amount, Currency: payment.Currency})
	}))
	t.Cleanup(f.rail.Close)
	return f
}

func (f *crashFixture) submissionCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, count := range f.submissions {
		n += count
	}
	return n
}

func (f *crashFixture) settleAndAssertOnce(t *testing.T, ctx context.Context) {
	t.Helper()
	for _, id := range f.ids {
		payload := fmt.Sprintf(`{"event_id":"evt_%s","provider_ref":"mb_%s","client_reference":"%s","status":"settled","amount":1000,"currency":"USD"}`, id, id, id)
		req := httptest.NewRequest("POST", "/v1/webhooks/mockbank", strings.NewReader(payload))
		timestamp := strconv.FormatInt(time.Now().Unix(), 10)
		req.Header.Set(webhooks.TimestampHeader, timestamp)
		req.Header.Set(webhooks.SignatureHeader, webhooks.Sign([]byte(crashWebhookSecret), timestamp, []byte(payload)))
		rec := httptest.NewRecorder()
		f.handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("settle %s: %d %s", id, rec.Code, rec.Body.String())
		}
		stored, err := transfers.Get(ctx, f.pool, id)
		if err != nil || stored.Status != transfers.StatusSettled || stored.ProviderRef == nil {
			t.Errorf("transfer %s not settled with a reference: %+v, %v", id, stored, err)
		}
		f.mu.Lock()
		count := f.submissions[id]
		f.mu.Unlock()
		if count != 1 {
			t.Errorf("transfer %s submitted %d times, want 1", id, count)
		}
		var movements int
		if err := f.pool.QueryRow(ctx, `SELECT count(*) FROM ledger_transactions WHERE reference IN ('transfer ' || $1, 'settlement ' || $1)`, id).Scan(&movements); err != nil {
			t.Fatal(err)
		}
		if movements != 2 {
			t.Errorf("transfer %s has %d ledger movements, want creation and settlement only", id, movements)
		}
	}
	if got := f.submissionCount(); got != len(f.ids) {
		t.Errorf("provider submissions = %d, want %d", got, len(f.ids))
	}
	for _, account := range []string{"acc_src", "acc_dst"} {
		balance, err := ledger.Balance(ctx, f.pool, account)
		if err != nil || balance != int64(len(f.ids))*1000 {
			t.Errorf("%s balance = %d, want %d: %v", account, balance, len(f.ids)*1000, err)
		}
	}
	var unbalanced int
	if err := f.pool.QueryRow(ctx, `
		SELECT count(*) FROM (
			SELECT txn_id FROM ledger_entries GROUP BY txn_id
			HAVING SUM(CASE WHEN direction = 'credit' THEN amount ELSE -amount END) <> 0
		) AS bad`).Scan(&unbalanced); err != nil {
		t.Fatal(err)
	}
	if unbalanced != 0 {
		t.Errorf("unbalanced ledger transactions = %d, want 0", unbalanced)
	}
}
