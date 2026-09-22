package worker

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Shailu-s/payments-platform/internal/ledger"
	"github.com/Shailu-s/payments-platform/internal/provider"
)

// seedFunded creates n transfers with their money already moved to settlement,
// exactly as the API leaves them.
func seedFunded(t *testing.T, n int, amount int64) []string {
	t.Helper()
	ctx := context.Background()
	resetDB(t)

	if _, err := testPool.Exec(ctx, `
		INSERT INTO accounts (id, currency, type)
		VALUES ('acc_src', 'USD', 'asset'), ('acc_dst', 'USD', 'liability')
		ON CONFLICT (id) DO NOTHING`); err != nil {
		t.Fatalf("accounts: %v", err)
	}
	if _, err := testPool.Exec(ctx,
		`INSERT INTO api_keys (id, key_hash, prefix, name) VALUES ('ak_w','h','pk_live_w','w')`); err != nil {
		t.Fatalf("api key: %v", err)
	}
	if _, err := ledger.Record(ctx, testPool, "funding", []ledger.Entry{
		{AccountID: SettlementAccountID, Direction: ledger.DirectionDebit, Amount: amount * int64(n) * 2},
		{AccountID: "acc_src", Direction: ledger.DirectionCredit, Amount: amount * int64(n) * 2},
	}); err != nil {
		t.Fatalf("fund: %v", err)
	}

	ids := make([]string, n)
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("tr_e2e_%03d", i)
		ids[i] = id

		txnID, err := ledger.Record(ctx, testPool, "transfer "+id, []ledger.Entry{
			{AccountID: "acc_src", Direction: ledger.DirectionDebit, Amount: amount},
			{AccountID: SettlementAccountID, Direction: ledger.DirectionCredit, Amount: amount},
		})
		if err != nil {
			t.Fatalf("ledger for %s: %v", id, err)
		}
		if _, err := testPool.Exec(ctx, `
			INSERT INTO transfers (id, source_account, destination_account, amount,
				currency, status, api_key_id, idempotency_key, ledger_txn_id)
			VALUES ($1, 'acc_src', 'acc_dst', $2, 'USD', 'processing', 'ak_w', $3, $4)`,
			id, amount, "k_"+id, txnID); err != nil {
			t.Fatalf("transfer %s: %v", id, err)
		}
	}
	return ids
}

// countingProvider records how many times each transfer was submitted, so a
// test can prove none was sent twice.
type countingProvider struct {
	mu        sync.Mutex
	perRef    map[string]int
	delay     time.Duration
	submitted atomic.Int64
}

func newCountingProvider(delay time.Duration) *countingProvider {
	return &countingProvider{perRef: map[string]int{}, delay: delay}
}

func (c *countingProvider) Submit(ctx context.Context, req provider.SubmitRequest) (provider.Payment, error) {
	if c.delay > 0 {
		select {
		case <-ctx.Done():
			return provider.Payment{}, fmt.Errorf("%w: %v", provider.ErrUnknown, ctx.Err())
		case <-time.After(c.delay):
		}
	}

	c.mu.Lock()
	c.perRef[req.ClientReference]++
	c.mu.Unlock()
	c.submitted.Add(1)

	return provider.Payment{
		ProviderRef:     "mb_" + req.ClientReference,
		ClientReference: req.ClientReference,
		Status:          provider.StatusProcessing,
		Amount:          req.Amount,
		Currency:        req.Currency,
	}, nil
}

func (c *countingProvider) Get(ctx context.Context, ref string) (provider.Payment, error) {
	return provider.Payment{}, provider.ErrNotFound
}

func (c *countingProvider) GetByClientReference(ctx context.Context, ref string) (provider.Payment, error) {
	return provider.Payment{}, provider.ErrNotFound
}

func (c *countingProvider) duplicates() []string {
	c.mu.Lock()
	defer c.mu.Unlock()

	var out []string
	for ref, count := range c.perRef {
		if count > 1 {
			out = append(out, fmt.Sprintf("%s sent %d times", ref, count))
		}
	}
	return out
}

// ⭐ Two workers, 50 transfers, each sent to the provider exactly once.
//
// This is the claim test carried through to its consequence: SKIP LOCKED stops
// two workers claiming one row, and what that buys is that the rail is never
// told to pay the same vendor twice.
func TestTwoWorkersSendEachTransferOnce(t *testing.T) {
	const total = 50
	ids := seedFunded(t, total, 1000)
	ctx := context.Background()

	// A delay inside the send, so the two workers genuinely overlap rather
	// than finishing one batch before the other starts.
	rail := newCountingProvider(2 * time.Millisecond)

	cfg := DefaultConfig()
	cfg.BatchSize = 5
	cfg.RetryBackoff = time.Hour

	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				sent, err := New(testPool, rail, cfg).RunOnce(ctx)
				if err != nil {
					t.Errorf("RunOnce: %v", err)
					return
				}
				if sent == 0 {
					return
				}
			}
		}()
	}
	wg.Wait()

	if dupes := rail.duplicates(); len(dupes) > 0 {
		t.Errorf("transfers sent to the provider more than once: %v", dupes)
	}
	if got := rail.submitted.Load(); got != total {
		t.Errorf("%d submissions for %d transfers, want %d", got, total, total)
	}

	// Every transfer now carries a provider reference.
	var withoutRef int
	if err := testPool.QueryRow(ctx,
		`SELECT count(*) FROM transfers WHERE provider_ref IS NULL`).Scan(&withoutRef); err != nil {
		t.Fatalf("count: %v", err)
	}
	if withoutRef != 0 {
		t.Errorf("%d transfers were never sent", withoutRef)
	}

	// And settling them all credits the destination exactly once each.
	for _, id := range ids {
		if err := Settle(ctx, testPool, id); err != nil {
			t.Fatalf("Settle %s: %v", id, err)
		}
	}
	if got := balanceOf(t, "acc_dst"); got != total*1000 {
		t.Errorf("destination balance = %d, want %d", got, total*1000)
	}
}

// ⭐ A worker killed mid-run loses nothing and double-sends nothing.
//
// The first worker is cancelled while its batch is in flight. Its claimed
// transfers have had next_attempt_at pushed forward, so they are not
// immediately reclaimable — which is deliberate: a crashed worker costs one
// backoff rather than a tight retry loop against the rail.
func TestWorkerRestartLosesNothingAndDoubleSendsNothing(t *testing.T) {
	const total = 30
	seedFunded(t, total, 1000)
	ctx := context.Background()

	// Slow enough that cancelling lands mid-batch.
	rail := newCountingProvider(20 * time.Millisecond)

	cfg := DefaultConfig()
	cfg.BatchSize = 5
	// Short, so the second worker can pick up whatever the first abandoned.
	cfg.RetryBackoff = 200 * time.Millisecond
	cfg.PollInterval = 10 * time.Millisecond

	// First worker: killed after a moment.
	firstCtx, killFirst := context.WithCancel(ctx)
	firstDone := make(chan struct{})
	go func() {
		defer close(firstDone)
		New(testPool, rail, cfg).Run(firstCtx)
	}()

	time.Sleep(150 * time.Millisecond)
	killFirst()

	select {
	case <-firstDone:
	case <-time.After(5 * time.Second):
		t.Fatal("the first worker did not stop when cancelled")
	}

	sentBeforeRestart := rail.submitted.Load()
	if sentBeforeRestart == 0 {
		t.Fatal("the first worker sent nothing, so the restart proves nothing")
	}
	t.Logf("first worker sent %d of %d before being killed", sentBeforeRestart, total)

	// Second worker: finishes the job.
	secondCtx, stopSecond := context.WithCancel(ctx)
	secondDone := make(chan struct{})
	go func() {
		defer close(secondDone)
		New(testPool, rail, cfg).Run(secondCtx)
	}()

	deadline := time.Now().Add(15 * time.Second)
	for {
		var remaining int
		if err := testPool.QueryRow(ctx,
			`SELECT count(*) FROM transfers WHERE provider_ref IS NULL`).Scan(&remaining); err != nil {
			t.Fatalf("count: %v", err)
		}
		if remaining == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d transfers were never sent after the restart", remaining)
		}
		time.Sleep(50 * time.Millisecond)
	}

	stopSecond()
	select {
	case <-secondDone:
	case <-time.After(5 * time.Second):
		t.Fatal("the second worker did not stop when cancelled")
	}

	// Nothing lost: every transfer reached the rail.
	var withoutRef int
	if err := testPool.QueryRow(ctx,
		`SELECT count(*) FROM transfers WHERE provider_ref IS NULL`).Scan(&withoutRef); err != nil {
		t.Fatalf("count: %v", err)
	}
	if withoutRef != 0 {
		t.Errorf("%d transfers lost across the restart", withoutRef)
	}

	// Nothing double-sent: the interrupted batch was not re-sent by the second
	// worker on top of what the first already delivered.
	if dupes := rail.duplicates(); len(dupes) > 0 {
		t.Errorf("transfers sent twice across the restart: %v", dupes)
	}
}

// A worker must stop when asked, or every later test is noisier and phase 5's
// deliberate kill proves nothing.
func TestWorkerStopsWhenCancelled(t *testing.T) {
	seedFunded(t, 3, 1000)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		New(testPool, newCountingProvider(0), DefaultConfig()).Run(ctx)
	}()

	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the worker did not stop within 5s of being cancelled")
	}
}
