package worker

import (
	"context"
	"github.com/Shailu-s/payments-platform/internal/transfers"
	"testing"
	"time"

	"github.com/Shailu-s/payments-platform/internal/ledger"
	"github.com/Shailu-s/payments-platform/internal/provider"
)

// seedOne creates a funded source and a single transfer waiting to be sent,
// with the money already debited to settlement exactly as the API leaves it.
func seedOne(t *testing.T, amount int64) string {
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

	// Fund the source, then move the money to settlement as creating the
	// transfer would.
	if _, err := ledger.Record(ctx, testPool, "funding", []ledger.Entry{
		{AccountID: transfers.SettlementAccountID, Direction: ledger.DirectionDebit, Amount: amount * 10},
		{AccountID: "acc_src", Direction: ledger.DirectionCredit, Amount: amount * 10},
	}); err != nil {
		t.Fatalf("fund: %v", err)
	}

	const id = "tr_outcome"
	txnID, err := ledger.Record(ctx, testPool, "transfer "+id, []ledger.Entry{
		{AccountID: "acc_src", Direction: ledger.DirectionDebit, Amount: amount},
		{AccountID: transfers.SettlementAccountID, Direction: ledger.DirectionCredit, Amount: amount},
	})
	if err != nil {
		t.Fatalf("transfer ledger: %v", err)
	}

	if _, err := testPool.Exec(ctx, `
		INSERT INTO transfers (id, source_account, destination_account, amount,
			currency, status, api_key_id, idempotency_key, ledger_txn_id)
		VALUES ($1, 'acc_src', 'acc_dst', $2, 'USD', 'processing', 'ak_w', 'k_outcome', $3)`,
		id, amount, txnID); err != nil {
		t.Fatalf("transfer: %v", err)
	}
	return id
}

func statusOf(t *testing.T, id string) string {
	t.Helper()
	var status string
	if err := testPool.QueryRow(context.Background(),
		`SELECT status FROM transfers WHERE id = $1`, id).Scan(&status); err != nil {
		t.Fatalf("read status: %v", err)
	}
	return status
}

func balanceOf(t *testing.T, accountID string) int64 {
	t.Helper()
	b, err := ledger.Balance(context.Background(), testPool, accountID)
	if err != nil {
		t.Fatalf("balance %s: %v", accountID, err)
	}
	return b
}

// Outcome 1: accepted. The reference is stored, the transfer stays processing,
// and the destination has NOT been credited — the money is still ours.
func TestAcceptedTransferStoresReferenceAndWaits(t *testing.T) {
	id := seedOne(t, 50000)
	ctx := context.Background()
	w := New(testPool, accepts(), DefaultConfig())

	if _, err := w.RunOnce(ctx); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}

	if got := statusOf(t, id); got != "processing" {
		t.Errorf("status = %q, want processing: acceptance is not settlement", got)
	}

	var ref *string
	if err := testPool.QueryRow(ctx,
		`SELECT provider_ref FROM transfers WHERE id = $1`, id).Scan(&ref); err != nil {
		t.Fatalf("read provider_ref: %v", err)
	}
	if ref == nil || *ref == "" {
		t.Fatal("no provider_ref stored, so a webhook could not be matched to this transfer")
	}

	if got := balanceOf(t, "acc_dst"); got != 0 {
		t.Errorf("destination balance = %d, want 0: the money has not arrived yet", got)
	}
	if got := balanceOf(t, transfers.SettlementAccountID); got != -450000 {
		t.Errorf("settlement balance = %d, want -450000", got)
	}
}

// Outcome 2: rejected. Terminal, and the money goes back with a NEW ledger
// transaction rather than an edit.
func TestRejectedTransferIsReversed(t *testing.T) {
	id := seedOne(t, 50000)
	ctx := context.Background()

	before := balanceOf(t, "acc_src")

	w := New(testPool, rejects("insufficient funds at the rail"), DefaultConfig())
	if _, err := w.RunOnce(ctx); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}

	if got := statusOf(t, id); got != "failed" {
		t.Fatalf("status = %q, want failed", got)
	}

	// The money is back where it started.
	if got := balanceOf(t, "acc_src"); got != before+50000 {
		t.Errorf("source balance = %d, want %d: the money was not returned", got, before+50000)
	}

	// Two ledger transactions for one transfer, and the first is untouched.
	var movements int
	if err := testPool.QueryRow(ctx,
		`SELECT count(*) FROM ledger_transactions WHERE reference IN ($1, $2)`,
		"transfer "+id, "reversal "+id).Scan(&movements); err != nil {
		t.Fatalf("count movements: %v", err)
	}
	if movements != 2 {
		t.Errorf("%d ledger transactions for this transfer, want 2: the original "+
			"movement and its reversal", movements)
	}

	// Guarantee 6: the original entries still say what they always said.
	var originalDebit int64
	if err := testPool.QueryRow(ctx, `
		SELECT amount FROM ledger_entries e
		JOIN ledger_transactions t ON t.id = e.txn_id
		WHERE t.reference = $1 AND e.account_id = 'acc_src' AND e.direction = 'debit'`,
		"transfer "+id).Scan(&originalDebit); err != nil {
		t.Fatalf("read original entry: %v", err)
	}
	if originalDebit != 50000 {
		t.Errorf("the original debit is now %d, want 50000: entries must never be modified", originalDebit)
	}
}

// ⭐ Outcome 3: the outcome is unknown. The hardest case in the project.
//
// Not retried, because the rail may already have moved the money. Not failed,
// because it may have succeeded. Parked, and left alone.
func TestTimedOutTransferIsParkedNotRetriedNotFailed(t *testing.T) {
	id := seedOne(t, 50000)
	ctx := context.Background()

	fake := timesOut()
	w := New(testPool, fake, DefaultConfig())
	if _, err := w.RunOnce(ctx); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}

	if got := statusOf(t, id); got != "unresolved" {
		t.Fatalf("status = %q, want unresolved: a timeout is neither success nor failure", got)
	}

	// Not failed: the money stays in settlement, because reversing it would be
	// a guess that the payment did not happen.
	if got := balanceOf(t, "acc_src"); got != 450000 {
		t.Errorf("source balance = %d, want 450000: the money must not be returned "+
			"on a timeout, because the payment may have succeeded", got)
	}

	// Not retried: further passes must leave it alone. A second submission is
	// how a timeout becomes a double payment.
	for i := 0; i < 3; i++ {
		if _, err := w.RunOnce(ctx); err != nil {
			t.Fatalf("RunOnce %d: %v", i, err)
		}
	}
	if got := fake.submissionsFor(id); got != 1 {
		t.Errorf("the transfer was submitted %d times, want 1: retrying an unknown "+
			"outcome is how a timeout becomes a double payment", got)
	}
	if got := statusOf(t, id); got != "unresolved" {
		t.Errorf("status = %q after further passes, want unresolved", got)
	}
}

// A retryable failure is NOT the same as an unknown one: the rail refused to
// look at the request, so nothing happened and sending it again is safe.
func TestUnavailableProviderIsRetriedNotParked(t *testing.T) {
	id := seedOne(t, 50000)
	ctx := context.Background()

	cfg := DefaultConfig()
	cfg.RetryBackoff = 50 * time.Millisecond

	fake := unavailable()
	w := New(testPool, fake, cfg)
	if _, err := w.RunOnce(ctx); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}

	if got := statusOf(t, id); got != "processing" {
		t.Errorf("status = %q, want processing: a 503 means nothing happened", got)
	}

	time.Sleep(100 * time.Millisecond)
	if _, err := w.RunOnce(ctx); err != nil {
		t.Fatalf("second RunOnce: %v", err)
	}
	if got := fake.submissionsFor(id); got != 2 {
		t.Errorf("submitted %d times, want 2: a retryable failure must be retried", got)
	}
}

// The other half of parking: something must come back for it. A later lookup
// against the rail discovers the payment did settle.
func TestUnresolvedTransferIsRescuedByALookup(t *testing.T) {
	id := seedOne(t, 50000)
	ctx := context.Background()

	fake := timesOut()
	w := New(testPool, fake, DefaultConfig())
	if _, err := w.RunOnce(ctx); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if got := statusOf(t, id); got != "unresolved" {
		t.Fatalf("status = %q, want unresolved", got)
	}

	// The rail can now be asked, and says the payment went through.
	fake.mu.Lock()
	fake.lookup = func(ref string) (provider.Payment, error) {
		return provider.Payment{
			ProviderRef:     "mb_recovered",
			ClientReference: id,
			Status:          provider.StatusSettled,
			Amount:          50000,
			Currency:        "USD",
		}, nil
	}
	fake.mu.Unlock()

	w.resolveParked(ctx)

	if got := statusOf(t, id); got != "settled" {
		t.Fatalf("status = %q, want settled: the lookup found the payment", got)
	}
	if got := balanceOf(t, "acc_dst"); got != 50000 {
		t.Errorf("destination balance = %d, want 50000", got)
	}
}

// And if the rail never received it, the transfer goes back in the queue rather
// than staying parked forever.
func TestUnresolvedTransferTheProviderNeverSawIsRequeued(t *testing.T) {
	id := seedOne(t, 50000)
	ctx := context.Background()

	fake := timesOut()
	w := New(testPool, fake, DefaultConfig())
	if _, err := w.RunOnce(ctx); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}

	// lookup returns ErrNotFound by default: the rail has no record.
	w.resolveParked(ctx)

	if got := statusOf(t, id); got != "processing" {
		t.Errorf("status = %q, want processing: the rail never saw it, so sending "+
			"it again is safe", got)
	}
}

// A lookup that says the payment failed returns the money.
func TestUnresolvedTransferThatFailedIsReversed(t *testing.T) {
	id := seedOne(t, 50000)
	ctx := context.Background()

	fake := timesOut()
	w := New(testPool, fake, DefaultConfig())
	if _, err := w.RunOnce(ctx); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}

	reason := "blocked account"
	fake.mu.Lock()
	fake.lookup = func(ref string) (provider.Payment, error) {
		return provider.Payment{
			ProviderRef:   "mb_failed",
			Status:        provider.StatusFailed,
			Amount:        50000,
			FailureReason: &reason,
		}, nil
	}
	fake.mu.Unlock()

	w.resolveParked(ctx)

	if got := statusOf(t, id); got != "failed" {
		t.Fatalf("status = %q, want failed", got)
	}
	if got := balanceOf(t, "acc_src"); got != 500000 {
		t.Errorf("source balance = %d, want 500000: the money should be back", got)
	}
}

// ⭐ Guarantee 4: the same settlement applied twice has one financial effect.
func TestSettlingTwiceCreditsTheDestinationOnce(t *testing.T) {
	id := seedOne(t, 50000)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		if err := transfers.Settle(ctx, testPool, id); err != nil {
			t.Fatalf("Settle %d: %v", i, err)
		}
	}

	if got := balanceOf(t, "acc_dst"); got != 50000 {
		t.Errorf("destination balance = %d, want 50000: five settlements moved the "+
			"money %d times", got, got/50000)
	}

	var settlements int
	if err := testPool.QueryRow(ctx,
		`SELECT count(*) FROM ledger_transactions WHERE reference = $1`,
		"settlement "+id).Scan(&settlements); err != nil {
		t.Fatalf("count settlements: %v", err)
	}
	if settlements != 1 {
		t.Errorf("%d settlement ledger transactions, want 1", settlements)
	}
}

// The full accounting story of a successful transfer: two ledger transactions,
// the money ending where it should, and everything still balancing.
func TestSettlementMovesMoneyTheRestOfTheWay(t *testing.T) {
	id := seedOne(t, 50000)
	ctx := context.Background()
	w := New(testPool, accepts(), DefaultConfig())

	if _, err := w.RunOnce(ctx); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if err := transfers.Settle(ctx, testPool, id); err != nil {
		t.Fatalf("Settle: %v", err)
	}

	if got := statusOf(t, id); got != "settled" {
		t.Fatalf("status = %q, want settled", got)
	}
	if got := balanceOf(t, "acc_src"); got != 450000 {
		t.Errorf("source = %d, want 450000", got)
	}
	if got := balanceOf(t, "acc_dst"); got != 50000 {
		t.Errorf("destination = %d, want 50000", got)
	}
	if got := balanceOf(t, transfers.SettlementAccountID); got != -500000 {
		t.Errorf("settlement = %d, want -500000: it should be holding nothing "+
			"for this transfer any more", got)
	}

	// Guarantee 1 still holds across the whole thing.
	var total int64
	if err := testPool.QueryRow(ctx, `
		SELECT COALESCE(SUM(CASE WHEN direction = 'credit' THEN amount ELSE -amount END), 0)
		FROM ledger_entries`).Scan(&total); err != nil {
		t.Fatalf("sum: %v", err)
	}
	if total != 0 {
		t.Errorf("all entries sum to %d, want 0", total)
	}
}
