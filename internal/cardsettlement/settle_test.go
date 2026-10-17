package cardsettlement

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/Shailu-s/payments-platform/internal/clearing"
)

const settlementRef = "network settlement 2026-10-18"

// Twenty operators settle the same day at once: one row, one ledger
// transaction, and every caller gets that row back. The payable we built up
// from the day's lines goes to zero; our cash pays the net.
func TestSettleRecordsTheDayOnce(t *testing.T) {
	resetDB(t)
	ctx := context.Background()
	card := newCard(t, 100_000)
	importLines(t, testDate,
		line(clearing.KindPurchase, card.ID, 3000),
		line(clearing.KindPurchase, card.ID, 2000),
		line(clearing.KindRefund, card.ID, 500),
		// Rejected, so never posted: not ours to pay, and left to reconciliation.
		line(clearing.KindPurchase, "card_unknown", 900),
	)

	const callers = 20
	var wg sync.WaitGroup
	results := make([]Settlement, callers)
	created := make([]bool, callers)
	errs := make([]error, callers)
	for i := range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[i], created[i], errs[i] = Settle(ctx, testPool, testDate)
		}()
	}
	wg.Wait()

	first := 0
	for i := range callers {
		if errs[i] != nil {
			t.Fatalf("caller %d: %v", i, errs[i])
		}
		if created[i] {
			first++
		}
		if results[i].NetAmount != 4500 || results[i].LedgerTxnID == nil || *results[i].LedgerTxnID != *results[0].LedgerTxnID {
			t.Errorf("caller %d got %+v", i, results[i])
		}
	}
	s := results[0]
	if first != 1 {
		t.Errorf("%d callers created the settlement, want 1", first)
	}
	if s.Date != "2026-10-18" || s.PurchasesTotal != 5000 || s.RefundsTotal != 500 || s.RecordCount != 3 {
		t.Errorf("settlement = %+v", s)
	}
	if n := countWhere(t, `SELECT count(*) FROM network_settlements`); n != 1 {
		t.Errorf("%d settlement rows, want 1", n)
	}
	if n := countWhere(t, `SELECT count(*) FROM ledger_transactions WHERE reference = $1`, settlementRef); n != 1 {
		t.Errorf("%d settlement ledger transactions, want 1", n)
	}
	if got := balance(t, clearing.NetworkPayableAccountID); got != 0 {
		t.Errorf("payable = %d after paying the day, want 0", got)
	}
	// Balances are credits minus debits: the cash leg is credited with what we paid out.
	if got := balance(t, NetworkCashAccountID); got != 4500 {
		t.Errorf("network cash = %d, want 4500 paid", got)
	}

	again, ok, err := Settle(ctx, testPool, testDate)
	if err != nil || ok || again.NetAmount != 4500 || !again.CreatedAt.Equal(s.CreatedAt) {
		t.Errorf("rerun = %+v, created %v, %v", again, ok, err)
	}
	if n := countWhere(t, `SELECT count(*) FROM ledger_transactions WHERE reference = $1`, settlementRef); n != 1 {
		t.Errorf("rerun wrote a ledger transaction: %d", n)
	}
	if got, err := Get(ctx, testPool, testDate); err != nil || got.NetAmount != 4500 {
		t.Errorf("Get = %+v, %v", got, err)
	}
}

// A refund-heavy day: the network owes us, and the ledger runs the other way.
func TestSettleNetNegative(t *testing.T) {
	resetDB(t)
	ctx := context.Background()
	card := newCard(t, 10_000)
	importLines(t, testDate, line(clearing.KindRefund, card.ID, 700), line(clearing.KindPurchase, card.ID, 200))

	s, created, err := Settle(ctx, testPool, testDate)
	if err != nil || !created || s.NetAmount != -500 || s.LedgerTxnID == nil {
		t.Fatalf("settle = %+v, %v, %v", s, created, err)
	}
	if got := balance(t, clearing.NetworkPayableAccountID); got != 0 {
		t.Errorf("payable = %d, want 0", got)
	}
	if got := balance(t, NetworkCashAccountID); got != -500 {
		t.Errorf("network cash = %d, want -500 received", got)
	}
	var debit string
	if err := testPool.QueryRow(ctx, `SELECT account_id FROM ledger_entries WHERE txn_id = $1 AND direction = 'debit'`,
		*s.LedgerTxnID).Scan(&debit); err != nil || debit != NetworkCashAccountID {
		t.Errorf("debited %q, %v; want the network cash account", debit, err)
	}
}

// Purchases and refunds cancel out: the day is recorded, nothing moves.
func TestSettleNetZero(t *testing.T) {
	resetDB(t)
	ctx := context.Background()
	card := newCard(t, 10_000)
	importLines(t, testDate, line(clearing.KindPurchase, card.ID, 1000), line(clearing.KindRefund, card.ID, 1000))
	before := countWhere(t, `SELECT count(*) FROM ledger_transactions`)

	s, created, err := Settle(ctx, testPool, testDate)
	if err != nil || !created || s.NetAmount != 0 || s.LedgerTxnID != nil || s.PurchasesTotal != 1000 || s.RefundsTotal != 1000 {
		t.Fatalf("settle = %+v, %v, %v", s, created, err)
	}
	if after := countWhere(t, `SELECT count(*) FROM ledger_transactions`); after != before {
		t.Errorf("a zero net wrote %d ledger transactions", after-before)
	}
}

// A day with no file, or with lines not yet posted, is refused rather than
// locked in at a wrong total: the row, once written, is never rewritten.
func TestSettleRefusesAnIncompleteDay(t *testing.T) {
	resetDB(t)
	ctx := context.Background()
	if _, _, err := Settle(ctx, testPool, testDate); !errors.Is(err, ErrNoClearingFile) {
		t.Fatalf("no file: want ErrNoClearingFile, got %v", err)
	}
	card := newCard(t, 10_000)
	importLines(t, testDate, line(clearing.KindPurchase, card.ID, 1000))
	if _, err := testPool.Exec(ctx, `
		INSERT INTO clearing_records (clearing_id, file_id, card_id, kind, amount, currency, merchant_id, cleared_at, status)
		SELECT 'clr_deferred', id, $1, 'refund', 100, 'USD', 'mer_test', '2026-10-18T10:00:00Z', 'deferred'
		FROM clearing_files LIMIT 1`, card.ID); err != nil {
		t.Fatal(err)
	}
	if _, _, err := Settle(ctx, testPool, testDate); !errors.Is(err, ErrDeferredLines) {
		t.Fatalf("deferred line: want ErrDeferredLines, got %v", err)
	}
	if n := countWhere(t, `SELECT count(*) FROM network_settlements`); n != 0 {
		t.Errorf("%d settlements recorded for refused days", n)
	}
}
