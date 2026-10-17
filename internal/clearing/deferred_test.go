package clearing

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sync"
	"testing"
	"time"
)

// storeDeferred records lines as an import before refunds were posted left
// them: the file stored, every line deferred, no money moved.
func storeDeferred(tb testing.TB, lines ...Line) {
	tb.Helper()
	ctx := context.Background()
	raw := fileOf(lines...)
	hash := sha256.Sum256(raw)
	fileID := newID("clf")
	if _, err := testPool.Exec(ctx, `
		INSERT INTO clearing_files (id, file_date, sha256, raw, line_count) VALUES ($1, $2, $3, $4, $5)`,
		fileID, testDate, hex.EncodeToString(hash[:]), raw, len(lines)); err != nil {
		tb.Fatal(err)
	}
	for _, l := range lines {
		var authID *string
		if l.AuthID != "" {
			authID = &l.AuthID
		}
		if _, err := testPool.Exec(ctx, `
			INSERT INTO clearing_records (clearing_id, file_id, auth_id, card_id, kind, amount,
				currency, merchant_id, cleared_at, status)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
			l.ClearingID, fileID, authID, l.CardID, l.Kind, l.Amount, l.Currency, l.MerchantID, l.ClearedAt, StatusDeferred); err != nil {
			tb.Fatal(err)
		}
	}
}

// Four runs at once, then another: every deferred line is credited exactly
// once, matched, flagged or rejected as an import would have.
func TestPostDeferredIsIdempotent(t *testing.T) {
	resetDB(t)
	ctx := context.Background()
	const balance = 100_000
	card := newCard(t, balance)
	clearPurchase(t, card, "auth_old", 20_000)

	var lines []Line
	for i := range 20 {
		l := refund(fmt.Sprintf("clr_deferred_%02d", i), "auth_old", card.ID, 1000)
		l.ClearedAt = l.ClearedAt.Add(time.Duration(i) * time.Second)
		lines = append(lines, l)
	}
	offline := refund("clr_deferred_offline", "", card.ID, 500)
	ghost := refund("clr_deferred_ghost", "", "card_ghost", 700)
	storeDeferred(t, append(lines, offline, ghost)...)

	var wg sync.WaitGroup
	summaries := make([]DeferredSummary, 4)
	errs := make([]error, 4)
	start := make(chan struct{})
	for i := range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			summaries[i], errs[i] = PostDeferred(ctx, testPool, testConfig)
		}()
	}
	close(start)
	wg.Wait()
	var total DeferredSummary
	for i := range 4 {
		if errs[i] != nil {
			t.Fatalf("run %d: %v", i, errs[i])
		}
		total.Processed += summaries[i].Processed
		total.Credited += summaries[i].Credited
	}
	if total.Processed != 22 || total.Credited != 20_500 {
		t.Errorf("runs processed %d lines crediting %d, want 22 crediting 20500", total.Processed, total.Credited)
	}
	before := moneyState(t)
	again, err := PostDeferred(ctx, testPool, testConfig)
	if err != nil || again.Processed != 0 {
		t.Errorf("another run = %+v, %v, want nothing processed", again, err)
	}
	if moneyState(t) != before {
		t.Error("another run changed state")
	}

	if got := countWhere(t, `SELECT count(*) FROM clearing_records WHERE status = $1`, StatusDeferred); got != 0 {
		t.Errorf("deferred records left = %d, want 0", got)
	}
	if r := loadRecord(t, "clr_deferred_00"); r != (record{status: StatusPosted, credited: 1000, hasLedger: true}) {
		t.Errorf("matched record = %+v", r)
	}
	if r := loadRecord(t, offline.ClearingID); r != (record{status: StatusPosted, credited: 500, flag: FlagUnmatchedRefund, hasLedger: true}) {
		t.Errorf("unmatched record = %+v", r)
	}
	if r := loadRecord(t, ghost.ClearingID); r != (record{status: StatusRejected, flag: FlagUnknownCard}) {
		t.Errorf("unknown card record = %+v", r)
	}
	if got := countWhere(t, `SELECT count(*) FROM ledger_transactions WHERE reference LIKE 'refund %'`); got != 21 {
		t.Errorf("refund transactions = %d, want 21", got)
	}
	if got := countWhere(t, `SELECT count(*) FROM outbox_events WHERE event_type = $1`, EventRefundPosted); got != 21 {
		t.Errorf("refund events = %d, want 21", got)
	}
	if got := refunded(t, "auth_old"); got != 20_000 {
		t.Errorf("refunded = %d, want 20000", got)
	}
	if got := storedBalance(t, card.AccountID); got != balance-20_000+20_500 {
		t.Errorf("available = %d, want %d", got, balance-20_000+20_500)
	}
	assertNoDrift(t)
}
