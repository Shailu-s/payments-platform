package clearing

import (
	"context"
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/Shailu-s/payments-platform/internal/holds"
)

// Every way a matched line can differ from its hold: the ledger, the balances,
// the authorization, the daily counter and the record must all agree.
func TestClearingExactPartialAndTip(t *testing.T) {
	cases := []struct {
		name                              string
		held, cleared                     int64
		fromHeld, fromAvailable, released int64
		flag                              string
	}{
		{name: "exact", held: 5000, cleared: 5000, fromHeld: 5000},
		{name: "partial", held: 10_000, cleared: 7000, fromHeld: 7000, released: 3000},
		{name: "tip", held: 5000, cleared: 6000, fromHeld: 5000, fromAvailable: 1000},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resetDB(t)
			const balance = 20_000
			card := newCard(t, balance)
			placeHold(t, card, "auth_"+tc.name, tc.held)

			importLines(t, purchase("clr_"+tc.name, "auth_"+tc.name, card.ID, tc.cleared))

			r := loadRecord(t, "clr_"+tc.name)
			want := record{status: StatusPosted, fromHeld: tc.fromHeld, fromAvailable: tc.fromAvailable,
				released: tc.released, flag: tc.flag, hasLedger: true}
			if r != want {
				t.Errorf("record = %+v, want %+v", r, want)
			}
			if got := storedBalance(t, card.HeldAccountID); got != 0 {
				t.Errorf("held = %d, want 0", got)
			}
			if got := storedBalance(t, card.AccountID); got != balance-tc.cleared {
				t.Errorf("available = %d, want %d", got, balance-tc.cleared)
			}
			if got := payable(t); got != tc.cleared {
				t.Errorf("payable = %d, want %d", got, tc.cleared)
			}
			if a := loadAuth(t, "auth_"+tc.name); a != (authState{held: 0, released: tc.released, cleared: tc.cleared, clearedAt: true}) {
				t.Errorf("authorization = %+v", a)
			}
			if got := dailySpend(t, card.ID); got != tc.held-tc.released {
				t.Errorf("daily spend = %d, want %d: a partial clear frees the rest", got, tc.held-tc.released)
			}
			// One balanced ledger transaction for the whole line.
			if got := countWhere(t, `SELECT count(*) FROM ledger_transactions WHERE reference = 'clearing ' || $1`, "clr_"+tc.name); got != 1 {
				t.Errorf("ledger transactions = %d, want 1", got)
			}
			if got := countWhere(t, `SELECT count(*) FROM outbox_events WHERE event_type = $1 AND aggregate_id = $2`, EventPosted, "auth_"+tc.name); got != 1 {
				t.Errorf("posted events = %d, want 1", got)
			}
			assertNoDrift(t)
		})
	}
}

func TestOverToleranceIsPostedAndFlagged(t *testing.T) {
	resetDB(t)
	card := newCard(t, 20_000)
	placeHold(t, card, "auth_within", 5000)
	placeHold(t, card, "auth_over", 5000)

	// 20 % of 5000 is 1000: 6000 is at the limit, 6001 is over it.
	importLines(t,
		purchase("clr_within", "auth_within", card.ID, 6000),
		purchase("clr_over", "auth_over", card.ID, 6001))

	if r := loadRecord(t, "clr_within"); r.flag != "" || r.status != StatusPosted {
		t.Errorf("within tolerance = %+v, want posted unflagged", r)
	}
	r := loadRecord(t, "clr_over")
	if r.status != StatusPosted || r.flag != FlagOverTolerance || r.fromHeld != 5000 || r.fromAvailable != 1001 {
		t.Errorf("over tolerance = %+v, want posted 5000 + 1001 and flagged", r)
	}
	if got := payable(t); got != 12_001 {
		t.Errorf("payable = %d, want 12001", got)
	}
	assertNoDrift(t)
}

func TestExceedsCannotOverflow(t *testing.T) {
	if !exceeds(math.MaxInt64, 1, 20) || exceeds(1, math.MaxInt64, 20) || exceeds(math.MaxInt64, math.MaxInt64, 100) {
		t.Error("tolerance comparison overflowed")
	}
}

func TestOfflinePurchaseIsChargedAndFlagged(t *testing.T) {
	resetDB(t)
	card := newCard(t, 10_000)
	importLines(t,
		purchase("clr_offline", "", card.ID, 1200),
		purchase("clr_unknown_auth", "auth_never_seen", card.ID, 800))

	for _, id := range []string{"clr_offline", "clr_unknown_auth"} {
		r := loadRecord(t, id)
		if r.status != StatusPosted || r.flag != FlagUnauthorizedClearing || r.fromHeld != 0 || !r.hasLedger {
			t.Errorf("%s = %+v, want posted from available and flagged", id, r)
		}
	}
	if got := storedBalance(t, card.AccountID); got != 8000 {
		t.Errorf("available = %d, want 8000", got)
	}
	if got := payable(t); got != 2000 {
		t.Errorf("payable = %d, want 2000", got)
	}
	assertNoDrift(t)
}

func TestUnknownCardIsRejectedWithoutMoney(t *testing.T) {
	resetDB(t)
	s := importLines(t, purchase("clr_ghost", "", "card_ghost", 1200))
	if s.Statuses[StatusRejected] != 1 || s.Flags[FlagUnknownCard] != 1 {
		t.Errorf("summary = %+v, want 1 rejected unknown_card", s)
	}
	if r := loadRecord(t, "clr_ghost"); r.status != StatusRejected || r.flag != FlagUnknownCard || r.hasLedger {
		t.Errorf("record = %+v, want rejected without a ledger transaction", r)
	}
	if got := countWhere(t, `SELECT count(*) FROM ledger_transactions`); got != 0 {
		t.Errorf("ledger transactions = %d, want 0", got)
	}
	if got := countWhere(t, `SELECT count(*) FROM outbox_events`); got != 0 {
		t.Errorf("events = %d, want 0", got)
	}
}

func TestClearingAfterExpiryOrReversalChargesAvailable(t *testing.T) {
	resetDB(t)
	ctx := context.Background()
	card := newCard(t, 10_000)
	placeHold(t, card, "auth_expired", 2000)
	placeHold(t, card, "auth_reversed", 3000)
	release(t, "auth_expired", math.MaxInt64, holds.ReasonExpiry)
	release(t, "auth_reversed", 3000, holds.ReasonReversal)
	if got := storedBalance(t, card.AccountID); got != 10_000 {
		t.Fatalf("available after releases = %d, want 10000", got)
	}

	importLines(t,
		purchase("clr_expired", "auth_expired", card.ID, 2000),
		purchase("clr_reversed", "auth_reversed", card.ID, 3000))

	for id, flag := range map[string]string{"clr_expired": FlagClearedAfterExpiry, "clr_reversed": FlagClearedAfterReversal} {
		if r := loadRecord(t, id); r.status != StatusPosted || r.flag != flag || r.fromHeld != 0 || r.released != 0 {
			t.Errorf("%s = %+v, want posted from available, flagged %s", id, r, flag)
		}
	}
	if got := storedBalance(t, card.AccountID); got != 5000 {
		t.Errorf("available = %d, want 5000", got)
	}
	if got := storedBalance(t, card.HeldAccountID); got != 0 {
		t.Errorf("held = %d, want 0", got)
	}
	// Cleared now: the expirer and a late reversal find nothing to release.
	if n, err := holds.ExpireBatch(ctx, testPool, time.Now().Add(30*24*time.Hour), time.Hour, 10); err != nil || n != 0 {
		t.Errorf("expiry after clearing = %d, %v, want 0", n, err)
	}
	assertNoDrift(t)
}

func TestSecondClearingLineForSameAuthIsFlagged(t *testing.T) {
	resetDB(t)
	card := newCard(t, 10_000)
	placeHold(t, card, "auth_twice", 4000)
	second := purchase("clr_second", "auth_twice", card.ID, 4000)
	second.ClearedAt = second.ClearedAt.Add(time.Minute)
	importLines(t, purchase("clr_first", "auth_twice", card.ID, 4000), second)

	if r := loadRecord(t, "clr_first"); r.flag != "" || r.fromHeld != 4000 {
		t.Errorf("first = %+v, want cleared from the hold", r)
	}
	if r := loadRecord(t, "clr_second"); r.flag != FlagDuplicateClearing || r.fromHeld != 0 || r.fromAvailable != 4000 {
		t.Errorf("second = %+v, want charged from available and flagged duplicate", r)
	}
	if a := loadAuth(t, "auth_twice"); a.cleared != 8000 || a.held != 0 {
		t.Errorf("authorization = %+v, want 8000 cleared, nothing held", a)
	}
	if got := storedBalance(t, card.AccountID); got != 2000 {
		t.Errorf("available = %d, want 2000", got)
	}
	assertNoDrift(t)
}

// The network's amount is posted, but never more than the account has: what
// it cannot pay is recorded as shortfall, and the balance stops at zero.
func TestShortfallNeverOverdraws(t *testing.T) {
	resetDB(t)
	card := newCard(t, 1300)
	placeHold(t, card, "auth_short", 1000)

	s := importLines(t,
		purchase("clr_tip_short", "auth_short", card.ID, 1500),
		purchase("clr_offline_short", "", card.ID, 400))

	r := loadRecord(t, "clr_tip_short")
	if r.fromHeld != 1000 || r.fromAvailable != 300 || r.shortfall != 200 || r.flag != FlagOverTolerance+","+FlagShortfall {
		t.Errorf("tip = %+v, want 1000 held + 300 available, 200 short, flagged", r)
	}
	r = loadRecord(t, "clr_offline_short")
	if r.fromAvailable != 0 || r.shortfall != 400 || r.flag != FlagUnauthorizedClearing+","+FlagShortfall || r.hasLedger {
		t.Errorf("offline = %+v, want nothing charged, 400 short, no ledger transaction", r)
	}
	if got := storedBalance(t, card.AccountID); got != 0 {
		t.Errorf("available = %d, want 0", got)
	}
	if got := payable(t); got != 1300 {
		t.Errorf("payable = %d, want 1300: only what was there", got)
	}
	if s.Shortfall != 600 || s.Flags[FlagShortfall] != 2 {
		t.Errorf("summary = %+v, want 600 short on 2 lines", s)
	}
	assertNoDrift(t)
}

// Clearing, reversals and the expirer all race for the same holds. Each hold
// ends up either charged from the hold or released, never both: what was
// charged from it plus what was released is exactly what it held.
func TestClearingRacingReversalAndExpiry(t *testing.T) {
	resetDB(t)
	ctx := context.Background()
	const n = 40
	const amount = 1000
	card := newCard(t, 10*n*amount)
	var lines []Line
	for i := range n {
		id := fmt.Sprintf("auth_race_%02d", i)
		placeHold(t, card, id, amount)
		lines = append(lines, purchase(fmt.Sprintf("clr_race_%02d", i), id, card.ID, amount))
	}
	if _, err := testPool.Exec(ctx, `UPDATE card_authorizations SET created_at = now() - interval '8 days'`); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	errs := make(chan error, 3)
	start := make(chan struct{})
	wg.Add(3)
	go func() {
		defer wg.Done()
		<-start
		_, err := Import(ctx, testPool, fileOf(lines...), testDate, testConfig)
		errs <- err
	}()
	go func() {
		defer wg.Done()
		<-start
		_, err := holds.ExpireBatch(ctx, testPool, time.Now(), 7*24*time.Hour, n)
		errs <- err
	}()
	go func() {
		defer wg.Done()
		<-start
		for i := n - 1; i >= 0; i -= 2 {
			tx, err := testPool.Begin(ctx)
			if err != nil {
				errs <- err
				return
			}
			_, err = holds.Release(ctx, tx, fmt.Sprintf("auth_race_%02d", i), amount, holds.ReasonReversal, fmt.Sprintf("rev_%d", i))
			if err == nil {
				err = tx.Commit(ctx)
			}
			_ = tx.Rollback(ctx)
			if err != nil {
				errs <- err
				return
			}
		}
		errs <- nil
	}()
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}

	var bad int
	if err := testPool.QueryRow(ctx, `
		SELECT count(*) FROM card_authorizations a JOIN clearing_records r ON r.auth_id = a.auth_id
		WHERE a.held_amount <> 0 OR r.charged_from_held + a.released_amount <> $1
			OR (r.charged_from_held > 0 AND a.released_amount > 0)
			OR r.charged_from_held + r.charged_from_available <> $1`, amount).Scan(&bad); err != nil {
		t.Fatal(err)
	}
	if bad != 0 {
		t.Errorf("%d holds were both released and charged, or lost money", bad)
	}
	if got := countWhere(t, `SELECT count(*) FROM clearing_records WHERE status = 'posted'`); got != n {
		t.Errorf("posted = %d, want %d", got, n)
	}
	if got := storedBalance(t, card.HeldAccountID); got != 0 {
		t.Errorf("held = %d, want 0", got)
	}
	if got := payable(t); got != n*amount {
		t.Errorf("payable = %d, want %d", got, n*amount)
	}
	assertNoDrift(t)
}

func release(tb testing.TB, authID string, amount int64, reason string) {
	tb.Helper()
	ctx := context.Background()
	tx, err := testPool.Begin(ctx)
	if err != nil {
		tb.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if _, err := holds.Release(ctx, tx, authID, amount, reason, "msg_"+authID); err != nil {
		tb.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		tb.Fatal(err)
	}
}

// The contract clears a chain on its initial auth_id; a line naming the
// incremental instead still clears the chain's hold, on the initial row.
func TestClearingOnAnIncrementalClearsTheChain(t *testing.T) {
	resetDB(t)
	card := newCard(t, 10_000)
	placeHold(t, card, "auth_initial", 3000)
	if _, err := testPool.Exec(context.Background(), `
		INSERT INTO card_authorizations (auth_id, card_id, account_id, held_account_id, amount,
			currency, merchant_id, merchant_name, mcc, kind, original_auth_id, decision, approved_amount,
			request_fingerprint, held_amount)
		VALUES ('auth_increment', $1, $2, $3, 1000, 'USD', 'mer_test', 'Test', '7011', 'incremental',
			'auth_initial', 'approved', 1000, 'test', 0)`, card.ID, card.AccountID, card.HeldAccountID); err != nil {
		t.Fatal(err)
	}
	importLines(t, purchase("clr_chain", "auth_increment", card.ID, 3000))

	if r := loadRecord(t, "clr_chain"); r.fromHeld != 3000 || r.flag != "" {
		t.Errorf("record = %+v, want 3000 from the chain's hold", r)
	}
	if a := loadAuth(t, "auth_initial"); a.held != 0 || a.cleared != 3000 {
		t.Errorf("initial = %+v, want cleared", a)
	}
	assertNoDrift(t)
}
