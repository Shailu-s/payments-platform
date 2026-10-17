package clearing

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Shailu-s/payments-platform/internal/cards"
)

// clearPurchase places a hold of amount on card and clears it exactly, in a
// file of its own, so a refund can be matched against it.
func clearPurchase(tb testing.TB, card cards.Card, authID string, amount int64) {
	tb.Helper()
	placeHold(tb, card, authID, amount)
	importLines(tb, purchase("clr_"+authID, authID, card.ID, amount))
}

func refunded(tb testing.TB, authID string) int64 {
	tb.Helper()
	var n int64
	if err := testPool.QueryRow(context.Background(),
		`SELECT refunded_amount FROM card_authorizations WHERE auth_id = $1`, authID).Scan(&n); err != nil {
		tb.Fatal(err)
	}
	return n
}

// entriesOf is a ledger transaction's entries as text, to compare before and after.
func entriesOf(tb testing.TB, reference string) string {
	tb.Helper()
	var s string
	if err := testPool.QueryRow(context.Background(), `
		SELECT COALESCE(string_agg(e.id || ':' || e.account_id || ':' || e.direction || ':' || e.amount, ',' ORDER BY e.id), '')
		FROM ledger_entries e JOIN ledger_transactions t ON t.id = e.txn_id
		WHERE t.reference = $1`, reference).Scan(&s); err != nil {
		tb.Fatal(err)
	}
	return s
}

func TestRefundCreditsAvailableWithoutTouchingTheCharge(t *testing.T) {
	resetDB(t)
	const balance = 20_000
	card := newCard(t, balance)
	clearPurchase(t, card, "auth_shoes", 5000)
	charge := entriesOf(t, "clearing clr_auth_shoes")
	spend := dailySpend(t, card.ID)

	refundLine := refund("clr_shoes_back", "auth_shoes", card.ID, 5000)
	refundLine.ClearedAt = refundLine.ClearedAt.Add(time.Hour)
	s := importLines(t, refundLine)
	if s.Statuses[StatusPosted] != 1 || len(s.Flags) != 0 || s.Credited != 5000 {
		t.Errorf("summary = %+v, want 1 posted, unflagged, 5000 credited", s)
	}

	if got := entriesOf(t, "clearing clr_auth_shoes"); got != charge {
		t.Errorf("the charge's entries changed:\nbefore %s\nafter  %s", charge, got)
	}
	want := fmt.Sprintf("%s:credit:5000,%s:debit:5000", card.AccountID, NetworkPayableAccountID)
	var got string
	if err := testPool.QueryRow(context.Background(), `
		SELECT string_agg(e.account_id || ':' || e.direction || ':' || e.amount, ',' ORDER BY e.direction)
		FROM ledger_entries e JOIN ledger_transactions t ON t.id = e.txn_id
		WHERE t.reference = 'refund clr_shoes_back'`).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Errorf("refund entries = %s, want %s", got, want)
	}
	if r := loadRecord(t, "clr_shoes_back"); r != (record{status: StatusPosted, credited: 5000, hasLedger: true}) {
		t.Errorf("record = %+v", r)
	}
	if got := storedBalance(t, card.AccountID); got != balance {
		t.Errorf("available = %d, want %d", got, balance)
	}
	if got := payable(t); got != 0 {
		t.Errorf("payable = %d, want 0", got)
	}
	if a := loadAuth(t, "auth_shoes"); a.cleared != 5000 || a.held != 0 {
		t.Errorf("authorization = %+v, want cleared 5000 untouched", a)
	}
	if got := refunded(t, "auth_shoes"); got != 5000 {
		t.Errorf("refunded = %d, want 5000", got)
	}
	if got := dailySpend(t, card.ID); got != spend {
		t.Errorf("daily spend = %d, want %d unchanged", got, spend)
	}
	if got := countWhere(t, `SELECT count(*) FROM outbox_events WHERE event_type = $1 AND aggregate_id = 'auth_shoes'`, EventRefundPosted); got != 1 {
		t.Errorf("refund events = %d, want 1", got)
	}
	assertNoDrift(t)
}

func TestPartialAndRepeatedRefunds(t *testing.T) {
	resetDB(t)
	const balance = 20_000
	card := newCard(t, balance)
	clearPurchase(t, card, "auth_pair", 5000)

	first := refund("clr_r1", "auth_pair", card.ID, 2000)
	second := refund("clr_r2", "auth_pair", card.ID, 3000)
	second.ClearedAt = second.ClearedAt.Add(time.Minute)
	importLines(t, first, second)
	for _, id := range []string{"clr_r1", "clr_r2"} {
		if r := loadRecord(t, id); r.status != StatusPosted || r.flag != "" {
			t.Errorf("%s = %+v, want posted, matched", id, r)
		}
	}
	if got := refunded(t, "auth_pair"); got != 5000 {
		t.Errorf("refunded = %d, want 5000", got)
	}

	third := refund("clr_r3", "auth_pair", card.ID, 100)
	third.ClearedAt = third.ClearedAt.Add(2 * time.Minute)
	importLines(t, third)
	if r := loadRecord(t, "clr_r3"); r != (record{status: StatusPosted, credited: 100, flag: FlagRefundExceedsPurchase, hasLedger: true}) {
		t.Errorf("third refund = %+v, want posted and flagged %s", r, FlagRefundExceedsPurchase)
	}
	if got := refunded(t, "auth_pair"); got != 5100 {
		t.Errorf("refunded = %d, want 5100", got)
	}
	if got := storedBalance(t, card.AccountID); got != balance+100 {
		t.Errorf("available = %d, want %d", got, balance+100)
	}
	if got := payable(t); got != -100 {
		t.Errorf("payable = %d, want -100: the network owes us the excess", got)
	}
	assertNoDrift(t)
}

func TestUnmatchedRefundIsPostedAndFlagged(t *testing.T) {
	resetDB(t)
	const balance = 10_000
	card := newCard(t, balance)
	placeHold(t, card, "auth_open", 2000)

	lines := []Line{
		refund("clr_no_auth", "", card.ID, 300),
		refund("clr_unknown_auth", "auth_nowhere", card.ID, 400),
		refund("clr_never_cleared", "auth_open", card.ID, 500),
	}
	for i := range lines {
		lines[i].ClearedAt = lines[i].ClearedAt.Add(time.Duration(i) * time.Minute)
	}
	s := importLines(t, lines...)
	if s.Statuses[StatusPosted] != 3 || s.Flags[FlagUnmatchedRefund] != 3 || s.Credited != 1200 {
		t.Errorf("summary = %+v, want 3 posted unmatched, 1200 credited", s)
	}
	for _, l := range lines {
		if r := loadRecord(t, l.ClearingID); r != (record{status: StatusPosted, credited: l.Amount, flag: FlagUnmatchedRefund, hasLedger: true}) {
			t.Errorf("%s = %+v", l.ClearingID, r)
		}
	}
	if a := loadAuth(t, "auth_open"); a.held != 2000 || a.cleared != 0 {
		t.Errorf("open authorization = %+v, want its hold untouched", a)
	}
	if got := refunded(t, "auth_open"); got != 0 {
		t.Errorf("refunded on a never-cleared authorization = %d, want 0", got)
	}
	if got := storedBalance(t, card.AccountID); got != balance-2000+1200 {
		t.Errorf("available = %d, want %d", got, balance-2000+1200)
	}
	if got := storedBalance(t, card.HeldAccountID); got != 2000 {
		t.Errorf("held = %d, want 2000", got)
	}
	assertNoDrift(t)
}

func TestRefundForUnknownCardIsRejected(t *testing.T) {
	resetDB(t)
	card := newCard(t, 10_000)
	clearPurchase(t, card, "auth_known", 1000)

	ghost := refund("clr_ghost", "", "card_ghost", 700)
	onKnownAuth := refund("clr_ghost_auth", "auth_known", "card_ghost", 700)
	onKnownAuth.ClearedAt = onKnownAuth.ClearedAt.Add(time.Minute)
	s := importLines(t, ghost, onKnownAuth)
	if s.Statuses[StatusRejected] != 2 || s.Flags[FlagUnknownCard] != 2 || s.Credited != 0 {
		t.Errorf("summary = %+v, want 2 rejected unknown_card, nothing credited", s)
	}
	for _, id := range []string{"clr_ghost", "clr_ghost_auth"} {
		if r := loadRecord(t, id); r != (record{status: StatusRejected, flag: FlagUnknownCard}) {
			t.Errorf("%s = %+v, want rejected without money", id, r)
		}
	}
	if got := refunded(t, "auth_known"); got != 0 {
		t.Errorf("refunded = %d, want 0", got)
	}
	if got := countWhere(t, `SELECT count(*) FROM ledger_transactions WHERE reference LIKE 'refund %'`); got != 0 {
		t.Errorf("refund transactions = %d, want 0", got)
	}
	if got := countWhere(t, `SELECT count(*) FROM outbox_events WHERE event_type = $1`, EventRefundPosted); got != 0 {
		t.Errorf("refund events = %d, want 0", got)
	}
	assertNoDrift(t)
}

func TestRefundToClosedCardStillCreditsTheAccount(t *testing.T) {
	resetDB(t)
	const balance = 10_000
	card := newCard(t, balance)
	clearPurchase(t, card, "auth_closed", 4000)
	if _, err := cards.Transition(context.Background(), testPool, card.ID, cards.Close); err != nil {
		t.Fatal(err)
	}

	l := refund("clr_closed", "auth_closed", card.ID, 4000)
	l.ClearedAt = l.ClearedAt.Add(time.Hour)
	importLines(t, l)
	if r := loadRecord(t, "clr_closed"); r != (record{status: StatusPosted, credited: 4000, hasLedger: true}) {
		t.Errorf("record = %+v", r)
	}
	if got := storedBalance(t, card.AccountID); got != balance {
		t.Errorf("available = %d, want %d", got, balance)
	}
	assertNoDrift(t)
}

// The same refund line in the same file twice, in another file, and in one
// file imported by four importers at once: each line credits once.
func TestRefundLineImportedTwiceCreditsOnce(t *testing.T) {
	resetDB(t)
	ctx := context.Background()
	const balance = 100_000
	card := newCard(t, balance)
	clearPurchase(t, card, "auth_multi", 30_000)

	var lines []Line
	for i := range 20 {
		l := refund(fmt.Sprintf("clr_dup_%02d", i), "auth_multi", card.ID, 1000)
		l.ClearedAt = l.ClearedAt.Add(time.Duration(i) * time.Second)
		lines = append(lines, l)
	}
	raw := fileOf(lines...)
	var wg sync.WaitGroup
	errs := make([]error, 4)
	start := make(chan struct{})
	for i := range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, errs[i] = Import(ctx, testPool, raw, testDate, testConfig)
		}()
	}
	close(start)
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("import %d: %v", i, err)
		}
	}
	before := moneyState(t)
	if again := importLines(t, lines...); !again.Replayed || again.Processed != 0 || moneyState(t) != before {
		t.Errorf("re-import = %+v, want a replay that changes nothing", again)
	}
	// Another file carrying a line already posted, plus one new line.
	extra := refund("clr_dup_new", "auth_multi", card.ID, 1000)
	extra.ClearedAt = extra.ClearedAt.Add(time.Hour)
	s := importLines(t, lines[0], extra)
	if s.Processed != 1 || s.FromOtherFiles != 1 {
		t.Errorf("overlapping file = %+v, want 1 processed, 1 from the other file", s)
	}

	if got := countWhere(t, `SELECT count(*) FROM ledger_transactions WHERE reference LIKE 'refund %'`); got != 21 {
		t.Errorf("refund transactions = %d, want 21", got)
	}
	if got := refunded(t, "auth_multi"); got != 21_000 {
		t.Errorf("refunded = %d, want 21000", got)
	}
	if got := storedBalance(t, card.AccountID); got != balance-30_000+21_000 {
		t.Errorf("available = %d, want %d", got, balance-30_000+21_000)
	}
	assertNoDrift(t)
}

// A refund naming another card's purchase credits the card on the line, the
// one the network is paying, and is flagged for a person to look at.
func TestRefundOnAnotherCardsPurchaseIsFlagged(t *testing.T) {
	resetDB(t)
	const balance = 10_000
	buyer, other := newCard(t, balance), newCard(t, balance)
	clearPurchase(t, buyer, "auth_buyer", 3000)

	l := refund("clr_other", "auth_buyer", other.ID, 3000)
	l.ClearedAt = l.ClearedAt.Add(time.Hour)
	importLines(t, l)
	if r := loadRecord(t, "clr_other"); r != (record{status: StatusPosted, credited: 3000, flag: FlagCardMismatch, hasLedger: true}) {
		t.Errorf("record = %+v", r)
	}
	if got := storedBalance(t, other.AccountID); got != balance+3000 {
		t.Errorf("line's card available = %d, want %d", got, balance+3000)
	}
	if got := refunded(t, "auth_buyer"); got != 3000 {
		t.Errorf("refunded = %d, want 3000", got)
	}
	assertNoDrift(t)
}
