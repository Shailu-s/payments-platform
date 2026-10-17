package clearing

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestClearingSameFileTwiceChangesNothing(t *testing.T) {
	resetDB(t)
	card := newCard(t, 20_000)
	raw := fileOf(
		refund("clr_refund_1", "auth_1", card.ID, 5000),
		refund("clr_refund_2", "auth_2", card.ID, 3000),
		refund("clr_refund_3", "", card.ID, 1000),
	)

	first, err := Import(context.Background(), testPool, raw, testDate, testConfig)
	if err != nil {
		t.Fatal(err)
	}
	if first.Replayed || first.Processed != 3 || first.Statuses[StatusDeferred] != 3 {
		t.Fatalf("first import = %+v, want 3 processed and deferred", first)
	}
	before := moneyState(t)

	again, err := Import(context.Background(), testPool, raw, testDate, testConfig)
	if err != nil {
		t.Fatal(err)
	}
	if !again.Replayed || again.Processed != 0 || again.File.ID != first.File.ID {
		t.Errorf("second import = %+v, want a replay of %s processing nothing", again, first.File.ID)
	}
	if again.Statuses[StatusDeferred] != 3 || again.Amount != first.Amount {
		t.Errorf("replayed summary %+v differs from the first %+v", again, first)
	}
	if after := moneyState(t); after != before {
		t.Errorf("re-reading the file changed state:\nbefore %s\nafter  %s", before, after)
	}
	assertNoDrift(t)
}

func TestRefundLinesAreDeferred(t *testing.T) {
	resetDB(t)
	card := newCard(t, 10_000)
	placeHold(t, card, "auth_refunded", 2000)
	before := storedBalance(t, card.AccountID)

	s := importLines(t, refund("clr_refund", "auth_refunded", card.ID, 2000), refund("clr_refund_offline", "", card.ID, 500))
	if s.Statuses[StatusDeferred] != 2 {
		t.Fatalf("summary = %+v, want 2 deferred", s)
	}
	for _, id := range []string{"clr_refund", "clr_refund_offline"} {
		if r := loadRecord(t, id); r.status != StatusDeferred || r.hasLedger || r.fromHeld+r.fromAvailable+r.released != 0 {
			t.Errorf("%s = %+v, want deferred with no money", id, r)
		}
	}
	if got := storedBalance(t, card.AccountID); got != before {
		t.Errorf("available = %d, want %d", got, before)
	}
	if a := loadAuth(t, "auth_refunded"); a.held != 2000 || a.cleared != 0 {
		t.Errorf("authorization = %+v, want untouched", a)
	}
	if got := countWhere(t, `SELECT count(*) FROM outbox_events WHERE event_type = $1`, EventPosted); got != 0 {
		t.Errorf("posted events = %d, want 0", got)
	}
	assertNoDrift(t)
}

func TestInvalidFileWritesNothing(t *testing.T) {
	resetDB(t)
	good := "clr_1,auth_1,card_1,purchase,5000,USD,mer_1,2026-10-18T10:00:00Z\n"
	head := strings.Join(header, ",") + "\n"
	cases := map[string]string{
		"empty":               "",
		"bad header":          "clearing_id,auth_id,card_id,kind,amount,currency,merchant,cleared_at\n" + good,
		"reordered header":    "auth_id,clearing_id,card_id,kind,amount,currency,merchant_id,cleared_at\n" + good,
		"zero amount":         head + "clr_1,auth_1,card_1,purchase,0,USD,mer_1,2026-10-18T10:00:00Z\n",
		"negative amount":     head + "clr_1,auth_1,card_1,purchase,-5,USD,mer_1,2026-10-18T10:00:00Z\n",
		"decimal amount":      head + "clr_1,auth_1,card_1,purchase,50.00,USD,mer_1,2026-10-18T10:00:00Z\n",
		"signed amount":       head + "clr_1,auth_1,card_1,purchase,+5000,USD,mer_1,2026-10-18T10:00:00Z\n",
		"huge amount":         head + "clr_1,auth_1,card_1,purchase,9223372036854775808,USD,mer_1,2026-10-18T10:00:00Z\n",
		"duplicate id":        head + good + "clr_1,auth_2,card_1,purchase,700,USD,mer_1,2026-10-18T11:00:00Z\n",
		"currency":            head + "clr_1,auth_1,card_1,purchase,5000,EUR,mer_1,2026-10-18T10:00:00Z\n",
		"kind":                head + "clr_1,auth_1,card_1,chargeback,5000,USD,mer_1,2026-10-18T10:00:00Z\n",
		"missing card":        head + "clr_1,auth_1,,purchase,5000,USD,mer_1,2026-10-18T10:00:00Z\n",
		"missing clearing":    head + ",auth_1,card_1,purchase,5000,USD,mer_1,2026-10-18T10:00:00Z\n",
		"timestamp":           head + "clr_1,auth_1,card_1,purchase,5000,USD,mer_1,18/10/2026\n",
		"non-UTC timestamp":   head + "clr_1,auth_1,card_1,purchase,5000,USD,mer_1,2026-10-18T10:00:00+02:00\n",
		"other date":          head + "clr_1,auth_1,card_1,purchase,5000,USD,mer_1,2026-10-19T10:00:00Z\n",
		"short line":          head + "clr_1,auth_1,card_1,purchase,5000,USD,mer_1\n",
		"bad line after good": head + good + "clr_2,auth_2,card_1,purchase,abc,USD,mer_1,2026-10-18T10:00:00Z\n",
	}
	for name, raw := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := Import(context.Background(), testPool, []byte(raw), testDate, testConfig)
			if !errors.Is(err, ErrInvalidFile) {
				t.Errorf("import = %v, want ErrInvalidFile", err)
			}
		})
	}
	if _, err := Import(context.Background(), testPool, make([]byte, MaxFileBytes+1), testDate, testConfig); !errors.Is(err, ErrInvalidFile) {
		t.Errorf("oversized import = %v, want ErrInvalidFile", err)
	}
	if got := countWhere(t, `SELECT (SELECT count(*) FROM clearing_files) + (SELECT count(*) FROM clearing_records)`); got != 0 {
		t.Errorf("rows written = %d, want 0", got)
	}
}

func refund(clearingID, authID, cardID string, amount int64) Line {
	l := purchase(clearingID, authID, cardID, amount)
	l.Kind = KindRefund
	return l
}
