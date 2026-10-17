package clearing

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestClearingSameFileTwiceChangesNothing(t *testing.T) {
	resetDB(t)
	card := newCard(t, 20_000)
	placeHold(t, card, "auth_exact", 5000)
	placeHold(t, card, "auth_partial", 4000)
	raw := fileOf(
		purchase("clr_exact", "auth_exact", card.ID, 5000),
		purchase("clr_partial", "auth_partial", card.ID, 3000),
		refund("clr_refund", "auth_exact", card.ID, 1000),
	)

	first, err := Import(context.Background(), testPool, raw, testDate, testConfig)
	if err != nil {
		t.Fatal(err)
	}
	if first.Replayed || first.Processed != 3 || first.Statuses[StatusPosted] != 3 || first.Credited != 1000 {
		t.Fatalf("first import = %+v, want 3 processed and posted, 1000 credited", first)
	}
	before := moneyState(t)

	again, err := Import(context.Background(), testPool, raw, testDate, testConfig)
	if err != nil {
		t.Fatal(err)
	}
	if !again.Replayed || again.Processed != 0 || again.File.ID != first.File.ID {
		t.Errorf("second import = %+v, want a replay of %s processing nothing", again, first.File.ID)
	}
	if again.Statuses[StatusPosted] != 3 || again.ChargedFromHeld != first.ChargedFromHeld || again.Credited != first.Credited {
		t.Errorf("replayed summary %+v differs from the first %+v", again, first)
	}
	if after := moneyState(t); after != before {
		t.Errorf("re-reading the file changed state:\nbefore %s\nafter  %s", before, after)
	}
	assertNoDrift(t)
}

// Two importers of one file at once: the file is stored once and every line
// is posted once, whichever importer gets to it.
func TestClearingConcurrentImportsOfOneFile(t *testing.T) {
	resetDB(t)
	card := newCard(t, 100_000)
	var lines []Line
	for i := range 20 {
		id := "auth_c_" + strings.Repeat("x", i)
		placeHold(t, card, id, 1000)
		lines = append(lines, purchase("clr_c_"+strings.Repeat("x", i), id, card.ID, 1200))
	}
	raw := fileOf(lines...)

	var wg sync.WaitGroup
	summaries := make([]Summary, 4)
	errs := make([]error, 4)
	start := make(chan struct{})
	for i := range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			summaries[i], errs[i] = Import(context.Background(), testPool, raw, testDate, testConfig)
		}()
	}
	close(start)
	wg.Wait()
	processed := 0
	for i := range 4 {
		if errs[i] != nil {
			t.Fatalf("import %d: %v", i, errs[i])
		}
		processed += summaries[i].Processed
	}
	if processed != len(lines) {
		t.Errorf("processed %d lines across importers, want %d", processed, len(lines))
	}
	if got := countWhere(t, `SELECT count(*) FROM clearing_files`); got != 1 {
		t.Errorf("files = %d, want 1", got)
	}
	if got := countWhere(t, `SELECT count(*) FROM ledger_transactions WHERE reference LIKE 'clearing %'`); got != len(lines) {
		t.Errorf("clearing transactions = %d, want %d", got, len(lines))
	}
	if got := payable(t); got != 1200*int64(len(lines)) {
		t.Errorf("payable = %d, want %d", got, 1200*len(lines))
	}
	if got := storedBalance(t, card.AccountID); got != 100_000-1200*int64(len(lines)) {
		t.Errorf("available = %d, want %d", got, 100_000-1200*len(lines))
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

// The commit of one line fails, as a crash there would: the lines before it
// are posted, it and the rest are not. Running the same file again posts the
// rest, and every line exactly once.
func TestClearingCrashMidFileRerunPostsEachLineOnce(t *testing.T) {
	resetDB(t)
	ctx := context.Background()
	card := newCard(t, 50_000)
	var lines []Line
	for _, n := range []string{"a", "b", "c", "d", "e"} {
		placeHold(t, card, "auth_crash_"+n, 1000)
		lines = append(lines, purchase("clr_crash_"+n, "auth_crash_"+n, card.ID, 1000))
	}
	// A deferred constraint trigger fails at COMMIT, after every statement of
	// the line has run: the point of no return.
	if _, err := testPool.Exec(ctx, `
		CREATE FUNCTION fail_clr_crash_c() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN
			IF NEW.clearing_id = 'clr_crash_c' THEN RAISE EXCEPTION 'crash at commit'; END IF;
			RETURN NULL;
		END $$;
		CREATE CONSTRAINT TRIGGER crash_at_commit AFTER INSERT ON clearing_records
			DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION fail_clr_crash_c();`); err != nil {
		t.Fatal(err)
	}
	dropped := false
	drop := func() {
		if dropped {
			return
		}
		dropped = true
		if _, err := testPool.Exec(ctx, `DROP TRIGGER crash_at_commit ON clearing_records; DROP FUNCTION fail_clr_crash_c()`); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(drop)

	raw := fileOf(lines...)
	if _, err := Import(ctx, testPool, raw, testDate, testConfig); err == nil || !strings.Contains(err.Error(), "crash at commit") {
		t.Fatalf("import = %v, want the crash", err)
	}
	if got := countWhere(t, `SELECT count(*) FROM clearing_records`); got != 2 {
		t.Fatalf("records after the crash = %d, want 2 (a, b)", got)
	}
	if a := loadAuth(t, "auth_crash_c"); a.held != 1000 || a.cleared != 0 {
		t.Errorf("crashed line moved money: %+v", a)
	}
	drop()

	s, err := Import(ctx, testPool, raw, testDate, testConfig)
	if err != nil {
		t.Fatal(err)
	}
	if !s.Replayed || s.Processed != 3 || s.Statuses[StatusPosted] != 5 || s.Unprocessed != 0 {
		t.Errorf("rerun = %+v, want a replay processing the remaining 3 of 5", s)
	}
	if got := countWhere(t, `SELECT count(*) FROM ledger_transactions WHERE reference LIKE 'clearing %'`); got != 5 {
		t.Errorf("clearing transactions = %d, want 5: one per line", got)
	}
	if got := countWhere(t, `SELECT count(*) FROM outbox_events WHERE event_type = $1`, EventPosted); got != 5 {
		t.Errorf("posted events = %d, want 5", got)
	}
	if got, want := storedBalance(t, card.HeldAccountID), int64(0); got != want {
		t.Errorf("held = %d, want %d", got, want)
	}
	if got := payable(t); got != 5000 {
		t.Errorf("payable = %d, want 5000", got)
	}
	assertNoDrift(t)
}

// A second file for the same date may repeat lines of the first: they are
// recorded once, and the second file reports them as another file's.
func TestOverlappingFilesPostSharedLinesOnce(t *testing.T) {
	resetDB(t)
	card := newCard(t, 10_000)
	placeHold(t, card, "auth_one", 1000)
	placeHold(t, card, "auth_two", 2000)
	one := purchase("clr_one", "auth_one", card.ID, 1000)
	two := purchase("clr_two", "auth_two", card.ID, 2000)
	two.ClearedAt = two.ClearedAt.Add(time.Hour)
	importLines(t, one)
	s := importLines(t, one, two)
	if s.Replayed || s.Processed != 1 || s.FromOtherFiles != 1 || s.Statuses[StatusPosted] != 2 {
		t.Errorf("second file = %+v, want 1 processed, 1 from the other file", s)
	}
	if got := payable(t); got != 3000 {
		t.Errorf("payable = %d, want 3000", got)
	}
	assertNoDrift(t)
}

// A day with nothing to clear is a header-only file, byte-identical every
// such day: each date is still its own file.
func TestHeaderOnlyFilesForDifferentDatesAreDistinct(t *testing.T) {
	resetDB(t)
	ctx := context.Background()
	raw := fileOf()
	first, err := Import(ctx, testPool, raw, testDate, testConfig)
	if err != nil {
		t.Fatal(err)
	}
	next, err := Import(ctx, testPool, raw, testDate.AddDate(0, 0, 1), testConfig)
	if err != nil {
		t.Fatal(err)
	}
	if next.Replayed || next.File.ID == first.File.ID || next.File.FileDate != "2026-10-19" {
		t.Errorf("next day's file = %+v, want a new file for 2026-10-19 (first %s)", next.File, first.File.ID)
	}
	again, err := Import(ctx, testPool, raw, testDate.AddDate(0, 0, 1), testConfig)
	if err != nil || !again.Replayed || again.File.ID != next.File.ID {
		t.Errorf("same date again = %+v, %v, want a replay of %s", again, err, next.File.ID)
	}
	if got := countWhere(t, `SELECT count(*) FROM clearing_files`); got != 2 {
		t.Errorf("files = %d, want 2", got)
	}
}

func refund(clearingID, authID, cardID string, amount int64) Line {
	l := purchase(clearingID, authID, cardID, amount)
	l.Kind = KindRefund
	return l
}
