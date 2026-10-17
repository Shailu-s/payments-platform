package cardrecon

import (
	"bytes"
	"context"
	"errors"
	"maps"
	"net/http/httptest"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/Shailu-s/payments-platform/internal/api"
	"github.com/Shailu-s/payments-platform/internal/cardnetwork"
	"github.com/Shailu-s/payments-platform/internal/cardsettlement"
	"github.com/Shailu-s/payments-platform/internal/clearing"
)

func wantCounts(tb testing.TB, got map[string]int, want map[string]int) {
	tb.Helper()
	full := map[string]int{}
	for _, class := range Classes {
		full[class] = want[class]
	}
	if !maps.Equal(got, full) {
		tb.Errorf("counts = %v\nwant     %v", got, full)
	}
}

// One deliberately corrupted day produces every class, each finding naming
// the line or authorization it is about, and the run moves no money.
func TestEveryClassIsDetected(t *testing.T) {
	resetDB(t)
	ctx := context.Background()
	card := newCard(t, 100_000)
	poor := newCard(t, 500)

	placeHold(t, card, "auth_match", 1000, 0)
	placeHold(t, card, "auth_over", 1000, 0)
	placeHold(t, card, "auth_dup", 1000, 0)
	releasedAuth(t, card, "auth_reversed", 1000, nil)
	placeHold(t, card, "auth_short", 600, 200)
	placeHold(t, card, "auth_raised", 1000, 0)
	placeHold(t, card, "auth_old", 1000, 0)
	if _, err := testPool.Exec(ctx, `UPDATE card_authorizations SET created_at = now() - interval '10 days'
		WHERE auth_id = 'auth_old'`); err != nil {
		t.Fatal(err)
	}
	expiredAt := testDate.Add(12 * time.Hour)
	releasedAuth(t, card, "auth_expired", 1000, &expiredAt)

	lines := []clearing.Line{
		purchase("auth_match", card.ID, 1000),    // MATCHED
		purchase("", card.ID, 700),               // unauthorized_clearing
		purchase("", "card_unknown", 900),        // unknown_card, rejected
		purchase("auth_over", card.ID, 1500),     // over_tolerance
		purchase("auth_dup", card.ID, 1000),      // MATCHED
		purchase("auth_dup", card.ID, 1000),      // duplicate_clearing
		purchase("auth_reversed", card.ID, 1000), // cleared_after_reversal
		refund("", card.ID, 300),                 // unmatched_refund
		purchase("", poor.ID, 800),               // unauthorized_clearing + shortfall
		purchase("auth_short", card.ID, 400),     // MATCHED; the authorization's hold_shortfall
		purchase("auth_raised", card.ID, 1000),   // MATCHED; raised after it cleared
		purchase("auth_advice", card.ID, 800),    // unauthorized_clearing: its advice comes later
	}
	importLines(t, lines...)
	raise(t, card, "auth_raised", "auth_raised_inc", 500)
	// The stand-in advice for the line cleared above: a hold on money already charged.
	placeHold(t, card, "auth_advice", 800, 0)
	if _, _, err := cardsettlement.Settle(ctx, testPool, testDate); err != nil {
		t.Fatal(err)
	}

	before := moneyState(t)
	run := reconcile(t, "run_every_class", statementOf("2026-10-18", lines...))
	if after := moneyState(t); after != before {
		t.Errorf("reconciliation changed money state:\nbefore %s\nafter  %s", before, after)
	}

	wantCounts(t, run.Counts, map[string]int{
		Matched: 4, ClearedNotAuthorized: 4, AuthorizedNotCleared: 2, AmountMismatch: 1,
		DuplicateClearing: 1, LateClearing: 1, RefundException: 1, FundingShortfall: 2,
		HoldAfterClearing: 2, SettlementMismatch: 1,
	})
	if run.SettlementDate != "2026-10-18" || run.RecordCount != len(lines) || run.HoldExpiryDays != 7 {
		t.Errorf("run = %+v", run)
	}

	type key struct{ class, about string }
	got := map[key]Finding{}
	for _, f := range allFindings(t, run.ID) {
		about := f.ClearingID
		if about == "" {
			about = f.AuthID
		}
		if about == "" {
			about = f.Reason
		}
		got[key{f.Class, about}] = f
	}
	want := map[key]int64{
		{ClearedNotAuthorized, lines[1].ClearingID}:  700,
		{ClearedNotAuthorized, lines[2].ClearingID}:  900,
		{AmountMismatch, lines[3].ClearingID}:        1500,
		{DuplicateClearing, lines[5].ClearingID}:     1000,
		{LateClearing, lines[6].ClearingID}:          1000,
		{RefundException, lines[7].ClearingID}:       300,
		{ClearedNotAuthorized, lines[8].ClearingID}:  800,
		{FundingShortfall, lines[8].ClearingID}:      300,
		{FundingShortfall, "auth_short"}:             200,
		{AuthorizedNotCleared, "auth_old"}:           1000,
		{AuthorizedNotCleared, "auth_expired"}:       1000,
		{HoldAfterClearing, "auth_raised_inc"}:       500,
		{ClearedNotAuthorized, lines[11].ClearingID}: 800,
		{HoldAfterClearing, "auth_advice"}:           800,
		{SettlementMismatch, ReasonStatementDiffers}: 900,
	}
	for k, amount := range want {
		f, ok := got[k]
		if !ok {
			t.Errorf("missing %s for %s", k.class, k.about)
			continue
		}
		if f.Amount != amount {
			t.Errorf("%s for %s: amount %d, want %d", k.class, k.about, f.Amount, amount)
		}
	}
	if len(got) != len(want) {
		t.Errorf("%d findings, want %d: %v", len(got), len(want), slices.Collect(maps.Keys(got)))
	}
	if f := got[key{SettlementMismatch, ReasonStatementDiffers}]; f.Statement == nil || f.Settlement == nil ||
		f.Statement.NetAmount != 9800 || f.Settlement.NetAmount != 8900 || !slices.Equal(f.Fields, []string{"purchases_total", "net_amount", "count"}) {
		t.Errorf("settlement evidence = %+v", f)
	}
	if f := got[key{HoldAfterClearing, "auth_raised_inc"}]; f.Authorization == nil || f.Authorization.OriginalAuthID != "auth_raised" ||
		f.Reason != "incremental_after_clearing" {
		t.Errorf("hold after clearing evidence = %+v", f.Authorization)
	}
	if f := got[key{HoldAfterClearing, "auth_advice"}]; f.Reason != "advice_after_clearing" {
		t.Errorf("advice after clearing reason = %q", f.Reason)
	}
	if f := got[key{LateClearing, lines[6].ClearingID}]; f.Record == nil || f.Record.Flag != clearing.FlagClearedAfterReversal {
		t.Errorf("late clearing evidence = %+v", f.Record)
	}

	only, err := Findings(ctx, testPool, run.ID, FundingShortfall, 100, 0)
	if err != nil || len(only) != 2 {
		t.Errorf("FUNDING_SHORTFALL filter = %d findings, %v", len(only), err)
	}
}

// All ten simulator scenarios through the real issuer, clearing, settlement
// and the network's own statement: only MATCHED, plus the one class a
// scenario creates on purpose (the offline purchase was never authorized).
func TestCleanDayIsAllMatched(t *testing.T) {
	resetDB(t)
	ctx := context.Background()
	const secret = "test-only-network-secret-never-use-in-production"
	issuer := httptest.NewServer(api.NewServer(testPool, []byte("test-webhook-secret-0123456789abcdef"), []byte(secret)).Handler())
	t.Cleanup(issuer.Close)
	network, err := cardnetwork.New(cardnetwork.Config{
		IssuerURL: issuer.URL, Secret: []byte(secret), AuthDeadline: 2 * time.Second,
		StandInLimit: 10_000, Clock: cardnetwork.NewFakeClock(time.Now()),
	})
	if err != nil {
		t.Fatal(err)
	}
	card := newCard(t, 100_000)
	var dates []string
	for i, scenario := range cardnetwork.Scenarios {
		seed := uint64(1500 + i)
		result, err := network.RunScenario(ctx, cardnetwork.ScenarioRequest{
			Scenario: scenario, CardID: card.ID, Amount: 2500, Seed: &seed,
		})
		if err != nil {
			t.Fatalf("%s: %v", scenario, err)
		}
		for _, d := range result.ClearingDates {
			if !slices.Contains(dates, d) {
				dates = append(dates, d)
			}
		}
	}
	slices.Sort(dates)

	total := map[string]int{}
	for _, date := range dates {
		day, _ := time.Parse(DateLayout, date)
		raw, err := network.ClearingFile(date)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := clearing.Import(ctx, testPool, raw, day, clearing.Config{OverTolerancePct: 20}); err != nil {
			t.Fatal(err)
		}
		if _, _, err := cardsettlement.Settle(ctx, testPool, day); err != nil {
			t.Fatal(err)
		}
		stmt, err := network.SettlementStatement(date)
		if err != nil {
			t.Fatal(err)
		}
		before := moneyState(t)
		run := reconcile(t, "run_clean_"+date, stmt)
		if moneyState(t) != before {
			t.Errorf("reconciling %s changed money state", date)
		}
		for class, n := range run.Counts {
			total[class] += n
		}
	}
	// Nine purchases (a full reversal clears nothing) and the late refund.
	wantCounts(t, total, map[string]int{Matched: 9, ClearedNotAuthorized: 1})
}

// The statement disagrees with what we paid, no payment was recorded, or a
// line was posted after the day was paid: each is reported, with both sides.
func TestSettlementMismatchIsDetected(t *testing.T) {
	ctx := context.Background()
	reasons := func(id string) []string {
		var r []string
		for _, f := range allFindings(t, id) {
			if f.Class == SettlementMismatch {
				r = append(r, f.Reason)
			}
		}
		return r
	}

	t.Run("missing settlement", func(t *testing.T) {
		resetDB(t)
		card := newCard(t, 10_000)
		lines := []clearing.Line{purchase("", card.ID, 1000)}
		importLines(t, lines...)
		run := reconcile(t, "run_missing", statementOf("2026-10-18", lines...))
		if got := reasons(run.ID); !slices.Equal(got, []string{ReasonNoSettlement}) {
			t.Errorf("reasons = %v", got)
		}
	})

	t.Run("tampered statement", func(t *testing.T) {
		resetDB(t)
		card := newCard(t, 10_000)
		lines := []clearing.Line{purchase("", card.ID, 1000), refund("", card.ID, 200)}
		importLines(t, lines...)
		if _, _, err := cardsettlement.Settle(ctx, testPool, testDate); err != nil {
			t.Fatal(err)
		}
		if run := reconcile(t, "run_honest", statementOf("2026-10-18", lines...)); run.Counts[SettlementMismatch] != 0 {
			t.Fatalf("honest statement: counts %v", run.Counts)
		}
		run := reconcile(t, "run_tampered", statement("2026-10-18", 1100, 200, 2))
		findings := allFindings(t, run.ID)
		var f *Finding
		for i := range findings {
			if findings[i].Class == SettlementMismatch {
				f = &findings[i]
			}
		}
		if run.Counts[SettlementMismatch] != 1 || f == nil || f.Reason != ReasonStatementDiffers || f.Amount != 100 ||
			!slices.Equal(f.Fields, []string{"purchases_total", "net_amount"}) {
			t.Errorf("tampered: counts %v, finding %+v", run.Counts, f)
		}
	})

	t.Run("line posted after settlement", func(t *testing.T) {
		resetDB(t)
		card := newCard(t, 10_000)
		first := purchase("", card.ID, 1000)
		importLines(t, first)
		if _, _, err := cardsettlement.Settle(ctx, testPool, testDate); err != nil {
			t.Fatal(err)
		}
		late := purchase("", card.ID, 400)
		importLines(t, first, late)
		run := reconcile(t, "run_late_line", statementOf("2026-10-18", first, late))
		if got := reasons(run.ID); !slices.Equal(got, []string{ReasonStatementDiffers, ReasonRecordsDiffer}) {
			t.Errorf("reasons = %v", got)
		}
	})
}

// A run ID is a retry key: the same statement again is the saved run, a
// different statement under the same ID is refused, and concurrent runs with
// one ID save one run.
func TestReconRunReplayAndConflict(t *testing.T) {
	resetDB(t)
	ctx := context.Background()
	card := newCard(t, 10_000)
	lines := []clearing.Line{purchase("", card.ID, 1000)}
	importLines(t, lines...)
	raw := statementOf("2026-10-18", lines...)

	const callers = 10
	var wg sync.WaitGroup
	runs := make([]Run, callers)
	errs := make([]error, callers)
	for i := range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			runs[i], errs[i] = Reconcile(ctx, testPool, "run_concurrent", bytes.NewReader(raw), testConfig)
		}()
	}
	wg.Wait()
	for i := range callers {
		if errs[i] != nil {
			t.Fatalf("caller %d: %v", i, errs[i])
		}
		if !runs[i].CreatedAt.Equal(runs[0].CreatedAt) || !maps.Equal(runs[i].Counts, runs[0].Counts) {
			t.Errorf("caller %d got a different run: %+v", i, runs[i])
		}
	}
	if n := countWhere(t, `SELECT count(*) FROM card_recon_runs`); n != 1 {
		t.Errorf("%d runs saved, want 1", n)
	}
	findings := countWhere(t, `SELECT count(*) FROM card_recon_findings`)
	if findings != 2 {
		t.Errorf("%d findings, want 2 (unauthorized line, no settlement)", findings)
	}

	again := reconcile(t, "run_concurrent", raw)
	if !again.CreatedAt.Equal(runs[0].CreatedAt) {
		t.Errorf("replay returned a new run: %+v", again)
	}
	if _, err := Reconcile(ctx, testPool, "run_concurrent", bytes.NewReader(statement("2026-10-18", 999, 0, 1)), testConfig); !errors.Is(err, ErrRunConflict) {
		t.Errorf("different bytes: want ErrRunConflict, got %v", err)
	}
	if n := countWhere(t, `SELECT count(*) FROM card_recon_findings`); n != findings {
		t.Errorf("replay and conflict wrote findings: %d", n)
	}
	if listed, err := ListRuns(ctx, testPool, 10, 0); err != nil || len(listed) != 1 || listed[0].ID != "run_concurrent" {
		t.Errorf("ListRuns = %+v, %v", listed, err)
	}
}

// Evidence is never edited or removed, by anyone with SQL access to the table.
func TestFindingsAreAppendOnly(t *testing.T) {
	resetDB(t)
	ctx := context.Background()
	card := newCard(t, 10_000)
	lines := []clearing.Line{purchase("", card.ID, 1000)}
	importLines(t, lines...)
	reconcile(t, "run_immutable", statementOf("2026-10-18", lines...))

	for _, stmt := range []string{
		`UPDATE card_recon_findings SET amount = 0`,
		`DELETE FROM card_recon_findings`,
		`UPDATE card_recon_runs SET counts = '{}'`,
		`DELETE FROM card_recon_runs`,
	} {
		if _, err := testPool.Exec(ctx, stmt); err == nil {
			t.Errorf("%s succeeded", stmt)
		}
	}
	if n := countWhere(t, `SELECT sum(amount)::int FROM card_recon_findings`); n != 2000 {
		t.Errorf("finding changed: %d intact", n)
	}
}

func TestParseStatementRejectsMalformed(t *testing.T) {
	const header = "settlement_date,purchases_total,refunds_total,net_amount,line_count,currency\n"
	for _, raw := range []string{
		"",
		header,
		header + "2026-10-18,100,0,100,1,USD\n2026-10-18,100,0,100,1,USD\n",
		"settlement_date,purchases,refunds,net,count,currency\n2026-10-18,100,0,100,1,USD\n",
		header + "18/10/2026,100,0,100,1,USD\n",
		header + "2026-10-18,100,0,90,1,USD\n",
		header + "2026-10-18,-100,0,-100,1,USD\n",
		header + "2026-10-18,100,0,100,-1,USD\n",
		header + "2026-10-18,100,0,100,1,EUR\n",
		header + "2026-10-18,1.5,0,1.5,1,USD\n",
		header + "2026-10-18,100,0,100,1\n",
	} {
		if _, err := ParseStatement([]byte(raw)); !errors.Is(err, ErrInvalidStatement) {
			t.Errorf("%q: want ErrInvalidStatement, got %v", raw, err)
		}
	}
	s, err := ParseStatement([]byte(header + "2026-10-18,100,250,-150,2,USD\n"))
	if err != nil || s.Date != "2026-10-18" || s.NetAmount != -150 || s.Count != 2 {
		t.Errorf("valid statement = %+v, %v", s, err)
	}
}
