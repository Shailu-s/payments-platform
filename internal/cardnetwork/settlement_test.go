package cardnetwork

import (
	"bytes"
	"context"
	"errors"
	"testing"
)

// The statement is the network's total of the same lines its clearing file
// lists: asked twice, it is the same bytes, and its totals are the file's.
func TestStatementIsDeterministic(t *testing.T) {
	issuer := newFakeIssuer(t, nil)
	n := newTestNetwork(t, issuer.srv.URL, NewFakeClock(testStart))
	ctx := context.Background()
	var refundDate string
	for i, name := range Scenarios {
		res, err := n.RunScenario(ctx, ScenarioRequest{Scenario: name, CardID: "card_test", Amount: 1_000, Seed: seed(uint64(i))})
		if err != nil {
			t.Fatal(err)
		}
		if name == ScenarioLateRefund {
			refundDate = res.ClearingDates[1]
		}
	}

	first, err := n.SettlementStatement("2026-10-18")
	if err != nil {
		t.Fatal(err)
	}
	second, _ := n.SettlementStatement("2026-10-18")
	if !bytes.Equal(first, second) {
		t.Fatal("same date, different bytes")
	}
	// Nine purchases of the ten scenarios clear on the day after: a full
	// reversal clears nothing; a tip, a partial and an incremental change the amount.
	const want = "settlement_date,purchases_total,refunds_total,net_amount,line_count,currency\n" +
		"2026-10-18,9400,0,9400,9,USD\n"
	if string(first) != want {
		t.Fatalf("statement:\n%s\nwant:\n%s", first, want)
	}
	lines := n.ClearingLines(testStart.AddDate(0, 0, 1))
	if s := StatementOf("2026-10-18", lines); s.PurchasesTotal != 9400 || s.LineCount != len(lines) {
		t.Fatalf("StatementOf = %+v over %d lines", s, len(lines))
	}

	refund, err := n.SettlementStatement(refundDate)
	if err != nil {
		t.Fatal(err)
	}
	if want := "settlement_date,purchases_total,refunds_total,net_amount,line_count,currency\n" +
		refundDate + ",0,1000,-1000,1,USD\n"; string(refund) != want {
		t.Fatalf("refund day statement:\n%s", refund)
	}

	empty, err := n.SettlementStatement("2026-01-01")
	if err != nil || string(empty) != "settlement_date,purchases_total,refunds_total,net_amount,line_count,currency\n2026-01-01,0,0,0,0,USD\n" {
		t.Fatalf("empty date: %q, %v", empty, err)
	}
	if _, err := n.SettlementStatement("18/10/2026"); !errors.Is(err, ErrInvalidDate) {
		t.Fatalf("want ErrInvalidDate, got %v", err)
	}
}
