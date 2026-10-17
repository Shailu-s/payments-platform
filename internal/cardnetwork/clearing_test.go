package cardnetwork

import (
	"bytes"
	"context"
	"encoding/csv"
	"errors"
	"testing"
)

func parseClearing(t *testing.T, b []byte) []map[string]string {
	t.Helper()
	records, err := csv.NewReader(bytes.NewReader(b)).ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(records) == 0 || !equalStrings(records[0], clearingHeader) {
		t.Fatalf("bad header: %v", records)
	}
	var lines []map[string]string
	for _, rec := range records[1:] {
		line := map[string]string{}
		for i, col := range clearingHeader {
			line[col] = rec[i]
		}
		lines = append(lines, line)
	}
	return lines
}

func TestClearingFileReflectsTheJournal(t *testing.T) {
	issuer := newFakeIssuer(t, nil)
	n := newTestNetwork(t, issuer.srv.URL, NewFakeClock(testStart))
	ctx := context.Background()
	runs := map[string]ScenarioResult{}
	for i, s := range []struct {
		name   string
		amount int64
	}{
		{ScenarioClearWithTip, 5_000},
		{ScenarioPartialClear, 10_000},
		{ScenarioReversalBeforeAuth, 3_000},
		{ScenarioOfflinePurchase, 1_200},
		{ScenarioLateRefund, 2_500},
		{ScenarioStandIn, 8_000},
	} {
		res, err := n.RunScenario(ctx, ScenarioRequest{Scenario: s.name, CardID: "card_test", Amount: s.amount, Seed: seed(uint64(i + 1))})
		if err != nil {
			t.Fatalf("%s: %v", s.name, err)
		}
		runs[s.name] = res
	}
	declined, err := n.RunScenario(ctx, ScenarioRequest{Scenario: ScenarioStandIn, CardID: "card_test", Amount: 20_000, Seed: seed(99)})
	if err != nil || declined.Authorizations[0].Decision != DecisionDeclined || len(declined.ClearingDates) != 0 {
		t.Fatalf("over-limit stand-in should be declined and clear nothing: %+v, %v", declined, err)
	}
	if got := runs[ScenarioReversalBeforeAuth].ClearingDates; len(got) != 0 {
		t.Fatalf("a reversed authorization clears on %v", got)
	}

	file, err := n.ClearingFile("2026-10-18")
	if err != nil {
		t.Fatal(err)
	}
	lines := parseClearing(t, file)
	byAuth := map[string]map[string]string{}
	for _, l := range lines {
		if l["currency"] != "USD" || l["card_id"] != "card_test" || l["cleared_at"] != "2026-10-18T10:00:00Z" {
			t.Errorf("line %v", l)
		}
		byAuth[l["auth_id"]] = l
	}
	authID := func(name string) string { return runs[name].Authorizations[0].AuthID }

	want := map[string]string{
		authID(ScenarioClearWithTip): "6000",
		authID(ScenarioPartialClear): "7000",
		authID(ScenarioLateRefund):   "2500",
		authID(ScenarioStandIn):      "8000",
		"":                           "1200",
	}
	if len(lines) != len(want) {
		t.Fatalf("want %d lines, got %d:\n%s", len(want), len(lines), file)
	}
	for id, amount := range want {
		l, ok := byAuth[id]
		if !ok || l["amount"] != amount || l["kind"] != ClearingPurchase {
			t.Errorf("auth %q: want purchase %s, got %v", id, amount, l)
		}
	}
	if _, ok := byAuth[authID(ScenarioReversalBeforeAuth)]; ok {
		t.Error("the reversed authorization cleared")
	}

	refundDates := runs[ScenarioLateRefund].ClearingDates
	if len(refundDates) != 2 || refundDates[0] != "2026-10-18" || refundDates[1] < "2026-10-25" || refundDates[1] > "2026-11-17" {
		t.Fatalf("late refund dates %v", refundDates)
	}
	refundFile, err := n.ClearingFile(refundDates[1])
	if err != nil {
		t.Fatal(err)
	}
	refunds := parseClearing(t, refundFile)
	if len(refunds) != 1 || refunds[0]["kind"] != ClearingRefund || refunds[0]["amount"] != "2500" ||
		refunds[0]["auth_id"] != authID(ScenarioLateRefund) {
		t.Fatalf("refund file:\n%s", refundFile)
	}
}

func TestIncrementalClearsTheChainTotalOnTheInitialAuth(t *testing.T) {
	issuer := newFakeIssuer(t, nil)
	n := newTestNetwork(t, issuer.srv.URL, NewFakeClock(testStart))
	res, err := n.RunScenario(context.Background(), ScenarioRequest{Scenario: ScenarioIncremental, CardID: "card_test", Amount: 10_000, Seed: seed(3)})
	if err != nil {
		t.Fatal(err)
	}
	file, err := n.ClearingFile(res.ClearingDates[0])
	if err != nil {
		t.Fatal(err)
	}
	lines := parseClearing(t, file)
	if len(lines) != 1 || lines[0]["auth_id"] != res.Authorizations[0].AuthID || lines[0]["amount"] != "15000" {
		t.Fatalf("clearing:\n%s", file)
	}
}

func TestClearingExportIsStable(t *testing.T) {
	issuer := newFakeIssuer(t, nil)
	n := newTestNetwork(t, issuer.srv.URL, NewFakeClock(testStart))
	for i, name := range Scenarios {
		if _, err := n.RunScenario(context.Background(), ScenarioRequest{Scenario: name, CardID: "card_test", Amount: 1_000, Seed: seed(uint64(i))}); err != nil {
			t.Fatal(err)
		}
	}
	first, err := n.ClearingFile("2026-10-18")
	if err != nil {
		t.Fatal(err)
	}
	second, _ := n.ClearingFile("2026-10-18")
	if !bytes.Equal(first, second) {
		t.Fatal("same date, different bytes")
	}
	if lines := parseClearing(t, first); len(lines) < 2 {
		t.Fatalf("expected a populated file:\n%s", first)
	} else {
		for i := 1; i < len(lines); i++ {
			prev, cur := lines[i-1], lines[i]
			if prev["cleared_at"] > cur["cleared_at"] || (prev["cleared_at"] == cur["cleared_at"] && prev["clearing_id"] >= cur["clearing_id"]) {
				t.Fatalf("lines out of order:\n%s", first)
			}
		}
	}

	empty, err := n.ClearingFile("2026-01-01")
	if err != nil || string(empty) != "clearing_id,auth_id,card_id,kind,amount,currency,merchant_id,cleared_at\n" {
		t.Fatalf("empty date: %q, %v", empty, err)
	}
	if _, err := n.ClearingFile("18/10/2026"); !errors.Is(err, ErrInvalidDate) {
		t.Fatalf("want ErrInvalidDate, got %v", err)
	}
}
