package cardnetwork

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
)

func seed(v uint64) *uint64 { return &v }

// describe renders what the issuer received, naming authorizations A, B, ... in order
// of first appearance so tests can assert that retries reuse the same auth_id.
func describe(t *testing.T, reqs []received) []string {
	t.Helper()
	names := map[string]string{}
	name := func(id string) string {
		if _, ok := names[id]; !ok {
			names[id] = string(rune('A' + len(names)))
		}
		return names[id]
	}
	var out []string
	for _, req := range reqs {
		switch req.Path {
		case PathAuthorizations:
			var a Authorization
			mustUnmarshal(t, req.Body, &a)
			line := fmt.Sprintf("auth %s %s %d", name(a.AuthID), a.Kind, a.Amount)
			if a.OriginalAuthID != "" {
				line += " of " + name(a.OriginalAuthID)
			}
			out = append(out, line)
		case PathReversals:
			var r Reversal
			mustUnmarshal(t, req.Body, &r)
			out = append(out, fmt.Sprintf("reversal of %s %d", name(r.AuthID), r.Amount))
		case PathAdvices:
			var a Advice
			mustUnmarshal(t, req.Body, &a)
			out = append(out, fmt.Sprintf("advice for %s %d", name(a.AuthID), a.Amount))
		default:
			t.Fatalf("unexpected path %s", req.Path)
		}
	}
	return out
}

func mustUnmarshal(t *testing.T, b []byte, v any) {
	t.Helper()
	if err := json.Unmarshal(b, v); err != nil {
		t.Fatalf("decode %s: %v", b, err)
	}
}

func TestEachScenarioProducesItsMessages(t *testing.T) {
	cases := []struct {
		scenario string
		want     []string
		standIn  bool
	}{
		{ScenarioApproveAndClear, []string{"auth A initial 10000"}, false},
		{ScenarioClearWithTip, []string{"auth A initial 10000"}, false},
		{ScenarioPartialClear, []string{"auth A initial 10000"}, false},
		{ScenarioDuplicateAuth, []string{"auth A initial 10000", "auth A initial 10000"}, false},
		{ScenarioTimeoutThenRetry, []string{"auth A initial 10000", "auth A initial 10000"}, false},
		{ScenarioStandIn, []string{"auth A initial 10000", "auth A initial 10000", "advice for A 10000"}, true},
		{ScenarioReversalBeforeAuth, []string{"reversal of A 10000", "auth A initial 10000"}, false},
		{ScenarioIncremental, []string{"auth A initial 10000", "auth B incremental 5000 of A"}, false},
		{ScenarioOfflinePurchase, nil, false},
		{ScenarioLateRefund, []string{"auth A initial 10000"}, false},
	}
	if len(cases) != len(Scenarios) {
		t.Fatalf("table covers %d of %d scenarios", len(cases), len(Scenarios))
	}
	for _, tc := range cases {
		t.Run(tc.scenario, func(t *testing.T) {
			issuer := newFakeIssuer(t, nil)
			n := newTestNetwork(t, issuer.srv.URL, NewFakeClock(testStart))
			res, err := n.RunScenario(context.Background(), ScenarioRequest{
				Scenario: tc.scenario, CardID: "card_test", Amount: 10_000, Seed: seed(7),
			})
			if err != nil {
				t.Fatal(err)
			}
			got := describe(t, issuer.requests())
			if !equalStrings(got, tc.want) {
				t.Fatalf("issuer received\n  %q\nwant\n  %q", got, tc.want)
			}
			for _, a := range res.Authorizations {
				if a.Decision != DecisionApproved || a.StandIn != tc.standIn {
					t.Fatalf("authorization result %+v", a)
				}
			}
		})
	}
}

func TestSameSeedSameBytes(t *testing.T) {
	play := func() ([]received, []byte, []byte) {
		issuer := newFakeIssuer(t, nil)
		n := newTestNetwork(t, issuer.srv.URL, NewFakeClock(testStart))
		var dates []string
		for i, name := range Scenarios {
			res, err := n.RunScenario(context.Background(), ScenarioRequest{
				Scenario: name, CardID: "card_test", Amount: 4_000 + int64(i), Seed: seed(uint64(100 + i)),
			})
			if err != nil {
				t.Fatalf("%s: %v", name, err)
			}
			dates = append(dates, res.ClearingDates...)
		}
		journal, err := json.Marshal(n.Journal())
		if err != nil {
			t.Fatal(err)
		}
		var clearing []byte
		for _, d := range dates {
			file, err := n.ClearingFile(d)
			if err != nil {
				t.Fatal(err)
			}
			clearing = append(clearing, file...)
		}
		return issuer.requests(), journal, clearing
	}
	firstReqs, firstJournal, firstClearing := play()
	secondReqs, secondJournal, secondClearing := play()

	if len(firstReqs) != len(secondReqs) {
		t.Fatalf("%d messages vs %d", len(firstReqs), len(secondReqs))
	}
	for i := range firstReqs {
		if string(firstReqs[i].Body) != string(secondReqs[i].Body) ||
			firstReqs[i].Header.Get(SignatureHeader) != secondReqs[i].Header.Get(SignatureHeader) {
			t.Fatalf("message %d differs:\n%s\n%s", i, firstReqs[i].Body, secondReqs[i].Body)
		}
	}
	if string(firstJournal) != string(secondJournal) {
		t.Fatal("journals differ")
	}
	if len(firstClearing) == 0 || string(firstClearing) != string(secondClearing) {
		t.Fatalf("clearing files differ:\n%s\n%s", firstClearing, secondClearing)
	}
}

func TestDifferentSeedsDifferentIDsAndReusedSeedsAreRefused(t *testing.T) {
	issuer := newFakeIssuer(t, nil)
	n := newTestNetwork(t, issuer.srv.URL, NewFakeClock(testStart))
	ctx := context.Background()
	req := ScenarioRequest{Scenario: ScenarioApproveAndClear, CardID: "card_test", Amount: 100, Seed: seed(1)}

	first, err := n.RunScenario(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := n.RunScenario(ctx, req); !errors.Is(err, ErrIDReused) {
		t.Fatalf("want ErrIDReused for a reused seed, got %v", err)
	}
	if got := len(issuer.requests()); got != 1 {
		t.Fatalf("a refused scenario sent messages: %d requests", got)
	}
	req.Seed = seed(2)
	second, err := n.RunScenario(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if first.Authorizations[0].AuthID == second.Authorizations[0].AuthID {
		t.Fatal("different seeds produced the same auth_id")
	}

	unseeded, err := n.RunScenario(ctx, ScenarioRequest{Scenario: ScenarioApproveAndClear, CardID: "card_test", Amount: 100})
	if err != nil || unseeded.Seed > maxSeed {
		t.Fatalf("unseeded run: seed %d, %v", unseeded.Seed, err)
	}
}
