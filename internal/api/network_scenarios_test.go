package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Shailu-s/payments-platform/internal/cardnetwork"
)

// newTestNetwork is the simulator pointed at h.
func newTestNetwork(t *testing.T, h http.Handler) *cardnetwork.Network {
	t.Helper()
	issuer := httptest.NewServer(h)
	t.Cleanup(issuer.Close)

	network, err := cardnetwork.New(cardnetwork.Config{
		IssuerURL:    issuer.URL,
		Secret:       []byte(testNetworkSecret),
		AuthDeadline: 2 * time.Second,
		StandInLimit: 10_000,
		// Lost answers wait out the deadline on this clock, not in real time;
		// it starts now so signatures fall inside the replay window.
		Clock: cardnetwork.NewFakeClock(time.Now()),
	})
	if err != nil {
		t.Fatal(err)
	}
	return network
}

// The simulator is the counterparty: it signs, enforces its deadline, retries,
// stands in and advises exactly as the contract says, against the real handler.
// Afterwards the network's record and ours must tell the same story.
func TestNetworkScenariosAgainstTheIssuer(t *testing.T) {
	h, apiKey := newTestServer(t)
	network := newTestNetwork(t, h)

	c := newCard(t, h, apiKey, 100_000, 50_000, 200_000)
	cases := []struct {
		scenario string
		standIn  bool
		attempts int
	}{
		{cardnetwork.ScenarioApproveAndClear, false, 1},
		{cardnetwork.ScenarioDuplicateAuth, false, 2},
		{cardnetwork.ScenarioTimeoutThenRetry, false, 2},
		{cardnetwork.ScenarioStandIn, true, 2},
	}
	var held int64
	for i, tc := range cases {
		t.Run(tc.scenario, func(t *testing.T) {
			seed := uint64(100 + i)
			result, err := network.RunScenario(context.Background(), cardnetwork.ScenarioRequest{
				Scenario: tc.scenario, CardID: c.card.ID, Amount: 2500, Seed: &seed,
			})
			if err != nil {
				t.Fatal(err)
			}
			if len(result.Authorizations) != 1 {
				t.Fatalf("authorizations = %+v, want one", result.Authorizations)
			}
			got := result.Authorizations[0]
			if got.Decision != cardnetwork.DecisionApproved || got.ApprovedAmount != 2500 ||
				got.StandIn != tc.standIn || got.Attempts != tc.attempts {
				t.Errorf("network decided %+v, want approved 2500, stand_in %v, %d attempts",
					got, tc.standIn, tc.attempts)
			}
			if tc.standIn && got.Advice != cardnetwork.AdviceAcknowledged {
				t.Errorf("advice = %q, want acknowledged", got.Advice)
			}

			a := loadAuth(t, got.AuthID)
			if a.decision != got.Decision || a.approved != got.ApprovedAmount || a.standIn != tc.standIn || a.shortfall != 0 {
				t.Errorf("our row = %+v, network decided %+v", a, got)
			}
			// Every answer the network heard, lost or not, is the stored one.
			for _, e := range result.Journal {
				if e.Message != cardnetwork.MessageAuthorization || len(e.Answer) == 0 {
					continue
				}
				var answer networkAnswer
				if err := json.Unmarshal(e.Answer, &answer); err != nil {
					t.Fatalf("journal answer %s: %v", e.Answer, err)
				}
				if answer.Decision != a.decision || answer.ApprovedAmount != a.approved {
					t.Errorf("attempt %d (%s) heard %+v, we stored %+v", e.Attempt, e.Outcome, answer, a)
				}
			}
			held += 2500
			if got := storedBalance(t, c.card.HeldAccountID); got != held {
				t.Errorf("held = %d, want %d: one hold per authorization", got, held)
			}
		})
	}
	if got := holdCount(t); got != len(cases) {
		t.Errorf("holds = %d, want %d", got, len(cases))
	}
	assertNoDrift(t)
}

// The network sends a full reversal, then the authorization it cancels: it
// clears nothing, so we must end up holding nothing.
func TestReversalBeforeAuthScenario(t *testing.T) {
	h, apiKey := newTestServer(t)
	network := newTestNetwork(t, h)
	c := newCard(t, h, apiKey, 100_000, 50_000, 200_000)

	seed := uint64(200)
	result, err := network.RunScenario(context.Background(), cardnetwork.ScenarioRequest{
		Scenario: cardnetwork.ScenarioReversalBeforeAuth, CardID: c.card.ID, Amount: 2500, Seed: &seed,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Authorizations) != 1 || result.Authorizations[0].Decision != cardnetwork.DecisionApproved {
		t.Fatalf("authorizations = %+v, want one approved", result.Authorizations)
	}
	for _, e := range result.Journal {
		if e.Message == cardnetwork.MessageReversal && e.Outcome != cardnetwork.OutcomeAcknowledged {
			t.Errorf("reversal attempt %d: %s", e.Attempt, e.Outcome)
		}
	}
	if got := heldOn(t, result.Authorizations[0].AuthID); got != 0 {
		t.Errorf("held_amount = %d, want 0", got)
	}
	if storedBalance(t, c.funding) != 100_000 || storedBalance(t, c.card.HeldAccountID) != 0 {
		t.Errorf("available %d, held %d, want 100000 and 0",
			storedBalance(t, c.funding), storedBalance(t, c.card.HeldAccountID))
	}
	assertNoDrift(t)
}
