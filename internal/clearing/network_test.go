package clearing

import (
	"context"
	"net/http/httptest"
	"slices"
	"testing"
	"time"

	"github.com/Shailu-s/payments-platform/internal/api"
	"github.com/Shailu-s/payments-platform/internal/cardnetwork"
)

const testNetworkSecret = "test-only-network-secret-never-use-in-production"

// The simulator authorizes against the real issuer handler, then hands us its
// clearing files. Imported, they must turn every hold the network says cleared
// into a charge, leave nothing held, and keep the ledger and balances in step.
func TestNetworkScenariosClearEndToEnd(t *testing.T) {
	resetDB(t)
	ctx := context.Background()
	issuer := httptest.NewServer(api.NewServer(testPool, []byte("test-webhook-secret-0123456789abcdef"), []byte(testNetworkSecret)).Handler())
	t.Cleanup(issuer.Close)
	network, err := cardnetwork.New(cardnetwork.Config{
		IssuerURL:    issuer.URL,
		Secret:       []byte(testNetworkSecret),
		AuthDeadline: 2 * time.Second,
		StandInLimit: 10_000,
		Clock:        cardnetwork.NewFakeClock(time.Now()),
	})
	if err != nil {
		t.Fatal(err)
	}
	const balance = 100_000
	card := newCard(t, balance)

	const amount = 2500
	// What each scenario's purchase line charges, from the contract.
	cleared := map[string]int64{
		cardnetwork.ScenarioApproveAndClear:    amount,
		cardnetwork.ScenarioClearWithTip:       amount * 6 / 5,
		cardnetwork.ScenarioPartialClear:       amount * 7 / 10,
		cardnetwork.ScenarioDuplicateAuth:      amount,
		cardnetwork.ScenarioTimeoutThenRetry:   amount,
		cardnetwork.ScenarioStandIn:            amount,
		cardnetwork.ScenarioReversalBeforeAuth: 0,
		cardnetwork.ScenarioIncremental:        amount + amount/2,
		cardnetwork.ScenarioOfflinePurchase:    amount,
		cardnetwork.ScenarioLateRefund:         amount,
	}
	var dates, authIDs []string
	var total int64
	for i, scenario := range cardnetwork.Scenarios {
		seed := uint64(1300 + i)
		result, err := network.RunScenario(ctx, cardnetwork.ScenarioRequest{
			Scenario: scenario, CardID: card.ID, Amount: amount, Seed: &seed,
		})
		if err != nil {
			t.Fatalf("%s: %v", scenario, err)
		}
		for _, d := range result.ClearingDates {
			if !slices.Contains(dates, d) {
				dates = append(dates, d)
			}
		}
		// The first authorization is the one holding the money, an incremental's too.
		if len(result.Authorizations) > 0 {
			authIDs = append(authIDs, result.Authorizations[0].AuthID)
		}
		total += cleared[scenario]
	}
	slices.Sort(dates)

	statuses := map[string]int{}
	flags := map[string]int{}
	for _, date := range dates {
		raw, err := network.ClearingFile(date)
		if err != nil {
			t.Fatal(err)
		}
		day, _ := time.Parse(DateLayout, date)
		s, err := Import(ctx, testPool, raw, day, testConfig)
		if err != nil {
			t.Fatalf("import %s: %v", date, err)
		}
		for k, v := range s.Statuses {
			statuses[k] += v
		}
		for k, v := range s.Flags {
			flags[k] += v
		}
		// The same file again is a replay that changes nothing.
		before := moneyState(t)
		if again, err := Import(ctx, testPool, raw, day, testConfig); err != nil || !again.Replayed || again.Processed != 0 {
			t.Errorf("re-import %s = %+v, %v", date, again, err)
		}
		if moneyState(t) != before {
			t.Errorf("re-importing %s changed state", date)
		}
	}

	// Nine purchases (a full reversal clears nothing) and the late refund.
	if statuses[StatusPosted] != 10 || statuses[StatusDeferred] != 0 || statuses[StatusRejected] != 0 {
		t.Errorf("statuses = %v, want 10 posted", statuses)
	}
	if len(flags) != 1 || flags[FlagUnauthorizedClearing] != 1 {
		t.Errorf("flags = %v, want only the offline purchase as unauthorized_clearing", flags)
	}
	for _, id := range authIDs {
		if a := loadAuth(t, id); a.held != 0 {
			t.Errorf("%s still holds %d after clearing", id, a.held)
		}
	}
	if got := storedBalance(t, card.HeldAccountID); got != 0 {
		t.Errorf("held = %d, want 0", got)
	}
	// The late refund gives its purchase back.
	if got := payable(t); got != total-amount {
		t.Errorf("payable = %d, want %d", got, total-amount)
	}
	if got := storedBalance(t, card.AccountID); got != balance-total+amount {
		t.Errorf("available = %d, want %d", got, balance-total+amount)
	}
	assertNoDrift(t)
}

// A purchase cleared the next day and refunded weeks later, both through the
// real issuer: the refund gives the whole charge back as a new transaction.
func TestLateRefundScenarioEndToEnd(t *testing.T) {
	resetDB(t)
	ctx := context.Background()
	issuer := httptest.NewServer(api.NewServer(testPool, []byte("test-webhook-secret-0123456789abcdef"), []byte(testNetworkSecret)).Handler())
	t.Cleanup(issuer.Close)
	network, err := cardnetwork.New(cardnetwork.Config{
		IssuerURL:    issuer.URL,
		Secret:       []byte(testNetworkSecret),
		AuthDeadline: 2 * time.Second,
		StandInLimit: 10_000,
		Clock:        cardnetwork.NewFakeClock(time.Now()),
	})
	if err != nil {
		t.Fatal(err)
	}
	const balance, amount = 50_000, 2500
	card := newCard(t, balance)
	seed := uint64(1400)
	result, err := network.RunScenario(ctx, cardnetwork.ScenarioRequest{
		Scenario: cardnetwork.ScenarioLateRefund, CardID: card.ID, Amount: amount, Seed: &seed,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.ClearingDates) != 2 || len(result.Authorizations) != 1 {
		t.Fatalf("scenario = %+v, want one authorization and two clearing dates", result)
	}
	authID := result.Authorizations[0].AuthID

	for i, date := range result.ClearingDates {
		raw, err := network.ClearingFile(date)
		if err != nil {
			t.Fatal(err)
		}
		day, _ := time.Parse(DateLayout, date)
		s, err := Import(ctx, testPool, raw, day, testConfig)
		if err != nil {
			t.Fatalf("import %s: %v", date, err)
		}
		if s.Statuses[StatusPosted] != 1 || len(s.Flags) != 0 {
			t.Errorf("file %d (%s) = %+v, want 1 posted, unflagged", i, date, s)
		}
		if i == 0 {
			if got := storedBalance(t, card.AccountID); got != balance-amount {
				t.Errorf("available after the purchase = %d, want %d", got, balance-amount)
			}
		}
	}
	if got := storedBalance(t, card.AccountID); got != balance {
		t.Errorf("available = %d, want %d", got, balance)
	}
	if got := storedBalance(t, card.HeldAccountID); got != 0 {
		t.Errorf("held = %d, want 0", got)
	}
	if got := payable(t); got != 0 {
		t.Errorf("payable = %d, want 0", got)
	}
	if a := loadAuth(t, authID); a.cleared != amount || a.held != 0 {
		t.Errorf("authorization = %+v, want cleared %d", a, amount)
	}
	if got := refunded(t, authID); got != amount {
		t.Errorf("refunded = %d, want %d", got, amount)
	}
	assertNoDrift(t)
}
