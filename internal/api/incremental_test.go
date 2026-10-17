package api

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/Shailu-s/payments-platform/internal/cardnetwork"
	"github.com/Shailu-s/payments-platform/internal/holds"
)

func approvedOn(t *testing.T, authID string) int64 {
	t.Helper()
	return int64(countWhere(t, `SELECT approved_amount::int FROM card_authorizations WHERE auth_id = $1`, authID))
}

func dailySpend(t *testing.T, cardID string) int64 {
	t.Helper()
	return int64(countWhere(t, `SELECT coalesce(sum(amount), 0)::int FROM card_daily_spend WHERE card_id = $1`, cardID))
}

// A hotel adds a night: the increase is held on top of the original, and the
// chain's whole hold lives on the initial row, which clearing and reversals
// refer to.
func TestIncrementalRaisesTheOriginalHold(t *testing.T) {
	h, apiKey := newTestServer(t)
	c := newCard(t, h, apiKey, 10000, 8000, 20000)
	authorizeTap(t, h, tap{authID: "auth_hotel", cardID: c.card.ID, amount: 4000})

	inc := tap{authID: "auth_hotel_night", cardID: c.card.ID, amount: 2000, kind: "incremental", original: "auth_hotel"}
	want := networkAnswer{AuthID: "auth_hotel_night", Decision: "approved", ApprovedAmount: 2000}
	if got := authorizeTap(t, h, inc); got != want {
		t.Fatalf("answer = %+v, want %+v", got, want)
	}
	// Asked again: the same answer, nothing raised twice.
	if got := authorizeTap(t, h, inc); got != want {
		t.Errorf("second answer = %+v, want %+v", got, want)
	}

	if approvedOn(t, "auth_hotel") != 6000 || heldOn(t, "auth_hotel") != 6000 {
		t.Errorf("original approved %d, held %d, want 6000 and 6000", approvedOn(t, "auth_hotel"), heldOn(t, "auth_hotel"))
	}
	if approvedOn(t, "auth_hotel_night") != 2000 || heldOn(t, "auth_hotel_night") != 0 {
		t.Errorf("incremental approved %d, held %d, want 2000 and 0",
			approvedOn(t, "auth_hotel_night"), heldOn(t, "auth_hotel_night"))
	}
	if storedBalance(t, c.funding) != 4000 || storedBalance(t, c.card.HeldAccountID) != 6000 {
		t.Errorf("available %d, held %d, want 4000 and 6000",
			storedBalance(t, c.funding), storedBalance(t, c.card.HeldAccountID))
	}
	if got := dailySpend(t, c.card.ID); got != 6000 {
		t.Errorf("daily spend = %d, want 6000", got)
	}
	if got := holdCount(t); got != 2 {
		t.Errorf("holds = %d, want 2", got)
	}

	// Reversing the incremental releases from the chain's hold on the original.
	reverse(t, h, "rev_night", "auth_hotel_night", 2000)
	if got := heldOn(t, "auth_hotel"); got != 4000 {
		t.Errorf("original held after reversing the night = %d, want 4000", got)
	}
	reverse(t, h, "rev_hotel", "auth_hotel", 6000)
	if storedBalance(t, c.funding) != 10000 || storedBalance(t, c.card.HeldAccountID) != 0 {
		t.Errorf("after a full reversal: available %d, held %d, want 10000 and 0",
			storedBalance(t, c.funding), storedBalance(t, c.card.HeldAccountID))
	}
	assertNoDrift(t)
}

// The network's incremental scenario end to end: the clearing line is the
// approved total on the initial auth_id, so that is what we must hold there.
func TestIncrementalScenario(t *testing.T) {
	h, apiKey := newTestServer(t)
	network := newTestNetwork(t, h)
	c := newCard(t, h, apiKey, 100_000, 50_000, 200_000)

	seed := uint64(300)
	result, err := network.RunScenario(context.Background(), cardnetwork.ScenarioRequest{
		Scenario: cardnetwork.ScenarioIncremental, CardID: c.card.ID, Amount: 4000, Seed: &seed,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Authorizations) != 2 {
		t.Fatalf("authorizations = %+v, want two", result.Authorizations)
	}
	initial, increment := result.Authorizations[0], result.Authorizations[1]
	if initial.Decision != cardnetwork.DecisionApproved || increment.Decision != cardnetwork.DecisionApproved ||
		increment.ApprovedAmount != 2000 {
		t.Fatalf("network decided %+v and %+v, want both approved, the increase 2000", initial, increment)
	}
	if approvedOn(t, initial.AuthID) != 6000 || heldOn(t, initial.AuthID) != 6000 {
		t.Errorf("initial approved %d, held %d, want 6000 and 6000", approvedOn(t, initial.AuthID), heldOn(t, initial.AuthID))
	}
	if got := storedBalance(t, c.card.HeldAccountID); got != 6000 {
		t.Errorf("held = %d, want 6000", got)
	}
	assertNoDrift(t)
}

// Each refusal is a stored answer and moves no money: not on the original,
// not in the ledger, not on the daily counter.
func TestIncrementalDeclines(t *testing.T) {
	h, apiKey := newTestServer(t)
	c := newCard(t, h, apiKey, 9000, 6000, 20000, "7995")
	other := newCard(t, h, apiKey, 10000, 6000, 20000)
	tight := newCard(t, h, apiKey, 10000, 6000, 6000)
	frozen := newCard(t, h, apiKey, 10000, 6000, 20000)

	authorizeTap(t, h, tap{authID: "auth_base", cardID: c.card.ID, amount: 4000})
	authorizeTap(t, h, tap{authID: "auth_declined_base", cardID: c.card.ID, amount: 7000})
	authorizeTap(t, h, tap{authID: "auth_reversed_base", cardID: c.card.ID, amount: 1000})
	reverse(t, h, "rev_reversed_base", "auth_reversed_base", 1000)
	authorizeTap(t, h, tap{authID: "auth_expired_base", cardID: c.card.ID, amount: 1000})
	if _, err := testPool.Exec(context.Background(),
		`UPDATE card_authorizations SET created_at = now() - interval '8 days' WHERE auth_id = 'auth_expired_base'`); err != nil {
		t.Fatal(err)
	}
	if _, err := holds.ExpireBatch(context.Background(), testPool, time.Now(), 7*24*time.Hour, 10); err != nil {
		t.Fatal(err)
	}
	authorizeTap(t, h, tap{authID: "auth_tight_base", cardID: tight.card.ID, amount: 4000})
	authorizeTap(t, h, tap{authID: "auth_tight_other", cardID: tight.card.ID, amount: 1500})
	authorizeTap(t, h, tap{authID: "auth_frozen_base", cardID: frozen.card.ID, amount: 1000})
	if rec := do(t, h, "POST", "/v1/cards/"+frozen.card.ID+"/freeze", apiKey, ""); rec.Code != http.StatusOK {
		t.Fatalf("freeze = %d", rec.Code)
	}
	// 500 left available on c.
	authorizeTap(t, h, tap{authID: "auth_drain", cardID: c.card.ID, amount: 4500})

	cases := []struct {
		name   string
		tap    tap
		reason string
	}{
		{"unknown original", tap{cardID: c.card.ID, amount: 100, original: "auth_nowhere"}, "original_not_found"},
		{"original is itself an incremental", tap{cardID: c.card.ID, amount: 100, original: "auth_inc_0"}, "original_not_found"},
		{"declined original", tap{cardID: c.card.ID, amount: 100, original: "auth_declined_base"}, "original_not_approved"},
		{"reversed original", tap{cardID: c.card.ID, amount: 100, original: "auth_reversed_base"}, "original_expired"},
		{"expired original", tap{cardID: c.card.ID, amount: 100, original: "auth_expired_base"}, "original_expired"},
		{"another card's original", tap{cardID: other.card.ID, amount: 100, original: "auth_base"}, "card_mismatch"},
		{"chain over the per-transaction limit", tap{cardID: c.card.ID, amount: 2001, original: "auth_base"}, "exceeds_transaction_limit"},
		{"frozen card", tap{cardID: frozen.card.ID, amount: 100, original: "auth_frozen_base"}, "card_inactive"},
		{"blocked mcc", tap{cardID: c.card.ID, amount: 100, mcc: "7995", original: "auth_base"}, "merchant_blocked"},
		{"currency", tap{cardID: c.card.ID, amount: 100, currency: "EUR", original: "auth_base"}, "invalid_currency"},
		// A chain of 4600 is within the limit; 6100 spent today is not.
		{"daily limit", tap{cardID: tight.card.ID, amount: 600, original: "auth_tight_base"}, "daily_limit_exceeded"},
		// 400 available after auth_inc_0.
		{"funds", tap{cardID: c.card.ID, amount: 600, original: "auth_base"}, "insufficient_funds"},
	}
	// An approved incremental, so "original is itself an incremental" has one.
	authorizeTap(t, h, tap{authID: "auth_inc_0", cardID: c.card.ID, amount: 100, kind: "incremental", original: "auth_base"})

	type snapshot struct{ available, held, baseApproved, baseHeld, daily, tightDaily, holds int64 }
	snap := func() snapshot {
		return snapshot{storedBalance(t, c.funding), storedBalance(t, c.card.HeldAccountID),
			approvedOn(t, "auth_base"), heldOn(t, "auth_base"), dailySpend(t, c.card.ID),
			dailySpend(t, tight.card.ID), int64(holdCount(t))}
	}
	before := snap()
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tc.tap.authID, tc.tap.kind = fmt.Sprintf("auth_inc_decline_%d", i), "incremental"
			want := networkAnswer{AuthID: tc.tap.authID, Decision: "declined", DeclineReason: tc.reason}
			if got := authorizeTap(t, h, tc.tap); got != want {
				t.Errorf("answer = %+v, want %+v", got, want)
			}
			if got := authorizeTap(t, h, tc.tap); got != want {
				t.Errorf("second answer = %+v, want %+v", got, want)
			}
		})
	}
	if after := snap(); after != before {
		t.Errorf("money moved on declines: before %+v, after %+v", before, after)
	}
	assertNoDrift(t)
}

// Increments of one chain race: the original's row lock serialises them, so
// the chain never passes the per-transaction limit, and the balance CHECK
// still decides funds.
func TestConcurrentIncrementalsRespectFundsAndLimits(t *testing.T) {
	cases := []struct {
		name                     string
		balance, perTransaction  int64
		wantApproved             int
		wantReason               string
		wantHeld, wantOriginalOn int64
	}{
		// 2000 + 8 × 1000 = the 10000 limit.
		{"per-transaction limit", 1_000_000, 10000, 8, "exceeds_transaction_limit", 10000, 10000},
		// 5000 in the account: 2000 + 3 × 1000.
		{"funds", 5000, 1_000_000, 3, "insufficient_funds", 5000, 5000},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h, apiKey := newTestServer(t)
			c := newCard(t, h, apiKey, tc.balance, tc.perTransaction, 10_000_000)
			authorizeTap(t, h, tap{authID: "auth_chain", cardID: c.card.ID, amount: 2000})

			const n = 20
			answers := make([]networkAnswer, n)
			codes := make([]int, n)
			var wg sync.WaitGroup
			start := make(chan struct{})
			for i := range n {
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-start
					answers[i], codes[i], _ = tryAuthorize(h, tap{authID: fmt.Sprintf("auth_link_%d", i),
						cardID: c.card.ID, amount: 1000, kind: "incremental", original: "auth_chain"})
				}()
			}
			close(start)
			wg.Wait()

			approved, refused := 0, 0
			for i := range n {
				switch {
				case codes[i] != http.StatusOK:
					t.Errorf("request %d = %d", i, codes[i])
				case answers[i].Decision == "approved":
					approved++
				case answers[i].DeclineReason == tc.wantReason:
					refused++
				default:
					t.Errorf("request %d = %+v", i, answers[i])
				}
			}
			if approved != tc.wantApproved || refused != n-tc.wantApproved {
				t.Errorf("approved %d, %s %d, want %d and %d", approved, tc.wantReason, refused, tc.wantApproved, n-tc.wantApproved)
			}
			if got := storedBalance(t, c.card.HeldAccountID); got != tc.wantHeld {
				t.Errorf("held = %d, want %d", got, tc.wantHeld)
			}
			if approvedOn(t, "auth_chain") != tc.wantOriginalOn || heldOn(t, "auth_chain") != tc.wantOriginalOn {
				t.Errorf("original approved %d, held %d, want %d", approvedOn(t, "auth_chain"), heldOn(t, "auth_chain"), tc.wantOriginalOn)
			}
			if got := dailySpend(t, c.card.ID); got != tc.wantHeld {
				t.Errorf("daily spend = %d, want %d", got, tc.wantHeld)
			}
			assertNoDrift(t)
		})
	}
}
