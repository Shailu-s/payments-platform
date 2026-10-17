package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"testing"

	"github.com/Shailu-s/payments-platform/internal/cardnetwork"
)

func reversalBody(reversalID, authID string, amount int64) string {
	return fmt.Sprintf(`{"reversal_id":%q,"auth_id":%q,"amount":%d,"sent_at":"2026-10-17T10:00:00Z"}`,
		reversalID, authID, amount)
}

// tryReverse delivers like the network: anything but a 2xx is redelivered.
func tryReverse(h http.Handler, reversalID, authID string, amount int64) (int, string) {
	var code int
	var status string
	for range 20 {
		rec := signedNetwork(h, cardnetwork.PathReversals, reversalBody(reversalID, authID, amount))
		code = rec.Code
		var body map[string]string
		_ = json.Unmarshal(rec.Body.Bytes(), &body)
		status = body["status"]
		if code != http.StatusServiceUnavailable {
			break
		}
	}
	return code, status
}

func reverse(t *testing.T, h http.Handler, reversalID, authID string, amount int64) string {
	t.Helper()
	code, status := tryReverse(h, reversalID, authID, amount)
	if code != http.StatusOK {
		t.Fatalf("reversal %s = %d (%s)", reversalID, code, status)
	}
	return status
}

func releases(t *testing.T) int {
	t.Helper()
	return countWhere(t, `SELECT count(*) FROM ledger_transactions WHERE reference LIKE 'release %'`)
}

// The network redelivers a reversal until it hears 2xx, and may send it twice
// at once: the reversal_id is claimed once, so the hold is released once.
func TestReversalReleasesTheHoldOnce(t *testing.T) {
	h, apiKey := newTestServer(t)
	c := newCard(t, h, apiKey, 10000, 5000, 20000)
	authorizeTap(t, h, tap{authID: "auth_void", cardID: c.card.ID, amount: 5000})

	const n = 20
	statuses := make([]string, n)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			code, status := tryReverse(h, "rev_void", "auth_void", 5000)
			statuses[i] = fmt.Sprintf("%d %s", code, status)
		}()
	}
	close(start)
	wg.Wait()

	released := 0
	for i, s := range statuses {
		switch s {
		case "200 released":
			released++
		case "200 already_processed":
		default:
			t.Errorf("delivery %d = %s", i, s)
		}
	}
	if released != 1 {
		t.Errorf("released %d times, want 1", released)
	}
	if got := releases(t); got != 1 {
		t.Errorf("release transactions = %d, want 1", got)
	}
	if storedBalance(t, c.funding) != 10000 || storedBalance(t, c.card.HeldAccountID) != 0 {
		t.Errorf("available %d, held %d, want 10000 and 0",
			storedBalance(t, c.funding), storedBalance(t, c.card.HeldAccountID))
	}
	if got := heldOn(t, "auth_void"); got != 0 {
		t.Errorf("held_amount = %d, want 0", got)
	}
	if got := countWhere(t, `SELECT count(*) FROM outbox_events WHERE event_type = 'card.authorization.reversed'`); got != 1 {
		t.Errorf("reversed events = %d, want 1", got)
	}
	assertNoDrift(t)
}

// Partial reversals that add up to more than the hold release the hold and
// no more: the held account never goes negative and no money is invented.
func TestDifferentReversalsNeverReleaseMoreThanHeld(t *testing.T) {
	h, apiKey := newTestServer(t)
	c := newCard(t, h, apiKey, 10000, 5000, 20000)
	// A second hold on the same held account: over-release would eat into it
	// rather than fail the balance CHECK.
	authorizeTap(t, h, tap{authID: "auth_partial", cardID: c.card.ID, amount: 5000})
	authorizeTap(t, h, tap{authID: "auth_other", cardID: c.card.ID, amount: 3000})

	const n = 10
	statuses := make([]string, n)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			code, status := tryReverse(h, fmt.Sprintf("rev_partial_%d", i), "auth_partial", 2000)
			statuses[i] = fmt.Sprintf("%d %s", code, status)
		}()
	}
	close(start)
	wg.Wait()

	released := 0
	for i, s := range statuses {
		switch s {
		case "200 released":
			released++
		case "200 nothing_held":
		default:
			t.Errorf("reversal %d = %s", i, s)
		}
	}
	// 2000 + 2000 + 1000.
	if released != 3 {
		t.Errorf("reversals that released = %d, want 3", released)
	}
	if got := heldOn(t, "auth_partial"); got != 0 {
		t.Errorf("held_amount = %d, want 0", got)
	}
	if got := storedBalance(t, c.card.HeldAccountID); got != 3000 {
		t.Errorf("held = %d, want 3000: the other hold untouched", got)
	}
	if got := storedBalance(t, c.funding); got != 7000 {
		t.Errorf("available = %d, want 7000", got)
	}
	assertNoDrift(t)
}

// A reversal may arrive before the authorization it cancels. It is parked,
// and the approval applies it in the same transaction as the hold.
func TestReversalBeforeAuthorizationIsAppliedOnArrival(t *testing.T) {
	t.Run("full", func(t *testing.T) {
		h, apiKey := newTestServer(t)
		c := newCard(t, h, apiKey, 10000, 5000, 20000)
		if got := reverse(t, h, "rev_early", "auth_early", 5000); got != "pending" {
			t.Fatalf("status = %q, want pending", got)
		}
		// Redelivered before the authorization: still one parked reversal.
		if got := reverse(t, h, "rev_early", "auth_early", 5000); got != "already_processed" {
			t.Errorf("redelivery status = %q, want already_processed", got)
		}

		// The network decided the order; the merchant hears approved.
		if got := authorizeTap(t, h, tap{authID: "auth_early", cardID: c.card.ID, amount: 5000}); got.Decision != "approved" || got.ApprovedAmount != 5000 {
			t.Fatalf("answer = %+v, want approved 5000", got)
		}
		if storedBalance(t, c.funding) != 10000 || storedBalance(t, c.card.HeldAccountID) != 0 {
			t.Errorf("available %d, held %d, want 10000 and 0",
				storedBalance(t, c.funding), storedBalance(t, c.card.HeldAccountID))
		}
		if holdCount(t) != 1 || releases(t) != 1 {
			t.Errorf("holds %d, releases %d, want 1 and 1: the ledger shows both", holdCount(t), releases(t))
		}
		if got := countWhere(t, `SELECT count(*) FROM pending_reversals WHERE applied_at IS NULL`); got != 0 {
			t.Errorf("unapplied reversals = %d, want 0", got)
		}
		// Asked again, the same answer, and nothing moves twice.
		authorizeTap(t, h, tap{authID: "auth_early", cardID: c.card.ID, amount: 5000})
		if got := releases(t); got != 1 {
			t.Errorf("releases after a retry = %d, want 1", got)
		}
		assertNoDrift(t)
	})

	t.Run("partial, by stand-in advice", func(t *testing.T) {
		h, apiKey := newTestServer(t)
		c := newCard(t, h, apiKey, 10000, 5000, 20000)
		reverse(t, h, "rev_advised", "auth_advised", 1500)
		advise(t, h, "adv_advised", "auth_advised", c.card.ID, 4000)

		if got := heldOn(t, "auth_advised"); got != 2500 {
			t.Errorf("held_amount = %d, want 2500", got)
		}
		if got := storedBalance(t, c.card.HeldAccountID); got != 2500 {
			t.Errorf("held = %d, want 2500", got)
		}
		assertNoDrift(t)
	})

	t.Run("racing its authorization", func(t *testing.T) {
		h, apiKey := newTestServer(t)
		c := newCard(t, h, apiKey, 1_000_000, 5000, 1_000_000)

		const n = 20
		var wg sync.WaitGroup
		start := make(chan struct{})
		for i := range n {
			authID := fmt.Sprintf("auth_race_%d", i)
			wg.Add(2)
			go func() {
				defer wg.Done()
				<-start
				tryAuthorize(h, tap{authID: authID, cardID: c.card.ID, amount: 1000})
			}()
			go func() {
				defer wg.Done()
				<-start
				tryReverse(h, "rev_"+authID, authID, 1000)
			}()
		}
		close(start)
		wg.Wait()

		if got := countWhere(t, `SELECT count(*) FROM card_authorizations WHERE decision = 'approved'`); got != n {
			t.Fatalf("approved = %d, want %d", got, n)
		}
		if got := countWhere(t, `SELECT count(*) FROM card_authorizations WHERE held_amount > 0`); got != 0 {
			t.Errorf("authorizations still holding = %d, want 0: a reversal was lost", got)
		}
		if got := countWhere(t, `SELECT count(*) FROM pending_reversals WHERE applied_at IS NULL`); got != 0 {
			t.Errorf("unapplied reversals = %d, want 0", got)
		}
		if got := storedBalance(t, c.card.HeldAccountID); got != 0 {
			t.Errorf("held = %d, want 0", got)
		}
		assertNoDrift(t)
	})
}

func TestReversalOfDeclinedOrUnknownAuthorizationMovesNoMoney(t *testing.T) {
	h, apiKey := newTestServer(t)
	c := newCard(t, h, apiKey, 10000, 5000, 20000)
	authorizeTap(t, h, tap{authID: "auth_held_too", cardID: c.card.ID, amount: 2000})
	if got := authorizeTap(t, h, tap{authID: "auth_declined", cardID: c.card.ID, amount: 6000}); got.Decision != "declined" {
		t.Fatalf("answer = %+v, want declined", got)
	}

	// Parked, not released: a stand-in advice could still approve it.
	if got := reverse(t, h, "rev_declined", "auth_declined", 6000); got != "pending" {
		t.Errorf("declined: status = %q, want pending", got)
	}
	if got := reverse(t, h, "rev_unknown", "auth_never", 2000); got != "pending" {
		t.Errorf("unknown: status = %q, want pending", got)
	}

	if got := releases(t); got != 0 {
		t.Errorf("releases = %d, want 0", got)
	}
	if storedBalance(t, c.funding) != 8000 || storedBalance(t, c.card.HeldAccountID) != 2000 {
		t.Errorf("available %d, held %d, want 8000 and 2000",
			storedBalance(t, c.funding), storedBalance(t, c.card.HeldAccountID))
	}
	assertNoDrift(t)
}

// Released money is no longer spent: it comes off the day's counter.
func TestReleaseFreesDailyLimit(t *testing.T) {
	h, apiKey := newTestServer(t)
	c := newCard(t, h, apiKey, 100000, 5000, 5000)
	authorizeTap(t, h, tap{authID: "auth_day_1", cardID: c.card.ID, amount: 4000})
	if got := authorizeTap(t, h, tap{authID: "auth_day_2", cardID: c.card.ID, amount: 3000}); got.DeclineReason != "daily_limit_exceeded" {
		t.Fatalf("second tap = %+v, want daily_limit_exceeded", got)
	}

	reverse(t, h, "rev_day_1", "auth_day_1", 4000)

	if got := authorizeTap(t, h, tap{authID: "auth_day_3", cardID: c.card.ID, amount: 3000}); got.Decision != "approved" {
		t.Errorf("tap after the reversal = %+v, want approved", got)
	}
	if got := countWhere(t, `SELECT amount::int FROM card_daily_spend WHERE card_id = $1`, c.card.ID); got != 3000 {
		t.Errorf("daily spend = %d, want 3000", got)
	}
	assertNoDrift(t)
}
