package holds

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/Shailu-s/payments-platform/internal/authorizations"
)

// Twenty releases of 3000 race for one 5000 hold: the row lock lets them
// release 5000 in total, never more, whatever the interleaving.
func TestReleaseNeverReleasesMoreThanHeld(t *testing.T) {
	resetDB(t)
	card := newCard(t, 10000)
	placeHold(t, card, "auth_race", 5000)

	const n = 20
	released := make([]int64, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			released[i], errs[i] = release(context.Background(), "auth_race", 3000, ReasonReversal, fmt.Sprintf("rev_%d", i))
		}()
	}
	close(start)
	wg.Wait()

	var total int64
	for i := range n {
		if errs[i] != nil {
			t.Errorf("release %d: %v", i, errs[i])
		}
		total += released[i]
	}
	if total != 5000 {
		t.Errorf("released %d in total, want 5000", total)
	}
	if got := storedBalance(t, card.HeldAccountID); got != 0 {
		t.Errorf("held = %d, want 0", got)
	}
	if got := storedBalance(t, card.AccountID); got != 10000 {
		t.Errorf("available = %d, want 10000", got)
	}
	var held, releasedAmount, daily int64
	if err := testPool.QueryRow(context.Background(), `
		SELECT a.held_amount, a.released_amount, d.amount
		FROM card_authorizations a JOIN card_daily_spend d ON d.card_id = a.card_id
		WHERE a.auth_id = 'auth_race'`).Scan(&held, &releasedAmount, &daily); err != nil {
		t.Fatal(err)
	}
	if held != 0 || releasedAmount != 5000 || daily != 0 {
		t.Errorf("held_amount %d, released_amount %d, daily spend %d, want 0, 5000, 0", held, releasedAmount, daily)
	}
	// One ledger move and one event per release that moved money.
	if got := countWhere(t, `SELECT count(*) FROM ledger_transactions WHERE reference LIKE 'release auth_race %'`); got != 2 {
		t.Errorf("release transactions = %d, want 2 (3000 then 2000)", got)
	}
	if got := countWhere(t, `SELECT count(*) FROM outbox_events WHERE event_type = $1`, EventReversed); got != 2 {
		t.Errorf("reversed events = %d, want 2", got)
	}
	assertNoDrift(t)
}

func TestReleaseOfNothingWritesNothing(t *testing.T) {
	resetDB(t)
	card := newCard(t, 1000)
	placeHold(t, card, "auth_small", 1000)
	if n, err := release(context.Background(), "auth_small", 1000, ReasonExpiry, ""); err != nil || n != 1000 {
		t.Fatalf("first release = %d, %v, want 1000", n, err)
	}
	if n, err := release(context.Background(), "auth_small", 1000, ReasonReversal, "rev_late"); err != nil || n != 0 {
		t.Errorf("second release = %d, %v, want 0", n, err)
	}
	if _, err := release(context.Background(), "auth_missing", 1000, ReasonReversal, "rev_x"); !errors.Is(err, authorizations.ErrNotFound) {
		t.Errorf("release of an unknown authorization: %v, want ErrNotFound", err)
	}
	if got := countWhere(t, `SELECT count(*) FROM outbox_events WHERE event_type LIKE 'card.authorization.%'`); got != 1 {
		t.Errorf("release events = %d, want 1", got)
	}
	if got := countWhere(t, `SELECT count(*) FROM card_authorizations WHERE expired_at IS NOT NULL`); got != 1 {
		t.Errorf("expired rows = %d, want 1", got)
	}
	assertNoDrift(t)
}
