package holds

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

const expiry = 7 * 24 * time.Hour

func backdate(tb testing.TB, authID string, age time.Duration) {
	tb.Helper()
	if _, err := testPool.Exec(context.Background(),
		`UPDATE card_authorizations SET created_at = now() - $2::interval WHERE auth_id = $1`,
		authID, age.String()); err != nil {
		tb.Fatal(err)
	}
}

// Two expirers at once: each old hold is released once, by one of them;
// young holds and holds already reversed are left alone.
func TestExpiryReleasesOldHoldsOnce(t *testing.T) {
	resetDB(t)
	card := newCard(t, 100_000)
	const old = 30
	for i := range old {
		id := fmt.Sprintf("auth_old_%d", i)
		placeHold(t, card, id, 1000)
		backdate(t, id, expiry+time.Hour)
	}
	placeHold(t, card, "auth_young", 2000)
	backdate(t, "auth_young", expiry-time.Hour)
	placeHold(t, card, "auth_reversed", 3000)
	backdate(t, "auth_reversed", expiry+time.Hour)
	if _, err := release(context.Background(), "auth_reversed", 3000, ReasonReversal, "rev_before_expiry"); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	counts := make([]int, 2)
	errs := make([]error, 2)
	for i := range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			counts[i], errs[i] = ExpireBatch(context.Background(), testPool, time.Now(), expiry, 1000)
		}()
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if counts[0]+counts[1] != old {
		t.Errorf("expired %d + %d, want %d in total", counts[0], counts[1], old)
	}

	if got := countWhere(t, `SELECT count(*) FROM card_authorizations WHERE expired_at IS NOT NULL AND held_amount = 0 AND released_amount = 1000`); got != old {
		t.Errorf("expired rows = %d, want %d", got, old)
	}
	if got := countWhere(t, `SELECT count(*) FROM card_authorizations WHERE auth_id = 'auth_young' AND held_amount = 2000 AND expired_at IS NULL`); got != 1 {
		t.Error("the young hold was touched")
	}
	if got := countWhere(t, `SELECT count(*) FROM card_authorizations WHERE auth_id = 'auth_reversed' AND released_amount = 3000 AND expired_at IS NULL`); got != 1 {
		t.Error("the reversed hold was expired too")
	}
	if got := countWhere(t, `SELECT count(*) FROM outbox_events WHERE event_type = $1`, EventExpired); got != old {
		t.Errorf("expired events = %d, want %d", got, old)
	}
	if got := storedBalance(t, card.HeldAccountID); got != 2000 {
		t.Errorf("held = %d, want 2000: only the young hold", got)
	}
	if got := storedBalance(t, card.AccountID); got != 98_000 {
		t.Errorf("available = %d, want 98000", got)
	}

	// Run again: nothing left to expire, nothing moves.
	if n, err := ExpireBatch(context.Background(), testPool, time.Now(), expiry, 1000); err != nil || n != 0 {
		t.Errorf("second run expired %d (%v), want 0", n, err)
	}
	assertNoDrift(t)
}

func TestExpireBatchStopsAtLimit(t *testing.T) {
	resetDB(t)
	card := newCard(t, 10_000)
	for i := range 5 {
		id := fmt.Sprintf("auth_limit_%d", i)
		placeHold(t, card, id, 1000)
		backdate(t, id, expiry+time.Hour)
	}
	if n, err := ExpireBatch(context.Background(), testPool, time.Now(), expiry, 3); err != nil || n != 3 {
		t.Fatalf("expired %d (%v), want 3", n, err)
	}
	if n, err := ExpireBatch(context.Background(), testPool, time.Now(), expiry, 3); err != nil || n != 2 {
		t.Fatalf("expired %d (%v), want the remaining 2", n, err)
	}
	assertNoDrift(t)
}

// BenchmarkExpireBatch expires 10,000 holds on one account, the worst case
// for the balance rows: every release locks the same two.
func BenchmarkExpireBatch(b *testing.B) {
	const holds = 10_000
	ctx := context.Background()
	for range b.N {
		b.StopTimer()
		resetDB(b)
		card := newCard(b, holds*100)
		seedHolds(b, card.ID, card.AccountID, card.HeldAccountID, holds, 100)
		b.StartTimer()

		started := time.Now()
		n, err := ExpireBatch(ctx, testPool, time.Now(), expiry, holds)
		elapsed := time.Since(started)
		if err != nil || n != holds {
			b.Fatalf("expired %d (%v), want %d", n, err, holds)
		}
		b.ReportMetric(float64(holds)/elapsed.Seconds(), "holds/s")
		b.ReportMetric(elapsed.Seconds(), "s/10k")
	}
}

// seedHolds writes n old holds of amount each, with one ledger
// move for their total, so the releases have money to give back.
func seedHolds(tb testing.TB, cardID, accountID, heldAccountID string, n int, amount int64) {
	tb.Helper()
	ctx := context.Background()
	if _, err := testPool.Exec(ctx, `
		WITH txn AS (
			INSERT INTO ledger_transactions (id, reference) VALUES ('ltx_seed', 'authorization seed') RETURNING id
		), entries AS (
			INSERT INTO ledger_entries (id, txn_id, account_id, direction, amount)
			SELECT 'le_seed_' || side, id, account, side, $3::bigint * $4
			FROM txn, (VALUES ('debit', $1::text), ('credit', $2::text)) AS e(side, account)
		)
		UPDATE account_balances
		SET balance = balance + CASE WHEN account_id = $1 THEN -$3::bigint * $4 ELSE $3::bigint * $4 END
		WHERE account_id IN ($1, $2)`, accountID, heldAccountID, amount, n); err != nil {
		tb.Fatal(err)
	}
	if _, err := testPool.Exec(ctx, `
		INSERT INTO card_authorizations (auth_id, card_id, account_id, held_account_id, amount,
			currency, merchant_id, merchant_name, mcc, kind, decision, approved_amount,
			request_fingerprint, held_amount, created_at)
		SELECT 'auth_bench_' || i, $1, $2, $3, $4, 'USD', 'mer_bench', 'Bench', '5814',
			'initial', 'approved', $4, 'bench', $4, now() - interval '8 days' - i * interval '1 second'
		FROM generate_series(1, $5) AS i`, cardID, accountID, heldAccountID, amount, n); err != nil {
		tb.Fatal(err)
	}
}
