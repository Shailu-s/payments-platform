package clearing

import (
	"context"
	"math"
	"testing"
)

// Every way a matched line can differ from its hold: the ledger, the balances,
// the authorization, the daily counter and the record must all agree.
func TestClearingExactPartialAndTip(t *testing.T) {
	cases := []struct {
		name                              string
		held, cleared                     int64
		fromHeld, fromAvailable, released int64
		flag                              string
	}{
		{name: "exact", held: 5000, cleared: 5000, fromHeld: 5000},
		{name: "partial", held: 10_000, cleared: 7000, fromHeld: 7000, released: 3000},
		{name: "tip", held: 5000, cleared: 6000, fromHeld: 5000, fromAvailable: 1000},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resetDB(t)
			const balance = 20_000
			card := newCard(t, balance)
			placeHold(t, card, "auth_"+tc.name, tc.held)

			importLines(t, purchase("clr_"+tc.name, "auth_"+tc.name, card.ID, tc.cleared))

			r := loadRecord(t, "clr_"+tc.name)
			want := record{status: StatusPosted, fromHeld: tc.fromHeld, fromAvailable: tc.fromAvailable,
				released: tc.released, flag: tc.flag, hasLedger: true}
			if r != want {
				t.Errorf("record = %+v, want %+v", r, want)
			}
			if got := storedBalance(t, card.HeldAccountID); got != 0 {
				t.Errorf("held = %d, want 0", got)
			}
			if got := storedBalance(t, card.AccountID); got != balance-tc.cleared {
				t.Errorf("available = %d, want %d", got, balance-tc.cleared)
			}
			if got := payable(t); got != tc.cleared {
				t.Errorf("payable = %d, want %d", got, tc.cleared)
			}
			if a := loadAuth(t, "auth_"+tc.name); a != (authState{held: 0, released: tc.released, cleared: tc.cleared, clearedAt: true}) {
				t.Errorf("authorization = %+v", a)
			}
			if got := dailySpend(t, card.ID); got != tc.held-tc.released {
				t.Errorf("daily spend = %d, want %d: a partial clear frees the rest", got, tc.held-tc.released)
			}
			// One balanced ledger transaction for the whole line.
			if got := countWhere(t, `SELECT count(*) FROM ledger_transactions WHERE reference = 'clearing ' || $1`, "clr_"+tc.name); got != 1 {
				t.Errorf("ledger transactions = %d, want 1", got)
			}
			if got := countWhere(t, `SELECT count(*) FROM outbox_events WHERE event_type = $1 AND aggregate_id = $2`, EventPosted, "auth_"+tc.name); got != 1 {
				t.Errorf("posted events = %d, want 1", got)
			}
			assertNoDrift(t)
		})
	}
}

func TestOverToleranceIsPostedAndFlagged(t *testing.T) {
	resetDB(t)
	card := newCard(t, 20_000)
	placeHold(t, card, "auth_within", 5000)
	placeHold(t, card, "auth_over", 5000)

	// 20 % of 5000 is 1000: 6000 is at the limit, 6001 is over it.
	importLines(t,
		purchase("clr_within", "auth_within", card.ID, 6000),
		purchase("clr_over", "auth_over", card.ID, 6001))

	if r := loadRecord(t, "clr_within"); r.flag != "" || r.status != StatusPosted {
		t.Errorf("within tolerance = %+v, want posted unflagged", r)
	}
	r := loadRecord(t, "clr_over")
	if r.status != StatusPosted || r.flag != FlagOverTolerance || r.fromHeld != 5000 || r.fromAvailable != 1001 {
		t.Errorf("over tolerance = %+v, want posted 5000 + 1001 and flagged", r)
	}
	if got := payable(t); got != 12_001 {
		t.Errorf("payable = %d, want 12001", got)
	}
	assertNoDrift(t)
}

func TestExceedsCannotOverflow(t *testing.T) {
	if !exceeds(math.MaxInt64, 1, 20) || exceeds(1, math.MaxInt64, 20) || exceeds(math.MaxInt64, math.MaxInt64, 100) {
		t.Error("tolerance comparison overflowed")
	}
}

// The contract clears a chain on its initial auth_id; a line naming the
// incremental instead still clears the chain's hold, on the initial row.
func TestClearingOnAnIncrementalClearsTheChain(t *testing.T) {
	resetDB(t)
	card := newCard(t, 10_000)
	placeHold(t, card, "auth_initial", 3000)
	if _, err := testPool.Exec(context.Background(), `
		INSERT INTO card_authorizations (auth_id, card_id, account_id, held_account_id, amount,
			currency, merchant_id, merchant_name, mcc, kind, original_auth_id, decision, approved_amount,
			request_fingerprint, held_amount)
		VALUES ('auth_increment', $1, $2, $3, 1000, 'USD', 'mer_test', 'Test', '7011', 'incremental',
			'auth_initial', 'approved', 1000, 'test', 0)`, card.ID, card.AccountID, card.HeldAccountID); err != nil {
		t.Fatal(err)
	}
	importLines(t, purchase("clr_chain", "auth_increment", card.ID, 3000))

	if r := loadRecord(t, "clr_chain"); r.fromHeld != 3000 || r.flag != "" {
		t.Errorf("record = %+v, want 3000 from the chain's hold", r)
	}
	if a := loadAuth(t, "auth_initial"); a.held != 0 || a.cleared != 3000 {
		t.Errorf("initial = %+v, want cleared", a)
	}
	assertNoDrift(t)
}
