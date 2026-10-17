package clearing

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Shailu-s/payments-platform/internal/cards"
)

// BenchmarkImportClearing imports a 10,000-line file of matched lines, a
// third each exact, tip and partial, against holds spread over ten cards,
// then imports the same bytes again.
func BenchmarkImportClearing(b *testing.B) {
	const (
		cardsN  = 10
		perCard = 1000
		amount  = 1000
	)
	ctx := context.Background()
	for range b.N {
		b.StopTimer()
		resetDB(b)
		var lines []Line
		for c := range cardsN {
			card := newCard(b, perCard*amount*2)
			prefix := fmt.Sprintf("auth_bench_%d_", c)
			seedHolds(b, card, prefix, perCard, amount)
			for i := range perCard {
				cleared := int64(amount)
				switch i % 3 {
				case 1:
					cleared = amount * 6 / 5
				case 2:
					cleared = amount * 7 / 10
				}
				lines = append(lines, purchase(fmt.Sprintf("clr_bench_%d_%d", c, i), fmt.Sprint(prefix, i), card.ID, cleared))
			}
		}
		raw := fileOf(lines...)
		b.StartTimer()

		started := time.Now()
		s, err := Import(ctx, testPool, raw, testDate, testConfig)
		elapsed := time.Since(started)
		if err != nil || s.Statuses[StatusPosted] != len(lines) || len(s.Flags) != 0 {
			b.Fatalf("import = %+v, %v", s, err)
		}
		started = time.Now()
		again, err := Import(ctx, testPool, raw, testDate, testConfig)
		replay := time.Since(started)
		if err != nil || !again.Replayed || again.Processed != 0 {
			b.Fatalf("re-import = %+v, %v", again, err)
		}
		b.ReportMetric(float64(len(lines))/elapsed.Seconds(), "lines/s")
		b.ReportMetric(elapsed.Seconds(), "s/10k")
		b.ReportMetric(float64(replay.Microseconds())/1000, "replay-ms")
	}
}

// seedHolds writes n approved holds of amount on card, with one ledger move
// for their total, as if n taps had been approved.
func seedHolds(tb testing.TB, card cards.Card, prefix string, n int, amount int64) {
	tb.Helper()
	ctx := context.Background()
	txn := "ltx_seed_" + card.ID
	if _, err := testPool.Exec(ctx, `
		WITH txn AS (
			INSERT INTO ledger_transactions (id, reference) VALUES ($5, 'authorization seed') RETURNING id
		), entries AS (
			INSERT INTO ledger_entries (id, txn_id, account_id, direction, amount)
			SELECT id || '_' || side, id, account, side, $3::bigint * $4
			FROM txn, (VALUES ('debit', $1::text), ('credit', $2::text)) AS e(side, account)
		)
		UPDATE account_balances
		SET balance = balance + CASE WHEN account_id = $1 THEN -$3::bigint * $4 ELSE $3::bigint * $4 END
		WHERE account_id IN ($1, $2)`, card.AccountID, card.HeldAccountID, amount, n, txn); err != nil {
		tb.Fatal(err)
	}
	if _, err := testPool.Exec(ctx, `
		INSERT INTO card_authorizations (auth_id, card_id, account_id, held_account_id, amount,
			currency, merchant_id, merchant_name, mcc, kind, decision, approved_amount,
			request_fingerprint, held_amount)
		SELECT $5 || (i - 1), $1, $2, $3, $4, 'USD', 'mer_bench', 'Bench', '5814',
			'initial', 'approved', $4, 'bench', $4
		FROM generate_series(1, $6) AS i`, card.ID, card.AccountID, card.HeldAccountID, amount, prefix, n); err != nil {
		tb.Fatal(err)
	}
	if _, err := testPool.Exec(ctx, `
		INSERT INTO card_daily_spend (card_id, day, amount) VALUES ($1, (now() AT TIME ZONE 'UTC')::date, $2)`,
		card.ID, amount*int64(n)); err != nil {
		tb.Fatal(err)
	}
}
