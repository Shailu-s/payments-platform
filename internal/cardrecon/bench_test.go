package cardrecon

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"
)

// BenchmarkCardReconcile reconciles a settled day of 10,000 authorizations,
// each cleared by one line, against the matching statement, then replays the
// run by its ID.
func BenchmarkCardReconcile(b *testing.B) {
	const n = 10_000
	ctx := context.Background()
	b.ReportAllocs()
	for i := range b.N {
		b.StopTimer()
		resetDB(b)
		seedClearedDay(b, n)
		raw := statement("2026-10-18", n*1000, 0, n)
		id := fmt.Sprintf("crn_bench_%d", i)
		b.StartTimer()

		started := time.Now()
		run, err := Reconcile(ctx, testPool, id, bytes.NewReader(raw), testConfig)
		elapsed := time.Since(started)
		if err != nil || run.Counts[Matched] != n || run.RecordCount != n || run.AuthorizationCount != n {
			b.Fatalf("run = %+v, %v", run, err)
		}
		for class, count := range run.Counts {
			if class != Matched && count != 0 {
				b.Fatalf("clean day reported %s: %v", class, run.Counts)
			}
		}
		b.StopTimer()
		started2 := time.Now()
		if _, err := Reconcile(ctx, testPool, id, bytes.NewReader(raw), testConfig); err != nil {
			b.Fatal(err)
		}
		replay := time.Since(started2)
		b.StartTimer()
		b.ReportMetric(float64(elapsed.Microseconds())/1000, "run-ms")
		b.ReportMetric(float64(replay.Microseconds())/1000, "replay-ms")
	}
}

// seedClearedDay writes n approved authorizations, each with its hold's
// ledger transaction, fully cleared by one posted line of testDate's file,
// and the day's settlement, as authorization, clearing and Settle leave them.
func seedClearedDay(tb testing.TB, n int) {
	tb.Helper()
	ctx := context.Background()
	clearedAt := testDate.Add(10 * time.Hour)
	for _, step := range []struct {
		q    string
		args []any
	}{
		{`INSERT INTO ledger_transactions (id, reference, created_at)
		  SELECT 'ltx_bench_' || i, 'authorization seed', now() - interval '1 hour' FROM generate_series(1, $1) AS i`,
			[]any{n}},
		{`INSERT INTO card_authorizations (auth_id, card_id, amount, currency, merchant_id, merchant_name, mcc,
			kind, decision, approved_amount, ledger_txn_id, request_fingerprint, held_amount, cleared_amount,
			cleared_at, created_at)
		  SELECT 'auth_bench_' || i, 'card_bench_' || (i % 10), 1000, 'USD', 'mer_bench', 'Bench', '5814',
			'initial', 'approved', 1000, 'ltx_bench_' || i, 'bench', 0, 1000, $2, now() - interval '1 hour'
		  FROM generate_series(1, $1) AS i`, []any{n, clearedAt}},
		{`INSERT INTO clearing_files (id, file_date, sha256, raw, line_count)
		  VALUES ('clf_bench', $1, repeat('0', 64), '', $2)`, []any{testDate, n}},
		{`INSERT INTO clearing_records (clearing_id, file_id, auth_id, card_id, kind, amount, currency,
			merchant_id, cleared_at, status, charged_from_held)
		  SELECT 'clr_bench_' || i, 'clf_bench', 'auth_bench_' || i, 'card_bench_' || (i % 10), 'purchase',
			1000, 'USD', 'mer_bench', $2, 'posted', 1000
		  FROM generate_series(1, $1) AS i`, []any{n, clearedAt}},
		{`INSERT INTO network_settlements (settlement_date, purchases_total, refunds_total, net_amount, record_count)
		  VALUES ($1, $2::bigint * 1000, 0, $2::bigint * 1000, $2)`, []any{testDate, n}},
	} {
		if _, err := testPool.Exec(ctx, step.q, step.args...); err != nil {
			tb.Fatalf("%s: %v", step.q, err)
		}
	}
	if _, err := testPool.Exec(ctx, `ANALYZE card_authorizations; ANALYZE clearing_records; ANALYZE clearing_files`); err != nil {
		tb.Fatal(err)
	}
}
