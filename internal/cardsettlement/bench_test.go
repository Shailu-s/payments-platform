package cardsettlement

import (
	"context"
	"testing"
	"time"
)

// BenchmarkSettle settles a day of 10,000 posted lines, 9,000 purchases and
// 1,000 refunds, then settles it again.
func BenchmarkSettle(b *testing.B) {
	const lines = 10_000
	ctx := context.Background()
	for range b.N {
		b.StopTimer()
		resetDB(b)
		seedPostedDay(b, lines)
		b.StartTimer()

		started := time.Now()
		s, created, err := Settle(ctx, testPool, testDate)
		elapsed := time.Since(started)
		if err != nil || !created || s.RecordCount != lines || s.NetAmount != 9_000*1000-1_000*500 {
			b.Fatalf("settle = %+v, %v, %v", s, created, err)
		}
		started = time.Now()
		if _, again, err := Settle(ctx, testPool, testDate); err != nil || again {
			b.Fatalf("rerun created %v, %v", again, err)
		}
		replay := time.Since(started)
		b.ReportMetric(float64(elapsed.Microseconds())/1000, "settle-ms")
		b.ReportMetric(float64(replay.Microseconds())/1000, "replay-ms")
	}
}

// seedPostedDay writes a clearing file for testDate and n posted records on
// it, every tenth a refund, as an import would leave them.
func seedPostedDay(tb testing.TB, n int) {
	tb.Helper()
	ctx := context.Background()
	if _, err := testPool.Exec(ctx, `
		INSERT INTO clearing_files (id, file_date, sha256, raw, line_count)
		VALUES ('clf_bench', $1, repeat('0', 64), '', $2)`, testDate, n); err != nil {
		tb.Fatal(err)
	}
	if _, err := testPool.Exec(ctx, `
		INSERT INTO clearing_records (clearing_id, file_id, card_id, kind, amount, currency, merchant_id,
			cleared_at, status, charged_from_available, credited_amount)
		SELECT 'clr_bench_' || i, 'clf_bench', 'card_bench_' || (i % 10),
			CASE WHEN i % 10 = 0 THEN 'refund' ELSE 'purchase' END,
			CASE WHEN i % 10 = 0 THEN 500 ELSE 1000 END, 'USD', 'mer_bench', $1, 'posted',
			CASE WHEN i % 10 = 0 THEN 0 ELSE 1000 END, CASE WHEN i % 10 = 0 THEN 500 ELSE 0 END
		FROM generate_series(1, $2) AS i`, testDate.Add(10*time.Hour), n); err != nil {
		tb.Fatal(err)
	}
}
