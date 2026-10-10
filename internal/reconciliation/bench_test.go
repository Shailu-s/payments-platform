package reconciliation

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

func benchmarkRows(n int) []Record {
	rows := make([]Record, n)
	for i := range rows {
		rows[i] = Record{ProviderRef: fmt.Sprintf("mb_%06d", i), ClientReference: fmt.Sprintf("tr_%06d", i), Amount: 50000, Currency: "USD", Status: "settled"}
	}
	return rows
}

func BenchmarkReadReport10000Payments(b *testing.B) {
	raw := reportCSV(benchmarkRows(10000))
	b.ReportAllocs()
	b.SetBytes(int64(len(raw)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		report, err := ReadReport(strings.NewReader(raw))
		if err != nil || len(report.Records) != 10000 {
			b.Fatalf("rows=%d err=%v", len(report.Records), err)
		}
	}
}

func BenchmarkReconcile10000Payments(b *testing.B) {
	pool := isolatedDB(b)
	ctx := context.Background()
	rows := benchmarkRows(10000)
	seedPayments(b, pool, rows)
	raw := reportCSV(rows)
	var before int64
	if err := pool.QueryRow(ctx, `SELECT pg_total_relation_size('reconciliation_runs')+pg_total_relation_size('reconciliation_findings')`).Scan(&before); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(raw)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		run, err := Reconcile(ctx, pool, fmt.Sprintf("bench_%d", i), strings.NewReader(raw))
		if err != nil || run.Counts[Matched] != 10000 {
			b.Fatalf("run=%+v err=%v", run, err)
		}
	}
	b.StopTimer()
	var after int64
	if err := pool.QueryRow(ctx, `SELECT pg_total_relation_size('reconciliation_runs')+pg_total_relation_size('reconciliation_findings')`).Scan(&after); err != nil {
		b.Fatal(err)
	}
	b.ReportMetric(float64(after-before)/float64(b.N), "stored-B/run")
}
