package reconciliation

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Shailu-s/payments-platform/internal/ledger"
	"github.com/jackc/pgx/v5/pgxpool"
)

func financialSnapshot(t *testing.T, pool *pgxpool.Pool) string {
	t.Helper()
	var snapshot string
	err := pool.QueryRow(context.Background(), `SELECT jsonb_build_object(
		'entries', (SELECT jsonb_agg(to_jsonb(e) ORDER BY id) FROM ledger_entries e),
		'transactions', (SELECT jsonb_agg(to_jsonb(x) ORDER BY id) FROM ledger_transactions x),
		'transfers', (SELECT jsonb_agg(to_jsonb(x) ORDER BY id) FROM transfers x))::text`).Scan(&snapshot)
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func TestProviderDiscrepanciesAreDetectedWithoutFinancialMutation(t *testing.T) {
	pool := isolatedDB(t)
	ctx := context.Background()
	row := func(id string) Record {
		return Record{ProviderRef: "mb_" + id, ClientReference: "tr_" + id, Amount: 50000, Currency: "USD", Status: "settled"}
	}
	internal := []Record{row("matched"), row("amount"), row("status"), row("missing"), row("duplicate")}
	internal[2].Status = "processing"
	seedPayments(t, pool, internal)
	if _, err := ledger.Record(ctx, pool, "reconciliation test funding", []ledger.Entry{
		{AccountID: "acc_src", Direction: ledger.DirectionCredit, Amount: 500000},
		{AccountID: "acc_dst", Direction: ledger.DirectionDebit, Amount: 500000},
	}); err != nil {
		t.Fatal(err)
	}
	external := []Record{row("matched"), row("amount"), row("status"), row("orphan"), row("duplicate"), row("duplicate")}
	external[1].Amount = 49000
	before := financialSnapshot(t, pool)
	raw := reportCSV(external)
	run, err := Reconcile(ctx, pool, "run_corrupted", strings.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]int{Matched: 1, AmountMismatch: 1, StatusMismatch: 1, MissingExternal: 1, MissingInternal: 1, DuplicateExternal: 1}
	if !reflect.DeepEqual(run.Counts, want) {
		t.Fatalf("counts=%v want=%v", run.Counts, want)
	}
	findings, err := Findings(ctx, pool, run.ID, AmountMismatch, "tr_amount", 100, 0)
	if err != nil || len(findings) != 1 || findings[0].Internal.Amount != 50000 || findings[0].External[0].Amount != 49000 {
		t.Fatalf("amount evidence=%+v err=%v", findings, err)
	}
	if before != financialSnapshot(t, pool) {
		t.Fatal("reconciliation mutated transfer or ledger data")
	}
	replayed, err := Reconcile(ctx, pool, run.ID, strings.NewReader(raw))
	if err != nil || !reflect.DeepEqual(run, replayed) {
		t.Fatalf("retry changed run: %+v %v", replayed, err)
	}
	for _, query := range []string{
		"UPDATE reconciliation_runs SET internal_rows=0 WHERE id='run_corrupted'",
		"DELETE FROM reconciliation_runs WHERE id='run_corrupted'",
		"UPDATE reconciliation_findings SET evidence='{}' WHERE run_id='run_corrupted'",
		"DELETE FROM reconciliation_findings WHERE run_id='run_corrupted'",
	} {
		if _, err := pool.Exec(ctx, query); err == nil {
			t.Fatalf("allowed mutation: %s", query)
		}
	}
	t.Logf("internal=%d external=%d counts=%v preparation=%dms", run.InternalRows, run.ExternalRows, run.Counts, run.PreparationMS)
}

func TestConcurrentRerunsPersistOneOriginalSnapshot(t *testing.T) {
	pool := isolatedDB(t)
	ctx := context.Background()
	row := Record{ProviderRef: "mb_retry", ClientReference: "tr_retry", Amount: 50000, Currency: "USD", Status: "processing"}
	seedPayments(t, pool, []Record{row})
	raw := reportCSV([]Record{row})
	const n = 20
	runs, errs := make([]Run, n), make([]error, n)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			runs[i], errs[i] = Reconcile(ctx, pool, "run_retry", strings.NewReader(raw))
		}()
	}
	close(start)
	wg.Wait()
	for i := range runs {
		if errs[i] != nil || !reflect.DeepEqual(runs[0], runs[i]) {
			t.Fatalf("concurrent run %d=%+v err=%v", i, runs[i], errs[i])
		}
	}
	if _, err := pool.Exec(ctx, "UPDATE transfers SET status='settled' WHERE id='tr_retry'"); err != nil {
		t.Fatal(err)
	}
	replayed, err := Reconcile(ctx, pool, "run_retry", strings.NewReader(raw))
	if err != nil || !reflect.DeepEqual(replayed, runs[0]) {
		t.Fatalf("retry resnapshotted: %+v %v", replayed, err)
	}
	changed := strings.Replace(raw, ",processing\n", ",settled\n", 1)
	if _, err := Reconcile(ctx, pool, "run_retry", strings.NewReader(changed)); !errors.Is(err, ErrRunConflict) {
		t.Fatalf("changed report reused run ID: %v", err)
	}
	fresh, err := Reconcile(ctx, pool, "run_fresh", strings.NewReader(raw))
	if err != nil || fresh.Counts[StatusMismatch] != 1 {
		t.Fatalf("new run did not capture fresh truth: %+v %v", fresh, err)
	}
	var count int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM reconciliation_findings WHERE run_id='run_retry'").Scan(&count); err != nil || count != 1 {
		t.Fatalf("duplicated results: count=%d err=%v", count, err)
	}
}

func TestFailedCommitLeavesNoPartialRunAndCanRetry(t *testing.T) {
	pool := isolatedDB(t)
	ctx := context.Background()
	row := Record{ProviderRef: "mb_atomic", ClientReference: "tr_atomic", Amount: 50000, Currency: "USD", Status: "settled"}
	seedPayments(t, pool, []Record{row})
	raw := reportCSV([]Record{row})
	_, err := pool.Exec(ctx, `CREATE FUNCTION reject_reconciliation_commit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected run commit failure'; END; $$;
		CREATE CONSTRAINT TRIGGER reject_reconciliation_commit AFTER INSERT ON reconciliation_findings DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION reject_reconciliation_commit();`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Reconcile(ctx, pool, "run_atomic", strings.NewReader(raw)); err == nil {
		t.Fatal("failed commit reported success")
	}
	if _, err := GetRun(ctx, pool, "run_atomic"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("partial run visible: %v", err)
	}
	if _, err := pool.Exec(ctx, "DROP TRIGGER reject_reconciliation_commit ON reconciliation_findings; DROP FUNCTION reject_reconciliation_commit()"); err != nil {
		t.Fatal(err)
	}
	if run, err := Reconcile(ctx, pool, "run_atomic", strings.NewReader(raw)); err != nil || run.Counts[Matched] != 1 {
		t.Fatalf("retry failed: %+v %v", run, err)
	}
}

func TestSnapshotScopeIncludesUnknownOutcomesButNotUnsubmittedInstructions(t *testing.T) {
	pool := isolatedDB(t)
	ctx := context.Background()
	rows := []Record{
		{ClientReference: "tr_unknown", Amount: 500, Currency: "USD", Status: "unresolved"},
		{ClientReference: "tr_attempted", Amount: 500, Currency: "USD", Status: "processing"},
		{ClientReference: "tr_pending", Amount: 500, Currency: "USD", Status: "processing"},
		{ClientReference: "tr_rejected", Amount: 500, Currency: "USD", Status: "failed"},
		{ProviderRef: "mb_future", ClientReference: "tr_future", Amount: 500, Currency: "USD", Status: "settled"},
	}
	seedPayments(t, pool, rows)
	if _, err := pool.Exec(ctx, "UPDATE transfers SET attempt_count=0 WHERE id='tr_pending'; UPDATE transfers SET created_at=now()+interval '1 hour' WHERE id='tr_future'"); err != nil {
		t.Fatal(err)
	}
	at, internal, err := snapshot(ctx, pool, time.Now().UTC(), nil)
	if err != nil || at.IsZero() || len(internal) != 2 || internal[0].ClientReference != "tr_attempted" || internal[1].ClientReference != "tr_unknown" {
		t.Fatalf("scope=%+v err=%v", internal, err)
	}
}

func TestInvalidReportDoesNotCreateSuccessfulRun(t *testing.T) {
	pool := isolatedDB(t)
	if _, err := Reconcile(context.Background(), pool, "run_invalid", strings.NewReader("not a report")); err == nil {
		t.Fatal("invalid report succeeded")
	}
	if _, err := GetRun(context.Background(), pool, "run_invalid"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("invalid input persisted success: %v", err)
	}
}
