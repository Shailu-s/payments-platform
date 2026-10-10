package reconciliation

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/Shailu-s/payments-platform/internal/testdb"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func isolatedDB(t testing.TB) *pgxpool.Pool {
	t.Helper()
	var nonce [12]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		t.Fatal(err)
	}
	schema := "test_recon_" + hex.EncodeToString(nonce[:])
	pool, err := testdb.Connect(context.Background(), schema)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := pool.Exec(context.Background(), "DROP SCHEMA "+pgx.Identifier{schema}.Sanitize()+" CASCADE"); err != nil {
			t.Error(err)
		}
		pool.Close()
	})
	return pool
}

func seedPayments(t testing.TB, pool *pgxpool.Pool, rows []Record) {
	t.Helper()
	ctx := context.Background()
	_, err := pool.Exec(ctx, `INSERT INTO accounts (id,currency,type) VALUES ('acc_src','USD','asset'),('acc_dst','USD','asset');
		INSERT INTO api_keys (id,key_hash,prefix,name) VALUES ('key_recon','hash_recon','recon','reconciliation test');`)
	if err != nil {
		t.Fatal(err)
	}
	batch := &pgx.Batch{}
	for _, row := range rows {
		batch.Queue(`INSERT INTO transfers (id,source_account,destination_account,amount,currency,status,api_key_id,provider_ref,attempt_count,created_at)
			VALUES ($1,'acc_src','acc_dst',$2,$3,$4,'key_recon',NULLIF($5,''),1,now()-interval '1 minute')`, row.ClientReference, row.Amount, row.Currency, row.Status, row.ProviderRef)
	}
	if err := pool.SendBatch(ctx, batch).Close(); err != nil {
		t.Fatal(err)
	}
}

func reportCSV(rows []Record) string {
	var b strings.Builder
	fmt.Fprintf(&b, "snapshot_at,%s\nprovider_ref,client_reference,amount,currency,status\n", time.Now().UTC().Format(time.RFC3339Nano))
	for _, r := range rows {
		fmt.Fprintf(&b, "%s,%s,%d,%s,%s\n", r.ProviderRef, r.ClientReference, r.Amount, r.Currency, r.Status)
	}
	return b.String()
}

func TestFailedLookupWithoutSavedProviderRefStillMatches(t *testing.T) {
	pool := isolatedDB(t)
	internal := Record{ClientReference: "tr_failed_lookup", Amount: 50000, Currency: "USD", Status: "failed"}
	seedPayments(t, pool, []Record{internal})
	external := internal
	external.ProviderRef = "mb_failed_lookup"
	run, err := Reconcile(context.Background(), pool, "run_failed_lookup", strings.NewReader(reportCSV([]Record{external})))
	if err != nil || run.Counts[Matched] != 1 || run.Counts[MissingInternal] != 0 {
		t.Fatalf("run=%+v err=%v, want existing failed transfer linked by client reference", run, err)
	}
}

func TestReconcilePersistsCleanMatchAndEvidence(t *testing.T) {
	pool := isolatedDB(t)
	row := Record{ProviderRef: "mb_clean", ClientReference: "tr_clean", Amount: 50000, Currency: "USD", Status: "settled"}
	seedPayments(t, pool, []Record{row})
	raw := reportCSV([]Record{row})
	run, err := Reconcile(context.Background(), pool, "run_clean", strings.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	if run.Counts[Matched] != 1 || run.InternalRows != 1 || run.ExternalRows != 1 || run.InternalCapturedAt.IsZero() || run.ProviderCapturedAt.IsZero() {
		t.Fatalf("run=%+v", run)
	}
	findings, err := Findings(context.Background(), pool, run.ID, "", "", 100, 0)
	if err != nil || len(findings) != 1 || findings[0].Classification != Matched || findings[0].Internal.Amount != 50000 {
		t.Fatalf("findings=%+v err=%v", findings, err)
	}
	var evidence []byte
	if err := pool.QueryRow(context.Background(), "SELECT provider_report FROM reconciliation_runs WHERE id=$1", run.ID).Scan(&evidence); err != nil || string(evidence) != raw {
		t.Fatalf("raw evidence lost: %v", err)
	}
}
