package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"net/http/httptest"
	"testing"

	"github.com/Shailu-s/payments-platform/internal/ledger"
	"github.com/Shailu-s/payments-platform/internal/reconciliation"
	"github.com/Shailu-s/payments-platform/internal/testdb"
	"github.com/jackc/pgx/v5"
)

func TestRealBankReportDetectsLostCallbackWithoutSettlingPayment(t *testing.T) {
	ctx := context.Background()
	var nonce [12]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		t.Fatal(err)
	}
	schema := "test_recon_bank_" + hex.EncodeToString(nonce[:])
	pool, err := testdb.Connect(ctx, schema)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := pool.Exec(ctx, "DROP SCHEMA "+pgx.Identifier{schema}.Sanitize()+" CASCADE"); err != nil {
			t.Error(err)
		}
		pool.Close()
	})
	store := NewStore()
	p, _, err := store.Submit(Payment{ClientReference: "tr_lost_callback", Amount: 50000, Currency: "USD"})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.Settle(p.ProviderRef, StatusSettled, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO accounts (id,currency,type) VALUES ('acc_src','USD','asset'),('acc_dst','USD','asset');
		INSERT INTO api_keys (id,key_hash,prefix,name) VALUES ('key_bank','hash_bank','bank','bank test');`); err != nil {
		t.Fatal(err)
	}
	txnID, err := ledger.Record(ctx, pool, "reserve tr_lost_callback", []ledger.Entry{
		{AccountID: "acc_src", Direction: ledger.DirectionDebit, Amount: 50000},
		{AccountID: "acc_settlement_usd", Direction: ledger.DirectionCredit, Amount: 50000},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO transfers (id,source_account,destination_account,amount,currency,status,api_key_id,provider_ref,ledger_txn_id,attempt_count)
		VALUES ('tr_lost_callback','acc_src','acc_dst',50000,'USD','processing','key_bank',$1,$2,1)`, p.ProviderRef, txnID); err != nil {
		t.Fatal(err)
	}
	bank := httptest.NewServer(NewServer(store, wellBehaved{}, "", []byte(testWebhookSecret)).Handler())
	defer bank.Close()
	report, err := reconciliation.FetchReport(ctx, bank.Client(), bank.URL)
	if err != nil {
		t.Fatal(err)
	}
	run, err := reconciliation.Reconcile(ctx, pool, "run_bank", bytes.NewReader(report.Raw))
	if err != nil || run.Counts[reconciliation.StatusMismatch] != 1 {
		t.Fatalf("run=%+v err=%v", run, err)
	}
	var status string
	var entries int
	if err := pool.QueryRow(ctx, "SELECT status,(SELECT count(*) FROM ledger_entries) FROM transfers WHERE id='tr_lost_callback'").Scan(&status, &entries); err != nil || status != "processing" || entries != 2 {
		t.Fatalf("reconciliation changed money: status=%s entries=%d err=%v", status, entries, err)
	}
	balance, err := ledger.Balance(ctx, pool, "acc_dst")
	if err != nil || balance != 0 {
		t.Fatalf("destination credited automatically: balance=%d err=%v", balance, err)
	}
}
