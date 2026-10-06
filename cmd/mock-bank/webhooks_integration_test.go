package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Shailu-s/payments-platform/internal/api"
	"github.com/Shailu-s/payments-platform/internal/auth"
	"github.com/Shailu-s/payments-platform/internal/ledger"
	"github.com/Shailu-s/payments-platform/internal/provider"
	"github.com/Shailu-s/payments-platform/internal/testdb"
	"github.com/Shailu-s/payments-platform/internal/transfers"
	"github.com/Shailu-s/payments-platform/internal/worker"
)

func TestSignedMockBankWebhookSettlesOnlyWithMatchingKey(t *testing.T) {
	for _, matching := range []bool{true, false} {
		t.Run(fmt.Sprintf("matching_key_%t", matching), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			pool, err := testdb.Connect(ctx, fmt.Sprintf("test_signed_bank_%d", time.Now().UnixNano()))
			if err != nil {
				t.Fatalf("%s", testdb.ConnectionHint(err))
			}
			defer pool.Close()
			if _, err := pool.Exec(ctx, `INSERT INTO accounts (id, currency, type) VALUES ('acc_src', 'USD', 'asset'), ('acc_dst', 'USD', 'liability')`); err != nil {
				t.Fatal(err)
			}
			if _, err := ledger.Record(ctx, pool, "funding", []ledger.Entry{
				{AccountID: transfers.SettlementAccountID, Direction: ledger.DirectionDebit, Amount: 1000},
				{AccountID: "acc_src", Direction: ledger.DirectionCredit, Amount: 1000},
			}); err != nil {
				t.Fatal(err)
			}
			plaintext, key, err := auth.Generate("signed bank integration")
			if err != nil {
				t.Fatal(err)
			}
			if err := auth.Insert(ctx, pool, key); err != nil {
				t.Fatal(err)
			}
			handler := api.NewServer(pool, []byte(testWebhookSecret)).Handler()
			receiver := httptest.NewServer(handler)
			defer receiver.Close()
			bankSecret := []byte(testWebhookSecret)
			if !matching {
				bankSecret = []byte(strings.Repeat("x", 32))
			}
			bank := NewServer(NewStore(), wellBehaved{settleDelay: time.Millisecond}, receiver.URL+"/v1/webhooks/mockbank", bankSecret)
			rail := httptest.NewServer(bank.Handler())
			defer rail.Close()
			defer bank.WaitForPending(ctx)

			req := httptest.NewRequest("POST", "/v1/transfers", strings.NewReader(`{"source_account":"acc_src","destination_account":"acc_dst","amount":500,"currency":"USD"}`))
			req.Header.Set("Authorization", "Bearer "+plaintext)
			req.Header.Set("Idempotency-Key", "signed-bank")
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)
			if rec.Code != http.StatusAccepted {
				t.Fatalf("create transfer = %d: %s", rec.Code, rec.Body.String())
			}
			var created struct{ ID string }
			if err := json.Unmarshal(rec.Body.Bytes(), &created); err != nil {
				t.Fatal(err)
			}
			sent, err := worker.New(pool, provider.New(rail.URL, time.Second), worker.DefaultConfig()).SendByID(ctx, created.ID)
			if err != nil || !sent {
				t.Fatalf("send = %t, err = %v", sent, err)
			}
			bank.WaitForPending(ctx)
			stored, err := transfers.Get(ctx, pool, created.ID)
			if err != nil {
				t.Fatal(err)
			}
			wantStatus, wantBalance, wantEvents := transfers.StatusProcessing, int64(0), 0
			if matching {
				wantStatus, wantBalance, wantEvents = transfers.StatusSettled, 500, 1
			}
			if stored.Status != wantStatus {
				t.Errorf("transfer status = %s, want %s", stored.Status, wantStatus)
			}
			balance, err := ledger.Balance(ctx, pool, "acc_dst")
			if err != nil || balance != wantBalance {
				t.Errorf("destination balance = %d, want %d: %v", balance, wantBalance, err)
			}
			var events int
			if err := pool.QueryRow(ctx, `SELECT count(*) FROM webhook_events`).Scan(&events); err != nil {
				t.Fatal(err)
			}
			if events != wantEvents {
				t.Errorf("recorded events = %d, want %d", events, wantEvents)
			}
		})
	}
}
