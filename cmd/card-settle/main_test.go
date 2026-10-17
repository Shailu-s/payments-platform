package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"testing"
	"time"

	"github.com/Shailu-s/payments-platform/internal/clearing"
	"github.com/Shailu-s/payments-platform/internal/testdb"
	"github.com/jackc/pgx/v5"
)

func TestCommandSettlesADayOnce(t *testing.T) {
	ctx := context.Background()
	var nonce [12]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		t.Fatal(err)
	}
	schema := "test_settle_cli_" + hex.EncodeToString(nonce[:])
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
	// A file whose lines are all for unknown cards: imported, nothing posted.
	const file = "clearing_id,auth_id,card_id,kind,amount,currency,merchant_id,cleared_at\n" +
		"clr_cli_1,,card_ghost,purchase,1200,USD,mer_transit,2026-10-18T10:00:00Z\n"
	if _, err := clearing.Import(ctx, pool, []byte(file), time.Date(2026, 10, 18, 0, 0, 0, 0, time.UTC), clearing.Config{}); err != nil {
		t.Fatal(err)
	}
	t.Setenv("DATABASE_URL", testdb.DSN())
	t.Setenv("PGOPTIONS", "-c search_path="+schema)

	type result struct {
		Date      string  `json:"settlement_date"`
		Net       int64   `json:"net_amount"`
		LedgerTxn *string `json:"ledger_txn_id"`
		Created   bool    `json:"created"`
	}
	for i, wantCreated := range []bool{true, false} {
		var output bytes.Buffer
		if err := run(ctx, []string{"-date", "2026-10-18"}, &output); err != nil {
			t.Fatal(err)
		}
		var got result
		if err := json.Unmarshal(output.Bytes(), &got); err != nil || got.Date != "2026-10-18" || got.Net != 0 ||
			got.LedgerTxn != nil || got.Created != wantCreated {
			t.Fatalf("run %d: output=%s err=%v", i, output.String(), err)
		}
	}
	if err := run(ctx, []string{"-date", "2026-10-19"}, &bytes.Buffer{}); err == nil {
		t.Fatal("settled a date with no clearing file")
	}
}

func TestInvalidArgumentsFailBeforeConnecting(t *testing.T) {
	for _, args := range [][]string{{}, {"-date", "18/10/2026"}, {"-date", "2026-10-18", "extra"}} {
		if err := run(context.Background(), args, &bytes.Buffer{}); err == nil {
			t.Fatalf("invalid args accepted: %v", args)
		}
	}
}
