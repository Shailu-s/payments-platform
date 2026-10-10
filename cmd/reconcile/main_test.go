package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Shailu-s/payments-platform/internal/reconciliation"
	"github.com/Shailu-s/payments-platform/internal/testdb"
	"github.com/jackc/pgx/v5"
)

func TestCommandDownloadsSavesAndInspectsReport(t *testing.T) {
	ctx := context.Background()
	var nonce [12]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		t.Fatal(err)
	}
	schema := "test_recon_cli_" + hex.EncodeToString(nonce[:])
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
	raw := fmt.Sprintf("snapshot_at,%s\nprovider_ref,client_reference,amount,currency,status\nmb_cli,tr_cli,500,USD,settled\n", time.Now().UTC().Format(time.RFC3339Nano))
	bank := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/settlements" {
			t.Errorf("unexpected path %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "text/csv")
		fmt.Fprint(w, raw)
	}))
	defer bank.Close()
	t.Setenv("DATABASE_URL", testdb.DSN())
	t.Setenv("PGOPTIONS", "-c search_path="+schema)
	t.Setenv("PROVIDER_URL", bank.URL)
	var output bytes.Buffer
	if err := run(ctx, []string{"-run-id", "cli_run"}, &output); err != nil {
		t.Fatal(err)
	}
	var saved reconciliation.Run
	if err := json.Unmarshal(output.Bytes(), &saved); err != nil || saved.Counts[reconciliation.MissingInternal] != 1 {
		t.Fatalf("output=%s err=%v", output.String(), err)
	}
	output.Reset()
	if err := run(ctx, []string{"-inspect", "cli_run", "-classification", "MISSING_INTERNAL"}, &output); err != nil {
		t.Fatal(err)
	}
	var detail struct {
		Run      reconciliation.Run       `json:"run"`
		Findings []reconciliation.Finding `json:"findings"`
	}
	if err := json.Unmarshal(output.Bytes(), &detail); err != nil || len(detail.Findings) != 1 || detail.Findings[0].External[0].Amount != 500 {
		t.Fatalf("detail=%s err=%v", output.String(), err)
	}
	output.Reset()
	if err := run(ctx, []string{"-list"}, &output); err != nil {
		t.Fatal(err)
	}
	var runs []reconciliation.Run
	if err := json.Unmarshal(output.Bytes(), &runs); err != nil || len(runs) != 1 {
		t.Fatalf("list=%s err=%v", output.String(), err)
	}
	scheduledCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	writer := &cancelingWriter{cancel: cancel}
	if err := run(scheduledCtx, []string{"-interval", "1ms"}, writer); !errors.Is(err, context.Canceled) {
		t.Fatalf("schedule did not stop on cancellation: %v", err)
	}
	decoder := json.NewDecoder(&writer.Buffer)
	var first, second reconciliation.Run
	if err := decoder.Decode(&first); err != nil {
		t.Fatal(err)
	}
	if err := decoder.Decode(&second); err != nil {
		t.Fatal(err)
	}
	if first.ID == second.ID || first.ID == "" || second.ID == "" {
		t.Fatal("schedule reused an old run ID")
	}
}

type cancelingWriter struct {
	bytes.Buffer
	writes int
	cancel context.CancelFunc
}

func (w *cancelingWriter) Write(p []byte) (int, error) {
	n, err := w.Buffer.Write(p)
	w.writes++
	if w.writes == 2 {
		w.cancel()
	}
	return n, err
}

func TestInvalidCommandModesFailBeforeConnecting(t *testing.T) {
	for _, args := range [][]string{{"-list", "-inspect", "run"}, {"-interval", "1h", "-run-id", "same"}, {"-interval", "1h", "-report", "old.csv"}, {"-limit", "0"}, {"-classification", "INVALID"}} {
		if err := run(context.Background(), args, &bytes.Buffer{}); err == nil {
			t.Fatalf("invalid args accepted: %v", args)
		}
	}
}
