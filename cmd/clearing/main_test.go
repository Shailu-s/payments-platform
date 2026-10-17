package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/Shailu-s/payments-platform/internal/clearing"
	"github.com/Shailu-s/payments-platform/internal/testdb"
	"github.com/jackc/pgx/v5"
)

const fileBody = "clearing_id,auth_id,card_id,kind,amount,currency,merchant_id,cleared_at\n" +
	"clr_cli_1,,card_ghost,purchase,1200,USD,mer_transit,2026-10-18T10:00:00Z\n" +
	"clr_cli_2,auth_x,card_ghost,refund,500,USD,mer_transit,2026-10-18T11:00:00Z\n"

func TestCommandDownloadsImportsListsAndInspects(t *testing.T) {
	ctx := context.Background()
	var nonce [12]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		t.Fatal(err)
	}
	schema := "test_clearing_cli_" + hex.EncodeToString(nonce[:])
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
	network := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/clearing" || r.URL.Query().Get("date") != "2026-10-18" {
			t.Errorf("unexpected request %s", r.URL)
		}
		w.Header().Set("Content-Type", "text/csv")
		fmt.Fprint(w, fileBody)
	}))
	defer network.Close()
	t.Setenv("DATABASE_URL", testdb.DSN())
	t.Setenv("PGOPTIONS", "-c search_path="+schema)
	t.Setenv("CLEARING_OVER_TOLERANCE_PCT", "20")

	var output bytes.Buffer
	if err := run(ctx, []string{"-download", network.URL, "-date", "2026-10-18"}, &output); err != nil {
		t.Fatal(err)
	}
	var imported clearing.Summary
	if err := json.Unmarshal(output.Bytes(), &imported); err != nil || imported.Statuses[clearing.StatusRejected] != 2 ||
		imported.Replayed {
		t.Fatalf("import output=%s err=%v", output.String(), err)
	}

	// The same bytes from disk are a replay.
	path := filepath.Join(t.TempDir(), "clearing-2026-10-18.csv")
	if err := os.WriteFile(path, []byte(fileBody), 0o600); err != nil {
		t.Fatal(err)
	}
	output.Reset()
	if err := run(ctx, []string{"-file", path, "-date", "2026-10-18"}, &output); err != nil {
		t.Fatal(err)
	}
	var again clearing.Summary
	if err := json.Unmarshal(output.Bytes(), &again); err != nil || !again.Replayed || again.File.ID != imported.File.ID {
		t.Fatalf("replay output=%s err=%v", output.String(), err)
	}

	output.Reset()
	if err := run(ctx, []string{"-list"}, &output); err != nil {
		t.Fatal(err)
	}
	var files []clearing.File
	if err := json.Unmarshal(output.Bytes(), &files); err != nil || len(files) != 1 || files[0].LineCount != 2 {
		t.Fatalf("list=%s err=%v", output.String(), err)
	}

	output.Reset()
	if err := run(ctx, []string{"-inspect", imported.File.ID, "-flag", clearing.FlagUnknownCard}, &output); err != nil {
		t.Fatal(err)
	}
	var detail struct {
		Summary clearing.Summary  `json:"summary"`
		Records []clearing.Record `json:"records"`
	}
	if err := json.Unmarshal(output.Bytes(), &detail); err != nil || len(detail.Records) != 2 ||
		detail.Records[0].ClearingID != "clr_cli_1" || detail.Summary.Flags[clearing.FlagUnknownCard] != 2 {
		t.Fatalf("inspect=%s err=%v", output.String(), err)
	}
}

func TestInvalidCommandModesFailBeforeConnecting(t *testing.T) {
	for _, args := range [][]string{
		{},
		{"-list", "-inspect", "clf_x"},
		{"-file", "a.csv"},
		{"-file", "a.csv", "-download", "http://network", "-date", "2026-10-18"},
		{"-download", "http://network", "-date", "18/10/2026"},
		{"-list", "-date", "2026-10-18"},
		{"-list", "-flag", "shortfall"},
		{"-inspect", "clf_x", "-flag", "invented"},
		{"-list", "-limit", "0"},
	} {
		if err := run(context.Background(), args, &bytes.Buffer{}); err == nil {
			t.Fatalf("invalid args accepted: %v", args)
		}
	}
}
