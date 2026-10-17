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

	"github.com/Shailu-s/payments-platform/internal/cardrecon"
	"github.com/Shailu-s/payments-platform/internal/testdb"
	"github.com/jackc/pgx/v5"
)

const statementBody = "settlement_date,purchases_total,refunds_total,net_amount,line_count,currency\n" +
	"2026-10-18,1200,0,1200,1,USD\n"

func TestCommandDownloadsReconcilesListsAndInspects(t *testing.T) {
	ctx := context.Background()
	var nonce [12]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		t.Fatal(err)
	}
	schema := "test_card_recon_cli_" + hex.EncodeToString(nonce[:])
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
		if r.URL.Path != "/settlement" || r.URL.Query().Get("date") != "2026-10-18" {
			t.Errorf("unexpected request %s", r.URL)
		}
		w.Header().Set("Content-Type", "text/csv")
		fmt.Fprint(w, statementBody)
	}))
	defer network.Close()
	t.Setenv("DATABASE_URL", testdb.DSN())
	t.Setenv("PGOPTIONS", "-c search_path="+schema)
	t.Setenv("HOLD_EXPIRY_DAYS", "7")

	// Nothing was cleared or paid on our side: the statement alone disagrees.
	var output bytes.Buffer
	if err := run(ctx, []string{"-download", network.URL, "-date", "2026-10-18", "-run-id", "crn_cli"}, &output); err != nil {
		t.Fatal(err)
	}
	var saved cardrecon.Run
	if err := json.Unmarshal(output.Bytes(), &saved); err != nil || saved.Counts[cardrecon.SettlementMismatch] != 2 {
		t.Fatalf("run output=%s err=%v", output.String(), err)
	}

	// The same bytes from disk under the same ID are the saved run.
	path := filepath.Join(t.TempDir(), "settlement-2026-10-18.csv")
	if err := os.WriteFile(path, []byte(statementBody), 0o600); err != nil {
		t.Fatal(err)
	}
	output.Reset()
	if err := run(ctx, []string{"-statement", path, "-date", "2026-10-18", "-run-id", "crn_cli"}, &output); err != nil {
		t.Fatal(err)
	}
	var again cardrecon.Run
	if err := json.Unmarshal(output.Bytes(), &again); err != nil || !again.CreatedAt.Equal(saved.CreatedAt) {
		t.Fatalf("replay output=%s err=%v", output.String(), err)
	}
	if err := run(ctx, []string{"-statement", path, "-date", "2026-10-19"}, &bytes.Buffer{}); err == nil {
		t.Fatal("accepted a statement for a different date")
	}

	output.Reset()
	if err := run(ctx, []string{"-list"}, &output); err != nil {
		t.Fatal(err)
	}
	var runs []cardrecon.Run
	if err := json.Unmarshal(output.Bytes(), &runs); err != nil || len(runs) != 1 || runs[0].ID != "crn_cli" {
		t.Fatalf("list=%s err=%v", output.String(), err)
	}

	output.Reset()
	if err := run(ctx, []string{"-inspect", "crn_cli", "-class", cardrecon.SettlementMismatch}, &output); err != nil {
		t.Fatal(err)
	}
	var detail struct {
		Run      cardrecon.Run       `json:"run"`
		Findings []cardrecon.Finding `json:"findings"`
	}
	if err := json.Unmarshal(output.Bytes(), &detail); err != nil || len(detail.Findings) != 2 ||
		detail.Findings[0].Reason != cardrecon.ReasonNoSettlement || detail.Findings[0].Statement.NetAmount != 1200 {
		t.Fatalf("inspect=%s err=%v", output.String(), err)
	}
}

func TestInvalidCommandModesFailBeforeConnecting(t *testing.T) {
	for _, args := range [][]string{
		{},
		{"-list", "-inspect", "crn_x"},
		{"-statement", "s.csv"},
		{"-statement", "s.csv", "-download", "http://network", "-date", "2026-10-18"},
		{"-download", "http://network", "-date", "18/10/2026"},
		{"-list", "-date", "2026-10-18"},
		{"-list", "-run-id", "crn_x"},
		{"-list", "-class", "LATE_CLEARING"},
		{"-inspect", "crn_x", "-class", "MATCHED"},
		{"-inspect", "crn_x", "-class", "invented"},
		{"-list", "-limit", "0"},
	} {
		if err := run(context.Background(), args, &bytes.Buffer{}); err == nil {
			t.Fatalf("invalid args accepted: %v", args)
		}
	}
}
