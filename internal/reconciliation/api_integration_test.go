package reconciliation_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Shailu-s/payments-platform/internal/api"
	"github.com/Shailu-s/payments-platform/internal/auth"
	"github.com/Shailu-s/payments-platform/internal/reconciliation"
	"github.com/Shailu-s/payments-platform/internal/testdb"
	"github.com/jackc/pgx/v5"
)

func TestReconciliationInspectionRequiresAuthAndReturnsSavedEvidence(t *testing.T) {
	ctx := context.Background()
	var nonce [12]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		t.Fatal(err)
	}
	schema := "test_recon_api_" + hex.EncodeToString(nonce[:])
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
	plaintext, key, err := auth.Generate("reconciliation HTTP test")
	if err != nil {
		t.Fatal(err)
	}
	if err := auth.Insert(ctx, pool, key); err != nil {
		t.Fatal(err)
	}
	raw := fmt.Sprintf("snapshot_at,%s\nprovider_ref,client_reference,amount,currency,status\nmb_orphan,tr_orphan,50000,USD,settled\n", time.Now().UTC().Format(time.RFC3339Nano))
	if _, err := reconciliation.Reconcile(ctx, pool, "run_http", strings.NewReader(raw)); err != nil {
		t.Fatal(err)
	}
	h := api.NewServer(pool, []byte("test-only-mockbank-key-for-reconciliation")).Handler()
	request := func(path, key string) *httptest.ResponseRecorder {
		r := httptest.NewRequest("GET", path, nil)
		if key != "" {
			r.Header.Set("Authorization", "Bearer "+key)
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w
	}
	for _, path := range []string{"/v1/reconciliation/runs", "/v1/reconciliation/runs/run_http", "/v1/reconciliation/runs/run_http/findings"} {
		if w := request(path, ""); w.Code != http.StatusUnauthorized {
			t.Fatalf("unauthenticated %s=%d", path, w.Code)
		}
		if w := request(path, plaintext); w.Code != http.StatusOK {
			t.Fatalf("%s=%d %s", path, w.Code, w.Body.String())
		}
	}
	w := request("/v1/reconciliation/runs/run_http/findings?classification=MISSING_INTERNAL&client_reference=tr_orphan", plaintext)
	var response struct {
		Data []reconciliation.Finding `json:"data"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil || len(response.Data) != 1 || response.Data[0].External[0].Amount != 50000 {
		t.Fatalf("evidence=%s err=%v", w.Body.String(), err)
	}
	for _, path := range []string{"/v1/reconciliation/runs?limit=0", "/v1/reconciliation/runs?offset=-1", "/v1/reconciliation/runs/run_http/findings?classification=INVALID"} {
		if w := request(path, plaintext); w.Code != http.StatusBadRequest {
			t.Fatalf("invalid %s=%d %s", path, w.Code, w.Body.String())
		}
	}
	if w := request("/v1/reconciliation/runs/missing/findings", plaintext); w.Code != http.StatusNotFound {
		t.Fatalf("unknown run=%d", w.Code)
	}
}
