package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// doWithKey sends a request carrying an Idempotency-Key. Each call builds its
// own body reader, because a shared one is consumed by whichever goroutine
// reaches it first and the rest send nothing.
func doWithKey(h http.Handler, method, path, apiKey, idempotencyKey, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	if apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+apiKey)
	}
	if idempotencyKey != "" {
		req.Header.Set("Idempotency-Key", idempotencyKey)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

// ⭐ Guarantee 2: the same idempotency key never creates two transfers.
//
// One hundred identical requests, released together, must produce exactly one
// transfer and one hundred identical responses.
//
// PHASE 3.1: this test is expected to FAIL. The implementation reads by key and
// inserts if nothing came back, and every goroutine runs that read before any
// of them writes. Watching it fail is the point — the failure output is the
// evidence behind the guarantee.
func TestIdempotentRequestsCreateOneTransfer(t *testing.T) {
	h, apiKey := newTestServer(t)
	ctx := context.Background()

	source := createAccount(t, h, apiKey, "asset")
	destination := createAccount(t, h, apiKey, "liability")

	const requests = 100
	const idempotencyKey = "7da2f1c9-4e1b-4a22-9f3e-1d0c8b7a6e55"
	body := fmt.Sprintf(`{"source_account":%q,"destination_account":%q,"amount":50000,"currency":"USD"}`,
		source.ID, destination.ID)

	// Released together, so the requests genuinely overlap. Without this they
	// trickle out one at a time and the race never happens.
	var start sync.WaitGroup
	start.Add(1)
	var done sync.WaitGroup

	statuses := make([]int, requests)
	transferIDs := make([]string, requests)

	for i := 0; i < requests; i++ {
		done.Add(1)
		go func(i int) {
			defer done.Done()
			start.Wait()

			rec := doWithKey(h, "POST", "/v1/transfers", apiKey, idempotencyKey, body)
			statuses[i] = rec.Code

			var response transferResponse
			if err := json.Unmarshal(rec.Body.Bytes(), &response); err == nil {
				transferIDs[i] = response.ID
			}
		}(i)
	}

	start.Done()
	done.Wait()

	// The assertion that matters is the row count, not the responses. Counting
	// 202s would pass while the database held three transfers, because all
	// three requests succeeded as far as their callers could tell. The money is
	// in the table, so the table is what gets counted.
	var created int
	if err := testPool.QueryRow(ctx,
		`SELECT count(*) FROM transfers WHERE idempotency_key = $1`, idempotencyKey).Scan(&created); err != nil {
		t.Fatalf("count transfers: %v", err)
	}
	if created != 1 {
		t.Errorf("%d concurrent identical requests created %d transfers, want 1: "+
			"the same instruction was paid for %d times", requests, created, created)
	}

	// Every caller must get the same answer. A client that retries and receives
	// a different transfer id has no way to know which one is real.
	distinct := map[string]bool{}
	for _, id := range transferIDs {
		if id != "" {
			distinct[id] = true
		}
	}
	if len(distinct) != 1 {
		t.Errorf("callers received %d distinct transfer ids, want 1", len(distinct))
	}

	// And none of them may be an error: a retry answered with a 409 leaves the
	// client unable to tell "already done" from "rejected", and its only safe
	// move is to retry harder.
	for i, status := range statuses {
		if status != http.StatusAccepted {
			t.Errorf("request %d returned %d, want 202", i, status)
			break
		}
	}

	// The ledger must agree: one transfer means one movement of money.
	var balance int64
	if err := testPool.QueryRow(ctx, `
		SELECT COALESCE(SUM(CASE WHEN direction = 'credit' THEN amount ELSE -amount END), 0)
		FROM ledger_entries WHERE account_id = $1`, source.ID).Scan(&balance); err != nil {
		t.Fatalf("source balance: %v", err)
	}
	if balance != -50000 {
		t.Errorf("source balance is %d, want -50000: the money moved %d times",
			balance, balance/-50000)
	}
}

// A sequential retry must return the original transfer, not a new one. This is
// the case the naive implementation does handle, and it is worth pinning down
// so 3.2 cannot regress it.
func TestSequentialRetryReturnsTheSameTransfer(t *testing.T) {
	h, apiKey := newTestServer(t)

	source := createAccount(t, h, apiKey, "asset")
	destination := createAccount(t, h, apiKey, "liability")

	const idempotencyKey = "retry-me-please"
	body := fmt.Sprintf(`{"source_account":%q,"destination_account":%q,"amount":50000,"currency":"USD"}`,
		source.ID, destination.ID)

	first := doWithKey(h, "POST", "/v1/transfers", apiKey, idempotencyKey, body)
	if first.Code != http.StatusAccepted {
		t.Fatalf("first request status = %d, want 202: %s", first.Code, first.Body.String())
	}
	second := doWithKey(h, "POST", "/v1/transfers", apiKey, idempotencyKey, body)
	if second.Code != http.StatusAccepted {
		t.Fatalf("retry status = %d, want 202: %s", second.Code, second.Body.String())
	}

	var firstTransfer, secondTransfer transferResponse
	if err := json.Unmarshal(first.Body.Bytes(), &firstTransfer); err != nil {
		t.Fatalf("decode first: %v", err)
	}
	if err := json.Unmarshal(second.Body.Bytes(), &secondTransfer); err != nil {
		t.Fatalf("decode retry: %v", err)
	}
	if firstTransfer.ID != secondTransfer.ID {
		t.Errorf("retry returned transfer %s, want the original %s",
			secondTransfer.ID, firstTransfer.ID)
	}

	var created int
	if err := testPool.QueryRow(context.Background(),
		`SELECT count(*) FROM transfers`).Scan(&created); err != nil {
		t.Fatalf("count: %v", err)
	}
	if created != 1 {
		t.Errorf("a sequential retry created %d transfers, want 1", created)
	}
}

// The header is required. An optional key is forgotten exactly when it matters.
func TestTransferRequiresAnIdempotencyKey(t *testing.T) {
	h, apiKey := newTestServer(t)

	source := createAccount(t, h, apiKey, "asset")
	destination := createAccount(t, h, apiKey, "liability")
	body := fmt.Sprintf(`{"source_account":%q,"destination_account":%q,"amount":50000,"currency":"USD"}`,
		source.ID, destination.ID)

	tests := []struct{ name, key string }{
		{"missing", ""},
		{"blank", "   "},
		{"too long", strings.Repeat("k", maxIdempotencyKeyLength+1)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rec := doWithKey(h, "POST", "/v1/transfers", apiKey, tc.key, body)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400: %s", rec.Code, rec.Body.String())
			}
			if got := decodeError(t, rec).Code; got != CodeInvalidRequest {
				t.Errorf("error code = %q, want %q", got, CodeInvalidRequest)
			}

			var created int
			if err := testPool.QueryRow(context.Background(),
				`SELECT count(*) FROM transfers`).Scan(&created); err != nil {
				t.Fatalf("count: %v", err)
			}
			if created != 0 {
				t.Errorf("a rejected request created %d transfers, want 0", created)
			}
		})
	}
}

// Different keys are different payments. Two genuine payments to the same
// vendor for the same amount must both go through.
func TestDifferentKeysCreateDifferentTransfers(t *testing.T) {
	h, apiKey := newTestServer(t)

	source := createAccount(t, h, apiKey, "asset")
	destination := createAccount(t, h, apiKey, "liability")
	body := fmt.Sprintf(`{"source_account":%q,"destination_account":%q,"amount":50000,"currency":"USD"}`,
		source.ID, destination.ID)

	first := doWithKey(h, "POST", "/v1/transfers", apiKey, "invoice-001", body)
	second := doWithKey(h, "POST", "/v1/transfers", apiKey, "invoice-002", body)

	if first.Code != http.StatusAccepted || second.Code != http.StatusAccepted {
		t.Fatalf("statuses = %d and %d, want 202 and 202", first.Code, second.Code)
	}

	var created int
	if err := testPool.QueryRow(context.Background(),
		`SELECT count(*) FROM transfers`).Scan(&created); err != nil {
		t.Fatalf("count: %v", err)
	}
	if created != 2 {
		t.Errorf("two different keys created %d transfers, want 2", created)
	}
}
