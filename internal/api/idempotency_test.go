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
	"time"

	"github.com/Shailu-s/payments-platform/internal/auth"
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

// Same key, different request. A client bug, not a retry — and the dangerous
// kind, because returning the original transfer would let the caller believe a
// payment happened that never did.
func TestSameKeyDifferentBodyIsRejected(t *testing.T) {
	h, apiKey := newTestServer(t)
	ctx := context.Background()

	source := createAccount(t, h, apiKey, "asset")
	destination := createAccount(t, h, apiKey, "liability")

	const idempotencyKey = "invoice-4471"
	original := fmt.Sprintf(`{"source_account":%q,"destination_account":%q,"amount":50000,"currency":"USD"}`,
		source.ID, destination.ID)

	first := doWithKey(h, "POST", "/v1/transfers", apiKey, idempotencyKey, original)
	if first.Code != http.StatusAccepted {
		t.Fatalf("first request status = %d, want 202: %s", first.Code, first.Body.String())
	}
	var created transferResponse
	if err := json.Unmarshal(first.Body.Bytes(), &created); err != nil {
		t.Fatalf("decode: %v", err)
	}

	// Same key, a different amount.
	changed := fmt.Sprintf(`{"source_account":%q,"destination_account":%q,"amount":80000,"currency":"USD"}`,
		source.ID, destination.ID)
	second := doWithKey(h, "POST", "/v1/transfers", apiKey, idempotencyKey, changed)

	if second.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422: a reused key with a different body must not "+
			"silently return the original transfer: %s", second.Code, second.Body.String())
	}
	if got := decodeError(t, second).Code; got != CodeIdempotencyKeyReused {
		t.Errorf("error code = %q, want %q", got, CodeIdempotencyKeyReused)
	}

	// The original must be untouched, and no second transfer created.
	var count int
	if err := testPool.QueryRow(ctx, `SELECT count(*) FROM transfers`).Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 1 {
		t.Errorf("%d transfers exist, want 1", count)
	}

	var amount int64
	if err := testPool.QueryRow(ctx,
		`SELECT amount FROM transfers WHERE id = $1`, created.ID).Scan(&amount); err != nil {
		t.Fatalf("read original: %v", err)
	}
	if amount != 50000 {
		t.Errorf("the original transfer is now %d, want 50000: it must not be modified", amount)
	}
}

// A changed destination is as much a different request as a changed amount.
func TestSameKeyDifferentDestinationIsRejected(t *testing.T) {
	h, apiKey := newTestServer(t)

	source := createAccount(t, h, apiKey, "asset")
	first := createAccount(t, h, apiKey, "liability")
	second := createAccount(t, h, apiKey, "liability")

	const idempotencyKey = "payout-991"
	toFirst := fmt.Sprintf(`{"source_account":%q,"destination_account":%q,"amount":50000,"currency":"USD"}`,
		source.ID, first.ID)
	toSecond := fmt.Sprintf(`{"source_account":%q,"destination_account":%q,"amount":50000,"currency":"USD"}`,
		source.ID, second.ID)

	if rec := doWithKey(h, "POST", "/v1/transfers", apiKey, idempotencyKey, toFirst); rec.Code != http.StatusAccepted {
		t.Fatalf("first status = %d, want 202", rec.Code)
	}
	rec := doWithKey(h, "POST", "/v1/transfers", apiKey, idempotencyKey, toSecond)
	if rec.Code != http.StatusUnprocessableEntity {
		t.Errorf("status = %d, want 422: paying a different vendor with the same key "+
			"must be refused", rec.Code)
	}
}

// Two callers must not collide on each other's keys, and one must not be able
// to read another's transfer by guessing one.
func TestIdempotencyKeysAreScopedToTheCaller(t *testing.T) {
	h, firstKey := newTestServer(t)
	ctx := context.Background()

	secondPlaintext, secondKeyRow, err := auth.Generate("second caller")
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if err := auth.Insert(ctx, testPool, secondKeyRow); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	source := createAccount(t, h, firstKey, "asset")
	destination := createAccount(t, h, firstKey, "liability")

	const sharedKey = "both-callers-chose-this"
	body := fmt.Sprintf(`{"source_account":%q,"destination_account":%q,"amount":50000,"currency":"USD"}`,
		source.ID, destination.ID)

	if rec := doWithKey(h, "POST", "/v1/transfers", firstKey, sharedKey, body); rec.Code != http.StatusAccepted {
		t.Fatalf("first caller status = %d, want 202", rec.Code)
	}

	// The second caller sends the same key with the same body. The fingerprint
	// includes the api key, so this is a different request and is refused
	// rather than answered with the first caller's transfer.
	rec := doWithKey(h, "POST", "/v1/transfers", secondPlaintext, sharedKey, body)
	if rec.Code == http.StatusAccepted {
		var leaked transferResponse
		json.Unmarshal(rec.Body.Bytes(), &leaked)
		t.Errorf("the second caller received transfer %s, which belongs to the first",
			leaked.ID)
	}
	if rec.Code != http.StatusUnprocessableEntity {
		t.Errorf("status = %d, want 422", rec.Code)
	}
}

// A replay must return the ledger state of the original, not create more.
func TestReplayDoesNotMoveMoneyAgain(t *testing.T) {
	h, apiKey := newTestServer(t)
	ctx := context.Background()

	source := createAccount(t, h, apiKey, "asset")
	destination := createAccount(t, h, apiKey, "liability")

	const idempotencyKey = "only-once"
	body := fmt.Sprintf(`{"source_account":%q,"destination_account":%q,"amount":50000,"currency":"USD"}`,
		source.ID, destination.ID)

	for i := 0; i < 5; i++ {
		rec := doWithKey(h, "POST", "/v1/transfers", apiKey, idempotencyKey, body)
		if rec.Code != http.StatusAccepted {
			t.Fatalf("request %d status = %d, want 202: %s", i, rec.Code, rec.Body.String())
		}
	}

	var entries int
	if err := testPool.QueryRow(ctx, `SELECT count(*) FROM ledger_entries`).Scan(&entries); err != nil {
		t.Fatalf("count entries: %v", err)
	}
	if entries != 2 {
		t.Errorf("%d ledger entries after 5 identical requests, want 2: "+
			"a replay must not write new accounting", entries)
	}

	var balance int64
	if err := testPool.QueryRow(ctx, `
		SELECT COALESCE(SUM(CASE WHEN direction = 'credit' THEN amount ELSE -amount END), 0)
		FROM ledger_entries WHERE account_id = $1`, source.ID).Scan(&balance); err != nil {
		t.Fatalf("balance: %v", err)
	}
	if balance != -50000 {
		t.Errorf("source balance is %d, want -50000", balance)
	}
}

// The case most implementations get wrong: the key is taken but the winning
// transaction has not committed, so the row is not visible to anyone else.
//
// Simulated by taking the key in an open transaction and holding it while a
// second request arrives. What the second request must NOT do is conclude the
// transfer does not exist and create another one.
func TestKeyTakenButNotYetCommittedIsSerialised(t *testing.T) {
	h, apiKey := newTestServer(t)
	ctx := context.Background()

	source := createAccount(t, h, apiKey, "asset")
	destination := createAccount(t, h, apiKey, "liability")

	var apiKeyID string
	if err := testPool.QueryRow(ctx, `SELECT id FROM api_keys LIMIT 1`).Scan(&apiKeyID); err != nil {
		t.Fatalf("read api key: %v", err)
	}

	const idempotencyKey = "in-flight-key"

	// Hold the key in an uncommitted transaction, exactly as a winning request
	// would while it finishes the rest of its work.
	tx, err := testPool.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	holderID := newID("tr")
	// The same fingerprint the handler will compute for the second request, so
	// this stands in for a genuine first attempt rather than a different one.
	holderFingerprint := fingerprintRequest(apiKeyID, createTransferRequest{
		SourceAccount:      source.ID,
		DestinationAccount: destination.ID,
		Amount:             50000,
		Currency:           "USD",
	})
	if _, err := tx.Exec(ctx, `
		INSERT INTO transfers (id, source_account, destination_account, amount,
			currency, status, api_key_id, idempotency_key, request_fingerprint)
		VALUES ($1, $2, $3, 50000, 'USD', 'processing', $4, $5, $6)`,
		holderID, source.ID, destination.ID, apiKeyID, idempotencyKey,
		holderFingerprint); err != nil {
		tx.Rollback(ctx)
		t.Fatalf("hold the key: %v", err)
	}

	// While the key is held, nothing is visible to anyone else.
	var visible int
	if err := testPool.QueryRow(ctx, `SELECT count(*) FROM transfers`).Scan(&visible); err != nil {
		tx.Rollback(ctx)
		t.Fatalf("count: %v", err)
	}
	if visible != 0 {
		tx.Rollback(ctx)
		t.Fatalf("%d transfers visible, want 0: the holder has not committed", visible)
	}

	// A second request for the same key. Its insert blocks on the held index
	// entry rather than proceeding — that blocking IS the serialisation, and it
	// is what application code cannot do for itself.
	body := fmt.Sprintf(`{"source_account":%q,"destination_account":%q,"amount":50000,"currency":"USD"}`,
		source.ID, destination.ID)
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		done <- doWithKey(h, "POST", "/v1/transfers", apiKey, idempotencyKey, body)
	}()

	select {
	case rec := <-done:
		tx.Rollback(ctx)
		t.Fatalf("the second request returned %d without waiting: it should have "+
			"blocked on the held index entry", rec.Code)
	case <-time.After(300 * time.Millisecond):
		t.Log("the second request is blocked on the held index entry: the unique " +
			"index is serialising the two writers, which no mutex could do across " +
			"processes")
	}

	// Release the holder. The blocked request now discovers the key is taken.
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	select {
	case rec := <-done:
		// It must replay the holder's transfer, not create a second one.
		if rec.Code != http.StatusAccepted {
			t.Errorf("status = %d, want 202 replaying the committed transfer: %s",
				rec.Code, rec.Body.String())
		}
		var replayed transferResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &replayed); err == nil && replayed.ID != holderID {
			t.Errorf("replayed transfer %s, want the holder's %s", replayed.ID, holderID)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the second request never completed after the holder committed")
	}

	var created int
	if err := testPool.QueryRow(ctx, `SELECT count(*) FROM transfers`).Scan(&created); err != nil {
		t.Fatalf("count: %v", err)
	}
	if created != 1 {
		t.Errorf("%d transfers exist, want 1", created)
	}
}
