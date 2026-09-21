package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/Shailu-s/payments-platform/internal/ledger"
	"github.com/Shailu-s/payments-platform/internal/transfers"
)

func createTransfer(t *testing.T, h http.Handler, key, source, destination string, amount int64) transferResponse {
	t.Helper()
	body := fmt.Sprintf(`{"source_account":%q,"destination_account":%q,"amount":%d,"currency":"USD"}`,
		source, destination, amount)

	// A fresh key per call: these are distinct payments, not retries of one.
	rec := doWithKey(h, "POST", "/v1/transfers", key, newID("idem"), body)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("POST /v1/transfers status = %d, want 202: %s", rec.Code, rec.Body.String())
	}
	var got transferResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode transfer: %v", err)
	}
	return got
}

// 202 and "processing", never 201 and "settled": we accepted the instruction,
// the money has not reached the destination.
func TestCreateTransferAccepts202Processing(t *testing.T) {
	h, key := newTestServer(t)
	source := createAccount(t, h, key, "asset")
	destination := createAccount(t, h, key, "liability")

	got := createTransfer(t, h, key, source.ID, destination.ID, 50000)

	if !strings.HasPrefix(got.ID, "tr_") {
		t.Errorf("id = %q, want a tr_ prefix", got.ID)
	}
	if got.Status != transfers.StatusProcessing {
		t.Errorf("status = %q, want %q", got.Status, transfers.StatusProcessing)
	}
	if got.Amount != 50000 {
		t.Errorf("amount = %d, want 50000", got.Amount)
	}
	if got.LedgerTxnID == nil || *got.LedgerTxnID == "" {
		t.Error("transfer has no ledger transaction id")
	}
}

// The money is debited from the source and credited to SETTLEMENT, not to the
// destination. At this moment the money is ours and earmarked; the destination
// is credited in phase 4 when the provider confirms. Crediting it now would
// write a permanent lie into an append-only table.
func TestCreateTransferCreditsSettlementNotTheDestination(t *testing.T) {
	h, key := newTestServer(t)
	ctx := context.Background()
	source := createAccount(t, h, key, "asset")
	destination := createAccount(t, h, key, "liability")

	createTransfer(t, h, key, source.ID, destination.ID, 50000)

	sourceBalance, err := ledger.Balance(ctx, testPool, source.ID)
	if err != nil {
		t.Fatalf("Balance(source): %v", err)
	}
	if sourceBalance != -50000 {
		t.Errorf("source balance = %d, want -50000", sourceBalance)
	}

	destinationBalance, err := ledger.Balance(ctx, testPool, destination.ID)
	if err != nil {
		t.Fatalf("Balance(destination): %v", err)
	}
	if destinationBalance != 0 {
		t.Errorf("destination balance = %d, want 0: the money has not arrived yet", destinationBalance)
	}

	settlementBalance, err := ledger.Balance(ctx, testPool, settlementAccountID)
	if err != nil {
		t.Fatalf("Balance(settlement): %v", err)
	}
	if settlementBalance != 50000 {
		t.Errorf("settlement balance = %d, want 50000", settlementBalance)
	}
}

// Guarantee 1 holds across the API, not just inside the ledger package.
func TestCreateTransferKeepsTheBooksBalanced(t *testing.T) {
	h, key := newTestServer(t)
	ctx := context.Background()
	source := createAccount(t, h, key, "asset")
	destination := createAccount(t, h, key, "liability")

	for _, amount := range []int64{50000, 250, 1} {
		createTransfer(t, h, key, source.ID, destination.ID, amount)
	}

	var sum int64
	if err := testPool.QueryRow(ctx, `
		SELECT COALESCE(SUM(CASE WHEN direction = 'credit' THEN amount ELSE -amount END), 0)
		FROM ledger_entries`).Scan(&sum); err != nil {
		t.Fatalf("sum entries: %v", err)
	}
	if sum != 0 {
		t.Errorf("all entries sum to %d, want 0", sum)
	}
}

// The transfer and its ledger entries are one atomic unit. A transfer with no
// accounting behind it is an instruction nobody recorded; entries with no
// transfer are money moved for no stated reason.
func TestRejectedTransferLeavesNothingBehind(t *testing.T) {
	h, key := newTestServer(t)
	ctx := context.Background()
	source := createAccount(t, h, key, "asset")

	// The destination does not exist, so the request fails after validation
	// would have to touch the database.
	body := fmt.Sprintf(`{"source_account":%q,"destination_account":"acc_nope","amount":500,"currency":"USD"}`,
		source.ID)
	rec := doWithKey(h, "POST", "/v1/transfers", key, newID("idem"), body)
	if rec.Code == http.StatusAccepted {
		t.Fatal("a transfer to a nonexistent account was accepted")
	}

	assertNothingWritten(t, ctx)
}

func TestCreateTransferRejectsBadInput(t *testing.T) {
	h, key := newTestServer(t)
	source := createAccount(t, h, key, "asset")
	destination := createAccount(t, h, key, "liability")

	tests := []struct {
		name   string
		body   string
		status int
	}{
		{"empty body", ``, http.StatusBadRequest},
		{"missing source", fmt.Sprintf(`{"destination_account":%q,"amount":500,"currency":"USD"}`, destination.ID), http.StatusBadRequest},
		{"missing destination", fmt.Sprintf(`{"source_account":%q,"amount":500,"currency":"USD"}`, source.ID), http.StatusBadRequest},
		{"zero amount", fmt.Sprintf(`{"source_account":%q,"destination_account":%q,"amount":0,"currency":"USD"}`, source.ID, destination.ID), http.StatusBadRequest},
		{"negative amount", fmt.Sprintf(`{"source_account":%q,"destination_account":%q,"amount":-500,"currency":"USD"}`, source.ID, destination.ID), http.StatusBadRequest},
		{"same account", fmt.Sprintf(`{"source_account":%q,"destination_account":%q,"amount":500,"currency":"USD"}`, source.ID, source.ID), http.StatusBadRequest},
		{"wrong currency", fmt.Sprintf(`{"source_account":%q,"destination_account":%q,"amount":500,"currency":"EUR"}`, source.ID, destination.ID), http.StatusBadRequest},
		{"unknown source", fmt.Sprintf(`{"source_account":"acc_nope","destination_account":%q,"amount":500,"currency":"USD"}`, destination.ID), http.StatusBadRequest},
		{"unknown field", fmt.Sprintf(`{"source_account":%q,"destination_account":%q,"amount":500,"currency":"USD","fee":1}`, source.ID, destination.ID), http.StatusBadRequest},
		{"amount as a float", fmt.Sprintf(`{"source_account":%q,"destination_account":%q,"amount":500.75,"currency":"USD"}`, source.ID, destination.ID), http.StatusBadRequest},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rec := doWithKey(h, "POST", "/v1/transfers", key, newID("idem"), tc.body)
			if rec.Code != tc.status {
				t.Fatalf("status = %d, want %d: %s", rec.Code, tc.status, rec.Body.String())
			}
			assertNothingWritten(t, context.Background())
		})
	}
}

// Money is an integer in minor units. A fractional amount must be refused
// rather than silently truncated, which would move the wrong sum.
func TestCreateTransferRejectsAFractionalAmount(t *testing.T) {
	h, key := newTestServer(t)
	source := createAccount(t, h, key, "asset")
	destination := createAccount(t, h, key, "liability")

	body := fmt.Sprintf(`{"source_account":%q,"destination_account":%q,"amount":500.75,"currency":"USD"}`,
		source.ID, destination.ID)
	rec := doWithKey(h, "POST", "/v1/transfers", key, newID("idem"), body)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400: a fractional amount must not be truncated", rec.Code)
	}
}

// Guarantee 5: every financial state change records who asked for it.
func TestTransferRecordsTheAuthenticatingKey(t *testing.T) {
	h, key := newTestServer(t)
	ctx := context.Background()
	source := createAccount(t, h, key, "asset")
	destination := createAccount(t, h, key, "liability")

	created := createTransfer(t, h, key, source.ID, destination.ID, 500)

	var apiKeyID string
	if err := testPool.QueryRow(ctx,
		`SELECT api_key_id FROM transfers WHERE id = $1`, created.ID).Scan(&apiKeyID); err != nil {
		t.Fatalf("select api_key_id: %v", err)
	}
	if apiKeyID == "" {
		t.Fatal("transfer has no api key recorded")
	}

	var name string
	if err := testPool.QueryRow(ctx,
		`SELECT name FROM api_keys WHERE id = $1`, apiKeyID).Scan(&name); err != nil {
		t.Fatalf("the recorded api key does not exist: %v", err)
	}
	if name != "test" {
		t.Errorf("transfer attributed to key %q, want the caller's", name)
	}
}

func TestGetTransfer(t *testing.T) {
	h, key := newTestServer(t)
	source := createAccount(t, h, key, "asset")
	destination := createAccount(t, h, key, "liability")
	created := createTransfer(t, h, key, source.ID, destination.ID, 50000)

	rec := do(t, h, "GET", "/v1/transfers/"+created.ID, key, "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", rec.Code, rec.Body.String())
	}
	var got transferResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.ID != created.ID || got.Amount != 50000 {
		t.Errorf("got %+v, want the created transfer", got)
	}
}

func TestGetUnknownTransferIs404(t *testing.T) {
	h, key := newTestServer(t)

	rec := do(t, h, "GET", "/v1/transfers/tr_never_created", key, "")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
	if got := decodeError(t, rec).Code; got != CodeNotFound {
		t.Errorf("error code = %q, want %q", got, CodeNotFound)
	}
}

func TestListTransfersIsNewestFirstAndPages(t *testing.T) {
	h, key := newTestServer(t)
	source := createAccount(t, h, key, "asset")
	destination := createAccount(t, h, key, "liability")

	var created []string
	for i := 0; i < 5; i++ {
		created = append(created, createTransfer(t, h, key, source.ID, destination.ID, int64(100+i)).ID)
	}

	rec := do(t, h, "GET", "/v1/transfers?limit=2", key, "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", rec.Code, rec.Body.String())
	}
	var page listTransfersResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &page); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(page.Data) != 2 {
		t.Fatalf("page has %d transfers, want 2", len(page.Data))
	}
	if page.NextCursor == nil {
		t.Fatal("no next_cursor on a page that has more")
	}
	// Newest first: the last created comes back first.
	if page.Data[0].ID != created[4] {
		t.Errorf("first transfer = %s, want the newest %s", page.Data[0].ID, created[4])
	}

	// Walk the rest and confirm every transfer appears exactly once.
	seen := map[string]int{}
	for _, tr := range page.Data {
		seen[tr.ID]++
	}
	cursor := *page.NextCursor
	for cursor != "" {
		rec := do(t, h, "GET", "/v1/transfers?limit=2&cursor="+cursor, key, "")
		if rec.Code != http.StatusOK {
			t.Fatalf("paging status = %d: %s", rec.Code, rec.Body.String())
		}
		var next listTransfersResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &next); err != nil {
			t.Fatalf("decode: %v", err)
		}
		for _, tr := range next.Data {
			seen[tr.ID]++
		}
		if next.NextCursor == nil {
			break
		}
		cursor = *next.NextCursor
	}

	if len(seen) != 5 {
		t.Errorf("paging returned %d distinct transfers, want 5", len(seen))
	}
	for id, count := range seen {
		if count != 1 {
			t.Errorf("transfer %s appeared %d times across pages, want 1", id, count)
		}
	}
}

func TestListTransfersReturnsAnEmptyArrayNotNull(t *testing.T) {
	h, key := newTestServer(t)

	rec := do(t, h, "GET", "/v1/transfers", key, "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), `"data":[]`) {
		t.Errorf("empty list body = %s, want data as []", rec.Body.String())
	}
}

func TestListTransfersRejectsABadLimitOrCursor(t *testing.T) {
	h, key := newTestServer(t)

	for _, query := range []string{"?limit=0", "?limit=-1", "?limit=101", "?limit=abc", "?cursor=!!!not-base64"} {
		t.Run(query, func(t *testing.T) {
			rec := do(t, h, "GET", "/v1/transfers"+query, key, "")
			if rec.Code != http.StatusBadRequest {
				t.Errorf("status = %d, want 400", rec.Code)
			}
		})
	}
}

// assertNothingWritten confirms a rejected request left no transfer and no
// ledger rows. An error response means nothing if half the work was committed.
func assertNothingWritten(t *testing.T, ctx context.Context) {
	t.Helper()
	var transferCount, txnCount, entryCount int
	if err := testPool.QueryRow(ctx, `SELECT count(*) FROM transfers`).Scan(&transferCount); err != nil {
		t.Fatalf("count transfers: %v", err)
	}
	if err := testPool.QueryRow(ctx, `SELECT count(*) FROM ledger_transactions`).Scan(&txnCount); err != nil {
		t.Fatalf("count ledger transactions: %v", err)
	}
	if err := testPool.QueryRow(ctx, `SELECT count(*) FROM ledger_entries`).Scan(&entryCount); err != nil {
		t.Fatalf("count ledger entries: %v", err)
	}
	if transferCount != 0 || txnCount != 0 || entryCount != 0 {
		t.Errorf("a rejected request left %d transfers, %d ledger transactions, %d entries; want 0, 0, 0",
			transferCount, txnCount, entryCount)
	}
}

// A failure that happens after the transfer row is written and before the
// ledger entries are. Validation cannot catch this one, so only the database
// transaction prevents a transfer existing with no accounting behind it.
//
// The settlement account is removed to force it: ledger.Record then violates a
// foreign key, mid-transaction.
func TestTransferRollsBackWhenTheLedgerWriteFails(t *testing.T) {
	h, key := newTestServer(t)
	ctx := context.Background()
	source := createAccount(t, h, key, "asset")
	destination := createAccount(t, h, key, "liability")

	if _, err := testPool.Exec(ctx,
		`DELETE FROM accounts WHERE id = $1`, settlementAccountID); err != nil {
		t.Fatalf("remove settlement account: %v", err)
	}

	body := fmt.Sprintf(`{"source_account":%q,"destination_account":%q,"amount":50000,"currency":"USD"}`,
		source.ID, destination.ID)
	rec := doWithKey(h, "POST", "/v1/transfers", key, newID("idem"), body)

	if rec.Code == http.StatusAccepted {
		t.Fatal("a transfer was accepted although its ledger entries could not be written")
	}

	// The transfer row must not survive its own failed accounting.
	var transferCount int
	if err := testPool.QueryRow(ctx, `SELECT count(*) FROM transfers`).Scan(&transferCount); err != nil {
		t.Fatalf("count transfers: %v", err)
	}
	if transferCount != 0 {
		t.Errorf("%d transfers survived a failed ledger write, want 0", transferCount)
	}

	var txnCount int
	if err := testPool.QueryRow(ctx, `SELECT count(*) FROM ledger_transactions`).Scan(&txnCount); err != nil {
		t.Fatalf("count ledger transactions: %v", err)
	}
	if txnCount != 0 {
		t.Errorf("%d orphaned ledger transactions, want 0", txnCount)
	}
}
