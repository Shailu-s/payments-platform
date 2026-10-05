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
	fund(t, source.ID, 1000000)

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

// The destination must not be credited before provider confirmation.
func TestCreateTransferCreditsSettlementNotTheDestination(t *testing.T) {
	h, key := newTestServer(t)
	ctx := context.Background()
	source := createAccount(t, h, key, "asset")
	destination := createAccount(t, h, key, "liability")
	fund(t, source.ID, 1000000)

	createTransfer(t, h, key, source.ID, destination.ID, 50000)

	sourceBalance, err := ledger.Balance(ctx, testPool, source.ID)
	if err != nil {
		t.Fatalf("Balance(source): %v", err)
	}
	const funded = 1000000
	if sourceBalance != funded-50000 {
		t.Errorf("source balance = %d, want %d", sourceBalance, funded-50000)
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
	// Settlement lent the account its funding and received the transfer back.
	if settlementBalance != -(funded - 50000) {
		t.Errorf("settlement balance = %d, want %d", settlementBalance, -(funded - 50000))
	}
}

// Guarantee 1: transfers created through the API keep the ledger balanced.
func TestCreateTransferKeepsTheBooksBalanced(t *testing.T) {
	h, key := newTestServer(t)
	ctx := context.Background()
	source := createAccount(t, h, key, "asset")
	destination := createAccount(t, h, key, "liability")
	fund(t, source.ID, 1000000)

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

func TestRejectedTransferLeavesNothingBehind(t *testing.T) {
	h, key := newTestServer(t)
	ctx := context.Background()
	source := createAccount(t, h, key, "asset")

	// Missing-account validation rejects this before any writes.
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
	fund(t, source.ID, 1000000)

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
	fund(t, source.ID, 1000000)

	body := fmt.Sprintf(`{"source_account":%q,"destination_account":%q,"amount":500.75,"currency":"USD"}`,
		source.ID, destination.ID)
	rec := doWithKey(h, "POST", "/v1/transfers", key, newID("idem"), body)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400: a fractional amount must not be truncated", rec.Code)
	}
}

// Guarantee 5: transfer creation records the authorising caller.
func TestTransferRecordsTheAuthenticatingKey(t *testing.T) {
	h, key := newTestServer(t)
	ctx := context.Background()
	source := createAccount(t, h, key, "asset")
	destination := createAccount(t, h, key, "liability")
	fund(t, source.ID, 1000000)

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
	fund(t, source.ID, 1000000)
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
	fund(t, source.ID, 1000000)

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
	if page.Data[0].ID != created[4] {
		t.Errorf("first transfer = %s, want the newest %s", page.Data[0].ID, created[4])
	}

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

// Ignore funding rows when checking that rejection wrote no transfer accounting.
func assertNothingWritten(t *testing.T, ctx context.Context) {
	t.Helper()
	counts := func() (transfers, txns, entries int) {
		if err := testPool.QueryRow(ctx, `SELECT count(*) FROM transfers`).Scan(&transfers); err != nil {
			t.Fatalf("count transfers: %v", err)
		}
		if err := testPool.QueryRow(ctx, `SELECT count(*) FROM ledger_transactions`).Scan(&txns); err != nil {
			t.Fatalf("count ledger transactions: %v", err)
		}
		if err := testPool.QueryRow(ctx, `SELECT count(*) FROM ledger_entries`).Scan(&entries); err != nil {
			t.Fatalf("count ledger entries: %v", err)
		}
		return
	}

	transferCount, _, _ := counts()
	if transferCount != 0 {
		t.Errorf("a rejected request left %d transfers, want 0", transferCount)
	}

	var orphaned int
	if err := testPool.QueryRow(ctx, `
		SELECT count(*) FROM ledger_transactions WHERE reference LIKE 'transfer %'`).Scan(&orphaned); err != nil {
		t.Fatalf("count orphaned: %v", err)
	}
	if orphaned != 0 {
		t.Errorf("a rejected request left %d transfer ledger transactions, want 0", orphaned)
	}
}

// Remove settlement to force an FK failure after the transfer insert, testing rollback
// rather than validation before BEGIN.
func TestTransferRollsBackWhenTheLedgerWriteFails(t *testing.T) {
	h, key := newTestServer(t)
	ctx := context.Background()
	source := createAccount(t, h, key, "asset")
	destination := createAccount(t, h, key, "liability")

	// Funded from a second account rather than settlement, because settlement
	// is about to be removed and a ledger entry against it would block that.
	funder := createAccount(t, h, key, "asset")
	if _, err := ledger.Record(ctx, testPool, "funding", []ledger.Entry{
		{AccountID: funder.ID, Direction: ledger.DirectionDebit, Amount: 1000000},
		{AccountID: source.ID, Direction: ledger.DirectionCredit, Amount: 1000000},
	}); err != nil {
		t.Fatalf("fund: %v", err)
	}

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

	var transferCount int
	if err := testPool.QueryRow(ctx, `SELECT count(*) FROM transfers`).Scan(&transferCount); err != nil {
		t.Fatalf("count transfers: %v", err)
	}
	if transferCount != 0 {
		t.Errorf("%d transfers survived a failed ledger write, want 0", transferCount)
	}

	// Only transfer movements: the funding transaction above is legitimate.
	var txnCount int
	if err := testPool.QueryRow(ctx,
		`SELECT count(*) FROM ledger_transactions WHERE reference LIKE 'transfer %'`).Scan(&txnCount); err != nil {
		t.Fatalf("count ledger transactions: %v", err)
	}
	if txnCount != 0 {
		t.Errorf("%d orphaned ledger transactions, want 0", txnCount)
	}
}
