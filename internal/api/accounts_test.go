package api

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/Shailu-s/payments-platform/internal/ledger"
)

func createAccount(t *testing.T, h http.Handler, key, accountType string) accountResponse {
	t.Helper()
	rec := do(t, h, "POST", "/v1/accounts", key, `{"currency":"USD","type":"`+accountType+`"}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("POST /v1/accounts status = %d, want 201: %s", rec.Code, rec.Body.String())
	}
	var acc accountResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &acc); err != nil {
		t.Fatalf("decode account: %v", err)
	}
	return acc
}

func TestCreateAccountReturns201(t *testing.T) {
	h, key := newTestServer(t)

	acc := createAccount(t, h, key, "asset")
	if !strings.HasPrefix(acc.ID, "acc_") {
		t.Errorf("id = %q, want an acc_ prefix", acc.ID)
	}
	if acc.Currency != "USD" || acc.Type != "asset" {
		t.Errorf("got currency %q type %q, want USD/asset", acc.Currency, acc.Type)
	}
	if acc.Balance != 0 {
		t.Errorf("a new account has balance %d, want 0", acc.Balance)
	}
	if acc.CreatedAt.IsZero() {
		t.Error("created_at is zero")
	}
}

func TestCreateAccountRejectsBadInput(t *testing.T) {
	h, key := newTestServer(t)

	tests := []struct{ name, body string }{
		{"empty body", ``},
		{"not json", `not json at all`},
		{"missing currency", `{"type":"asset"}`},
		{"unsupported currency", `{"currency":"EUR","type":"asset"}`},
		{"missing type", `{"currency":"USD"}`},
		{"unknown type", `{"currency":"USD","type":"chequing"}`},
		{"unknown field", `{"currency":"USD","type":"asset","balance":9999}`},
		{"two objects", `{"currency":"USD","type":"asset"}{"currency":"USD","type":"asset"}`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rec := do(t, h, "POST", "/v1/accounts", key, tc.body)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400: %s", rec.Code, rec.Body.String())
			}
			if got := decodeError(t, rec).Code; got != CodeInvalidRequest {
				t.Errorf("error code = %q, want %q", got, CodeInvalidRequest)
			}
		})
	}
}

// Misspelled fields must not silently fall back to zero values.
func TestCreateAccountRejectsUnknownFieldsRatherThanIgnoringThem(t *testing.T) {
	h, key := newTestServer(t)

	rec := do(t, h, "POST", "/v1/accounts", key, `{"currency":"USD","type":"asset","typo":1}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}

	// Excluding the system settlement accounts, which the harness restores
	// because migrations create them.
	var count int
	if err := testPool.QueryRow(context.Background(),
		`SELECT count(*) FROM accounts WHERE id NOT IN ('acc_settlement_usd', 'acc_network_payable_usd')`).Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 0 {
		t.Errorf("a rejected request created %d accounts, want 0", count)
	}
}

func TestGetAccountReturnsEachAccountsBalance(t *testing.T) {
	h, key := newTestServer(t)
	ctx := context.Background()

	source := createAccount(t, h, key, "asset")
	destination := createAccount(t, h, key, "settlement")

	if _, err := ledger.Record(ctx, testPool, "test movement", []ledger.Entry{
		{AccountID: destination.ID, Direction: ledger.DirectionDebit, Amount: 50000},
		{AccountID: source.ID, Direction: ledger.DirectionCredit, Amount: 50000},
	}); err != nil {
		t.Fatalf("Record: %v", err)
	}

	rec := do(t, h, "GET", "/v1/accounts/"+source.ID, key, "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", rec.Code, rec.Body.String())
	}
	var got accountResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Balance != 50000 {
		t.Errorf("balance = %d, want 50000", got.Balance)
	}

	rec = do(t, h, "GET", "/v1/accounts/"+destination.ID, key, "")
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Balance != -50000 {
		t.Errorf("balance = %d, want -50000", got.Balance)
	}
}

// The stored balance is what GET serves, so it must agree with the entries
// after real movements; settlement still reports the sum of its entries.
func TestGetAccountBalanceMatchesTheLedger(t *testing.T) {
	h, key := newTestServer(t)
	ctx := context.Background()

	source := createAccount(t, h, key, "asset")
	destination := createAccount(t, h, key, "liability")
	fund(t, source.ID, 100000)
	for _, amount := range []int64{12345, 600, 7} {
		createTransfer(t, h, key, source.ID, destination.ID, amount)
	}

	getBalance := func(id string) int64 {
		t.Helper()
		rec := do(t, h, "GET", "/v1/accounts/"+id, key, "")
		if rec.Code != http.StatusOK {
			t.Fatalf("GET %s status = %d, want 200: %s", id, rec.Code, rec.Body.String())
		}
		var got accountResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
			t.Fatalf("decode: %v", err)
		}
		return got.Balance
	}

	for _, id := range []string{source.ID, destination.ID, settlementAccountID} {
		derived, err := ledger.Balance(ctx, testPool, id)
		if err != nil {
			t.Fatalf("Balance(%s): %v", id, err)
		}
		if got := getBalance(id); got != derived {
			t.Errorf("GET %s balance = %d, derived from entries = %d", id, got, derived)
		}
	}
	if got := getBalance(source.ID); got != 100000-12345-600-7 {
		t.Errorf("source balance = %d, want %d", got, 100000-12345-600-7)
	}
	if got := getBalance(settlementAccountID); got != 12345+600+7-100000 {
		t.Errorf("settlement balance = %d, want %d", got, 12345+600+7-100000)
	}

	// Pin the read path: a deliberately drifted row shows that GET serves the
	// stored number rather than summing history.
	if _, err := testPool.Exec(ctx,
		`UPDATE account_balances SET balance = balance + 1 WHERE account_id = $1`, source.ID); err != nil {
		t.Fatalf("drift: %v", err)
	}
	if got := getBalance(source.ID); got != 100000-12345-600-7+1 {
		t.Errorf("source balance = %d after drifting the stored row, want the stored %d",
			got, 100000-12345-600-7+1)
	}
}

// Balance alone cannot distinguish an unknown account from an unused one.
func TestGetUnknownAccountIs404NotAZeroBalance(t *testing.T) {
	h, key := newTestServer(t)

	rec := do(t, h, "GET", "/v1/accounts/acc_never_created", key, "")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404: %s", rec.Code, rec.Body.String())
	}
	if got := decodeError(t, rec).Code; got != CodeNotFound {
		t.Errorf("error code = %q, want %q", got, CodeNotFound)
	}
}

func TestCreateAccountNormalisesCase(t *testing.T) {
	h, key := newTestServer(t)

	rec := do(t, h, "POST", "/v1/accounts", key, `{"currency":"usd","type":"ASSET"}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201: %s", rec.Code, rec.Body.String())
	}
	var acc accountResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &acc); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if acc.Currency != "USD" || acc.Type != "asset" {
		t.Errorf("got %q/%q, want USD/asset", acc.Currency, acc.Type)
	}
}
