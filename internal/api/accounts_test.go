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

// An unknown field is rejected rather than ignored: a caller who sends
// "ammount" should be told, not silently charged zero.
func TestCreateAccountRejectsUnknownFieldsRatherThanIgnoringThem(t *testing.T) {
	h, key := newTestServer(t)

	rec := do(t, h, "POST", "/v1/accounts", key, `{"currency":"USD","type":"asset","typo":1}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}

	var count int
	if err := testPool.QueryRow(context.Background(), `SELECT count(*) FROM accounts`).Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 0 {
		t.Errorf("a rejected request created %d accounts, want 0", count)
	}
}

func TestGetAccountReturnsADerivedBalance(t *testing.T) {
	h, key := newTestServer(t)
	ctx := context.Background()

	source := createAccount(t, h, key, "asset")
	destination := createAccount(t, h, key, "settlement")

	if _, err := ledger.Record(ctx, testPool, "test movement", []ledger.Entry{
		{AccountID: source.ID, Direction: ledger.DirectionDebit, Amount: 50000},
		{AccountID: destination.ID, Direction: ledger.DirectionCredit, Amount: 50000},
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
	if got.Balance != -50000 {
		t.Errorf("balance = %d, want -50000", got.Balance)
	}

	rec = do(t, h, "GET", "/v1/accounts/"+destination.ID, key, "")
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Balance != 50000 {
		t.Errorf("balance = %d, want 50000", got.Balance)
	}
}

// ledger.Balance returns 0 for an id that was never created, so the handler
// must look the account up separately. Using Balance as an existence check
// would return a cheerful 200 with a zero balance for a typo.
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
