package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"testing"
)

func issueCard(t *testing.T, h http.Handler, apiKey, idempotencyKey, accountID string) cardResponse {
	t.Helper()
	body := fmt.Sprintf(`{"account_id":%q,"per_transaction_limit":50000,"daily_limit":200000}`, accountID)
	rec := doWithKey(h, "POST", "/v1/cards", apiKey, idempotencyKey, body)
	if rec.Code != http.StatusCreated {
		t.Fatalf("POST /v1/cards = %d, want 201: %s", rec.Code, rec.Body.String())
	}
	return decodeCard(t, rec.Body.Bytes())
}

func decodeCard(t *testing.T, body []byte) cardResponse {
	t.Helper()
	var card cardResponse
	if err := json.Unmarshal(body, &card); err != nil {
		t.Fatalf("decode card: %v: %s", err, body)
	}
	return card
}

func TestIssueCardCreatesOneHeldAccountPerFundingAccount(t *testing.T) {
	h, apiKey := newTestServer(t)
	funding := createAccount(t, h, apiKey, "asset")

	first := issueCard(t, h, apiKey, "card-1", funding.ID)
	second := issueCard(t, h, apiKey, "card-2", funding.ID)

	if !strings.HasPrefix(first.ID, "card_") || first.ID == second.ID {
		t.Errorf("card ids %q and %q, want two distinct card_ ids", first.ID, second.ID)
	}
	if first.Status != "active" || first.AccountID != funding.ID {
		t.Errorf("card = %+v, want active on %s", first, funding.ID)
	}
	if !regexp.MustCompile(`^[0-9]{4}$`).MatchString(first.Last4) {
		t.Errorf("last4 = %q, want four digits", first.Last4)
	}
	if first.PerTransactionLimit != 50000 || first.DailyLimit != 200000 {
		t.Errorf("limits = %d/%d, want 50000/200000", first.PerTransactionLimit, first.DailyLimit)
	}
	if first.HeldAccountID == "" || first.HeldAccountID == funding.ID {
		t.Fatalf("held account = %q, want a separate account", first.HeldAccountID)
	}
	if second.HeldAccountID != first.HeldAccountID {
		t.Errorf("second card held account %q, want the shared %q", second.HeldAccountID, first.HeldAccountID)
	}
	if got := countRows(t, "card_accounts"); got != 1 {
		t.Errorf("card_accounts = %d rows, want 1", got)
	}

	// The held account is an ordinary tracked account with a zero balance.
	rec := do(t, h, "GET", "/v1/accounts/"+first.HeldAccountID, apiKey, "")
	if rec.Code != http.StatusOK {
		t.Fatalf("GET held account = %d: %s", rec.Code, rec.Body.String())
	}
	var held accountResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &held); err != nil {
		t.Fatal(err)
	}
	if held.Type != "asset" || held.Currency != "USD" || held.Balance != 0 {
		t.Errorf("held account = %+v, want a USD asset with balance 0", held)
	}

	rec = do(t, h, "GET", "/v1/cards/"+first.ID, apiKey, "")
	if rec.Code != http.StatusOK {
		t.Fatalf("GET card = %d: %s", rec.Code, rec.Body.String())
	}
	if got := decodeCard(t, rec.Body.Bytes()); got != first {
		t.Errorf("GET card = %+v, want %+v", got, first)
	}
	rec = do(t, h, "GET", "/v1/cards/card_missing", apiKey, "")
	if rec.Code != http.StatusNotFound || decodeError(t, rec).Code != CodeNotFound {
		t.Errorf("GET missing card = %d: %s", rec.Code, rec.Body.String())
	}
}

// Without the claim on card_accounts deciding who creates the held account,
// concurrent first cards create one held account each and orphan the rest.
func TestConcurrentFirstCardsShareOneHeldAccount(t *testing.T) {
	h, apiKey := newTestServer(t)
	funding := createAccount(t, h, apiKey, "asset")

	const n = 20
	held := make([]string, n)
	codes := make([]int, n)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			body := fmt.Sprintf(`{"account_id":%q,"per_transaction_limit":100,"daily_limit":100}`, funding.ID)
			rec := doWithKey(h, "POST", "/v1/cards", apiKey, fmt.Sprintf("first-card-%d", i), body)
			codes[i] = rec.Code
			var card cardResponse
			_ = json.Unmarshal(rec.Body.Bytes(), &card)
			held[i] = card.HeldAccountID
		}()
	}
	close(start)
	wg.Wait()

	for i, code := range codes {
		if code != http.StatusCreated {
			t.Fatalf("request %d = %d, want 201", i, code)
		}
		if held[i] != held[0] {
			t.Errorf("request %d held account %q, want the shared %q", i, held[i], held[0])
		}
	}
	if got := countRows(t, "cards"); got != n {
		t.Errorf("cards = %d, want %d", got, n)
	}
	if got := countRows(t, "card_accounts"); got != 1 {
		t.Errorf("card_accounts = %d rows, want 1", got)
	}
	// The funding account and its one held account; anything else is an orphan.
	var accounts, balances int
	if err := testPool.QueryRow(context.Background(), `
		SELECT (SELECT count(*) FROM accounts WHERE type <> 'settlement'),
		       (SELECT count(*) FROM account_balances)`).Scan(&accounts, &balances); err != nil {
		t.Fatal(err)
	}
	if accounts != 2 || balances != 2 {
		t.Errorf("accounts = %d, balance rows = %d, want 2 and 2: orphan held accounts", accounts, balances)
	}
}

// Guarantee 2 for cards: the same idempotency key never issues two cards.
func TestIssueCardIsIdempotent(t *testing.T) {
	h, apiKey := newTestServer(t)
	funding := createAccount(t, h, apiKey, "asset")
	body := fmt.Sprintf(`{"account_id":%q,"per_transaction_limit":50000,"daily_limit":200000}`, funding.ID)

	const n = 20
	ids := make([]string, n)
	codes := make([]int, n)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			rec := doWithKey(h, "POST", "/v1/cards", apiKey, "same-card-key", body)
			codes[i] = rec.Code
			var card cardResponse
			_ = json.Unmarshal(rec.Body.Bytes(), &card)
			ids[i] = card.ID
		}()
	}
	close(start)
	wg.Wait()

	for i := range n {
		if codes[i] != http.StatusCreated || ids[i] != ids[0] {
			t.Errorf("request %d = %d card %q, want 201 with %q", i, codes[i], ids[i], ids[0])
		}
	}
	if got := countRows(t, "cards"); got != 1 {
		t.Errorf("cards = %d, want 1", got)
	}
	if got := countRows(t, "card_accounts"); got != 1 {
		t.Errorf("card_accounts = %d, want 1", got)
	}
	var issued int
	if err := testPool.QueryRow(context.Background(),
		`SELECT count(*) FROM outbox_events WHERE event_type = 'card.issued'`).Scan(&issued); err != nil {
		t.Fatal(err)
	}
	if issued != 1 {
		t.Errorf("card.issued events = %d, want 1", issued)
	}

	different := fmt.Sprintf(`{"account_id":%q,"per_transaction_limit":50000,"daily_limit":300000}`, funding.ID)
	rec := doWithKey(h, "POST", "/v1/cards", apiKey, "same-card-key", different)
	if rec.Code != http.StatusUnprocessableEntity || decodeError(t, rec).Code != CodeIdempotencyKeyReused {
		t.Errorf("same key, different body = %d: %s, want 422 %s", rec.Code, rec.Body.String(), CodeIdempotencyKeyReused)
	}
	if got := countRows(t, "cards"); got != 1 {
		t.Errorf("cards after key reuse = %d, want 1", got)
	}
}

func TestIssueCardRejectsBadInput(t *testing.T) {
	h, apiKey := newTestServer(t)
	funding := createAccount(t, h, apiKey, "asset")
	held := issueCard(t, h, apiKey, "existing-card", funding.ID).HeldAccountID

	body := func(account string, perTransaction, daily int64) string {
		return fmt.Sprintf(`{"account_id":%q,"per_transaction_limit":%d,"daily_limit":%d}`, account, perTransaction, daily)
	}
	cases := []struct {
		name, key, body string
	}{
		{"no idempotency key", "", body(funding.ID, 100, 100)},
		{"missing account", "k1", body("", 100, 100)},
		{"unknown account", "k2", body("acc_missing", 100, 100)},
		{"settlement account", "k3", body(settlementAccountID, 100, 100)},
		{"held account as funding", "k4", body(held, 100, 100)},
		{"zero per-transaction limit", "k5", body(funding.ID, 0, 100)},
		{"negative per-transaction limit", "k6", body(funding.ID, -1, 100)},
		{"daily below per-transaction", "k7", body(funding.ID, 200, 100)},
		{"unknown field", "k8", `{"account_id":"` + funding.ID + `","per_transaction_limit":1,"daily_limit":1,"pan":"4111"}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := doWithKey(h, "POST", "/v1/cards", apiKey, tc.key, tc.body)
			if rec.Code != http.StatusBadRequest || decodeError(t, rec).Code != CodeInvalidRequest {
				t.Errorf("= %d: %s, want 400 %s", rec.Code, rec.Body.String(), CodeInvalidRequest)
			}
		})
	}
	if got := countRows(t, "cards"); got != 1 {
		t.Errorf("cards = %d, want only the existing card", got)
	}
	if got := countRows(t, "card_accounts"); got != 1 {
		t.Errorf("card_accounts = %d, want 1", got)
	}
}
