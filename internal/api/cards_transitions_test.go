package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"sync"
	"testing"
	"time"
)

func cardEvents(t *testing.T, cardID string) []string {
	t.Helper()
	rows, err := testPool.Query(context.Background(), `
		SELECT event_type FROM outbox_events WHERE aggregate_id = $1 ORDER BY created_at, id`, cardID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var events []string
	for rows.Next() {
		var e string
		if err := rows.Scan(&e); err != nil {
			t.Fatal(err)
		}
		events = append(events, e)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return events
}

func cardStatus(t *testing.T, cardID string) string {
	t.Helper()
	var status string
	if err := testPool.QueryRow(context.Background(),
		`SELECT status FROM cards WHERE id = $1`, cardID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	return status
}

func TestCardTransitions(t *testing.T) {
	// How to bring a new active card into each starting state.
	setup := map[string][]string{
		"active": nil,
		"frozen": {"freeze"},
		"closed": {"close"},
	}
	cases := []struct {
		from, action string
		wantCode     int
		wantStatus   string
		wantEvent    bool
	}{
		{"active", "freeze", http.StatusOK, "frozen", true},
		{"active", "unfreeze", http.StatusOK, "active", false},
		{"active", "close", http.StatusOK, "closed", true},
		{"frozen", "freeze", http.StatusOK, "frozen", false},
		{"frozen", "unfreeze", http.StatusOK, "active", true},
		{"frozen", "close", http.StatusOK, "closed", true},
		{"closed", "freeze", http.StatusConflict, "closed", false},
		{"closed", "unfreeze", http.StatusConflict, "closed", false},
		{"closed", "close", http.StatusOK, "closed", false},
	}
	for _, tc := range cases {
		t.Run(tc.from+" "+tc.action, func(t *testing.T) {
			h, apiKey := newTestServer(t)
			funding := createAccount(t, h, apiKey, "asset")
			card := issueCard(t, h, apiKey, "transition", funding.ID)
			for _, step := range setup[tc.from] {
				if rec := do(t, h, "POST", "/v1/cards/"+card.ID+"/"+step, apiKey, ""); rec.Code != http.StatusOK {
					t.Fatalf("setup %s = %d: %s", step, rec.Code, rec.Body.String())
				}
			}
			before := len(cardEvents(t, card.ID))

			rec := do(t, h, "POST", "/v1/cards/"+card.ID+"/"+tc.action, apiKey, "")
			if rec.Code != tc.wantCode {
				t.Fatalf("%s from %s = %d, want %d: %s", tc.action, tc.from, rec.Code, tc.wantCode, rec.Body.String())
			}
			if tc.wantCode == http.StatusOK {
				if got := decodeCard(t, rec.Body.Bytes()); got.Status != tc.wantStatus || got.HeldAccountID != card.HeldAccountID {
					t.Errorf("response card = %+v, want status %s", got, tc.wantStatus)
				}
			} else if code := decodeError(t, rec).Code; code != CodeInvalidCardState {
				t.Errorf("error code = %q, want %q", code, CodeInvalidCardState)
			}
			if got := cardStatus(t, card.ID); got != tc.wantStatus {
				t.Errorf("stored status = %q, want %q", got, tc.wantStatus)
			}
			added := len(cardEvents(t, card.ID)) - before
			if want := map[bool]int{true: 1, false: 0}[tc.wantEvent]; added != want {
				t.Errorf("events added = %d, want %d", added, want)
			}
		})
	}

	t.Run("missing card", func(t *testing.T) {
		h, apiKey := newTestServer(t)
		for _, action := range []string{"freeze", "unfreeze", "close"} {
			rec := do(t, h, "POST", "/v1/cards/card_missing/"+action, apiKey, "")
			if rec.Code != http.StatusNotFound || decodeError(t, rec).Code != CodeNotFound {
				t.Errorf("%s missing card = %d: %s", action, rec.Code, rec.Body.String())
			}
		}
	})
}

// A freeze racing a close must never leave a closed card frozen or active,
// and the audit trail must hold exactly the changes that happened.
func TestConcurrentFreezeAndCloseEndClosed(t *testing.T) {
	h, apiKey := newTestServer(t)
	funding := createAccount(t, h, apiKey, "asset")

	const rounds = 30
	for i := range rounds {
		card := issueCard(t, h, apiKey, fmt.Sprintf("race-%d", i), funding.ID)

		var freeze, closing int
		var wg sync.WaitGroup
		start := make(chan struct{})
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			freeze = doWithKey(h, "POST", "/v1/cards/"+card.ID+"/freeze", apiKey, "", "").Code
		}()
		go func() {
			defer wg.Done()
			<-start
			closing = doWithKey(h, "POST", "/v1/cards/"+card.ID+"/close", apiKey, "", "").Code
		}()
		close(start)
		wg.Wait()

		if closing != http.StatusOK {
			t.Fatalf("round %d: close = %d, want 200", i, closing)
		}
		if freeze != http.StatusOK && freeze != http.StatusConflict {
			t.Fatalf("round %d: freeze = %d, want 200 or 409", i, freeze)
		}
		if got := cardStatus(t, card.ID); got != "closed" {
			t.Fatalf("round %d: status = %q, want closed", i, got)
		}
		// A freeze that won ran first; one that lost changed nothing. Sorted:
		// created_at is each transaction's start, not its commit order.
		want := []string{"card.closed", "card.issued"}
		if freeze == http.StatusOK {
			want = []string{"card.closed", "card.frozen", "card.issued"}
		}
		got := cardEvents(t, card.ID)
		slices.Sort(got)
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("round %d: freeze %d, events %v, want %v", i, freeze, got, want)
		}
	}
}

func TestEveryCardChangeIsAudited(t *testing.T) {
	ctx := context.Background()
	h, apiKey := newTestServer(t)
	funding := createAccount(t, h, apiKey, "asset")
	card := issueCard(t, h, apiKey, "audited", funding.ID)
	for _, action := range []string{"freeze", "unfreeze", "close"} {
		if rec := do(t, h, "POST", "/v1/cards/"+card.ID+"/"+action, apiKey, ""); rec.Code != http.StatusOK {
			t.Fatalf("%s = %d: %s", action, rec.Code, rec.Body.String())
		}
	}

	rows, err := testPool.Query(ctx, `
		SELECT event_type, topic, payload FROM outbox_events
		WHERE aggregate_id = $1 ORDER BY created_at, id`, card.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	want := []struct{ event, status string }{
		{"card.issued", "active"},
		{"card.frozen", "frozen"},
		{"card.unfrozen", "active"},
		{"card.closed", "closed"},
	}
	var i int
	for rows.Next() {
		var eventType, topic string
		var payload []byte
		if err := rows.Scan(&eventType, &topic, &payload); err != nil {
			t.Fatal(err)
		}
		if i >= len(want) {
			t.Fatalf("extra event %s", eventType)
		}
		var got struct {
			CardID     string    `json:"card_id"`
			AccountID  string    `json:"account_id"`
			Status     string    `json:"status"`
			OccurredAt time.Time `json:"occurred_at"`
		}
		if err := json.Unmarshal(payload, &got); err != nil {
			t.Fatal(err)
		}
		if eventType != want[i].event || topic != "cards" || got.CardID != card.ID ||
			got.AccountID != funding.ID || got.Status != want[i].status || got.OccurredAt.IsZero() {
			t.Errorf("event %d = %s on %s %+v, want %s with status %s", i, eventType, topic, got, want[i].event, want[i].status)
		}
		i++
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if i != len(want) {
		t.Fatalf("events = %d, want %d", i, len(want))
	}
}

// The card change and its audit event commit together or not at all.
func TestEveryCardChangeIsAuditedInItsTransaction(t *testing.T) {
	ctx := context.Background()
	h, apiKey := newTestServer(t)
	funding := createAccount(t, h, apiKey, "asset")
	card := issueCard(t, h, apiKey, "before-failure", funding.ID)
	other := createAccount(t, h, apiKey, "asset")

	if _, err := testPool.Exec(ctx, `
		CREATE FUNCTION reject_card_event() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN
			RAISE EXCEPTION 'injected outbox insert failure';
		END $$;
		CREATE TRIGGER reject_card_event BEFORE INSERT ON outbox_events
		FOR EACH ROW EXECUTE FUNCTION reject_card_event();`); err != nil {
		t.Fatal(err)
	}
	removeFailure := func() {
		if _, err := testPool.Exec(ctx, `DROP TRIGGER IF EXISTS reject_card_event ON outbox_events;
			DROP FUNCTION IF EXISTS reject_card_event();`); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(removeFailure)

	body := fmt.Sprintf(`{"account_id":%q,"per_transaction_limit":100,"daily_limit":100}`, other.ID)
	if rec := doWithKey(h, "POST", "/v1/cards", apiKey, "during-failure", body); rec.Code != http.StatusInternalServerError {
		t.Fatalf("issue with outbox failure = %d, want 500: %s", rec.Code, rec.Body.String())
	}
	if rec := do(t, h, "POST", "/v1/cards/"+card.ID+"/freeze", apiKey, ""); rec.Code != http.StatusInternalServerError {
		t.Fatalf("freeze with outbox failure = %d, want 500: %s", rec.Code, rec.Body.String())
	}

	if got := countRows(t, "cards"); got != 1 {
		t.Errorf("cards = %d, want only the card issued before the failure", got)
	}
	// The failed first card for other must not leave its held account behind.
	if got := countRows(t, "card_accounts"); got != 1 {
		t.Errorf("card_accounts = %d, want 1", got)
	}
	var accounts int
	if err := testPool.QueryRow(ctx, `SELECT count(*) FROM accounts WHERE type <> 'settlement'`).Scan(&accounts); err != nil {
		t.Fatal(err)
	}
	if accounts != 3 {
		t.Errorf("accounts = %d, want two funding accounts and one held account", accounts)
	}
	if got := cardStatus(t, card.ID); got != "active" {
		t.Errorf("status after failed freeze = %q, want active", got)
	}

	removeFailure()
	if rec := do(t, h, "POST", "/v1/cards/"+card.ID+"/freeze", apiKey, ""); rec.Code != http.StatusOK {
		t.Fatalf("freeze after recovery = %d: %s", rec.Code, rec.Body.String())
	}
	if rec := doWithKey(h, "POST", "/v1/cards", apiKey, "during-failure", body); rec.Code != http.StatusCreated {
		t.Fatalf("issue retry after recovery = %d: %s", rec.Code, rec.Body.String())
	}
	if got := fmt.Sprint(cardEvents(t, card.ID)); got != "[card.issued card.frozen]" {
		t.Errorf("events = %s, want issued then frozen", got)
	}
}
