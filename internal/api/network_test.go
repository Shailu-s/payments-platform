package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Shailu-s/payments-platform/internal/cardnetwork"
	"github.com/Shailu-s/payments-platform/internal/ledger"
	"github.com/Shailu-s/payments-platform/internal/webhooks"
)

const testNetworkSecret = "test-only-network-secret-never-use-in-production"

// signedNetwork sends body to path signed as the card network signs it.
func signedNetwork(h http.Handler, path, body string) *httptest.ResponseRecorder {
	timestamp := strconv.FormatInt(time.Now().Unix(), 10)
	req := httptest.NewRequest("POST", path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(cardnetwork.TimestampHeader, timestamp)
	req.Header.Set(cardnetwork.SignatureHeader, webhooks.Sign([]byte(testNetworkSecret), timestamp, []byte(body)))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

type tap struct {
	authID, cardID string
	amount         int64
	currency, mcc  string
	kind           string
}

func (tp tap) body() string {
	currency, mcc, kind := tp.currency, tp.mcc, tp.kind
	if currency == "" {
		currency = "USD"
	}
	if mcc == "" {
		mcc = "5814"
	}
	if kind == "" {
		kind = "initial"
	}
	original := ""
	if kind == "incremental" {
		original = `,"original_auth_id":"auth_original"`
	}
	return fmt.Sprintf(`{"auth_id":%q,"card_id":%q,"amount":%d,"currency":%q,`+
		`"merchant":{"id":"mer_coffee","name":"Corner Coffee","mcc":%q},"kind":%q%s,"sent_at":"2026-10-17T10:00:00Z"}`,
		tp.authID, tp.cardID, tp.amount, currency, mcc, kind, original)
}

// authorizeTap asks like the network: a 503 is no answer, so it asks again
// with the same auth_id. It fails the test on anything but a 200 in the end.
func authorizeTap(t *testing.T, h http.Handler, tp tap) networkAnswer {
	t.Helper()
	answer, code, raw := tryAuthorize(h, tp)
	if code != http.StatusOK {
		t.Fatalf("authorize %s = %d: %s", tp.authID, code, raw)
	}
	return answer
}

func tryAuthorize(h http.Handler, tp tap) (networkAnswer, int, string) {
	var rec *httptest.ResponseRecorder
	for range 20 {
		rec = signedNetwork(h, cardnetwork.PathAuthorizations, tp.body())
		if rec.Code != http.StatusServiceUnavailable {
			break
		}
	}
	var answer networkAnswer
	if rec.Code == http.StatusOK {
		_ = json.Unmarshal(rec.Body.Bytes(), &answer)
	}
	return answer, rec.Code, rec.Body.String()
}

type cardFixture struct {
	card    cardResponse
	funding string
}

// newCard issues a card on a fresh funding account holding balance.
func newCard(t *testing.T, h http.Handler, apiKey string, balance, perTransaction, daily int64, blocked ...string) cardFixture {
	t.Helper()
	funding := createAccount(t, h, apiKey, "asset")
	if balance > 0 {
		fund(t, funding.ID, balance)
	}
	mccs, _ := json.Marshal(blocked)
	if blocked == nil {
		mccs = []byte("[]")
	}
	body := fmt.Sprintf(`{"account_id":%q,"per_transaction_limit":%d,"daily_limit":%d,"blocked_mccs":%s}`,
		funding.ID, perTransaction, daily, mccs)
	rec := doWithKey(h, "POST", "/v1/cards", apiKey, "card-"+funding.ID, body)
	if rec.Code != http.StatusCreated {
		t.Fatalf("POST /v1/cards = %d: %s", rec.Code, rec.Body.String())
	}
	return cardFixture{card: decodeCard(t, rec.Body.Bytes()), funding: funding.ID}
}

func storedBalance(t *testing.T, accountID string) int64 {
	t.Helper()
	balance, _, err := ledger.StoredBalance(context.Background(), testPool, accountID)
	if err != nil {
		t.Fatal(err)
	}
	return balance
}

func assertNoDrift(t *testing.T) {
	t.Helper()
	drifts, err := ledger.VerifyBalances(context.Background(), testPool)
	if err != nil {
		t.Fatal(err)
	}
	if len(drifts) != 0 {
		t.Errorf("stored balances drifted from the ledger: %+v", drifts)
	}
}

func countWhere(t *testing.T, q string, args ...any) int {
	t.Helper()
	var n int
	if err := testPool.QueryRow(context.Background(), q, args...).Scan(&n); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	return n
}

func holds(t *testing.T) int {
	t.Helper()
	return countWhere(t, `SELECT count(*) FROM ledger_transactions WHERE reference LIKE 'authorization %'`)
}

func TestApprovedTapMovesAvailableToHeld(t *testing.T) {
	h, apiKey := newTestServer(t)
	c := newCard(t, h, apiKey, 10000, 5000, 20000)

	answer := authorizeTap(t, h, tap{authID: "auth_coffee", cardID: c.card.ID, amount: 5000})
	if answer != (networkAnswer{AuthID: "auth_coffee", Decision: "approved", ApprovedAmount: 5000}) {
		t.Fatalf("answer = %+v, want approved 5000", answer)
	}

	if got := storedBalance(t, c.funding); got != 5000 {
		t.Errorf("available = %d, want 5000", got)
	}
	if got := storedBalance(t, c.card.HeldAccountID); got != 5000 {
		t.Errorf("held = %d, want 5000", got)
	}
	derived, err := ledger.Balance(context.Background(), testPool, c.card.HeldAccountID)
	if err != nil || derived != 5000 {
		t.Errorf("held from the ledger = %d (%v), want 5000", derived, err)
	}
	assertNoDrift(t)

	var decision, ledgerTxn string
	var daily int64
	if err := testPool.QueryRow(context.Background(), `
		SELECT a.decision, a.ledger_txn_id, d.amount
		FROM card_authorizations a JOIN card_daily_spend d ON d.card_id = a.card_id
		WHERE a.auth_id = 'auth_coffee'`).Scan(&decision, &ledgerTxn, &daily); err != nil {
		t.Fatal(err)
	}
	if decision != "approved" || daily != 5000 {
		t.Errorf("stored decision %q, daily spend %d, want approved and 5000", decision, daily)
	}
	if got := countWhere(t, `SELECT count(*) FROM ledger_transactions WHERE id = $1 AND reference = 'authorization auth_coffee'`, ledgerTxn); got != 1 {
		t.Errorf("the row's ledger_txn_id %s is not the hold", ledgerTxn)
	}
	if got := countWhere(t, `SELECT count(*) FROM outbox_events WHERE event_type = 'card.authorization.approved' AND aggregate_id = 'auth_coffee'`); got != 1 {
		t.Errorf("approved events = %d, want 1", got)
	}
}

// Guarantee 2 for taps: retries and duplicates of one auth_id place one hold
// and get one answer, however many arrive at once.
func TestSameAuthIDAlwaysGetsTheSameAnswer(t *testing.T) {
	cases := []struct {
		name    string
		balance int64
		want    networkAnswer
	}{
		{"approved", 10000, networkAnswer{AuthID: "auth_dup", Decision: "approved", ApprovedAmount: 3000}},
		// The first attempt's transaction rolls back on the overdraft; the
		// decline stored after it must still be the only answer.
		{"declined for funds", 2000, networkAnswer{AuthID: "auth_dup", Decision: "declined", DeclineReason: "insufficient_funds"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h, apiKey := newTestServer(t)
			c := newCard(t, h, apiKey, tc.balance, 5000, 20000)

			const n = 20
			answers := make([]networkAnswer, n)
			codes := make([]int, n)
			var wg sync.WaitGroup
			start := make(chan struct{})
			for i := range n {
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-start
					answers[i], codes[i], _ = tryAuthorize(h, tap{authID: "auth_dup", cardID: c.card.ID, amount: 3000})
				}()
			}
			close(start)
			wg.Wait()

			for i := range n {
				if codes[i] != http.StatusOK || answers[i] != tc.want {
					t.Errorf("request %d = %d %+v, want 200 %+v", i, codes[i], answers[i], tc.want)
				}
			}
			if got := countRows(t, "card_authorizations"); got != 1 {
				t.Errorf("authorizations = %d, want 1", got)
			}
			wantHolds := 0
			if tc.want.Decision == "approved" {
				wantHolds = 1
			}
			if got := holds(t); got != wantHolds {
				t.Errorf("holds = %d, want %d", got, wantHolds)
			}
			if got := storedBalance(t, c.card.HeldAccountID); got != tc.want.ApprovedAmount {
				t.Errorf("held = %d, want %d", got, tc.want.ApprovedAmount)
			}
			if got := countRows(t, "outbox_events") - countWhere(t,
				`SELECT count(*) FROM outbox_events WHERE event_type = 'card.issued'`); got != 1 {
				t.Errorf("authorization events = %d, want 1", got)
			}
			assertNoDrift(t)
		})
	}
}

// The daily limit is a counter checked and incremented in one statement, so
// concurrent taps cannot all read "under the limit" and all spend.
func TestConcurrentTapsRespectTheDailyLimit(t *testing.T) {
	h, apiKey := newTestServer(t)
	c := newCard(t, h, apiKey, 1_000_000, 3000, 10000)

	const n = 20
	answers := make([]networkAnswer, n)
	codes := make([]int, n)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			answers[i], codes[i], _ = tryAuthorize(h, tap{authID: fmt.Sprintf("auth_daily_%d", i), cardID: c.card.ID, amount: 3000})
		}()
	}
	close(start)
	wg.Wait()

	approved, overLimit := 0, 0
	for i := range n {
		switch {
		case codes[i] != http.StatusOK:
			t.Errorf("request %d = %d", i, codes[i])
		case answers[i].Decision == "approved":
			approved++
		case answers[i].DeclineReason == "daily_limit_exceeded":
			overLimit++
		default:
			t.Errorf("request %d = %+v", i, answers[i])
		}
	}
	// $100 daily limit, $30 taps: three fit.
	if approved != 3 || overLimit != n-3 {
		t.Errorf("approved %d, over the limit %d, want 3 and %d", approved, overLimit, n-3)
	}
	var spent int64
	if err := testPool.QueryRow(context.Background(),
		`SELECT amount FROM card_daily_spend WHERE card_id = $1`, c.card.ID).Scan(&spent); err != nil {
		t.Fatal(err)
	}
	if spent != 9000 {
		t.Errorf("daily spend = %d, want 9000", spent)
	}
	if got := storedBalance(t, c.card.HeldAccountID); got != 9000 {
		t.Errorf("held = %d, want 9000", got)
	}
	if got := countWhere(t, `SELECT count(*) FROM outbox_events WHERE event_type = 'card.authorization.declined'`); got != n-3 {
		t.Errorf("declined events = %d, want %d", got, n-3)
	}
	assertNoDrift(t)
}

func TestRuleDeclinesMoveNoMoney(t *testing.T) {
	h, apiKey := newTestServer(t)
	active := newCard(t, h, apiKey, 100000, 5000, 20000, "7995")
	frozen := newCard(t, h, apiKey, 100000, 5000, 20000)
	closed := newCard(t, h, apiKey, 100000, 5000, 20000)
	for _, step := range []struct{ card, action string }{
		{frozen.card.ID, "freeze"}, {closed.card.ID, "close"},
	} {
		if rec := do(t, h, "POST", "/v1/cards/"+step.card+"/"+step.action, apiKey, ""); rec.Code != http.StatusOK {
			t.Fatalf("%s = %d: %s", step.action, rec.Code, rec.Body.String())
		}
	}

	cases := []struct {
		name   string
		tap    tap
		reason string
	}{
		{"unknown card", tap{cardID: "card_missing", amount: 100}, "card_not_found"},
		{"frozen card", tap{cardID: frozen.card.ID, amount: 100}, "card_inactive"},
		{"closed card", tap{cardID: closed.card.ID, amount: 100}, "card_inactive"},
		{"currency", tap{cardID: active.card.ID, amount: 100, currency: "EUR"}, "invalid_currency"},
		{"per-transaction limit", tap{cardID: active.card.ID, amount: 5001}, "exceeds_transaction_limit"},
		{"blocked mcc", tap{cardID: active.card.ID, amount: 100, mcc: "7995"}, "merchant_blocked"},
		{"incremental", tap{cardID: active.card.ID, amount: 100, kind: "incremental"}, "incremental_not_supported"},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tc.tap.authID = fmt.Sprintf("auth_rule_%d", i)
			want := networkAnswer{AuthID: tc.tap.authID, Decision: "declined", DeclineReason: tc.reason}
			if got := authorizeTap(t, h, tc.tap); got != want {
				t.Errorf("answer = %+v, want %+v", got, want)
			}
			// Asked again, answered the same.
			if got := authorizeTap(t, h, tc.tap); got != want {
				t.Errorf("second answer = %+v, want %+v", got, want)
			}
		})
	}

	if got := countRows(t, "card_authorizations"); got != len(cases) {
		t.Errorf("authorizations = %d, want %d: declines are stored too", got, len(cases))
	}
	if got := holds(t); got != 0 {
		t.Errorf("holds = %d, want 0", got)
	}
	if got := countRows(t, "card_daily_spend"); got != 0 {
		t.Errorf("daily spend rows = %d, want 0", got)
	}
	for _, c := range []cardFixture{active, frozen, closed} {
		if got := storedBalance(t, c.funding); got != 100000 {
			t.Errorf("available on %s = %d, want 100000", c.funding, got)
		}
	}
	assertNoDrift(t)
}

func TestUnsignedOrTamperedNetworkRequestsAreRejected(t *testing.T) {
	h, apiKey := newTestServer(t)
	c := newCard(t, h, apiKey, 10000, 5000, 20000)
	body := tap{authID: "auth_forged", cardID: c.card.ID, amount: 5000}.body()

	now := strconv.FormatInt(time.Now().Unix(), 10)
	stale := strconv.FormatInt(time.Now().Add(-10*time.Minute).Unix(), 10)
	sign := func(secret, timestamp, body string) string {
		return webhooks.Sign([]byte(secret), timestamp, []byte(body))
	}
	tampered := strings.Replace(body, `"amount":5000`, `"amount":9000`, 1)

	cases := []struct {
		name, timestamp, signature, body string
	}{
		{"unsigned", "", "", body},
		{"wrong secret", now, sign("another-secret-of-at-least-32-bytes!!", now, body), body},
		{"tampered body", now, sign(testNetworkSecret, now, body), tampered},
		{"stale timestamp", stale, sign(testNetworkSecret, stale, body), body},
		{"mockbank headers", now, "", body},
	}
	for _, path := range []string{cardnetwork.PathAuthorizations, cardnetwork.PathAdvices} {
		for _, tc := range cases {
			t.Run(path+" "+tc.name, func(t *testing.T) {
				req := httptest.NewRequest("POST", path, strings.NewReader(tc.body))
				if tc.name == "mockbank headers" {
					req.Header.Set(webhooks.TimestampHeader, now)
					req.Header.Set(webhooks.SignatureHeader, sign(testNetworkSecret, now, body))
				} else {
					req.Header.Set(cardnetwork.TimestampHeader, tc.timestamp)
					req.Header.Set(cardnetwork.SignatureHeader, tc.signature)
				}
				rec := httptest.NewRecorder()
				h.ServeHTTP(rec, req)
				if rec.Code != http.StatusUnauthorized {
					t.Errorf("= %d: %s, want 401", rec.Code, rec.Body.String())
				}
			})
		}
	}
	if got := countRows(t, "card_authorizations") + countRows(t, "network_messages"); got != 0 {
		t.Errorf("rows stored = %d, want 0", got)
	}
	if got := storedBalance(t, c.funding); got != 10000 {
		t.Errorf("available = %d, want 10000", got)
	}
}

// Guarantee 3 for taps: the balance CHECK decides, so concurrent taps on one
// account cannot both spend the same money.
func TestTwoTapsAtOnceCannotOverspend(t *testing.T) {
	h, apiKey := newTestServer(t)
	c := newCard(t, h, apiKey, 5000, 5000, 1_000_000)

	const n = 20
	answers := make([]networkAnswer, n)
	codes := make([]int, n)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			answers[i], codes[i], _ = tryAuthorize(h, tap{authID: fmt.Sprintf("auth_race_%d", i), cardID: c.card.ID, amount: 3000})
		}()
	}
	close(start)
	wg.Wait()

	approved, broke := 0, 0
	for i := range n {
		switch {
		case codes[i] != http.StatusOK:
			t.Errorf("request %d = %d", i, codes[i])
		case answers[i].Decision == "approved":
			approved++
		case answers[i].DeclineReason == "insufficient_funds":
			broke++
		default:
			t.Errorf("request %d = %+v", i, answers[i])
		}
	}
	if approved != 1 || broke != n-1 {
		t.Errorf("approved %d, insufficient funds %d, want 1 and %d", approved, broke, n-1)
	}
	if got := storedBalance(t, c.funding); got != 2000 {
		t.Errorf("available = %d, want 2000", got)
	}
	if got := storedBalance(t, c.card.HeldAccountID); got != 3000 {
		t.Errorf("held = %d, want 3000", got)
	}
	if got := countRows(t, "card_authorizations"); got != n {
		t.Errorf("authorizations = %d, want %d: every decline is stored", got, n)
	}
	// A decline for funds rolled back its daily spend with the hold.
	var spent int64
	if err := testPool.QueryRow(context.Background(),
		`SELECT amount FROM card_daily_spend WHERE card_id = $1`, c.card.ID).Scan(&spent); err != nil {
		t.Fatal(err)
	}
	if spent != 3000 {
		t.Errorf("daily spend = %d, want 3000", spent)
	}
	if got := countWhere(t, `SELECT count(*) FROM outbox_events WHERE event_type = 'card.authorization.declined'`); got != n-1 {
		t.Errorf("declined events = %d, want %d", got, n-1)
	}
	assertNoDrift(t)
}

func adviceBody(adviceID, authID, cardID string, amount int64) string {
	return fmt.Sprintf(`{"advice_id":%q,"auth_id":%q,"card_id":%q,"amount":%d,`+
		`"merchant":{"id":"mer_coffee","name":"Corner Coffee","mcc":"5814"},`+
		`"decision":"approved","reason":"stand_in","sent_at":"2026-10-17T10:00:04Z"}`,
		adviceID, authID, cardID, amount)
}

func advise(t *testing.T, h http.Handler, adviceID, authID, cardID string, amount int64) string {
	t.Helper()
	rec := signedNetwork(h, cardnetwork.PathAdvices, adviceBody(adviceID, authID, cardID, amount))
	if rec.Code != http.StatusOK {
		t.Fatalf("advice %s = %d: %s", adviceID, rec.Code, rec.Body.String())
	}
	var body map[string]string
	_ = json.Unmarshal(rec.Body.Bytes(), &body)
	return body["status"]
}

type storedAuth struct {
	decision      string
	approved      int64
	standIn       bool
	shortfall     int64
	hasLedgerTxn  bool
	declineReason *string
}

func loadAuth(t *testing.T, authID string) storedAuth {
	t.Helper()
	var a storedAuth
	if err := testPool.QueryRow(context.Background(), `
		SELECT decision, approved_amount, stand_in, hold_shortfall, ledger_txn_id IS NOT NULL, decline_reason
		FROM card_authorizations WHERE auth_id = $1`, authID).
		Scan(&a.decision, &a.approved, &a.standIn, &a.shortfall, &a.hasLedgerTxn, &a.declineReason); err != nil {
		t.Fatalf("load %s: %v", authID, err)
	}
	return a
}

// An advice is the network's decision, not a question: the merchant was told
// yes, so the hold is placed whatever we answered, if we answered at all.
func TestStandInAdviceIsHonoured(t *testing.T) {
	t.Run("after a decline", func(t *testing.T) {
		h, apiKey := newTestServer(t)
		c := newCard(t, h, apiKey, 10000, 5000, 20000)
		// Declined by us (over the per-transaction limit), approved by stand-in.
		if got := authorizeTap(t, h, tap{authID: "auth_flip", cardID: c.card.ID, amount: 6000}); got.Decision != "declined" {
			t.Fatalf("answer = %+v, want declined", got)
		}
		if got := advise(t, h, "adv_flip", "auth_flip", c.card.ID, 6000); got != "processed" {
			t.Errorf("advice status = %q, want processed", got)
		}

		a := loadAuth(t, "auth_flip")
		if a.decision != "approved" || a.approved != 6000 || !a.standIn || a.shortfall != 0 || a.declineReason != nil {
			t.Errorf("stored = %+v, want approved 6000 by stand-in, no shortfall, no reason", a)
		}
		if storedBalance(t, c.funding) != 4000 || storedBalance(t, c.card.HeldAccountID) != 6000 {
			t.Errorf("available %d, held %d, want 4000 and 6000",
				storedBalance(t, c.funding), storedBalance(t, c.card.HeldAccountID))
		}
		// The network asks again: it gets the network's decision, now ours too.
		if got := authorizeTap(t, h, tap{authID: "auth_flip", cardID: c.card.ID, amount: 6000}); got.Decision != "approved" || got.ApprovedAmount != 6000 {
			t.Errorf("answer after the advice = %+v, want approved 6000", got)
		}
		assertNoDrift(t)
	})

	t.Run("with no authorization seen", func(t *testing.T) {
		h, apiKey := newTestServer(t)
		c := newCard(t, h, apiKey, 10000, 5000, 20000)
		advise(t, h, "adv_unseen", "auth_unseen", c.card.ID, 4000)

		a := loadAuth(t, "auth_unseen")
		if a.decision != "approved" || a.approved != 4000 || !a.standIn || !a.hasLedgerTxn {
			t.Errorf("stored = %+v, want an approved, held stand-in", a)
		}
		if got := storedBalance(t, c.card.HeldAccountID); got != 4000 {
			t.Errorf("held = %d, want 4000", got)
		}
		assertNoDrift(t)
	})

	t.Run("for a frozen card and past its limits", func(t *testing.T) {
		h, apiKey := newTestServer(t)
		c := newCard(t, h, apiKey, 10000, 1000, 1000)
		if rec := do(t, h, "POST", "/v1/cards/"+c.card.ID+"/freeze", apiKey, ""); rec.Code != http.StatusOK {
			t.Fatalf("freeze = %d", rec.Code)
		}
		advise(t, h, "adv_frozen", "auth_frozen", c.card.ID, 3000)

		if a := loadAuth(t, "auth_frozen"); a.decision != "approved" || a.shortfall != 0 {
			t.Errorf("stored = %+v, want approved and fully held", a)
		}
		if got := storedBalance(t, c.card.HeldAccountID); got != 3000 {
			t.Errorf("held = %d, want 3000", got)
		}
		var spent int64
		if err := testPool.QueryRow(context.Background(),
			`SELECT amount FROM card_daily_spend WHERE card_id = $1`, c.card.ID).Scan(&spent); err != nil || spent != 3000 {
			t.Errorf("daily spend = %d (%v), want 3000: counted even past the limit", spent, err)
		}
		assertNoDrift(t)
	})

	t.Run("after our approval", func(t *testing.T) {
		h, apiKey := newTestServer(t)
		c := newCard(t, h, apiKey, 10000, 5000, 20000)
		authorizeTap(t, h, tap{authID: "auth_late", cardID: c.card.ID, amount: 2000})
		advise(t, h, "adv_late", "auth_late", c.card.ID, 2000)

		if a := loadAuth(t, "auth_late"); a.decision != "approved" || !a.standIn {
			t.Errorf("stored = %+v, want approved, marked stand-in", a)
		}
		if got := holds(t); got != 1 {
			t.Errorf("holds = %d, want 1: the approval already held it", got)
		}
		if got := storedBalance(t, c.card.HeldAccountID); got != 2000 {
			t.Errorf("held = %d, want 2000", got)
		}
	})

	t.Run("duplicate advice is a no-op", func(t *testing.T) {
		h, apiKey := newTestServer(t)
		c := newCard(t, h, apiKey, 10000, 5000, 20000)

		const n = 10
		statuses := make([]string, n)
		var wg sync.WaitGroup
		start := make(chan struct{})
		for i := range n {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				rec := signedNetwork(h, cardnetwork.PathAdvices, adviceBody("adv_twice", "auth_twice", c.card.ID, 2500))
				var body map[string]string
				_ = json.Unmarshal(rec.Body.Bytes(), &body)
				statuses[i] = fmt.Sprintf("%d %s", rec.Code, body["status"])
			}()
		}
		close(start)
		wg.Wait()

		processed := 0
		for i, s := range statuses {
			switch s {
			case "200 processed":
				processed++
			case "200 already_processed":
			default:
				t.Errorf("delivery %d = %s", i, s)
			}
		}
		if processed != 1 {
			t.Errorf("processed %d times, want 1", processed)
		}
		if got := holds(t); got != 1 {
			t.Errorf("holds = %d, want 1", got)
		}
		if got := storedBalance(t, c.card.HeldAccountID); got != 2500 {
			t.Errorf("held = %d, want 2500", got)
		}
		if got := countWhere(t, `SELECT count(*) FROM outbox_events WHERE event_type = 'card.authorization.stand_in'`); got != 1 {
			t.Errorf("stand-in events = %d, want 1", got)
		}
		assertNoDrift(t)
	})

	t.Run("short of funds holds what there is", func(t *testing.T) {
		h, apiKey := newTestServer(t)
		c := newCard(t, h, apiKey, 2000, 5000, 20000)
		advise(t, h, "adv_short", "auth_short", c.card.ID, 5000)

		a := loadAuth(t, "auth_short")
		if a.decision != "approved" || a.approved != 5000 || a.shortfall != 3000 {
			t.Errorf("stored = %+v, want approved 5000 with a 3000 shortfall", a)
		}
		if storedBalance(t, c.funding) != 0 || storedBalance(t, c.card.HeldAccountID) != 2000 {
			t.Errorf("available %d, held %d, want 0 and 2000: never overdrawn",
				storedBalance(t, c.funding), storedBalance(t, c.card.HeldAccountID))
		}
		// Nothing left: the next stand-in holds nothing and owes it all.
		advise(t, h, "adv_empty", "auth_empty", c.card.ID, 1000)
		if a := loadAuth(t, "auth_empty"); a.shortfall != 1000 || a.hasLedgerTxn {
			t.Errorf("stored = %+v, want a 1000 shortfall and no ledger write", a)
		}
		assertNoDrift(t)
	})

	t.Run("for an unknown card", func(t *testing.T) {
		h, _ := newTestServer(t)
		advise(t, h, "adv_ghost", "auth_ghost", "card_ghost", 1500)

		if a := loadAuth(t, "auth_ghost"); a.decision != "approved" || a.shortfall != 1500 || a.hasLedgerTxn {
			t.Errorf("stored = %+v, want approved with the whole amount short", a)
		}
		if got := holds(t); got != 0 {
			t.Errorf("holds = %d, want 0", got)
		}
	})
}
