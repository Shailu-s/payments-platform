package api

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Shailu-s/payments-platform/internal/cardnetwork"
)

// A process with every admission slot taken refuses the next tap, advice,
// reversal and transfer in milliseconds, writes nothing for them, and says
// so with a retryable 503.
func TestExcessRequestsAreRefusedImmediately(t *testing.T) {
	h, apiKey := newTestServer(t)
	c := newCard(t, h, apiKey, 100000, 50000, 100000)
	destination := createAccount(t, h, apiKey, "liability")

	s := newServerOn(testPool)
	s.admit = make(chan struct{}, 2)
	s.admit <- struct{}{}
	s.admit <- struct{}{}
	full := s.Handler()

	requests := []struct {
		name string
		send func() *httptest.ResponseRecorder
	}{
		{"authorization", func() *httptest.ResponseRecorder {
			return signedNetwork(full, cardnetwork.PathAuthorizations,
				tap{authID: "auth_refused", cardID: c.card.ID, amount: 500}.body())
		}},
		{"advice", func() *httptest.ResponseRecorder {
			return signedNetwork(full, cardnetwork.PathAdvices, fmt.Sprintf(
				`{"advice_id":"adv_refused","auth_id":"auth_refused","card_id":%q,"amount":500,`+
					`"merchant":{"id":"mer_coffee","name":"Corner Coffee","mcc":"5814"},"decision":"approved"}`,
				c.card.ID))
		}},
		{"reversal", func() *httptest.ResponseRecorder {
			return signedNetwork(full, cardnetwork.PathReversals,
				`{"reversal_id":"rev_refused","auth_id":"auth_refused","amount":500}`)
		}},
		{"transfer", func() *httptest.ResponseRecorder {
			return postTransfer(full, apiKey, "refused-transfer", fmt.Sprintf(
				`{"source_account":%q,"destination_account":%q,"amount":500,"currency":"USD"}`,
				c.funding, destination.ID)).rec
		}},
	}
	for _, req := range requests {
		start := time.Now()
		rec := req.send()
		elapsed := time.Since(start)
		if rec.Code != http.StatusServiceUnavailable {
			t.Errorf("%s = %d, want 503: %s", req.name, rec.Code, rec.Body.String())
			continue
		}
		if code := decodeError(t, rec).Code; code != CodeOverloaded {
			t.Errorf("%s error code = %q, want %q", req.name, code, CodeOverloaded)
		}
		if rec.Header().Get("Retry-After") != "1" {
			t.Errorf("%s Retry-After = %q, want 1", req.name, rec.Header().Get("Retry-After"))
		}
		if elapsed > 50*time.Millisecond {
			t.Errorf("%s refused after %v, want under 50ms", req.name, elapsed)
		}
	}

	if n := countRows(t, "card_authorizations"); n != 0 {
		t.Errorf("refused requests left %d authorizations, want 0", n)
	}
	if n := countRows(t, "transfers"); n != 0 {
		t.Errorf("refused requests left %d transfers, want 0", n)
	}
	if n := countWhere(t, `SELECT count(*) FROM ledger_transactions WHERE reference NOT LIKE 'fund%'`); n != 0 {
		t.Errorf("refused requests left %d ledger transactions, want 0", n)
	}

	// Reads are not admission-limited: a full process still answers them.
	if rec := do(t, full, "GET", "/v1/cards/"+c.card.ID, apiKey, ""); rec.Code != http.StatusOK {
		t.Errorf("GET card on a full process = %d, want 200", rec.Code)
	}

	// A freed slot admits again.
	<-s.admit
	if _, code, raw := tryAuthorize(full, tap{authID: "auth_admitted", cardID: c.card.ID, amount: 500}); code != http.StatusOK {
		t.Errorf("authorization after a slot freed = %d: %s", code, raw)
	}
}
