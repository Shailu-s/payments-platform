package api

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Shailu-s/payments-platform/internal/cardnetwork"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
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

// With every connection of the money routes' pool taken, a tap and a transfer
// are refused within milliseconds instead of waiting out the 1 s network or
// 5 s request deadline for a connection.
func TestSlowPoolAcquireReturns503Fast(t *testing.T) {
	h, apiKey := newTestServer(t)
	ctx := context.Background()
	c := newCard(t, h, apiKey, 100000, 50000, 100000)
	destination := createAccount(t, h, apiKey, "liability")

	pool := smallPool(t, 2)
	s := newServerOn(testPool)
	s.hot = newBoundedPool(pool, poolAcquireWait)
	handler := s.Handler()

	var held []*pgxpool.Conn
	for range 2 {
		conn, err := pool.Acquire(ctx)
		if err != nil {
			t.Fatalf("Acquire: %v", err)
		}
		held = append(held, conn)
	}

	timed := func(send func() *httptest.ResponseRecorder) (*httptest.ResponseRecorder, time.Duration) {
		start := time.Now()
		rec := send()
		return rec, time.Since(start)
	}
	tapRec, tapElapsed := timed(func() *httptest.ResponseRecorder {
		return signedNetwork(handler, cardnetwork.PathAuthorizations,
			tap{authID: "auth_starved", cardID: c.card.ID, amount: 500}.body())
	})
	transferRec, transferElapsed := timed(func() *httptest.ResponseRecorder {
		return postTransfer(handler, apiKey, "starved-transfer", fmt.Sprintf(
			`{"source_account":%q,"destination_account":%q,"amount":500,"currency":"USD"}`,
			c.funding, destination.ID)).rec
	})
	for _, got := range []struct {
		name    string
		rec     *httptest.ResponseRecorder
		elapsed time.Duration
	}{{"authorization", tapRec, tapElapsed}, {"transfer", transferRec, transferElapsed}} {
		t.Logf("%s answered %d in %v", got.name, got.rec.Code, got.elapsed)
		if got.rec.Code != http.StatusServiceUnavailable {
			t.Errorf("%s = %d, want 503: %s", got.name, got.rec.Code, got.rec.Body.String())
			continue
		}
		if code := decodeError(t, got.rec).Code; code != CodeOverloaded {
			t.Errorf("%s error code = %q, want %q", got.name, code, CodeOverloaded)
		}
		if got.elapsed > 100*time.Millisecond {
			t.Errorf("%s refused after %v, want under 100ms", got.name, got.elapsed)
		}
	}
	if n := countRows(t, "card_authorizations"); n != 0 {
		t.Errorf("refused tap left %d authorizations, want 0", n)
	}
	if n := countRows(t, "transfers"); n != 0 {
		t.Errorf("refused transfer left %d transfers, want 0", n)
	}

	for _, conn := range held {
		conn.Release()
	}
	authorizeTap(t, handler, tap{authID: "auth_starved", cardID: c.card.ID, amount: 500})
	if got := postTransfer(handler, apiKey, "starved-transfer", fmt.Sprintf(
		`{"source_account":%q,"destination_account":%q,"amount":500,"currency":"USD"}`,
		c.funding, destination.ID)); got.rec.Code != http.StatusAccepted {
		t.Errorf("transfer once connections are free = %d: %s", got.rec.Code, got.rec.Body.String())
	}
	assertNoDrift(t)
}

// slowReads makes the reads before the transaction, the card lookup and the
// account check, time out inside a request whose own deadline is far away.
type slowReads struct{ DB }

func (d slowReads) isSlow(sql string) bool {
	return strings.Contains(sql, "FROM cards c") || strings.Contains(sql, "FROM accounts a")
}

func (d slowReads) timeout(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, 10*time.Millisecond)
}

func (d slowReads) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	if !d.isSlow(sql) {
		return d.DB.QueryRow(ctx, sql, args...)
	}
	short, cancel := d.timeout(ctx)
	defer cancel()
	var slept string
	return errRow{d.DB.QueryRow(short, `SELECT pg_sleep(1)::text`).Scan(&slept)}
}

func (d slowReads) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	if !d.isSlow(sql) {
		return d.DB.Query(ctx, sql, args...)
	}
	short, cancel := d.timeout(ctx)
	defer cancel()
	var slept string
	return nil, d.DB.QueryRow(short, `SELECT pg_sleep(1)::text`).Scan(&slept)
}

// A read that times out before the transaction is a retryable 503, not a 500:
// nothing was written, and the retry may well find the database free.
func TestCardLookupTimeoutIs503(t *testing.T) {
	h, apiKey := newTestServer(t)
	c := newCard(t, h, apiKey, 100000, 50000, 100000)
	destination := createAccount(t, h, apiKey, "liability")

	s := newServerOn(testPool)
	s.hot = slowReads{testPool}
	handler := s.Handler()

	tapRec := signedNetwork(handler, cardnetwork.PathAuthorizations,
		tap{authID: "auth_slow_card", cardID: c.card.ID, amount: 500}.body())
	transferRec := postTransfer(handler, apiKey, "slow-accounts", fmt.Sprintf(
		`{"source_account":%q,"destination_account":%q,"amount":500,"currency":"USD"}`,
		c.funding, destination.ID)).rec
	for name, rec := range map[string]*httptest.ResponseRecorder{"authorization": tapRec, "transfer": transferRec} {
		if rec.Code != http.StatusServiceUnavailable {
			t.Errorf("%s with a timed-out read = %d, want 503: %s", name, rec.Code, rec.Body.String())
			continue
		}
		if code := decodeError(t, rec).Code; code != CodeRequestTimeout {
			t.Errorf("%s error code = %q, want %q", name, code, CodeRequestTimeout)
		}
	}
	if n := countRows(t, "card_authorizations") + countRows(t, "transfers"); n != 0 {
		t.Errorf("timed-out reads left %d rows, want 0", n)
	}
}
