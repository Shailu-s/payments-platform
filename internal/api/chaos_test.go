package api

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Shailu-s/payments-platform/internal/cardnetwork"
)

// tapLoad keeps workers tapping cards with fresh auth_ids, one attempt each,
// so a chaos test runs while other authorizations hold the same balance rows,
// daily counters and pool connections.
type tapLoad struct {
	stop     chan struct{}
	wg       sync.WaitGroup
	answered atomic.Int64
	busy     atomic.Int64
}

func startTapLoad(h http.Handler, cardIDs []string, workers int) *tapLoad {
	l := &tapLoad{stop: make(chan struct{})}
	for w := range workers {
		l.wg.Add(1)
		go func() {
			defer l.wg.Done()
			for i := 0; ; i++ {
				select {
				case <-l.stop:
					return
				default:
				}
				body := tap{authID: fmt.Sprintf("auth_load_%d_%d", w, i),
					cardID: cardIDs[rand.IntN(len(cardIDs))], amount: 100}.body()
				switch signedNetwork(h, cardnetwork.PathAuthorizations, body).Code {
				case http.StatusOK:
					l.answered.Add(1)
				case http.StatusServiceUnavailable:
					l.busy.Add(1)
				}
			}
		}()
	}
	return l
}

func (l *tapLoad) Stop() (answered, busy int64) {
	close(l.stop)
	l.wg.Wait()
	return l.answered.Load(), l.busy.Load()
}

// askUntilAnswered redelivers on 503 like the network, more patiently than
// tryAuthorize: under background load a busy account is expected, not a failure.
func askUntilAnswered(h http.Handler, path, body string) *httptest.ResponseRecorder {
	var rec *httptest.ResponseRecorder
	for range 200 {
		rec = signedNetwork(h, path, body)
		if rec.Code != http.StatusServiceUnavailable {
			return rec
		}
		time.Sleep(2 * time.Millisecond)
	}
	return rec
}

func chaosCards(t *testing.T, h http.Handler, apiKey string, n int) []cardFixture {
	t.Helper()
	cs := make([]cardFixture, n)
	for i := range cs {
		cs[i] = newCard(t, h, apiKey, 100_000_000, 50_000, 1_000_000_000)
	}
	return cs
}

func cardIDs(cs []cardFixture) []string {
	ids := make([]string, len(cs))
	for i, c := range cs {
		ids[i] = c.card.ID
	}
	return ids
}

// assertHeldMatchesRows: every held account holds exactly what its cards'
// authorizations say they hold, so no hold exists without a row and no row
// claims a hold the ledger never made.
func assertHeldMatchesRows(t *testing.T) {
	t.Helper()
	if got := countWhere(t, `
		SELECT count(*) FROM card_accounts ca
		JOIN account_balances b ON b.account_id = ca.held_account_id
		WHERE b.balance <> (
			SELECT COALESCE(sum(a.held_amount), 0) FROM card_authorizations a
			JOIN cards c ON c.id = a.card_id WHERE c.account_id = ca.account_id)`); got != 0 {
		t.Errorf("held accounts disagreeing with their authorizations = %d, want 0", got)
	}
}

// The network retries and duplicates authorizations while the card keeps
// being used: every copy of one auth_id, however many arrive at once and
// however busy the account is, gets one answer, one row and one hold.
func TestDuplicateAuthorizationsUnderLoad(t *testing.T) {
	h, apiKey := newTestServer(t)
	cs := chaosCards(t, h, apiKey, 10)
	load := startTapLoad(h, cardIDs(cs), 8)

	const waves, copies = 3, 20
	type reply struct {
		code   int
		answer networkAnswer
	}
	replies := map[string][]reply{}
	var mu sync.Mutex
	for wave := range waves {
		var wg sync.WaitGroup
		start := make(chan struct{})
		for i, c := range cs {
			authID := fmt.Sprintf("auth_dup_%d_%d", wave, i)
			body := tap{authID: authID, cardID: c.card.ID, amount: 3000}.body()
			for range copies {
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-start
					rec := askUntilAnswered(h, cardnetwork.PathAuthorizations, body)
					var answer networkAnswer
					_ = json.Unmarshal(rec.Body.Bytes(), &answer)
					mu.Lock()
					replies[authID] = append(replies[authID], reply{rec.Code, answer})
					mu.Unlock()
				}()
			}
		}
		close(start)
		wg.Wait()
	}
	answered, busy := load.Stop()
	if answered == 0 {
		t.Fatal("background load answered nothing: the test ran without load")
	}

	for authID, rs := range replies {
		want := networkAnswer{AuthID: authID, Decision: "approved", ApprovedAmount: 3000}
		for i, r := range rs {
			if r.code != http.StatusOK || r.answer != want {
				t.Errorf("%s copy %d = %d %+v, want 200 %+v", authID, i, r.code, r.answer, want)
			}
		}
		if got := countWhere(t, `SELECT count(*) FROM card_authorizations WHERE auth_id = $1`, authID); got != 1 {
			t.Errorf("%s rows = %d, want 1", authID, got)
		}
		if got := countWhere(t, `SELECT count(*) FROM ledger_transactions WHERE reference = $1`, "authorization "+authID); got != 1 {
			t.Errorf("%s holds = %d, want 1", authID, got)
		}
		if got := countWhere(t, `SELECT count(*) FROM outbox_events WHERE aggregate_id = $1`, authID); got != 1 {
			t.Errorf("%s events = %d, want 1", authID, got)
		}
	}
	assertHeldMatchesRows(t)
	assertNoDrift(t)
	t.Logf("%d auth_ids x %d concurrent copies, alongside %d background taps (%d busy 503s)",
		len(replies), copies, answered, busy)
}

// Reversals that arrive before, or at the same moment as, the authorization
// they cancel, at volume and under load: none is lost and none releases twice.
func TestReversalBeforeAuthorizationUnderLoad(t *testing.T) {
	h, apiKey := newTestServer(t)
	cs := chaosCards(t, h, apiKey, 10)
	network := newTestNetwork(t, h)
	load := startTapLoad(h, cardIDs(cs), 8)

	// The simulator's scenario, many at once: the reversal is delivered and
	// acknowledged, then the authorization it cancels.
	const scenarios, workers = 200, 8
	results := make(chan cardnetwork.ScenarioResult, scenarios)
	errs := make(chan error, scenarios)
	jobs := make(chan int)
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				seed := uint64(5000 + i)
				result, err := network.RunScenario(context.Background(), cardnetwork.ScenarioRequest{
					Scenario: cardnetwork.ScenarioReversalBeforeAuth, CardID: cs[i%len(cs)].card.ID,
					Amount: 2500, Seed: &seed,
				})
				if err != nil {
					errs <- err
					continue
				}
				results <- result
			}
		}()
	}
	// And each authorization racing its own reversal.
	const pairs = 200
	for i := range pairs {
		authID := fmt.Sprintf("auth_race_%d", i)
		card := cs[i%len(cs)].card.ID
		wg.Add(2)
		go func() {
			defer wg.Done()
			askUntilAnswered(h, cardnetwork.PathAuthorizations, tap{authID: authID, cardID: card, amount: 1000}.body())
		}()
		go func() {
			defer wg.Done()
			askUntilAnswered(h, cardnetwork.PathReversals, reversalBody("rev_"+authID, authID, 1000))
		}()
	}
	for i := range scenarios {
		jobs <- i
	}
	close(jobs)
	wg.Wait()
	close(results)
	close(errs)
	answered, busy := load.Stop()
	for err := range errs {
		t.Fatal(err)
	}
	if answered == 0 {
		t.Fatal("background load answered nothing: the test ran without load")
	}

	var targets []string
	for result := range results {
		a := result.Authorizations[0]
		if a.Decision != cardnetwork.DecisionApproved {
			t.Errorf("network decided %+v, want approved", a)
		}
		for _, e := range result.Journal {
			if e.Message == cardnetwork.MessageReversal && e.Outcome != cardnetwork.OutcomeAcknowledged {
				t.Errorf("reversal for %s attempt %d: %s", a.AuthID, e.Attempt, e.Outcome)
			}
		}
		targets = append(targets, a.AuthID)
	}
	for i := range pairs {
		targets = append(targets, fmt.Sprintf("auth_race_%d", i))
	}
	if len(targets) != scenarios+pairs {
		t.Fatalf("targets = %d, want %d", len(targets), scenarios+pairs)
	}
	if got := countWhere(t, `SELECT count(*) FROM card_authorizations
		WHERE auth_id = ANY($1) AND decision = 'approved' AND held_amount = 0 AND released_amount = approved_amount`,
		targets); got != len(targets) {
		t.Errorf("approved and fully released = %d, want %d: a reversal was lost", got, len(targets))
	}
	if got := countWhere(t, `SELECT count(*) FROM ledger_transactions
		WHERE reference LIKE 'release %' AND split_part(reference, ' ', 2) = ANY($1)`, targets); got != len(targets) {
		t.Errorf("release transactions = %d, want %d: one each", got, len(targets))
	}
	if got := countWhere(t, `SELECT count(*) FROM pending_reversals WHERE applied_at IS NULL`); got != 0 {
		t.Errorf("unapplied reversals = %d, want 0", got)
	}
	assertHeldMatchesRows(t)
	assertNoDrift(t)
	t.Logf("%d early and %d racing reversals, alongside %d background taps (%d busy 503s)",
		scenarios, pairs, answered, busy)
}
