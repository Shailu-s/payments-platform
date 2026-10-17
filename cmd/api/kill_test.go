package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	mrand "math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/Shailu-s/payments-platform/internal/auth"
	"github.com/Shailu-s/payments-platform/internal/cardnetwork"
	"github.com/Shailu-s/payments-platform/internal/cards"
	"github.com/Shailu-s/payments-platform/internal/ledger"
	"github.com/Shailu-s/payments-platform/internal/testdb"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	killNetworkSecret = "test-only-network-secret-never-use-in-production"
	killWebhookSecret = "test-only-webhook-secret-never-use-in-production"
)

// The API process dies mid-authorization: SIGKILL, so no deferred rollback,
// no graceful drain, connections cut mid-transaction. The network does what
// it does when an issuer vanishes (retry, stand in, advise with backoff)
// against the restarted process. Afterwards the network's record and ours
// must agree on every authorization, and no hold may exist without its row or
// a row without its hold.
func TestAPIKilledMidAuthorizationLeavesNoPartialHold(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	schema := fmt.Sprintf("test_api_kill_%d", time.Now().UnixNano())
	pool, err := testdb.Connect(ctx, schema)
	if err != nil {
		t.Fatalf("%s", testdb.ConnectionHint(err))
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DROP SCHEMA "`+schema+`" CASCADE`)
		pool.Close()
	})
	cardIDs := seedKillCards(t, ctx, pool, 20)

	addr := freeAddr(t)
	first := startAPIProcess(t, ctx, schema, addr)
	network, err := cardnetwork.New(cardnetwork.Config{
		IssuerURL:    "http://" + addr,
		Secret:       []byte(killNetworkSecret),
		AuthDeadline: 2 * time.Second,
		StandInLimit: 1_000_000,
	})
	if err != nil {
		t.Fatal(err)
	}

	var mu sync.Mutex
	var results []cardnetwork.AuthResult
	var failures []error
	count := func() int {
		mu.Lock()
		defer mu.Unlock()
		return len(results) + len(failures)
	}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	const workers = 16
	for w := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				r, err := network.Authorize(ctx, cardnetwork.Authorization{
					AuthID:   fmt.Sprintf("auth_kill_%d_%d", w, i),
					CardID:   cardIDs[mrand.IntN(len(cardIDs))],
					Amount:   100 + mrand.Int64N(900),
					Currency: cardnetwork.Currency,
					Merchant: cardnetwork.Merchant{ID: "mer_coffee", Name: "Corner Coffee", MCC: "5814"},
					Kind:     cardnetwork.KindInitial,
				})
				mu.Lock()
				if err != nil {
					failures = append(failures, err)
				} else {
					results = append(results, r)
				}
				mu.Unlock()
			}
		}()
	}

	waitUntil(t, ctx, func() bool { return count() >= 300 })
	if err := first.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	var exit *exec.ExitError
	if err := first.Wait(); !errors.As(err, &exit) || exit.Sys().(syscall.WaitStatus).Signal() != syscall.SIGKILL {
		t.Fatalf("api exit = %v, want SIGKILL", err)
	}
	killedAt := count()
	// Long enough for the network to see refused connections and stand in.
	time.Sleep(300 * time.Millisecond)
	startAPIProcess(t, ctx, schema, addr)
	waitUntil(t, ctx, func() bool { return count() >= killedAt+300 })
	close(stop)
	// In-flight Authorize calls finish their retries and advice deliveries.
	wg.Wait()
	if len(failures) > 0 {
		t.Fatalf("network failed to decide %d authorizations, first: %v", len(failures), failures[0])
	}

	cut, standIns := 0, 0
	for _, e := range network.Journal() {
		switch {
		case e.Message == cardnetwork.MessageAuthorization && e.Outcome == cardnetwork.OutcomeTransportError:
			cut++
		case e.Outcome == cardnetwork.OutcomeStandIn:
			standIns++
		}
	}
	if cut == 0 || standIns == 0 {
		t.Fatalf("transport errors %d, stand-ins %d: the kill did not land mid-flight", cut, standIns)
	}

	ours := map[string]killRow{}
	rows, err := pool.Query(ctx, `SELECT auth_id, decision, approved_amount, held_amount, stand_in FROM card_authorizations`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var r killRow
		if err := rows.Scan(&r.authID, &r.decision, &r.approved, &r.held, &r.standIn); err != nil {
			t.Fatal(err)
		}
		ours[r.authID] = r
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(ours) != len(results) {
		t.Errorf("our rows = %d, network authorizations = %d", len(ours), len(results))
	}
	for _, r := range results {
		row, ok := ours[r.AuthID]
		switch {
		case !ok:
			t.Errorf("%s: network decided %+v, we have no row", r.AuthID, r)
		case row.decision != r.Decision || row.approved != r.ApprovedAmount || row.standIn != r.StandIn:
			t.Errorf("%s: network decided %+v, we stored %+v", r.AuthID, r, row)
		case r.Decision == cardnetwork.DecisionApproved && row.held != r.ApprovedAmount:
			t.Errorf("%s: network approved %d, we hold %d", r.AuthID, r.ApprovedAmount, row.held)
		case r.StandIn && r.Advice != cardnetwork.AdviceAcknowledged:
			t.Errorf("%s: stand-in advice %q, want acknowledged", r.AuthID, r.Advice)
		}
	}

	for _, check := range []struct{ what, q string }{
		{"approved rows without their hold", `
			SELECT count(*) FROM card_authorizations a
			LEFT JOIN ledger_transactions t ON t.id = a.ledger_txn_id
			WHERE a.decision = 'approved' AND a.held_amount > 0 AND t.id IS NULL`},
		{"holds without an approved row", `
			SELECT count(*) FROM ledger_transactions t
			WHERE t.reference LIKE 'authorization %' AND NOT EXISTS (
				SELECT 1 FROM card_authorizations a WHERE a.ledger_txn_id = t.id AND a.decision = 'approved')`},
		{"rows without their event", `
			SELECT count(*) FROM card_authorizations a
			WHERE NOT EXISTS (SELECT 1 FROM outbox_events e WHERE e.aggregate_id = a.auth_id)`},
		{"held accounts disagreeing with their rows", `
			SELECT count(*) FROM card_accounts ca
			JOIN account_balances b ON b.account_id = ca.held_account_id
			WHERE b.balance <> (
				SELECT COALESCE(sum(a.held_amount), 0) FROM card_authorizations a
				JOIN cards c ON c.id = a.card_id WHERE c.account_id = ca.account_id)`},
		{"unbalanced ledger transactions", `
			SELECT count(*) FROM (
				SELECT txn_id FROM ledger_entries GROUP BY txn_id
				HAVING SUM(CASE WHEN direction = 'credit' THEN amount ELSE -amount END) <> 0) bad`},
	} {
		var n int
		if err := pool.QueryRow(ctx, check.q).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n != 0 {
			t.Errorf("%s = %d, want 0", check.what, n)
		}
	}
	drifts, err := ledger.VerifyBalances(ctx, pool)
	if err != nil {
		t.Fatal(err)
	}
	if len(drifts) != 0 {
		t.Errorf("stored balances drifted: %+v", drifts)
	}
	t.Logf("SIGKILL pid=%d after %d decisions; %d authorizations in all, %d attempts cut or refused, %d stand-ins advised",
		first.Process.Pid, killedAt, len(results), cut, standIns)
}

type killRow struct {
	authID, decision string
	approved, held   int64
	standIn          bool
}

// TestAPIProcess is the child: the real cmd/api main, configured by the parent.
func TestAPIProcess(t *testing.T) {
	if os.Getenv("PAYMENTS_API_HELPER") != "1" {
		t.Skip("subprocess helper")
	}
	main()
}

func startAPIProcess(t *testing.T, ctx context.Context, schema, addr string) *exec.Cmd {
	t.Helper()
	dsn, err := url.Parse(testdb.DSN())
	if err != nil {
		t.Fatal(err)
	}
	q := dsn.Query()
	q.Set("search_path", schema)
	dsn.RawQuery = q.Encode()

	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(ctx, binary, "-test.run=^TestAPIProcess$")
	cmd.Env = append(os.Environ(), "PAYMENTS_API_HELPER=1", "DATABASE_URL="+dsn.String(), "API_ADDR="+addr,
		"MOCKBANK_WEBHOOK_SECRET="+killWebhookSecret, "NETWORK_SECRET="+killNetworkSecret)
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if cmd.ProcessState == nil {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	})
	waitUntil(t, ctx, func() bool {
		resp, err := http.Get("http://" + addr + "/readyz")
		if err != nil {
			return false
		}
		resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	})
	return cmd
}

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

func waitUntil(t *testing.T, ctx context.Context, done func() bool) {
	t.Helper()
	for !done() {
		select {
		case <-ctx.Done():
			t.Fatalf("waiting: %v", ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// seedKillCards issues n cards on accounts funded far beyond the run, with
// limits it never reaches: every decision is about the process, not the money.
func seedKillCards(t *testing.T, ctx context.Context, pool *pgxpool.Pool, n int) []string {
	t.Helper()
	_, key, err := auth.Generate("api kill test")
	if err != nil {
		t.Fatal(err)
	}
	if err := auth.Insert(ctx, pool, key); err != nil {
		t.Fatal(err)
	}
	ids := make([]string, n)
	for i := range ids {
		funding := randomID("acc")
		if _, err := pool.Exec(ctx, `INSERT INTO accounts (id, currency, type) VALUES ($1, 'USD', 'asset')`, funding); err != nil {
			t.Fatal(err)
		}
		if _, err := ledger.Record(ctx, pool, "funding", []ledger.Entry{
			{AccountID: "acc_settlement_usd", Direction: ledger.DirectionDebit, Amount: 100_000_000},
			{AccountID: funding, Direction: ledger.DirectionCredit, Amount: 100_000_000},
		}); err != nil {
			t.Fatal(err)
		}
		card, err := cards.Issue(ctx, pool, cards.Card{
			ID: randomID("card"), AccountID: funding, PerTransactionLimit: 1_000_000, DailyLimit: 1_000_000_000,
			APIKeyID: key.ID, IdempotencyKey: randomID("idem"), RequestFingerprint: "test",
		})
		if err != nil {
			t.Fatal(err)
		}
		ids[i] = card.ID
	}
	return ids
}

func randomID(prefix string) string {
	var b [12]byte
	_, _ = rand.Read(b[:])
	return prefix + "_" + hex.EncodeToString(b[:])
}
