package api

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/Shailu-s/payments-platform/internal/ledger"
)

// fund puts money into an account by recording a ledger transaction directly.
// There is no deposit endpoint in V1, and the source of the money is not what
// these tests are about.
func fund(t *testing.T, accountID string, amount int64) {
	t.Helper()
	if _, err := ledger.Record(context.Background(), testPool, "funding", []ledger.Entry{
		{AccountID: settlementAccountID, Direction: ledger.DirectionDebit, Amount: amount},
		{AccountID: accountID, Direction: ledger.DirectionCredit, Amount: amount},
	}); err != nil {
		t.Fatalf("fund %s: %v", accountID, err)
	}
}

// ⭐ Guarantee 3: money cannot be overspent by concurrent requests.
//
// The scenario is the classic one: an account holds $1,000 and two $700
// transfers arrive together, so exactly one must succeed. It is driven with
// more than two requests deliberately.
//
// Two goroutines is the right STORY and an unreliable TEST. Measured on this
// machine: at two requests the race fired in roughly one run out of three, and
// at eight in four out of five — the rest of the time they serialised on the
// connection pool and the test passed while the bug was still there. Sixteen
// fires every time. A test that only sometimes detects the bug is not evidence,
// and a green run from it means nothing.
//
// PHASE 3.3: expected to FAIL. The balance check reads, decides, then writes,
// and every request reads the full balance before any has spent against it.
func TestConcurrentTransfersCannotOverspend(t *testing.T) {
	h, apiKey := newTestServer(t)
	ctx := context.Background()

	source := createAccount(t, h, apiKey, "asset")
	destination := createAccount(t, h, apiKey, "liability")
	fund(t, source.ID, 100000) // $1,000.00

	// $700 each against $1,000: exactly one is affordable, whatever the number
	// of attempts.
	const amount = 70000
	const attempts = 16
	body := fmt.Sprintf(`{"source_account":%q,"destination_account":%q,"amount":%d,"currency":"USD"}`,
		source.ID, destination.ID, amount)

	var accepted, rejected atomic.Int64

	// Released together, so the requests genuinely overlap.
	var start sync.WaitGroup
	start.Add(1)
	var done sync.WaitGroup

	for i := 0; i < attempts; i++ {
		done.Add(1)
		go func(i int) {
			defer done.Done()
			start.Wait()

			rec := doWithKey(h, "POST", "/v1/transfers", apiKey,
				fmt.Sprintf("overspend-%d", i), body)
			switch rec.Code {
			case http.StatusAccepted:
				accepted.Add(1)
			case http.StatusUnprocessableEntity:
				rejected.Add(1)
			default:
				t.Errorf("request %d returned an unexpected %d: %s", i, rec.Code, rec.Body.String())
			}
		}(i)
	}

	start.Done()
	done.Wait()

	if got := accepted.Load(); got != 1 {
		t.Errorf("%d of %d concurrent $700 transfers were accepted against $1,000, want 1",
			got, attempts)
	}
	if got := rejected.Load(); got != attempts-1 {
		t.Errorf("%d transfers were rejected, want %d", got, attempts-1)
	}

	// The assertion that matters: the account may not go negative.
	balance, err := ledger.Balance(ctx, testPool, source.ID)
	if err != nil {
		t.Fatalf("Balance: %v", err)
	}
	if balance < 0 {
		t.Errorf("source balance is %d: the account was overspent by %d", balance, -balance)
	}
	if balance != 30000 {
		t.Errorf("source balance is %d, want 30000: exactly one transfer should have happened", balance)
	}

	// And the losers must have written NOTHING. An error response means little
	// if half a transfer was committed.
	var created int
	if err := testPool.QueryRow(ctx, `SELECT count(*) FROM transfers`).Scan(&created); err != nil {
		t.Fatalf("count: %v", err)
	}
	if created != 1 {
		t.Errorf("%d transfers exist, want 1", created)
	}
}

// The lesson of the phase, kept as a permanent assertion.
//
// The ledger invariant does NOT catch an overspend. This writes an overdrawn
// account directly through the ledger, bypassing the API's balance check, and
// shows that every transaction still balances while the account is negative.
//
// Guarantee 1 and guarantee 3 look related and protect against completely
// different failures: "debits equal credits" is a property of one transaction
// in isolation, "this account is not overdrawn" is a property of all of that
// account's transactions together. No amount of per-transaction checking
// produces the second, which is why the fix is a lock rather than a stricter
// balance rule.
func TestTheLedgerInvariantDoesNotCatchAnOverspend(t *testing.T) {
	h, apiKey := newTestServer(t)
	ctx := context.Background()

	source := createAccount(t, h, apiKey, "asset")
	fund(t, source.ID, 100000) // $1,000

	// Two $700 movements recorded straight through the ledger, as the broken
	// concurrent path did before the fix.
	for i := 0; i < 2; i++ {
		if _, err := ledger.Record(ctx, testPool, "overspend", []ledger.Entry{
			{AccountID: source.ID, Direction: ledger.DirectionDebit, Amount: 70000},
			{AccountID: settlementAccountID, Direction: ledger.DirectionCredit, Amount: 70000},
		}); err != nil {
			t.Fatalf("Record: %v", err)
		}
	}

	// Every ledger transaction balances.
	var unbalanced int
	if err := testPool.QueryRow(ctx, `
		SELECT count(*) FROM (
			SELECT txn_id
			FROM ledger_entries
			GROUP BY txn_id
			HAVING SUM(CASE WHEN direction = 'credit' THEN amount ELSE -amount END) <> 0
		) AS bad`).Scan(&unbalanced); err != nil {
		t.Fatalf("check invariant: %v", err)
	}
	if unbalanced != 0 {
		t.Fatalf("%d ledger transactions do not balance", unbalanced)
	}

	// Every entry in the database sums to zero: nothing was invented or lost.
	var total int64
	if err := testPool.QueryRow(ctx, `
		SELECT COALESCE(SUM(CASE WHEN direction = 'credit' THEN amount ELSE -amount END), 0)
		FROM ledger_entries`).Scan(&total); err != nil {
		t.Fatalf("sum entries: %v", err)
	}
	if total != 0 {
		t.Fatalf("all entries sum to %d, want 0", total)
	}

	// And the account is overdrawn.
	balance, err := ledger.Balance(ctx, testPool, source.ID)
	if err != nil {
		t.Fatalf("Balance: %v", err)
	}
	if balance != -40000 {
		t.Fatalf("source balance is %d, want -40000", balance)
	}

	t.Logf("every transaction balances, every entry sums to 0, and the account "+
		"is %d overdrawn: the invariant is blind to this", -balance)

	// The money did not vanish — settlement is holding what the account never had.
	settlement, err := ledger.Balance(ctx, testPool, settlementAccountID)
	if err != nil {
		t.Fatalf("settlement balance: %v", err)
	}
	if balance+settlement != 0 {
		t.Errorf("account %d and settlement %d do not cancel", balance, settlement)
	}
}

// A single transfer larger than the balance is refused. This is the sequential
// case, which the naive check does handle — and it is why the broken version
// survives review.
func TestSingleTransferCannotExceedTheBalance(t *testing.T) {
	h, apiKey := newTestServer(t)

	source := createAccount(t, h, apiKey, "asset")
	destination := createAccount(t, h, apiKey, "liability")
	fund(t, source.ID, 50000)

	body := fmt.Sprintf(`{"source_account":%q,"destination_account":%q,"amount":50001,"currency":"USD"}`,
		source.ID, destination.ID)
	rec := doWithKey(h, "POST", "/v1/transfers", apiKey, "one-cent-too-much", body)

	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422: %s", rec.Code, rec.Body.String())
	}
	if got := decodeError(t, rec).Code; got != CodeInsufficientFunds {
		t.Errorf("error code = %q, want %q", got, CodeInsufficientFunds)
	}

	var count int
	if err := testPool.QueryRow(context.Background(), `SELECT count(*) FROM transfers`).Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 0 {
		t.Errorf("a refused transfer created %d rows, want 0", count)
	}
}

// Spending exactly the balance is allowed. An off-by-one here refuses valid
// payments, which is a bug in the other direction.
func TestSpendingTheEntireBalanceIsAllowed(t *testing.T) {
	h, apiKey := newTestServer(t)
	ctx := context.Background()

	source := createAccount(t, h, apiKey, "asset")
	destination := createAccount(t, h, apiKey, "liability")
	fund(t, source.ID, 50000)

	body := fmt.Sprintf(`{"source_account":%q,"destination_account":%q,"amount":50000,"currency":"USD"}`,
		source.ID, destination.ID)
	rec := doWithKey(h, "POST", "/v1/transfers", apiKey, "exact-balance", body)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202: spending the whole balance is valid: %s",
			rec.Code, rec.Body.String())
	}

	balance, err := ledger.Balance(ctx, testPool, source.ID)
	if err != nil {
		t.Fatalf("Balance: %v", err)
	}
	if balance != 0 {
		t.Errorf("balance = %d, want 0", balance)
	}
}

// An over-eager lock that refuses valid transfers is also a bug, and it is the
// failure mode of a lock taken too broadly. N transfers against a balance for
// exactly N must ALL succeed.
func TestExactlyAffordableConcurrentTransfersAllSucceed(t *testing.T) {
	h, apiKey := newTestServer(t)
	ctx := context.Background()

	source := createAccount(t, h, apiKey, "asset")
	destination := createAccount(t, h, apiKey, "liability")

	const transferCount = 10
	const amount = 10000
	fund(t, source.ID, transferCount*amount) // exactly enough for all of them

	body := fmt.Sprintf(`{"source_account":%q,"destination_account":%q,"amount":%d,"currency":"USD"}`,
		source.ID, destination.ID, amount)

	var accepted atomic.Int64
	var start sync.WaitGroup
	start.Add(1)
	var done sync.WaitGroup

	for i := 0; i < transferCount; i++ {
		done.Add(1)
		go func(i int) {
			defer done.Done()
			start.Wait()
			rec := doWithKey(h, "POST", "/v1/transfers", apiKey,
				fmt.Sprintf("affordable-%d", i), body)
			if rec.Code == http.StatusAccepted {
				accepted.Add(1)
			}
		}(i)
	}
	start.Done()
	done.Wait()

	if got := accepted.Load(); got != transferCount {
		t.Errorf("%d of %d affordable transfers succeeded, want all of them: "+
			"a lock that refuses valid payments is also a bug", got, transferCount)
	}

	balance, err := ledger.Balance(ctx, testPool, source.ID)
	if err != nil {
		t.Fatalf("Balance: %v", err)
	}
	if balance != 0 {
		t.Errorf("balance = %d, want 0", balance)
	}
}
