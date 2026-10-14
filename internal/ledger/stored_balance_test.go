package ledger

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"testing"
	"time"
)

const settlement = "acc_settlement_usd"

func fundAccount(t *testing.T, id string, amount int64) {
	t.Helper()
	if _, err := Record(context.Background(), testPool, "funding", []Entry{
		{AccountID: settlement, Direction: DirectionDebit, Amount: amount},
		{AccountID: id, Direction: DirectionCredit, Amount: amount},
	}); err != nil {
		t.Fatalf("fund %s: %v", id, err)
	}
}

func storedBalance(t *testing.T, id string) int64 {
	t.Helper()
	balance, tracked, err := StoredBalance(context.Background(), testPool, id)
	if err != nil {
		t.Fatalf("StoredBalance(%s): %v", id, err)
	}
	if !tracked {
		t.Fatalf("%s has no stored balance row", id)
	}
	return balance
}

func assertNoDrift(t *testing.T) {
	t.Helper()
	drifts, err := VerifyBalances(context.Background(), testPool)
	if err != nil {
		t.Fatalf("VerifyBalances: %v", err)
	}
	for _, d := range drifts {
		t.Errorf("drift on %s: stored %d, derived %d, missing row %t", d.AccountID, d.Stored, d.Derived, d.Missing)
	}
}

// The stored balance is only trustworthy if every concurrent write keeps it
// equal to the entries it summarises.
func TestStoredBalanceTracksEveryMovement(t *testing.T) {
	resetDB(t)
	ctx := context.Background()

	accounts := []string{"acc_storm_0", "acc_storm_1", "acc_storm_2", "acc_storm_3", "acc_storm_4"}
	for _, id := range accounts {
		createAccount(t, id)
		fundAccount(t, id, 100000)
	}

	const workers = 64
	const movesEach = 25
	var wg sync.WaitGroup
	errs := make(chan error, workers*movesEach)
	for w := range workers {
		wg.Add(1)
		go func(seed uint64) {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(seed, seed))
			for range movesEach {
				from := accounts[rng.IntN(len(accounts))]
				to := accounts[rng.IntN(len(accounts))]
				amount := int64(rng.IntN(5000) + 1)
				var entries []Entry
				switch {
				case from == to:
					// Money arriving from outside, through settlement.
					entries = []Entry{
						{AccountID: settlement, Direction: DirectionDebit, Amount: amount},
						{AccountID: to, Direction: DirectionCredit, Amount: amount},
					}
				case rng.IntN(3) == 0:
					// A split: part to another account, part to settlement.
					entries = []Entry{
						{AccountID: from, Direction: DirectionDebit, Amount: amount + 1},
						{AccountID: to, Direction: DirectionCredit, Amount: amount},
						{AccountID: settlement, Direction: DirectionCredit, Amount: 1},
					}
				default:
					entries = []Entry{
						{AccountID: from, Direction: DirectionDebit, Amount: amount},
						{AccountID: to, Direction: DirectionCredit, Amount: amount},
					}
				}
				if _, err := Record(ctx, testPool, "storm", entries); err != nil && !errors.Is(err, ErrInsufficientFunds) {
					errs <- err
				}
			}
		}(uint64(w))
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("Record: %v", err)
	}

	assertNoDrift(t)
	for _, id := range accounts {
		if b := storedBalance(t, id); b < 0 {
			t.Errorf("%s stored balance %d is negative", id, b)
		}
	}
}

// The overdraft guard is in the database, so it holds for every caller of
// Record, not only the transfer API.
func TestLedgerRefusesOverdraft(t *testing.T) {
	resetDB(t)
	ctx := context.Background()
	createAccount(t, "acc_spender")
	fundAccount(t, "acc_spender", 1000)

	_, err := Record(ctx, testPool, "one cent too much", []Entry{
		{AccountID: "acc_spender", Direction: DirectionDebit, Amount: 1001},
		{AccountID: settlement, Direction: DirectionCredit, Amount: 1001},
	})
	if !errors.Is(err, ErrInsufficientFunds) {
		t.Fatalf("Record error = %v, want ErrInsufficientFunds", err)
	}

	// Only the funding transaction remains.
	if txns, entries := countRows(t); txns != 1 || entries != 2 {
		t.Errorf("after a refused overdraft: %d transactions and %d entries, want 1 and 2", txns, entries)
	}
	if b := storedBalance(t, "acc_spender"); b != 1000 {
		t.Errorf("stored balance = %d, want 1000", b)
	}
	assertNoDrift(t)
}

// A→B locks A then B and B→A locks B then A unless the order is fixed;
// Postgres would abort one of them with a deadlock (40P01).
func TestOppositeTransfersDoNotDeadlock(t *testing.T) {
	resetDB(t)
	ctx := context.Background()
	// Ids sort opposite to the first pair's entry order, so the fix is exercised.
	createAccount(t, "acc_z_first")
	createAccount(t, "acc_a_second")
	fundAccount(t, "acc_z_first", 1000000)
	fundAccount(t, "acc_a_second", 1000000)

	move := func(from, to string) error {
		_, err := Record(ctx, testPool, "ping-pong", []Entry{
			{AccountID: from, Direction: DirectionDebit, Amount: 1},
			{AccountID: to, Direction: DirectionCredit, Amount: 1},
		})
		return err
	}

	const iterations = 200
	const perDirection = 4
	var wg sync.WaitGroup
	errs := make(chan error, 2*perDirection*iterations)
	for range perDirection {
		for _, pair := range [][2]string{{"acc_z_first", "acc_a_second"}, {"acc_a_second", "acc_z_first"}} {
			wg.Add(1)
			go func(from, to string) {
				defer wg.Done()
				for range iterations {
					// Each deadlock costs deadlock_timeout to detect; one is proof enough.
					if err := move(from, to); err != nil {
						errs <- err
						return
					}
				}
			}(pair[0], pair[1])
		}
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("opposite transfer failed: %v", err)
	}

	if a, z := storedBalance(t, "acc_a_second"), storedBalance(t, "acc_z_first"); a != 1000000 || z != 1000000 {
		t.Errorf("balances = %d and %d, want 1000000 each: the moves cancel out", a, z)
	}
	assertNoDrift(t)
}

// Every payment touches settlement, so it must not have a row that every
// payment would queue on.
func TestSettlementAccountHasNoBalanceRow(t *testing.T) {
	resetDB(t)
	ctx := context.Background()
	createAccount(t, "acc_one")
	createAccount(t, "acc_two")
	fundAccount(t, "acc_one", 1000)
	fundAccount(t, "acc_two", 1000)

	if _, tracked, err := StoredBalance(ctx, testPool, settlement); err != nil || tracked {
		t.Fatalf("settlement tracked = %t (err %v), want no stored balance row", tracked, err)
	}

	// An open transaction that has written to settlement must not block another.
	first, err := testPool.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	defer first.Rollback(ctx)
	if _, err := Record(ctx, first, "held", []Entry{
		{AccountID: "acc_one", Direction: DirectionDebit, Amount: 100},
		{AccountID: settlement, Direction: DirectionCredit, Amount: 100},
	}); err != nil {
		t.Fatalf("Record in open transaction: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		_, err := Record(ctx, testPool, "concurrent", []Entry{
			{AccountID: "acc_two", Direction: DirectionDebit, Amount: 100},
			{AccountID: settlement, Direction: DirectionCredit, Amount: 100},
		})
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("concurrent settlement write: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("a settlement write waited for another transaction: settlement is being locked")
	}
}

// The row is created by a database trigger, so raw SQL inserts are covered too.
func TestNewAccountsGetABalanceRow(t *testing.T) {
	resetDB(t)
	ctx := context.Background()

	for _, typ := range []string{"asset", "liability", "settlement"} {
		id := fmt.Sprintf("acc_raw_%s", typ)
		if _, err := testPool.Exec(ctx,
			`INSERT INTO accounts (id, currency, type) VALUES ($1, 'USD', $2)`, id, typ); err != nil {
			t.Fatalf("insert %s account: %v", typ, err)
		}
		balance, tracked, err := StoredBalance(ctx, testPool, id)
		if err != nil {
			t.Fatalf("StoredBalance(%s): %v", id, err)
		}
		if want := typ != "settlement"; tracked != want {
			t.Errorf("%s account tracked = %t, want %t", typ, tracked, want)
		}
		if balance != 0 {
			t.Errorf("%s account starts at %d, want 0", typ, balance)
		}
	}
	assertNoDrift(t)
}
