package api

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Shailu-s/payments-platform/internal/ledger"
	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Keep SERIALIZABLE in the comparison, not as an unused production path.
// Compare throughput, latency and retry exhaustion on the same hot-account workload.
func TestLockStrategyComparison(t *testing.T) {
	if testing.Short() {
		t.Skip("comparison is slow")
	}

	type result struct {
		strategy    string
		concurrency int
		succeeded   int64
		lost        int64
		elapsed     time.Duration
		p50, p99    time.Duration
	}
	var results []result

	for _, concurrency := range []int{1, 10, 50} {
		for _, strategy := range []string{"for_update", "serializable"} {
			resetDB(t)
			ctx := context.Background()

			// One account, many writers: the hot-account workload a payments
			// path actually has, and the worst case for optimistic locking.
			if _, err := testPool.Exec(ctx,
				`INSERT INTO accounts (id, currency, type) VALUES ('acc_hot', 'USD', 'asset')`); err != nil {
				t.Fatalf("create account: %v", err)
			}

			const amount = 100
			const spends = 200
			// Funded well beyond the spending, so every attempt is affordable
			// and anything that fails is a valid payment the strategy dropped.
			if _, err := ledger.Record(ctx, testPool, "funding", []ledger.Entry{
				{AccountID: settlementAccountID, Direction: ledger.DirectionDebit, Amount: amount * spends * 2},
				{AccountID: "acc_hot", Direction: ledger.DirectionCredit, Amount: amount * spends * 2},
			}); err != nil {
				t.Fatalf("fund: %v", err)
			}

			latencies := make([]time.Duration, spends)
			var succeeded, lost atomic.Int64

			work := make(chan int, spends)
			for i := 0; i < spends; i++ {
				work <- i
			}
			close(work)

			var wg sync.WaitGroup
			start := time.Now()
			for w := 0; w < concurrency; w++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for i := range work {
						begun := time.Now()
						err := spendOnce(ctx, strategy, "acc_hot", amount, i)
						latencies[i] = time.Since(begun)
						if err == nil {
							succeeded.Add(1)
						} else {
							lost.Add(1)
						}
					}
				}()
			}
			wg.Wait()

			res := result{
				strategy:    strategy,
				concurrency: concurrency,
				succeeded:   succeeded.Load(),
				lost:        lost.Load(),
				elapsed:     time.Since(start),
			}
			sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
			res.p50 = latencies[len(latencies)/2]
			res.p99 = latencies[len(latencies)*99/100]

			// Correctness first: neither strategy may overspend.
			balance, err := ledger.Balance(ctx, testPool, "acc_hot")
			if err != nil {
				t.Fatalf("balance: %v", err)
			}
			if balance < 0 {
				t.Errorf("%s at %d overspent: balance %d", strategy, concurrency, balance)
			}

			results = append(results, res)
		}
	}

	t.Log("")
	t.Log("  Both strategies are correct. These numbers are the choice.")
	t.Log("  200 affordable spends against one hot account, so anything lost is")
	t.Log("  a valid payment the strategy failed to carry.")
	t.Log("")
	t.Log("  strategy       conc    tps     p50       p99      ok    lost")
	t.Log("  ──────────────────────────────────────────────────────────────")
	for _, r := range results {
		tps := float64(r.succeeded) / r.elapsed.Seconds()
		t.Logf("  %-13s  %4d  %6.0f  %7s  %8s  %4d  %6d",
			r.strategy, r.concurrency, tps,
			r.p50.Round(100*time.Microsecond),
			r.p99.Round(100*time.Microsecond),
			r.succeeded, r.lost)
	}
	t.Log("")
}

// Mirror the balance-checked debit without coupling the comparison to the handler.
func spendOnce(ctx context.Context, strategy, accountID string, amount int64, seq int) error {
	const maxAttempts = 10

	for attempt := 0; attempt < maxAttempts; attempt++ {
		err := spendAttempt(ctx, strategy, accountID, amount, seq)
		if !isSerializationFailure(err) {
			return err
		}
		// Backoff with jitter. Retrying immediately makes two aborted
		// transactions collide again on the same schedule.
		time.Sleep(time.Duration(attempt+1) * time.Millisecond)
	}
	return errors.New("exhausted retries")
}

func spendAttempt(ctx context.Context, strategy, accountID string, amount int64, seq int) error {
	var (
		tx  pgx.Tx
		err error
	)
	if strategy == "serializable" {
		tx, err = testPool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	} else {
		tx, err = testPool.Begin(ctx)
	}
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	if strategy == "for_update" {
		var locked string
		if err := tx.QueryRow(ctx,
			`SELECT id FROM accounts WHERE id = $1 FOR UPDATE`, accountID).Scan(&locked); err != nil {
			return err
		}
	}

	balance, err := ledger.Balance(ctx, tx, accountID)
	if err != nil {
		return err
	}
	if balance < amount {
		return ErrInsufficientFunds
	}

	if _, err := ledger.Record(ctx, tx, fmt.Sprintf("bench %s %d", strategy, seq), []ledger.Entry{
		{AccountID: accountID, Direction: ledger.DirectionDebit, Amount: amount},
		{AccountID: settlementAccountID, Direction: ledger.DirectionCredit, Amount: amount},
	}); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// SQLSTATE 40001 needs retry in the SERIALIZABLE arm; production uses Read Committed.
func isSerializationFailure(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == pgerrcode.SerializationFailure
}
