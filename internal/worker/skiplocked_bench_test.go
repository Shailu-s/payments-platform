package worker

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Evidence for PLAN.md §7: what SKIP LOCKED actually buys.
//
// The correctness tests pass either way — row locking alone already prevents
// two workers claiming the same transfer. That is worth knowing and worth
// saying: SKIP LOCKED is not what makes the queue correct. What it changes is
// whether adding workers adds throughput.
//
// Without it, a second worker's claim query BLOCKS on the rows the first has
// locked. It waits for that transaction to commit, then re-reads and takes
// different work — so workers take turns instead of working in parallel, and
// the queue drains at one worker's pace no matter how many are running.
//
//	go test ./internal/worker/ -run TestSkipLockedThroughput -v
func TestSkipLockedThroughput(t *testing.T) {
	if testing.Short() {
		t.Skip("throughput comparison is slow")
	}

	// Each claim holds its lock for this long before committing, standing in
	// for the work a real worker does inside the transaction. With no hold at
	// all the transactions are too short to contend and the difference cannot
	// be seen.
	const holdFor = 5 * time.Millisecond
	const transfersPerRun = 120
	const workers = 6

	type result struct {
		mode    string
		elapsed time.Duration
		claimed int64
		blocked time.Duration
	}
	var results []result

	for _, skipLocked := range []bool{true, false} {
		mode := "SKIP LOCKED"
		if !skipLocked {
			mode = "FOR UPDATE only"
		}

		seedTransfers(t, transfersPerRun)
		ctx := context.Background()

		var claimed atomic.Int64
		var totalWait atomic.Int64

		var start sync.WaitGroup
		start.Add(1)
		var done sync.WaitGroup

		began := time.Now()
		for w := 0; w < workers; w++ {
			done.Add(1)
			go func() {
				defer done.Done()
				start.Wait()
				for {
					waited, n, err := claimHolding(ctx, skipLocked, 5, holdFor)
					if err != nil {
						t.Errorf("claim: %v", err)
						return
					}
					totalWait.Add(int64(waited))
					if n == 0 {
						return
					}
					claimed.Add(int64(n))
				}
			}()
		}
		start.Done()
		done.Wait()

		results = append(results, result{
			mode:    mode,
			elapsed: time.Since(began),
			claimed: claimed.Load(),
			blocked: time.Duration(totalWait.Load()),
		})
	}

	t.Log("")
	t.Logf("  %d transfers, %d workers, each claim holding its lock %s", transfersPerRun, workers, holdFor)
	t.Log("")
	t.Log("  mode              elapsed    claimed   total time in the claim query")
	t.Log("  ────────────────────────────────────────────────────────────────────")
	for _, r := range results {
		t.Logf("  %-16s  %7s  %9d   %s",
			r.mode, r.elapsed.Round(time.Millisecond), r.claimed, r.blocked.Round(time.Millisecond))
	}
	t.Log("")

	if len(results) == 2 {
		ratio := float64(results[1].elapsed) / float64(results[0].elapsed)
		t.Logf("  without SKIP LOCKED the same work took %.1fx as long", ratio)
	}
}

// claimHolding claims a batch and holds the transaction open for hold, so the
// lock is observable to other workers. Returns how long the claim query itself
// took, which is where blocking shows up.
func claimHolding(ctx context.Context, skipLocked bool, limit int, hold time.Duration) (time.Duration, int, error) {
	lockClause := "FOR UPDATE SKIP LOCKED"
	if !skipLocked {
		lockClause = "FOR UPDATE"
	}

	tx, err := testPool.Begin(ctx)
	if err != nil {
		return 0, 0, err
	}
	defer tx.Rollback(ctx)

	q := fmt.Sprintf(`
		UPDATE transfers
		SET attempt_count = attempt_count + 1, next_attempt_at = now() + interval '1 hour'
		WHERE id IN (
			SELECT id FROM transfers
			WHERE status = 'processing' AND provider_ref IS NULL
			  AND (next_attempt_at IS NULL OR next_attempt_at <= now())
			ORDER BY next_attempt_at NULLS FIRST, created_at
			%s
			LIMIT %d
		)
		RETURNING id`, lockClause, limit)

	began := time.Now()
	rows, err := tx.Query(ctx, q)
	if err != nil {
		return time.Since(began), 0, err
	}
	count := 0
	for rows.Next() {
		count++
	}
	rows.Close()
	waited := time.Since(began)

	if err := rows.Err(); err != nil {
		return waited, 0, err
	}

	// The work a real worker does before committing.
	if count > 0 {
		time.Sleep(hold)
	}
	return waited, count, tx.Commit(ctx)
}
