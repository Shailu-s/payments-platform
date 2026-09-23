package worker

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestClaimReturnsDueTransfers(t *testing.T) {
	seedTransfers(t, 5)
	ctx := context.Background()

	claimed, err := Claim(ctx, testPool, 10, time.Minute)
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if len(claimed) != 5 {
		t.Errorf("claimed %d transfers, want 5", len(claimed))
	}

	// Claiming pushes next_attempt_at forward, so an immediate second claim
	// takes nothing: a worker that dies mid-send costs one backoff rather than
	// a tight retry loop against the rail.
	again, err := Claim(ctx, testPool, 10, time.Minute)
	if err != nil {
		t.Fatalf("second Claim: %v", err)
	}
	if len(again) != 0 {
		t.Errorf("a second claim took %d transfers, want 0: they are not due yet", len(again))
	}
}

func TestClaimRespectsTheLimit(t *testing.T) {
	seedTransfers(t, 20)

	claimed, err := Claim(context.Background(), testPool, 7, time.Minute)
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if len(claimed) != 7 {
		t.Errorf("claimed %d, want 7", len(claimed))
	}
}

// Backoff is a timestamp, not a sleep, so it survives a restart.
func TestClaimTakesTransfersWhoseBackoffHasExpired(t *testing.T) {
	seedTransfers(t, 3)
	ctx := context.Background()

	if _, err := Claim(ctx, testPool, 10, 50*time.Millisecond); err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if again, _ := Claim(ctx, testPool, 10, time.Minute); len(again) != 0 {
		t.Fatalf("claimed %d before the backoff expired, want 0", len(again))
	}

	time.Sleep(100 * time.Millisecond)

	due, err := Claim(ctx, testPool, 10, time.Minute)
	if err != nil {
		t.Fatalf("Claim after backoff: %v", err)
	}
	if len(due) != 3 {
		t.Errorf("claimed %d after the backoff expired, want 3", len(due))
	}
}

// A transfer that already has a provider reference has been sent, and must not
// be sent again. This is what stops a redelivered claim paying twice.
func TestClaimSkipsTransfersAlreadySentToTheProvider(t *testing.T) {
	seedTransfers(t, 4)
	ctx := context.Background()

	if _, err := testPool.Exec(ctx,
		`UPDATE transfers SET provider_ref = 'mb_already' WHERE id = 'tr_w1'`); err != nil {
		t.Fatalf("set provider_ref: %v", err)
	}

	claimed, err := Claim(ctx, testPool, 10, time.Minute)
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if len(claimed) != 3 {
		t.Errorf("claimed %d, want 3: a transfer already sent must not be claimed", len(claimed))
	}
	for _, c := range claimed {
		if c.ID == "tr_w1" {
			t.Error("claimed a transfer that already has a provider reference")
		}
	}
}

// ⭐ Two workers, 50 transfers, each claimed exactly once. One worker proves
// nothing about a queue.
func TestTwoWorkersNeverClaimTheSameTransfer(t *testing.T) {
	const total = 50
	seedTransfers(t, total)
	ctx := context.Background()

	var mu sync.Mutex
	claims := map[string]int{}

	var start sync.WaitGroup
	start.Add(1)
	var done sync.WaitGroup

	for w := 0; w < 2; w++ {
		done.Add(1)
		go func(worker int) {
			defer done.Done()
			start.Wait()

			for {
				batch, err := Claim(ctx, testPool, 5, time.Hour)
				if err != nil {
					t.Errorf("worker %d: %v", worker, err)
					return
				}
				if len(batch) == 0 {
					return
				}
				mu.Lock()
				for _, tr := range batch {
					claims[tr.ID]++
				}
				mu.Unlock()
			}
		}(w)
	}

	start.Done()
	done.Wait()

	if len(claims) != total {
		t.Errorf("%d distinct transfers claimed, want %d", len(claims), total)
	}
	for id, count := range claims {
		if count != 1 {
			t.Errorf("transfer %s was claimed %d times, want 1: two workers would "+
				"each send it to the provider", id, count)
		}
	}
}

// Ten workers, to make the point harder to reach by luck.
func TestManyWorkersNeverClaimTheSameTransfer(t *testing.T) {
	const total = 200
	const workers = 10
	seedTransfers(t, total)
	ctx := context.Background()

	var mu sync.Mutex
	claims := map[string]int{}

	var start sync.WaitGroup
	start.Add(1)
	var done sync.WaitGroup

	for w := 0; w < workers; w++ {
		done.Add(1)
		go func() {
			defer done.Done()
			start.Wait()
			for {
				batch, err := Claim(ctx, testPool, 10, time.Hour)
				if err != nil || len(batch) == 0 {
					return
				}
				mu.Lock()
				for _, tr := range batch {
					claims[tr.ID]++
				}
				mu.Unlock()
			}
		}()
	}

	start.Done()
	done.Wait()

	duplicates := 0
	for _, count := range claims {
		if count != 1 {
			duplicates++
		}
	}
	if duplicates != 0 {
		t.Errorf("%d transfers were claimed more than once by %d workers", duplicates, workers)
	}
	if len(claims) != total {
		t.Errorf("%d of %d transfers were claimed", len(claims), total)
	}
}
