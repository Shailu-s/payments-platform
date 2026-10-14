package api

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

func trackedAccounts(g *accountGate) int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.slots)
}

func TestAccountGateCapsInFlightPerAccount(t *testing.T) {
	g := newAccountGate(2, 50*time.Millisecond)
	ctx := context.Background()

	for i := range 2 {
		release, err := g.acquire(ctx, "acc_hot")
		if err != nil {
			t.Fatalf("acquire %d: %v", i, err)
		}
		defer release()
	}

	start := time.Now()
	if _, err := g.acquire(ctx, "acc_hot"); !errors.Is(err, ErrAccountBusy) {
		t.Fatalf("third acquire err = %v, want ErrAccountBusy", err)
	}
	if waited := time.Since(start); waited < 50*time.Millisecond {
		t.Errorf("refused after %v, want it to wait the full 50ms first", waited)
	}

	// The cap is per account: another account is not affected.
	release, err := g.acquire(ctx, "acc_other")
	if err != nil {
		t.Fatalf("other account: %v", err)
	}
	release()
}

func TestAccountGateWaiterGetsAReleasedSlot(t *testing.T) {
	g := newAccountGate(1, time.Second)
	ctx := context.Background()

	release, err := g.acquire(ctx, "acc_hot")
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	done := make(chan error, 1)
	go func() {
		next, err := g.acquire(ctx, "acc_hot")
		if err == nil {
			next()
		}
		done <- err
	}()

	time.Sleep(20 * time.Millisecond)
	release()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("waiter: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("waiter never got the released slot")
	}
}

func TestAccountGateHonoursCancellation(t *testing.T) {
	g := newAccountGate(1, time.Minute)
	release, err := g.acquire(context.Background(), "acc_hot")
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	start := time.Now()
	if _, err := g.acquire(ctx, "acc_hot"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want context.DeadlineExceeded", err)
	}
	if waited := time.Since(start); waited > 500*time.Millisecond {
		t.Errorf("cancelled waiter returned after %v, want promptly", waited)
	}

	release()
	if n := trackedAccounts(g); n != 0 {
		t.Errorf("%d accounts tracked after everyone left, want 0", n)
	}
}

// Every account that ever paid must not stay in memory.
func TestAccountGateForgetsIdleAccounts(t *testing.T) {
	g := newAccountGate(1, time.Millisecond)
	ctx := context.Background()

	for i := range 200 {
		release, err := g.acquire(ctx, fmt.Sprintf("acc_%d", i))
		if err != nil {
			t.Fatalf("acquire %d: %v", i, err)
		}
		// A refused waiter must also drop its reference.
		if _, err := g.acquire(ctx, fmt.Sprintf("acc_%d", i)); !errors.Is(err, ErrAccountBusy) {
			t.Fatalf("second acquire %d err = %v, want ErrAccountBusy", i, err)
		}
		release()
	}
	if n := trackedAccounts(g); n != 0 {
		t.Errorf("%d accounts tracked after all released, want 0", n)
	}
}
