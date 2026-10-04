package worker

import (
	"context"
	"sync"
	"testing"
	"time"
)

// ⭐ Guarantee 7: an event delivered twice sends the payment once.
//
// Kafka delivers at least once, so the consumer will see the same
// transfer.created again — after a crash, a rebalance, or a relay that
// republished. The second delivery must find the transfer already claimed and
// do nothing.
func TestSendByIDTwiceSubmitsOnce(t *testing.T) {
	ctx := context.Background()
	id := seedOne(t, 5000)
	rail := accepts()
	w := New(testPool, rail, DefaultConfig())

	sent, err := w.SendByID(ctx, id)
	if err != nil {
		t.Fatalf("first delivery: %v", err)
	}
	if !sent {
		t.Fatalf("first delivery sent = false, want true: the transfer was waiting to be sent")
	}

	sent, err = w.SendByID(ctx, id)
	if err != nil {
		t.Fatalf("second delivery: %v", err)
	}
	if sent {
		t.Errorf("second delivery sent = true, want false: the transfer was already sent")
	}

	if n := rail.submissionsFor(id); n != 1 {
		t.Errorf("the rail received %s %d times, want 1", id, n)
	}
}

// An event for a transfer that does not exist is not an error: there is
// nothing to send, and retrying would block the partition forever.
func TestSendByIDUnknownTransferIsSkipped(t *testing.T) {
	resetDB(t)
	sent, err := New(testPool, accepts(), DefaultConfig()).SendByID(context.Background(), "tr_never_existed")
	if err != nil {
		t.Fatalf("SendByID: %v", err)
	}
	if sent {
		t.Errorf("sent = true for a transfer that does not exist")
	}
}

// ⭐ The cost of two paths: the consumer and the poller race for the same
// transfers, and each must still be sent exactly once.
//
// The poller claims in batches, the consumer one id at a time, both at once.
// They share the claim guard — no provider_ref yet, row lock, SKIP LOCKED —
// and that guard is the only thing standing between this and a double payment.
func TestConsumerAndPollerSendEachTransferOnce(t *testing.T) {
	const total = 50
	ids := seedFunded(t, total, 1000)
	ctx := context.Background()

	rail := newCountingProvider(2 * time.Millisecond)
	cfg := DefaultConfig()
	cfg.BatchSize = 5
	cfg.RetryBackoff = time.Hour

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { // the poller
		defer wg.Done()
		for {
			sent, err := New(testPool, rail, cfg).RunOnce(ctx)
			if err != nil {
				t.Errorf("RunOnce: %v", err)
				return
			}
			if sent == 0 {
				return
			}
		}
	}()
	go func() { // the consumer, handed every transfer.created
		defer wg.Done()
		w := New(testPool, rail, cfg)
		for _, id := range ids {
			if _, err := w.SendByID(ctx, id); err != nil {
				t.Errorf("SendByID %s: %v", id, err)
			}
		}
	}()
	wg.Wait()

	if dupes := rail.duplicates(); len(dupes) > 0 {
		t.Errorf("transfers sent to the provider more than once: %v", dupes)
	}
	if got := rail.submitted.Load(); got != total {
		t.Errorf("%d submissions for %d transfers, want %d", got, total, total)
	}
}
