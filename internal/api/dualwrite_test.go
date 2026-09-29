package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync/atomic"
	"testing"

	"github.com/Shailu-s/payments-platform/internal/ledger"
)

// failingPublisher stands in for a publish that does not reach the queue —
// a broker that is down, a network partition, or the process dying between
// the commit and the send. From the caller's side all three look the same:
// the transfer is committed and the event is not.
type failingPublisher struct {
	attempts atomic.Int64
	err      error
}

func (p *failingPublisher) Publish(ctx context.Context, event TransferEvent) error {
	p.attempts.Add(1)
	return p.err
}

// ⭐ The dual-write problem, demonstrated before it is fixed.
//
// PHASE 5.1: this test is expected to FAIL, and its output is the
// deliverable. It asserts what we WANT — a transfer and its event are
// always both present or both absent — against an implementation that
// cannot provide it.
//
// The constraint: the database commit and the publish are two separate
// systems. Postgres cannot roll back a publish; the queue cannot roll back
// a commit. No ordering of the two is safe, and no retry closes the gap,
// because the failure case is the process no longer existing.
func TestDualWriteLosesTheEvent(t *testing.T) {
	ctx := context.Background()
	resetDB(t)

	publisher := &failingPublisher{err: errors.New("broker unreachable")}
	h, apiKey := newTestServerWithPublisher(t, publisher)

	source := createAccount(t, h, apiKey, "asset")
	destination := createAccount(t, h, apiKey, "liability")
	fund(t, source.ID, 1000000)

	body := fmt.Sprintf(
		`{"source_account":%q,"destination_account":%q,"amount":50000,"currency":"USD"}`,
		source.ID, destination.ID)

	rec := doWithKey(h, "POST", "/v1/transfers", apiKey, "dual-write-1", body)

	// The caller is told everything is fine. It is not.
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202: %s", rec.Code, rec.Body.String())
	}

	// The publish was attempted and failed.
	if got := publisher.attempts.Load(); got != 1 {
		t.Fatalf("publish attempted %d times, want 1", got)
	}

	// The transfer is committed and durable.
	var status string
	var providerRef *string
	if err := testPool.QueryRow(ctx,
		`SELECT status, provider_ref FROM transfers WHERE idempotency_key = 'dual-write-1'`,
	).Scan(&status, &providerRef); err != nil {
		t.Fatalf("read transfer: %v", err)
	}
	if status != "processing" {
		t.Fatalf("status = %q, want processing", status)
	}

	// The money has already left the source account.
	sourceBalance, err := ledger.Balance(ctx, testPool, source.ID)
	if err != nil {
		t.Fatalf("balance: %v", err)
	}

	// THE ASSERTION. There is no outbox table yet, so "the event exists"
	// cannot be a query — the event exists nowhere at all, which is the
	// whole problem.
	//
	// Be precise about what is lost. The payment itself is NOT stuck: the
	// worker finds work by polling the transfers table, not by reading
	// events, so it will still send this to the rail. What is lost is every
	// consumer that learns about transfers ONLY from events — customer
	// notification and reconciliation. They will never hear of this one.
	t.Errorf(
		"a transfer exists that no event consumer will ever hear of:\n"+
			"  transfer            committed, status %q, provider_ref %v\n"+
			"  source balance      %d (money has left the account)\n"+
			"  event published     NO — the publish failed after the commit\n"+
			"  recorded anywhere   NO — nothing records that an event is owed,\n"+
			"                      so nothing will ever retry the publish\n"+
			"\n"+
			"  The worker still sends the payment (it polls the table). But the\n"+
			"  customer is never notified and reconciliation never sees it.\n"+
			"\n"+
			"  The commit and the publish are two systems. Reversing the order\n"+
			"  produces an event for a transfer that does not exist instead.",
		status, providerRef, sourceBalance)
}

// The other half of "no ordering is safe": publish first, then commit.
//
// The damage is different and arguably worse. A consumer receives an event
// for a transfer id that does not exist in the database — so it either
// tells a customer about a payment that never happened, or waits forever,
// because it cannot tell "not committed yet" from "never existed".
func TestPublishBeforeCommitLosesTheTransfer(t *testing.T) {
	ctx := context.Background()
	resetDB(t)

	// A publisher that succeeds. The failure is the commit that follows.
	published := make([]TransferEvent, 0, 1)
	publisher := &recordingPublisher{events: &published}

	transferID := newID("tr")
	if err := publisher.Publish(ctx, TransferEvent{
		TransferID: transferID,
		Amount:     50000,
		Currency:   "USD",
	}); err != nil {
		t.Fatalf("publish: %v", err)
	}

	// ...and the process dies here, before the transaction commits. Nothing
	// is inserted.

	var exists bool
	if err := testPool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM transfers WHERE id = $1)`, transferID).Scan(&exists); err != nil {
		t.Fatalf("check transfer: %v", err)
	}

	if len(published) != 1 {
		t.Fatalf("published %d events, want 1", len(published))
	}

	t.Errorf(
		"an event exists for a transfer that does not:\n"+
			"  event published     YES, transfer_id %s\n"+
			"  transfer in the db  %v\n"+
			"\n"+
			"  A consumer receiving this cannot distinguish 'not committed yet'\n"+
			"  from 'never existed', so it either waits forever or tells a\n"+
			"  customer about a payment that never happened.",
		transferID, exists)
}

type recordingPublisher struct {
	events *[]TransferEvent
}

func (p *recordingPublisher) Publish(ctx context.Context, event TransferEvent) error {
	*p.events = append(*p.events, event)
	return nil
}
