package main

import (
	"math/rand/v2"
	"time"
)

// Behaviour is the single place where MockBank decides how to act. Every
// handler asks it rather than deciding for itself.
//
// Chaos scenarios arrive after phase 7 — forced timeouts, duplicate webhooks,
// settling before acknowledging, wrong amounts. The seam exists now so that
// work is a new implementation of this interface rather than a rewrite of the
// handlers. Today there is one implementation and it behaves well.
type Behaviour interface {
	// SubmitDelay is how long to take before answering POST /transfers. A
	// delay longer than the caller's deadline is how a timeout is produced.
	SubmitDelay() time.Duration

	// Outcome decides what happens to a payment, and how long after acceptance
	// the decision is reached.
	Outcome(p Payment) (status string, failureReason *string, after time.Duration)

	// WebhookAttempts is how many times to deliver one event. More than one
	// exercises the receiver's idempotency, which is guarantee 4.
	WebhookAttempts() int
}

// wellBehaved answers quickly, settles everything, and delivers each event
// once. The baseline against which misbehaviour is defined later.
type wellBehaved struct {
	settleDelay time.Duration
}

func (b wellBehaved) SubmitDelay() time.Duration { return 0 }

func (b wellBehaved) Outcome(Payment) (string, *string, time.Duration) {
	// A little jitter, so tests cannot accidentally depend on a fixed delay
	// and callers cannot assume settlement timing. The contract says timing is
	// a range, not a value.
	jitter := time.Duration(rand.Int64N(int64(b.settleDelay/2 + 1)))
	return StatusSettled, nil, b.settleDelay + jitter
}

func (b wellBehaved) WebhookAttempts() int { return 1 }
