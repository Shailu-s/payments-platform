package main

import (
	"math/rand/v2"
	"time"
)

// Behaviour controls timing, outcomes and duplicate delivery for provider failure tests.
type Behaviour interface {
	// SubmitDelay is how long to take before answering POST /transfers. A
	// delay longer than the caller's deadline is how a timeout is produced.
	SubmitDelay() time.Duration

	// Outcome decides what happens to a payment, and how long after acceptance
	// the decision is reached.
	Outcome(p Payment) (status string, failureReason *string, after time.Duration)

	// WebhookAttempts controls duplicate deliveries, not transport retries.
	WebhookAttempts() int
}

// wellBehaved settles payments without injecting failures.
type wellBehaved struct {
	settleDelay time.Duration
}

func (b wellBehaved) SubmitDelay() time.Duration { return 0 }

func (b wellBehaved) Outcome(Payment) (string, *string, time.Duration) {
	// Jitter enforces the contract's timing range rather than a fixed delay.
	jitter := time.Duration(rand.Int64N(int64(b.settleDelay/2 + 1)))
	return StatusSettled, nil, b.settleDelay + jitter
}

func (b wellBehaved) WebhookAttempts() int { return 1 }
