package api

import (
	"context"
	"errors"
	"sync"
	"time"
)

// ErrAccountBusy means the source account already has as many payments in
// flight as it may, or its balance lock could not be taken in time.
var ErrAccountBusy = errors.New("account busy")

const (
	// Payments from one account serialise on its balance row anyway; more in
	// flight than this only park pool connections that other accounts need.
	maxInFlightPerAccount = 4
	// Long enough to absorb a short burst, short enough to fail fast.
	accountSlotWait = 100 * time.Millisecond
)

// accountGate bounds concurrent transfer creations per source account in this
// process, so one hot account cannot hold every pool connection. Across
// replicas the database lock timeout is the backstop.
type accountGate struct {
	limit int
	wait  time.Duration

	mu    sync.Mutex
	slots map[string]*accountSlots
}

type accountSlots struct {
	tokens chan struct{}
	// Holders plus waiters; the entry is deleted at zero so idle accounts
	// cost nothing.
	refs int
}

func newAccountGate(limit int, wait time.Duration) *accountGate {
	return &accountGate{limit: limit, wait: wait, slots: map[string]*accountSlots{}}
}

// acquire waits up to the gate's wait for a slot. The caller must call release
// exactly once when it no longer uses the database for this account.
func (g *accountGate) acquire(ctx context.Context, accountID string) (release func(), err error) {
	g.mu.Lock()
	slots, ok := g.slots[accountID]
	if !ok {
		slots = &accountSlots{tokens: make(chan struct{}, g.limit)}
		g.slots[accountID] = slots
	}
	slots.refs++
	g.mu.Unlock()

	release = func() {
		<-slots.tokens
		g.unref(accountID, slots)
	}

	// The uncontended path takes no timer.
	select {
	case slots.tokens <- struct{}{}:
		return release, nil
	default:
	}

	timer := time.NewTimer(g.wait)
	defer timer.Stop()
	select {
	case slots.tokens <- struct{}{}:
		return release, nil
	case <-timer.C:
		g.unref(accountID, slots)
		return nil, ErrAccountBusy
	case <-ctx.Done():
		g.unref(accountID, slots)
		return nil, ctx.Err()
	}
}

func (g *accountGate) unref(accountID string, slots *accountSlots) {
	g.mu.Lock()
	defer g.mu.Unlock()
	slots.refs--
	if slots.refs == 0 {
		delete(g.slots, accountID)
	}
}
