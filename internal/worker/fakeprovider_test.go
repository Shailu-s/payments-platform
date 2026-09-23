package worker

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/Shailu-s/payments-platform/internal/provider"
)

// fakeProvider is a rail that can be told to misbehave. The three outcomes only
// become testable if the provider can time out and reject on demand, which a
// real sandbox will not do.
type fakeProvider struct {
	mu sync.Mutex

	// submit decides what Submit returns. Set per test.
	submit func(req provider.SubmitRequest) (provider.Payment, error)
	// lookup decides what Get and GetByClientReference return.
	lookup func(ref string) (provider.Payment, error)

	submitted atomic.Int64
	lookups   atomic.Int64

	// received records every client_reference submitted, so a test can prove a
	// transfer was or was not sent twice.
	received []string
}

func (f *fakeProvider) Submit(ctx context.Context, req provider.SubmitRequest) (provider.Payment, error) {
	f.submitted.Add(1)

	f.mu.Lock()
	f.received = append(f.received, req.ClientReference)
	submit := f.submit
	f.mu.Unlock()

	if submit == nil {
		return provider.Payment{
			ProviderRef:     "mb_" + req.ClientReference,
			ClientReference: req.ClientReference,
			Status:          provider.StatusProcessing,
			Amount:          req.Amount,
			Currency:        req.Currency,
		}, nil
	}
	return submit(req)
}

func (f *fakeProvider) Get(ctx context.Context, providerRef string) (provider.Payment, error) {
	f.lookups.Add(1)
	f.mu.Lock()
	lookup := f.lookup
	f.mu.Unlock()

	if lookup == nil {
		return provider.Payment{}, provider.ErrNotFound
	}
	return lookup(providerRef)
}

func (f *fakeProvider) GetByClientReference(ctx context.Context, clientReference string) (provider.Payment, error) {
	return f.Get(ctx, clientReference)
}

func (f *fakeProvider) submissionsFor(clientReference string) int {
	f.mu.Lock()
	defer f.mu.Unlock()

	count := 0
	for _, ref := range f.received {
		if ref == clientReference {
			count++
		}
	}
	return count
}

// accepts returns a provider that accepts everything.
func accepts() *fakeProvider { return &fakeProvider{} }

// rejects returns a provider that declines everything, terminally.
func rejects(reason string) *fakeProvider {
	return &fakeProvider{
		submit: func(provider.SubmitRequest) (provider.Payment, error) {
			return provider.Payment{}, fmt.Errorf("%w: %s", provider.ErrRejected, reason)
		},
	}
}

// timesOut returns a provider whose answer never arrives: the instruction may
// or may not have been received, and nothing can distinguish the two.
func timesOut() *fakeProvider {
	return &fakeProvider{
		submit: func(provider.SubmitRequest) (provider.Payment, error) {
			return provider.Payment{}, fmt.Errorf("%w: context deadline exceeded", provider.ErrUnknown)
		},
	}
}

// unavailable returns a provider that refuses to consider the request at all.
// Nothing happened, so this is safe to retry — which is what distinguishes it
// from a timeout.
func unavailable() *fakeProvider {
	return &fakeProvider{
		submit: func(provider.SubmitRequest) (provider.Payment, error) {
			return provider.Payment{}, fmt.Errorf("%w: status 503", provider.ErrRetryable)
		},
	}
}
