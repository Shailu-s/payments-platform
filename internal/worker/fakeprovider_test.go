package worker

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/Shailu-s/payments-platform/internal/provider"
)

// fakeProvider injects submission and lookup outcomes deterministically.
type fakeProvider struct {
	mu sync.Mutex

	submit func(req provider.SubmitRequest) (provider.Payment, error)
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

func accepts() *fakeProvider { return &fakeProvider{} }

func rejects(reason string) *fakeProvider {
	return &fakeProvider{
		submit: func(provider.SubmitRequest) (provider.Payment, error) {
			return provider.Payment{}, fmt.Errorf("%w: %s", provider.ErrRejected, reason)
		},
	}
}

// Simulate an ambiguous submission, not proof the provider did nothing.
func timesOut() *fakeProvider {
	return &fakeProvider{
		submit: func(provider.SubmitRequest) (provider.Payment, error) {
			return provider.Payment{}, fmt.Errorf("%w: context deadline exceeded", provider.ErrUnknown)
		},
	}
}

// Simulate the contract's known-unprocessed refusal, unlike an ambiguous timeout.
func unavailable() *fakeProvider {
	return &fakeProvider{
		submit: func(provider.SubmitRequest) (provider.Payment, error) {
			return provider.Payment{}, fmt.Errorf("%w: status 503", provider.ErrRetryable)
		},
	}
}
