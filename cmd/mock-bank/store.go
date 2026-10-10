package main

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"sync"
	"time"
)

// Payment is MockBank's independent, in-memory payment record.
type Payment struct {
	ProviderRef     string     `json:"provider_ref"`
	ClientReference string     `json:"client_reference"`
	Status          string     `json:"status"`
	Amount          int64      `json:"amount"`
	Currency        string     `json:"currency"`
	Source          string     `json:"source"`
	Destination     string     `json:"destination"`
	ReceivedAt      time.Time  `json:"received_at"`
	SettledAt       *time.Time `json:"settled_at"`
	FailureReason   *string    `json:"failure_reason"`
}

const (
	StatusProcessing = "processing"
	StatusSettled    = "settled"
	StatusFailed     = "failed"
)

var (
	// ErrReferenceConflict is the same client_reference submitted with
	// different terms. A caller bug, and per the contract a 409.
	ErrReferenceConflict = errors.New("client_reference reused with different terms")
	ErrNotFound          = errors.New("payment not found")
)

type Store struct {
	mu sync.RWMutex
	// Client-reference lookup resolves submissions whose acknowledgement was lost.
	byProviderRef     map[string]*Payment
	byClientReference map[string]*Payment
}

func NewStore() *Store {
	return &Store{
		byProviderRef:     map[string]*Payment{},
		byClientReference: map[string]*Payment{},
	}
}

// Submit records an instruction or replays one with the same client reference.
// Deduplication lasts only as long as this in-memory store survives.
func (s *Store) Submit(p Payment) (stored *Payment, duplicate bool, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if existing, ok := s.byClientReference[p.ClientReference]; ok {
		// Same reference, different instruction: the caller has a bug, and
		// answering with the original payment would hide it.
		if existing.Amount != p.Amount ||
			existing.Currency != p.Currency ||
			existing.Source != p.Source ||
			existing.Destination != p.Destination {
			return nil, false, ErrReferenceConflict
		}
		copied := *existing
		return &copied, true, nil
	}

	p.ProviderRef = newProviderRef()
	p.Status = StatusProcessing
	p.ReceivedAt = time.Now().UTC()

	s.byProviderRef[p.ProviderRef] = &p
	s.byClientReference[p.ClientReference] = &p
	copied := p
	return &copied, false, nil
}

func (s *Store) ByProviderRef(ref string) (*Payment, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	p, ok := s.byProviderRef[ref]
	if !ok {
		return nil, ErrNotFound
	}
	copied := *p
	return &copied, nil
}

func (s *Store) ByClientReference(ref string) (*Payment, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	p, ok := s.byClientReference[ref]
	if !ok {
		return nil, ErrNotFound
	}
	copied := *p
	return &copied, nil
}

// Settle returns the stored payment and whether it transitioned; terminal states stay unchanged.
func (s *Store) Settle(ref, status string, failureReason *string) (*Payment, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	p, ok := s.byProviderRef[ref]
	if !ok {
		return nil, false, ErrNotFound
	}
	if p.Status != StatusProcessing {
		copied := *p
		return &copied, false, nil
	}

	p.Status = status
	if status == StatusSettled {
		now := time.Now().UTC()
		p.SettledAt = &now
	}
	p.FailureReason = failureReason

	copied := *p
	return &copied, true, nil
}

func (s *Store) All() []Payment {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make([]Payment, 0, len(s.byProviderRef))
	for _, p := range s.byProviderRef {
		out = append(out, *p)
	}
	return out
}

func (s *Store) Snapshot() (time.Time, []Payment) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	capturedAt := time.Now().UTC()
	out := make([]Payment, 0, len(s.byProviderRef))
	for _, p := range s.byProviderRef {
		out = append(out, *p)
	}
	return capturedAt, out
}

func (s *Store) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.byProviderRef = map[string]*Payment{}
	s.byClientReference = map[string]*Payment{}
}

func newProviderRef() string {
	var b [6]byte
	_, _ = rand.Read(b[:])
	return "mb_" + hex.EncodeToString(b[:])
}
