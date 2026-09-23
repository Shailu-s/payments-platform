// Package provider talks to the payment rail.
//
// Written against docs/mockbank-api.md rather than against MockBank's source.
// That ordering is the one mitigation available for having written both ends:
// it forces this client to handle every response the specification permits,
// not only the ones the current implementation happens to produce.
package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"
)

// The three outcomes a submission can have, and they are genuinely three rather
// than two. The third is the one that matters.
var (
	// ErrRejected is a terminal refusal: the rail considered the payment and
	// declined it. Not a failure of the request, and never retried.
	ErrRejected = errors.New("provider rejected the payment")

	// ErrUnknown means the outcome cannot be determined — a timeout, a
	// transport failure, an unreadable response. The money may or may not have
	// moved, and the caller must NOT guess. Retrying risks paying twice;
	// failing risks losing a payment that happened.
	ErrUnknown = errors.New("provider outcome unknown")

	// ErrRetryable is a transient refusal to consider the payment at all: the
	// rail was unavailable or rate limited. Nothing happened, so retrying is
	// safe — which is what distinguishes it from ErrUnknown.
	ErrRetryable = errors.New("provider temporarily unavailable")

	// ErrConflict is our bug: a client_reference reused with different terms.
	ErrConflict = errors.New("client reference reused with different terms")

	ErrNotFound = errors.New("provider has no record of this payment")
)

const (
	StatusProcessing = "processing"
	StatusSettled    = "settled"
	StatusFailed     = "failed"
)

// Payment is the rail's view of an instruction.
type Payment struct {
	ProviderRef     string     `json:"provider_ref"`
	ClientReference string     `json:"client_reference"`
	Status          string     `json:"status"`
	Amount          int64      `json:"amount"`
	Currency        string     `json:"currency"`
	ReceivedAt      time.Time  `json:"received_at"`
	SettledAt       *time.Time `json:"settled_at"`
	FailureReason   *string    `json:"failure_reason"`
}

type SubmitRequest struct {
	ClientReference string `json:"client_reference"`
	Amount          int64  `json:"amount"`
	Currency        string `json:"currency"`
	Source          string `json:"source"`
	Destination     string `json:"destination"`
}

type Client struct {
	baseURL string
	http    *http.Client
}

// DefaultTimeout is the deadline the contract recommends. It bounds how long we
// wait, and nothing more: the rail does not cancel work when we disconnect, so
// a payment accepted just before this deadline is still processed.
const DefaultTimeout = 5 * time.Second

func New(baseURL string, timeout time.Duration) *Client {
	if timeout == 0 {
		timeout = DefaultTimeout
	}
	return &Client{
		baseURL: baseURL,
		http:    &http.Client{Timeout: timeout},
	}
}

type errorBody struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

// Submit sends a payment instruction.
//
// The error it returns is the whole point of this function: the caller must be
// able to tell "declined" from "unavailable" from "I have no idea", because
// those three demand completely different actions.
func (c *Client) Submit(ctx context.Context, req SubmitRequest) (Payment, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return Payment{}, fmt.Errorf("encoding submit: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/transfers", bytes.NewReader(body))
	if err != nil {
		return Payment{}, fmt.Errorf("building submit: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(httpReq)
	if err != nil {
		// A timeout or transport failure. The request may have arrived and
		// been processed — the contract says all three cases look identical
		// from here — so this is unknown, not failed.
		return Payment{}, fmt.Errorf("%w: %v", ErrUnknown, err)
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		// The rail answered and we could not read it. Same ignorance.
		return Payment{}, fmt.Errorf("%w: reading response: %v", ErrUnknown, err)
	}

	switch {
	case resp.StatusCode == http.StatusAccepted:
		var payment Payment
		if err := json.Unmarshal(raw, &payment); err != nil {
			// Accepted, but we cannot read the reference. The money may be
			// moving and we cannot name the payment: unknown, and the caller
			// will have to resolve it by client_reference.
			return Payment{}, fmt.Errorf("%w: unreadable acceptance: %v", ErrUnknown, err)
		}
		if payment.ProviderRef == "" {
			return Payment{}, fmt.Errorf("%w: acceptance carried no provider_ref", ErrUnknown)
		}
		return payment, nil

	case resp.StatusCode == http.StatusUnprocessableEntity:
		return Payment{}, fmt.Errorf("%w: %s", ErrRejected, reason(raw))

	case resp.StatusCode == http.StatusConflict:
		return Payment{}, fmt.Errorf("%w: %s", ErrConflict, reason(raw))

	case resp.StatusCode == http.StatusTooManyRequests:
		return Payment{}, fmt.Errorf("%w: rate limited, retry after %s",
			ErrRetryable, resp.Header.Get("Retry-After"))

	case resp.StatusCode == http.StatusBadRequest:
		// Our request was malformed. Retrying sends the same malformed
		// request, so this is terminal rather than retryable.
		return Payment{}, fmt.Errorf("%w: %s", ErrRejected, reason(raw))

	case resp.StatusCode >= 500:
		// The rail refused to consider it. Nothing happened, so this is safe
		// to retry — which is exactly what distinguishes it from a timeout.
		return Payment{}, fmt.Errorf("%w: status %d: %s", ErrRetryable, resp.StatusCode, reason(raw))

	default:
		// A status the contract does not describe. Assuming it means "nothing
		// happened" would be a guess about money, so it is unknown.
		return Payment{}, fmt.Errorf("%w: unexpected status %d: %s",
			ErrUnknown, resp.StatusCode, reason(raw))
	}
}

// Get looks up a payment by the rail's own reference.
func (c *Client) Get(ctx context.Context, providerRef string) (Payment, error) {
	return c.lookup(ctx, c.baseURL+"/transfers/"+providerRef)
}

// GetByClientReference looks a payment up by our reference.
//
// This is how a submission that timed out before we learned a provider_ref gets
// resolved. Without it that state is unrecoverable: we would hold a transfer
// that may or may not have been paid, with no way to ask.
func (c *Client) GetByClientReference(ctx context.Context, clientReference string) (Payment, error) {
	return c.lookup(ctx, c.baseURL+"/transfers?client_reference="+clientReference)
}

func (c *Client) lookup(ctx context.Context, url string) (Payment, error) {
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return Payment{}, fmt.Errorf("building lookup: %w", err)
	}

	resp, err := c.http.Do(httpReq)
	if err != nil {
		return Payment{}, fmt.Errorf("%w: %v", ErrRetryable, err)
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return Payment{}, fmt.Errorf("%w: reading lookup: %v", ErrRetryable, err)
	}

	switch {
	case resp.StatusCode == http.StatusOK:
		var payment Payment
		if err := json.Unmarshal(raw, &payment); err != nil {
			return Payment{}, fmt.Errorf("%w: unreadable payment: %v", ErrRetryable, err)
		}
		return payment, nil

	case resp.StatusCode == http.StatusNotFound:
		// Per the contract, a 404 does NOT prove the payment never happened —
		// only that this reference is unknown to the rail.
		return Payment{}, ErrNotFound

	default:
		return Payment{}, fmt.Errorf("%w: lookup status %d: %s",
			ErrRetryable, resp.StatusCode, reason(raw))
	}
}

// reason extracts the contract's error message, falling back to the raw body
// for a response that does not follow it.
func reason(raw []byte) string {
	var body errorBody
	if err := json.Unmarshal(raw, &body); err == nil && body.Error.Code != "" {
		return body.Error.Code + ": " + body.Error.Message
	}
	if len(raw) > 200 {
		raw = raw[:200]
	}
	return strconv.Quote(string(raw))
}
