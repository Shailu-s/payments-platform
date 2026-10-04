// Package provider implements the rail contract in docs/mockbank-api.md.
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

var (
	// ErrRejected is a terminal refusal; resubmission will not help.
	ErrRejected = errors.New("provider rejected the payment")

	// ErrUnknown means money may have moved; resolve before retrying or reversing.
	ErrUnknown = errors.New("provider outcome unknown")

	// ErrRetryable means the contract permits retry after a transient refusal.
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

// DefaultTimeout bounds the client's wait, not the provider's processing.
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

// Submit sends an instruction, distinguishing rejection, retryable refusal,
// and unknown outcomes so callers do not retry ambiguous payments.
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
		// Transport failure does not prove the provider did not process it.
		return Payment{}, fmt.Errorf("%w: %v", ErrUnknown, err)
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return Payment{}, fmt.Errorf("%w: reading response: %v", ErrUnknown, err)
	}

	switch {
	case resp.StatusCode == http.StatusAccepted:
		var payment Payment
		if err := json.Unmarshal(raw, &payment); err != nil {
			// Acceptance may be real despite an unreadable response.
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
		return Payment{}, fmt.Errorf("%w: %s", ErrRejected, reason(raw))

	case resp.StatusCode >= 500:
		// MockBank's contract defines 5xx as unprocessed, unlike generic HTTP 5xx.
		return Payment{}, fmt.Errorf("%w: status %d: %s", ErrRetryable, resp.StatusCode, reason(raw))

	default:
		// Undocumented statuses cannot establish whether money moved.
		return Payment{}, fmt.Errorf("%w: unexpected status %d: %s",
			ErrUnknown, resp.StatusCode, reason(raw))
	}
}

// Get looks up a payment by the rail's own reference.
func (c *Client) Get(ctx context.Context, providerRef string) (Payment, error) {
	return c.lookup(ctx, c.baseURL+"/transfers/"+providerRef)
}

// GetByClientReference resolves submissions that returned no provider reference.
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
		// Unknown reference, not proof that no payment occurred.
		return Payment{}, ErrNotFound

	default:
		return Payment{}, fmt.Errorf("%w: lookup status %d: %s",
			ErrRetryable, resp.StatusCode, reason(raw))
	}
}

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
