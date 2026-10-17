package cardnetwork

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Shailu-s/payments-platform/internal/webhooks"
)

var (
	ErrIDReused       = errors.New("message id already used by this network")
	ErrInvalidMessage = errors.New("invalid message")
)

// authAttempts is the first attempt plus the one retry the contract allows before
// stand-in.
const authAttempts = 2

// deliveryBackoff spaces reversal and advice attempts; one attempt more than its
// length is made before giving up.
var deliveryBackoff = []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second}

type Config struct {
	IssuerURL    string
	Secret       []byte
	AuthDeadline time.Duration
	StandInLimit int64
	// Clock defaults to the wall clock.
	Clock Clock
	// Client defaults to a client without its own timeout; AuthDeadline bounds each attempt.
	Client *http.Client
}

type Network struct {
	issuerURL    string
	secret       []byte
	deadline     time.Duration
	standInLimit int64
	clock        Clock
	client       *http.Client

	mu       sync.Mutex
	journal  []Entry
	ids      map[string]struct{}
	auths    map[string]*authRecord
	reversed map[string]int64
}

func New(cfg Config) (*Network, error) {
	switch {
	case cfg.IssuerURL == "":
		return nil, errors.New("issuer URL is required")
	case len(cfg.Secret) < webhooks.MinSecretBytes:
		return nil, fmt.Errorf("network secret must contain at least %d bytes", webhooks.MinSecretBytes)
	case cfg.AuthDeadline <= 0:
		return nil, errors.New("authorization deadline must be positive")
	case cfg.StandInLimit < 0:
		return nil, errors.New("stand-in limit must not be negative")
	}
	n := &Network{
		issuerURL:    strings.TrimRight(cfg.IssuerURL, "/"),
		secret:       append([]byte(nil), cfg.Secret...),
		deadline:     cfg.AuthDeadline,
		standInLimit: cfg.StandInLimit,
		clock:        cfg.Clock,
		client:       cfg.Client,
	}
	if n.clock == nil {
		n.clock = wallClock{}
	}
	if n.client == nil {
		n.client = &http.Client{}
	}
	n.Reset()
	return n, nil
}

// Authorize asks the issuer to approve a, retrying once and standing in when it does
// not answer in time. SentAt is set from the network clock.
func (n *Network) Authorize(ctx context.Context, a Authorization) (AuthResult, error) {
	if err := validateAuthorization(a); err != nil {
		return AuthResult{}, err
	}
	if err := n.reserve(a.AuthID); err != nil {
		return AuthResult{}, err
	}
	a.SentAt = n.clock.Now()
	return n.authorize(ctx, a, authPlan{})
}

// Reverse releases r.Amount of an authorization, known to the network or not, and
// reports whether the issuer acknowledged it. SentAt is set from the network clock.
func (n *Network) Reverse(ctx context.Context, r Reversal) (bool, error) {
	if r.ReversalID == "" || r.AuthID == "" || r.Amount <= 0 {
		return false, fmt.Errorf("%w: reversal needs reversal_id, auth_id and a positive amount", ErrInvalidMessage)
	}
	if err := n.reserve(r.ReversalID); err != nil {
		return false, err
	}
	r.SentAt = n.clock.Now()
	return n.reverse(ctx, r)
}

func validateAuthorization(a Authorization) error {
	switch {
	case a.AuthID == "" || a.CardID == "":
		return fmt.Errorf("%w: auth_id and card_id are required", ErrInvalidMessage)
	case a.Amount <= 0:
		return fmt.Errorf("%w: amount must be positive minor units", ErrInvalidMessage)
	case a.Currency != Currency:
		return fmt.Errorf("%w: currency must be %s", ErrInvalidMessage, Currency)
	case a.Kind == KindInitial && a.OriginalAuthID != "":
		return fmt.Errorf("%w: an initial authorization has no original_auth_id", ErrInvalidMessage)
	case a.Kind == KindIncremental && a.OriginalAuthID == "":
		return fmt.Errorf("%w: an incremental authorization needs original_auth_id", ErrInvalidMessage)
	case a.Kind != KindInitial && a.Kind != KindIncremental:
		return fmt.Errorf("%w: kind must be initial or incremental", ErrInvalidMessage)
	}
	return nil
}

// authPlan scripts transport faults the issuer cannot cause itself.
type authPlan struct {
	// lostAnswers discards the issuer's first answers as if they arrived after the
	// deadline.
	lostAnswers int
	// duplicate delivers the message once more after the network has decided.
	duplicate bool
}

func (n *Network) authorize(ctx context.Context, a Authorization, plan authPlan) (AuthResult, error) {
	body, err := json.Marshal(a)
	if err != nil {
		return AuthResult{}, err
	}
	n.mu.Lock()
	n.auths[a.AuthID] = &authRecord{auth: a}
	n.mu.Unlock()

	var result AuthResult
	decided := false
	attempt := 0
	for attempt < authAttempts && !decided {
		attempt++
		status, raw, err := n.post(ctx, PathAuthorizations, body)
		if ctx.Err() != nil {
			return AuthResult{}, ctx.Err()
		}
		answer, outcome := classifyAnswer(a, status, raw, err)
		lost := outcome == OutcomeAnswered && attempt <= plan.lostAnswers
		if lost {
			outcome = OutcomeAnswerLost
		}
		n.record(Entry{Message: MessageAuthorization, ID: a.AuthID, Attempt: attempt,
			Outcome: outcome, Status: status, Body: body, Answer: rawAnswer(raw)})
		if lost {
			// The issuer believes it answered; the network waited out the deadline.
			if err := n.clock.Sleep(ctx, n.deadline); err != nil {
				return AuthResult{}, err
			}
			continue
		}
		if outcome == OutcomeAnswered {
			decided = true
			result = AuthResult{AuthID: a.AuthID, Decision: answer.Decision,
				ApprovedAmount: answer.ApprovedAmount, DeclineReason: answer.DeclineReason}
		}
	}
	if !decided {
		result = n.standIn(a)
	}
	result.Attempts = attempt

	n.mu.Lock()
	n.auths[a.AuthID].decided = true
	n.auths[a.AuthID].result = result
	n.mu.Unlock()

	if plan.duplicate {
		status, raw, _ := n.post(ctx, PathAuthorizations, body)
		if ctx.Err() != nil {
			return AuthResult{}, ctx.Err()
		}
		result.Attempts++
		n.record(Entry{Message: MessageAuthorization, ID: a.AuthID, Attempt: result.Attempts,
			Outcome: OutcomeDuplicate, Status: status, Body: body, Answer: rawAnswer(raw)})
	}

	if result.StandIn && result.Decision == DecisionApproved {
		acked, err := n.advise(ctx, a)
		if err != nil {
			return AuthResult{}, err
		}
		result.Advice = AdviceGivenUp
		if acked {
			result.Advice = AdviceAcknowledged
		}
	}
	return result, nil
}

// classifyAnswer accepts only a well-formed 200 for this auth_id; everything else
// is no answer.
func classifyAnswer(a Authorization, status int, raw []byte, err error) (AuthorizationAnswer, string) {
	if err != nil {
		return AuthorizationAnswer{}, transportOutcome(err)
	}
	if status != http.StatusOK {
		return AuthorizationAnswer{}, "status_" + strconv.Itoa(status)
	}
	var answer AuthorizationAnswer
	if json.Unmarshal(raw, &answer) != nil || answer.AuthID != a.AuthID {
		return AuthorizationAnswer{}, OutcomeInvalidAnswer
	}
	switch {
	case answer.Decision == DecisionApproved && answer.ApprovedAmount > 0 && answer.ApprovedAmount <= a.Amount:
	case answer.Decision == DecisionDeclined:
		answer.ApprovedAmount = 0
	default:
		return AuthorizationAnswer{}, OutcomeInvalidAnswer
	}
	return answer, OutcomeAnswered
}

func transportOutcome(err error) string {
	if errors.Is(err, context.DeadlineExceeded) {
		return OutcomeTimeout
	}
	return OutcomeTransportError
}

func (n *Network) standIn(a Authorization) AuthResult {
	result := AuthResult{AuthID: a.AuthID, StandIn: true, Decision: DecisionApproved, ApprovedAmount: a.Amount}
	if a.Amount > n.standInLimit {
		result.Decision, result.ApprovedAmount, result.DeclineReason = DecisionDeclined, 0, DeclineStandInLimit
	}
	n.record(Entry{Message: MessageAuthorization, ID: a.AuthID, Outcome: OutcomeStandIn})
	return result
}

// adviceID is derived from the auth_id: there is at most one advice per authorization.
func adviceID(authID string) string {
	return "adv_" + strings.TrimPrefix(authID, "auth_")
}

func (n *Network) advise(ctx context.Context, a Authorization) (bool, error) {
	advice := Advice{
		AdviceID: adviceID(a.AuthID),
		AuthID:   a.AuthID,
		CardID:   a.CardID,
		Amount:   a.Amount,
		Merchant: a.Merchant,
		Decision: DecisionApproved,
		Reason:   ReasonStandIn,
		SentAt:   n.clock.Now(),
	}
	body, err := json.Marshal(advice)
	if err != nil {
		return false, err
	}
	return n.deliver(ctx, MessageAdvice, advice.AdviceID, PathAdvices, body)
}

func (n *Network) reverse(ctx context.Context, r Reversal) (bool, error) {
	body, err := json.Marshal(r)
	if err != nil {
		return false, err
	}
	// The network's record changes when it sends the reversal, acknowledged or not.
	n.mu.Lock()
	n.reversed[r.AuthID] += r.Amount
	n.mu.Unlock()
	return n.deliver(ctx, MessageReversal, r.ReversalID, PathReversals, body)
}

// deliver retries a reversal or advice on any non-2xx answer with bounded backoff.
func (n *Network) deliver(ctx context.Context, message, id, path string, body []byte) (bool, error) {
	for attempt := 1; ; attempt++ {
		status, raw, err := n.post(ctx, path, body)
		if ctx.Err() != nil {
			return false, ctx.Err()
		}
		outcome := OutcomeAcknowledged
		switch {
		case err != nil:
			outcome = transportOutcome(err)
		case status < 200 || status > 299:
			outcome = "status_" + strconv.Itoa(status)
		}
		n.record(Entry{Message: message, ID: id, Attempt: attempt, Outcome: outcome,
			Status: status, Body: body, Answer: rawAnswer(raw)})
		if outcome == OutcomeAcknowledged {
			return true, nil
		}
		if attempt > len(deliveryBackoff) {
			n.record(Entry{Message: message, ID: id, Outcome: OutcomeGivenUp})
			return false, nil
		}
		if err := n.clock.Sleep(ctx, deliveryBackoff[attempt-1]); err != nil {
			return false, err
		}
	}
}

// post makes one signed attempt bounded by the authorization deadline.
func (n *Network) post(ctx context.Context, path string, body []byte) (int, []byte, error) {
	ctx, cancel := context.WithTimeout(ctx, n.deadline)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, n.issuerURL+path, bytes.NewReader(body))
	if err != nil {
		return 0, nil, err
	}
	timestamp := strconv.FormatInt(n.clock.Now().Unix(), 10)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(TimestampHeader, timestamp)
	req.Header.Set(SignatureHeader, webhooks.Sign(n.secret, timestamp, body))

	resp, err := n.client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	if err != nil {
		return resp.StatusCode, nil, err
	}
	return resp.StatusCode, raw, nil
}
