package cardnetwork

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"time"
)

const (
	ScenarioApproveAndClear    = "approve_and_clear"
	ScenarioClearWithTip       = "clear_with_tip"
	ScenarioPartialClear       = "partial_clear"
	ScenarioDuplicateAuth      = "duplicate_auth"
	ScenarioTimeoutThenRetry   = "timeout_then_retry"
	ScenarioStandIn            = "stand_in"
	ScenarioReversalBeforeAuth = "reversal_before_auth"
	ScenarioIncremental        = "incremental"
	ScenarioOfflinePurchase    = "offline_purchase"
	ScenarioLateRefund         = "late_refund"
)

// Scenarios lists every scenario in the order of the contract.
var Scenarios = []string{
	ScenarioApproveAndClear, ScenarioClearWithTip, ScenarioPartialClear, ScenarioDuplicateAuth,
	ScenarioTimeoutThenRetry, ScenarioStandIn, ScenarioReversalBeforeAuth, ScenarioIncremental,
	ScenarioOfflinePurchase, ScenarioLateRefund,
}

var ErrUnknownScenario = errors.New("unknown scenario")

const (
	// seedStream is the second PCG word; any constant works as long as it never changes.
	seedStream = 0x6d6f636b6e6574
	// clearingDelay is how long after its authorization a purchase clears.
	clearingDelay = 24 * time.Hour
	// maxSeed keeps drawn seeds exact in JSON consumers that use float64 numbers.
	maxSeed = 1<<53 - 1
)

var merchants = []Merchant{
	{ID: "mer_coffee", Name: "Corner Coffee", MCC: "5814"},
	{ID: "mer_grocer", Name: "Fresh Grocer", MCC: "5411"},
	{ID: "mer_fuel", Name: "Highway Fuel", MCC: "5542"},
	{ID: "mer_hotel", Name: "Harbour Hotel", MCC: "7011"},
	{ID: "mer_transit", Name: "City Transit", MCC: "4111"},
}

type ScenarioRequest struct {
	Scenario string    `json:"scenario"`
	CardID   string    `json:"card_id"`
	Amount   int64     `json:"amount"`
	Merchant *Merchant `json:"merchant,omitempty"`
	Seed     *uint64   `json:"seed,omitempty"`
}

type ScenarioResult struct {
	Scenario       string       `json:"scenario"`
	Seed           uint64       `json:"seed"`
	Authorizations []AuthResult `json:"authorizations"`
	// ClearingDates are the UTC dates on which this run's lines clear.
	ClearingDates []string `json:"clearing_dates"`
	Journal       []Entry  `json:"journal"`
}

// clearingPlan is a scripted clearing line. Lines tied to an authorization are
// resolved against the journal when a file is built; the scenario only states how
// the final amount relates to what was approved.
type clearingPlan struct {
	ClearingID string
	AuthID     string
	CardID     string
	Kind       string
	MerchantID string
	ClearedAt  time.Time
	// Amount is the line's amount when AuthID is empty.
	Amount int64
	// Num/Den scale the authorization's net approved amount when AuthID is set.
	Num, Den int64
}

const (
	ClearingPurchase = "purchase"
	ClearingRefund   = "refund"
)

func validateScenario(req ScenarioRequest) error {
	switch {
	case !slices.Contains(Scenarios, req.Scenario):
		return fmt.Errorf("%w: %q", ErrUnknownScenario, req.Scenario)
	case req.CardID == "":
		return fmt.Errorf("%w: card_id is required", ErrInvalidMessage)
	case req.Amount <= 0:
		return fmt.Errorf("%w: amount must be positive minor units", ErrInvalidMessage)
	case req.Scenario == ScenarioIncremental && req.Amount < 2:
		return fmt.Errorf("%w: incremental needs an amount of at least 2", ErrInvalidMessage)
	case req.Merchant != nil && (req.Merchant.ID == "" || req.Merchant.Name == "" || req.Merchant.MCC == ""):
		return fmt.Errorf("%w: merchant needs id, name and mcc", ErrInvalidMessage)
	}
	return nil
}

// RunScenario sends one scripted sequence to the issuer and records its clearing
// lines. Every identifier comes from the seed, so the same seed replays the same
// messages; reusing one already in the journal fails with ErrIDReused.
func (n *Network) RunScenario(ctx context.Context, req ScenarioRequest) (ScenarioResult, error) {
	if err := validateScenario(req); err != nil {
		return ScenarioResult{}, err
	}
	var seed uint64
	if req.Seed != nil {
		seed = *req.Seed
	} else {
		n.mu.Lock()
		seed = n.seeds.Uint64() & maxSeed
		n.mu.Unlock()
	}
	r := &run{
		n:   n,
		ctx: ctx,
		rng: rand.New(rand.NewPCG(seed, seedStream)),
		req: req,
	}
	if req.Merchant != nil {
		r.merchant = *req.Merchant
	} else {
		r.merchant = merchants[r.rng.IntN(len(merchants))]
	}
	if err := r.play(); err != nil {
		return ScenarioResult{}, err
	}

	result := ScenarioResult{
		Scenario:       req.Scenario,
		Seed:           seed,
		Authorizations: r.results,
		ClearingDates:  n.clearingDates(r.plans),
	}
	for _, e := range n.Journal() {
		if slices.Contains(r.messageIDs, e.ID) {
			result.Journal = append(result.Journal, e)
		}
	}
	return result, nil
}

type run struct {
	n          *Network
	ctx        context.Context
	rng        *rand.Rand
	req        ScenarioRequest
	merchant   Merchant
	messageIDs []string
	plans      []clearingPlan
	results    []AuthResult
}

func (r *run) id(prefix string) string {
	return fmt.Sprintf("%s_%012x", prefix, r.rng.Uint64()&0xffffffffffff)
}

func (r *run) auth(id string, amount int64) Authorization {
	return Authorization{
		AuthID:   id,
		CardID:   r.req.CardID,
		Amount:   amount,
		Currency: Currency,
		Merchant: r.merchant,
		Kind:     KindInitial,
	}
}

// claim reserves the run's identifiers and records its clearing lines before any
// message is sent, so a refused seed sends nothing.
func (r *run) claim(ids []string, plans []clearingPlan) error {
	all := slices.Clone(ids)
	for _, p := range plans {
		all = append(all, p.ClearingID)
	}
	if err := r.n.reserve(all...); err != nil {
		return err
	}
	for _, id := range ids {
		r.messageIDs = append(r.messageIDs, id)
		if strings.HasPrefix(id, "auth_") {
			r.messageIDs = append(r.messageIDs, adviceID(id))
		}
	}
	r.plans = plans
	r.n.mu.Lock()
	r.n.clearing = append(r.n.clearing, plans...)
	r.n.mu.Unlock()
	return nil
}

func (r *run) purchase(authID string, at time.Time, num, den int64) clearingPlan {
	return clearingPlan{
		ClearingID: r.id("clr"),
		AuthID:     authID,
		CardID:     r.req.CardID,
		Kind:       ClearingPurchase,
		MerchantID: r.merchant.ID,
		ClearedAt:  clearingTime(at),
		Num:        num,
		Den:        den,
	}
}

// clearingTime is truncated to the second the file prints, so the file's order is
// visible in the file itself.
func clearingTime(sent time.Time) time.Time {
	return sent.Add(clearingDelay).Truncate(time.Second)
}

func (r *run) send(a Authorization, plan authPlan) error {
	a.SentAt = r.n.clock.Now()
	res, err := r.n.authorize(r.ctx, a, plan)
	if err != nil {
		return err
	}
	r.results = append(r.results, res)
	return nil
}

// single plays the scenarios made of one authorization and one purchase line.
func (r *run) single(plan authPlan, num, den int64) error {
	authID := r.id("auth")
	now := r.n.clock.Now()
	if err := r.claim([]string{authID}, []clearingPlan{r.purchase(authID, now, num, den)}); err != nil {
		return err
	}
	return r.send(r.auth(authID, r.req.Amount), plan)
}

func (r *run) play() error {
	switch r.req.Scenario {
	case ScenarioApproveAndClear:
		return r.single(authPlan{}, 1, 1)
	case ScenarioClearWithTip:
		return r.single(authPlan{}, 6, 5)
	case ScenarioPartialClear:
		return r.single(authPlan{}, 7, 10)
	case ScenarioDuplicateAuth:
		return r.single(authPlan{duplicate: true}, 1, 1)
	case ScenarioTimeoutThenRetry:
		return r.single(authPlan{lostAnswers: 1}, 1, 1)
	case ScenarioStandIn:
		return r.single(authPlan{lostAnswers: authAttempts}, 1, 1)
	case ScenarioReversalBeforeAuth:
		return r.reversalBeforeAuth()
	case ScenarioIncremental:
		return r.incremental()
	case ScenarioOfflinePurchase:
		return r.offlinePurchase()
	case ScenarioLateRefund:
		return r.lateRefund()
	}
	return fmt.Errorf("%w: %q", ErrUnknownScenario, r.req.Scenario)
}

func (r *run) reversalBeforeAuth() error {
	authID, reversalID := r.id("auth"), r.id("rev")
	now := r.n.clock.Now()
	if err := r.claim([]string{authID, reversalID}, []clearingPlan{r.purchase(authID, now, 1, 1)}); err != nil {
		return err
	}
	reversal := Reversal{ReversalID: reversalID, AuthID: authID, Amount: r.req.Amount, SentAt: now}
	if _, err := r.n.reverse(r.ctx, reversal); err != nil {
		return err
	}
	return r.send(r.auth(authID, r.req.Amount), authPlan{})
}

func (r *run) incremental() error {
	initialID, incrementID := r.id("auth"), r.id("auth")
	now := r.n.clock.Now()
	if err := r.claim([]string{initialID, incrementID}, []clearingPlan{r.purchase(initialID, now, 1, 1)}); err != nil {
		return err
	}
	if err := r.send(r.auth(initialID, r.req.Amount), authPlan{}); err != nil {
		return err
	}
	increment := r.auth(incrementID, r.req.Amount/2)
	increment.Kind, increment.OriginalAuthID = KindIncremental, initialID
	return r.send(increment, authPlan{})
}

func (r *run) offlinePurchase() error {
	line := clearingPlan{
		ClearingID: r.id("clr"),
		CardID:     r.req.CardID,
		Kind:       ClearingPurchase,
		MerchantID: r.merchant.ID,
		ClearedAt:  clearingTime(r.n.clock.Now()),
		Amount:     r.req.Amount,
	}
	return r.claim(nil, []clearingPlan{line})
}

func (r *run) lateRefund() error {
	authID := r.id("auth")
	now := r.n.clock.Now()
	purchase := r.purchase(authID, now, 1, 1)
	refund := r.purchase(authID, now, 1, 1)
	refund.Kind = ClearingRefund
	refund.ClearedAt = purchase.ClearedAt.AddDate(0, 0, 7+r.rng.IntN(24))
	if err := r.claim([]string{authID}, []clearingPlan{purchase, refund}); err != nil {
		return err
	}
	return r.send(r.auth(authID, r.req.Amount), authPlan{})
}
