// Package cardnetwork simulates the card network described in docs/mock-network-api.md:
// it sends signed authorizations, reversals and advices to an issuer, applies the
// deadline, retry and stand-in rules, and keeps an in-memory journal from which it
// builds clearing files. It holds no platform state.
package cardnetwork

import "time"

const (
	PathAuthorizations = "/network/authorizations"
	PathReversals      = "/network/reversals"
	PathAdvices        = "/network/advices"

	Currency = "USD"

	KindInitial     = "initial"
	KindIncremental = "incremental"

	DecisionApproved = "approved"
	DecisionDeclined = "declined"

	ReasonStandIn        = "stand_in"
	DeclineStandInLimit  = "stand_in_limit"
	AdviceAcknowledged   = "acknowledged"
	AdviceGivenUp        = "given_up"
	MessageAuthorization = "authorization"
	MessageReversal      = "reversal"
	MessageAdvice        = "advice"
)

type Merchant struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	MCC  string `json:"mcc"`
}

type Authorization struct {
	AuthID         string    `json:"auth_id"`
	CardID         string    `json:"card_id"`
	Amount         int64     `json:"amount"`
	Currency       string    `json:"currency"`
	Merchant       Merchant  `json:"merchant"`
	Kind           string    `json:"kind"`
	OriginalAuthID string    `json:"original_auth_id,omitempty"`
	SentAt         time.Time `json:"sent_at"`
}

// AuthorizationAnswer is the issuer's reply to an Authorization.
type AuthorizationAnswer struct {
	AuthID         string `json:"auth_id"`
	Decision       string `json:"decision"`
	ApprovedAmount int64  `json:"approved_amount"`
	DeclineReason  string `json:"decline_reason,omitempty"`
}

type Reversal struct {
	ReversalID string    `json:"reversal_id"`
	AuthID     string    `json:"auth_id"`
	Amount     int64     `json:"amount"`
	SentAt     time.Time `json:"sent_at"`
}

type Advice struct {
	AdviceID string    `json:"advice_id"`
	AuthID   string    `json:"auth_id"`
	CardID   string    `json:"card_id"`
	Amount   int64     `json:"amount"`
	Merchant Merchant  `json:"merchant"`
	Decision string    `json:"decision"`
	Reason   string    `json:"reason"`
	SentAt   time.Time `json:"sent_at"`
}

// AuthResult is the network's decision for one authorization: the issuer's answer,
// or its own stand-in decision when the issuer did not answer in time.
type AuthResult struct {
	AuthID         string `json:"auth_id"`
	Decision       string `json:"decision"`
	ApprovedAmount int64  `json:"approved_amount"`
	DeclineReason  string `json:"decline_reason,omitempty"`
	StandIn        bool   `json:"stand_in"`
	Attempts       int    `json:"attempts"`
	// Advice is set only for stand-in approvals: acknowledged or given_up.
	Advice string `json:"advice,omitempty"`
}
