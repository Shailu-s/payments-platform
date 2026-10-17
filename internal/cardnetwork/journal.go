package cardnetwork

import (
	"encoding/json"
	"time"
)

// Attempt outcomes recorded in the journal.
const (
	OutcomeAnswered       = "answered"
	OutcomeAnswerLost     = "answer_lost"
	OutcomeDuplicate      = "duplicate"
	OutcomeAcknowledged   = "acknowledged"
	OutcomeTimeout        = "timeout"
	OutcomeTransportError = "transport_error"
	OutcomeInvalidAnswer  = "invalid_answer"
	OutcomeGivenUp        = "given_up"
	OutcomeStandIn        = "stand_in"
)

// Entry is one line of the network's own record: an attempt to deliver a message
// and what came back, or a decision the network took without the issuer.
type Entry struct {
	Seq     int             `json:"seq"`
	At      time.Time       `json:"at"`
	Message string          `json:"message"`
	ID      string          `json:"id"`
	Attempt int             `json:"attempt,omitempty"`
	Outcome string          `json:"outcome"`
	Status  int             `json:"status,omitempty"`
	Body    json.RawMessage `json:"body,omitempty"`
	Answer  json.RawMessage `json:"answer,omitempty"`
}

type authRecord struct {
	auth    Authorization
	decided bool
	result  AuthResult
}

func (n *Network) record(e Entry) {
	n.mu.Lock()
	defer n.mu.Unlock()
	e.Seq = len(n.journal) + 1
	e.At = n.clock.Now()
	n.journal = append(n.journal, e)
}

// Journal returns a copy of every entry recorded so far, in order.
func (n *Network) Journal() []Entry {
	n.mu.Lock()
	defer n.mu.Unlock()
	return append([]Entry{}, n.journal...)
}

// reserve claims message ids for one use each; a second use of any of them fails
// without claiming the rest.
func (n *Network) reserve(ids ...string) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	for _, id := range ids {
		if _, used := n.ids[id]; used {
			return ErrIDReused
		}
	}
	for _, id := range ids {
		n.ids[id] = struct{}{}
	}
	return nil
}

// Reset forgets the journal, every reserved id and every clearing line.
func (n *Network) Reset() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.journal = nil
	n.ids = map[string]struct{}{}
	n.auths = map[string]*authRecord{}
	n.reversed = map[string]int64{}
	n.clearing = nil
}

// rawAnswer keeps the issuer's reply readable in the journal even when it is not JSON.
func rawAnswer(b []byte) json.RawMessage {
	if len(b) == 0 {
		return nil
	}
	if json.Valid(b) {
		return append(json.RawMessage(nil), b...)
	}
	quoted, _ := json.Marshal(string(b))
	return quoted
}
