package cardnetwork

import (
	"bytes"
	"encoding/csv"
	"errors"
	"slices"
	"strconv"
	"strings"
	"time"
)

const DateLayout = "2006-01-02"

var ErrInvalidDate = errors.New("date must be YYYY-MM-DD")

var clearingHeader = []string{"clearing_id", "auth_id", "card_id", "kind", "amount", "currency", "merchant_id", "cleared_at"}

type ClearingLine struct {
	ClearingID string
	AuthID     string
	CardID     string
	Kind       string
	Amount     int64
	Currency   string
	MerchantID string
	ClearedAt  time.Time
}

// ClearingFile builds the clearing CSV for one UTC date from the journal as it
// stands. The same journal always yields the same bytes.
func (n *Network) ClearingFile(date string) ([]byte, error) {
	day, err := time.Parse(DateLayout, date)
	if err != nil {
		return nil, ErrInvalidDate
	}
	lines := n.ClearingLines(day)

	var buf bytes.Buffer
	w := csv.NewWriter(&buf)
	_ = w.Write(clearingHeader)
	for _, l := range lines {
		_ = w.Write([]string{l.ClearingID, l.AuthID, l.CardID, l.Kind, strconv.FormatInt(l.Amount, 10),
			l.Currency, l.MerchantID, l.ClearedAt.UTC().Format(time.RFC3339)})
	}
	w.Flush()
	if err := w.Error(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// ClearingLines returns the lines that clear on day's UTC date, ordered by
// cleared_at then clearing_id.
func (n *Network) ClearingLines(day time.Time) []ClearingLine {
	date := day.UTC().Format(DateLayout)
	n.mu.Lock()
	var lines []ClearingLine
	for _, p := range n.clearing {
		if p.ClearedAt.UTC().Format(DateLayout) != date {
			continue
		}
		if line, ok := n.resolveLocked(p); ok {
			lines = append(lines, line)
		}
	}
	n.mu.Unlock()

	slices.SortFunc(lines, func(a, b ClearingLine) int {
		if c := a.ClearedAt.Compare(b.ClearedAt); c != 0 {
			return c
		}
		return strings.Compare(a.ClearingID, b.ClearingID)
	})
	return lines
}

// resolveLocked turns a scripted line into a real one, or drops it: lines tied to
// an authorization clear only if the network approved it (itself or the issuer)
// and something is left after reversals. Caller holds n.mu.
func (n *Network) resolveLocked(p clearingPlan) (ClearingLine, bool) {
	line := ClearingLine{
		ClearingID: p.ClearingID,
		AuthID:     p.AuthID,
		CardID:     p.CardID,
		Kind:       p.Kind,
		Amount:     p.Amount,
		Currency:   Currency,
		MerchantID: p.MerchantID,
		ClearedAt:  p.ClearedAt,
	}
	if p.AuthID == "" {
		return line, true
	}
	net := n.netApprovedLocked(p.AuthID)
	for id, rec := range n.auths {
		if rec.auth.OriginalAuthID == p.AuthID {
			net += n.netApprovedLocked(id)
		}
	}
	line.Amount = net * p.Num / p.Den
	return line, line.Amount > 0
}

func (n *Network) netApprovedLocked(authID string) int64 {
	rec, ok := n.auths[authID]
	if !ok || !rec.decided || rec.result.Decision != DecisionApproved {
		return 0
	}
	return max(rec.result.ApprovedAmount-n.reversed[authID], 0)
}

// clearingDates lists, in order, the UTC dates on which the given lines clear.
func (n *Network) clearingDates(plans []clearingPlan) []string {
	n.mu.Lock()
	defer n.mu.Unlock()
	var dates []string
	for _, p := range plans {
		if _, ok := n.resolveLocked(p); !ok {
			continue
		}
		if d := p.ClearedAt.UTC().Format(DateLayout); !slices.Contains(dates, d) {
			dates = append(dates, d)
		}
	}
	slices.Sort(dates)
	return dates
}
