// Package cardrecon reconciles a settlement date three ways: our records
// (authorizations, holds, posted clearing lines), the network's clearing file
// as those records reflect it, and what we paid against the network's
// settlement statement. It detects and reports; it never moves money.
package cardrecon

import (
	"slices"
	"strings"
	"time"

	"github.com/Shailu-s/payments-platform/internal/clearing"
)

const (
	Matched              = "MATCHED"
	ClearedNotAuthorized = "CLEARED_NOT_AUTHORIZED"
	AuthorizedNotCleared = "AUTHORIZED_NOT_CLEARED"
	AmountMismatch       = "AMOUNT_MISMATCH"
	DuplicateClearing    = "DUPLICATE_CLEARING"
	LateClearing         = "LATE_CLEARING"
	RefundException      = "REFUND_EXCEPTION"
	FundingShortfall     = "FUNDING_SHORTFALL"
	HoldAfterClearing    = "HOLD_AFTER_CLEARING"
	SettlementMismatch   = "SETTLEMENT_MISMATCH"
)

// Classes lists every class in report order.
var Classes = []string{
	Matched, ClearedNotAuthorized, AuthorizedNotCleared, AmountMismatch, DuplicateClearing,
	LateClearing, RefundException, FundingShortfall, HoldAfterClearing, SettlementMismatch,
}

// flagClass maps each clearing flag to the class it is reported under.
var flagClass = map[string]string{
	clearing.FlagUnauthorizedClearing:  ClearedNotAuthorized,
	clearing.FlagUnknownCard:           ClearedNotAuthorized,
	clearing.FlagOverTolerance:         AmountMismatch,
	clearing.FlagDuplicateClearing:     DuplicateClearing,
	clearing.FlagClearedAfterExpiry:    LateClearing,
	clearing.FlagClearedAfterReversal:  LateClearing,
	clearing.FlagNothingHeld:           LateClearing,
	clearing.FlagRefundExceedsPurchase: RefundException,
	clearing.FlagUnmatchedRefund:       RefundException,
	clearing.FlagCardMismatch:          RefundException,
	clearing.FlagShortfall:             FundingShortfall,
}

// Settlement mismatch reasons.
const (
	ReasonNoSettlement       = "no_settlement_recorded"
	ReasonStatementDiffers   = "statement_differs_from_settlement"
	ReasonRecordsDiffer      = "records_differ_from_settlement"
	ReasonRecordsVsStatement = "records_differ_from_statement"
)

// Record is a clearing record of the settlement date, as stored.
type Record struct {
	ClearingID string    `json:"clearing_id"`
	AuthID     string    `json:"auth_id,omitempty"`
	CardID     string    `json:"card_id"`
	Kind       string    `json:"kind"`
	Amount     int64     `json:"amount"`
	Status     string    `json:"status"`
	Shortfall  int64     `json:"shortfall"`
	Credited   int64     `json:"credited"`
	Flag       string    `json:"flag,omitempty"`
	ClearedAt  time.Time `json:"cleared_at"`
	CreatedAt  time.Time `json:"created_at"`
}

// Authorization is an authorization in the run's scope, as stored.
type Authorization struct {
	AuthID         string     `json:"auth_id"`
	CardID         string     `json:"card_id"`
	Kind           string     `json:"kind"`
	OriginalAuthID string     `json:"original_auth_id,omitempty"`
	Decision       string     `json:"decision"`
	ApprovedAmount int64      `json:"approved_amount"`
	HeldAmount     int64      `json:"held_amount"`
	ReleasedAmount int64      `json:"released_amount"`
	ClearedAmount  int64      `json:"cleared_amount"`
	RefundedAmount int64      `json:"refunded_amount"`
	HoldShortfall  int64      `json:"hold_shortfall"`
	StandIn        bool       `json:"stand_in"`
	ExpiredAt      *time.Time `json:"expired_at,omitempty"`
	CreatedAt      time.Time  `json:"created_at"`
	// HoldPlacedAt is when this row's hold was written to the ledger.
	HoldPlacedAt *time.Time `json:"hold_placed_at,omitempty"`
	// ChainClearedAt is when the first purchase on its chain was posted.
	ChainClearedAt *time.Time `json:"chain_cleared_at,omitempty"`
}

// Snapshot is everything one run compares, read at one instant.
type Snapshot struct {
	Date           string
	CapturedAt     time.Time
	HoldExpiryDays int
	Records        []Record
	Authorizations []Authorization
	// Settlement is what we recorded paying; nil if the date is unsettled.
	Settlement *Totals
}

// Finding is one discrepancy and its evidence.
type Finding struct {
	Class         string         `json:"class"`
	Reason        string         `json:"reason"`
	AuthID        string         `json:"auth_id,omitempty"`
	ClearingID    string         `json:"clearing_id,omitempty"`
	CardID        string         `json:"card_id,omitempty"`
	Amount        int64          `json:"amount"`
	Record        *Record        `json:"record,omitempty"`
	Authorization *Authorization `json:"authorization,omitempty"`
	Statement     *Totals        `json:"statement,omitempty"`
	Settlement    *Totals        `json:"settlement,omitempty"`
	Records       *Totals        `json:"records,omitempty"`
	Fields        []string       `json:"fields,omitempty"`
}

// Classify compares a snapshot with the network's statement. It returns the
// findings in a stable order (records, then authorizations, then the
// settlement) and how many records matched, which are counted, not listed.
func Classify(snap Snapshot, stmt Statement) ([]Finding, int, error) {
	var findings []Finding
	matched := 0
	for i := range snap.Records {
		r := &snap.Records[i]
		classes := recordClasses(*r)
		if len(classes) == 0 {
			if r.Status == clearing.StatusPosted {
				matched++
			}
			continue
		}
		for _, class := range classes {
			f := Finding{Class: class, Reason: r.Flag, AuthID: r.AuthID, ClearingID: r.ClearingID,
				CardID: r.CardID, Amount: r.Amount, Record: r}
			if class == FundingShortfall {
				f.Amount = r.Shortfall
			}
			findings = append(findings, f)
		}
	}

	cutoff := snap.CapturedAt.AddDate(0, 0, -snap.HoldExpiryDays)
	day, _ := time.Parse(DateLayout, snap.Date)
	for i := range snap.Authorizations {
		a := &snap.Authorizations[i]
		at := func(class, reason string, amount int64) {
			findings = append(findings, Finding{Class: class, Reason: reason, AuthID: a.AuthID,
				CardID: a.CardID, Amount: amount, Authorization: a})
		}
		if a.Decision != "approved" {
			continue
		}
		if a.Kind == "initial" && a.ClearedAmount == 0 {
			switch {
			case a.ExpiredAt == nil && a.HeldAmount > 0 && a.CreatedAt.Before(cutoff):
				at(AuthorizedNotCleared, "hold_older_than_expiry", a.HeldAmount)
			case a.ExpiredAt != nil && !a.ExpiredAt.Before(day) && a.ExpiredAt.Before(day.AddDate(0, 0, 1)):
				at(AuthorizedNotCleared, "expired_without_clearing", a.ApprovedAmount)
			}
		}
		if a.HoldShortfall > 0 {
			at(FundingShortfall, "hold_shortfall", a.HoldShortfall)
		}
		if a.HoldPlacedAt != nil && a.ChainClearedAt != nil && a.HoldPlacedAt.After(*a.ChainClearedAt) {
			reason := "advice_after_clearing"
			if a.Kind == "incremental" {
				reason = "incremental_after_clearing"
			}
			at(HoldAfterClearing, reason, a.ApprovedAmount-a.HoldShortfall)
		}
	}

	ours, err := recordTotals(snap.Records)
	if err != nil {
		return nil, 0, err
	}
	network := stmt.Totals
	settlement := func(reason string, expected, actual Totals, fields []string) {
		f := Finding{Class: SettlementMismatch, Reason: reason, Statement: &network, Records: &ours,
			Settlement: snap.Settlement, Fields: fields}
		// The difference is a convenience; both sides' totals are the
		// evidence, so one that overflows is left at zero.
		if d, err := add(expected.NetAmount, -actual.NetAmount); err == nil {
			f.Amount = d
		}
		findings = append(findings, f)
	}
	if snap.Settlement == nil {
		settlement(ReasonNoSettlement, network, Totals{}, nil)
		if fields := differ(network, ours); len(fields) > 0 {
			settlement(ReasonRecordsVsStatement, network, ours, fields)
		}
	} else {
		if fields := differ(network, *snap.Settlement); len(fields) > 0 {
			settlement(ReasonStatementDiffers, network, *snap.Settlement, fields)
		}
		if fields := differ(ours, *snap.Settlement); len(fields) > 0 {
			settlement(ReasonRecordsDiffer, ours, *snap.Settlement, fields)
		}
	}
	return findings, matched, nil
}

// recordClasses are the classes a record's flags put it in, once each, in
// report order.
func recordClasses(r Record) []string {
	var classes []string
	if r.Flag != "" {
		for _, flag := range strings.Split(r.Flag, ",") {
			if class, ok := flagClass[flag]; ok && !slices.Contains(classes, class) {
				classes = append(classes, class)
			}
		}
	}
	if r.Shortfall > 0 && !slices.Contains(classes, FundingShortfall) {
		classes = append(classes, FundingShortfall)
	}
	slices.SortFunc(classes, func(a, b string) int { return slices.Index(Classes, a) - slices.Index(Classes, b) })
	return classes
}

// recordTotals adds up what Settle pays for: posted lines only.
func recordTotals(records []Record) (Totals, error) {
	var t Totals
	var err error
	for _, r := range records {
		if r.Status != clearing.StatusPosted {
			continue
		}
		into := &t.PurchasesTotal
		if r.Kind == clearing.KindRefund {
			into = &t.RefundsTotal
		}
		if *into, err = add(*into, r.Amount); err != nil {
			return Totals{}, err
		}
		t.Count++
	}
	t.NetAmount = t.PurchasesTotal - t.RefundsTotal
	return t, nil
}

func differ(a, b Totals) []string {
	var fields []string
	if a.PurchasesTotal != b.PurchasesTotal {
		fields = append(fields, "purchases_total")
	}
	if a.RefundsTotal != b.RefundsTotal {
		fields = append(fields, "refunds_total")
	}
	if a.NetAmount != b.NetAmount {
		fields = append(fields, "net_amount")
	}
	if a.Count != b.Count {
		fields = append(fields, "count")
	}
	return fields
}
