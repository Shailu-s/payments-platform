package reconciliation

import (
	"errors"
	"sort"
	"strings"
	"unicode/utf8"
)

const (
	Matched           = "MATCHED"
	AmountMismatch    = "AMOUNT_MISMATCH"
	StatusMismatch    = "STATUS_MISMATCH"
	MissingExternal   = "MISSING_EXTERNAL"
	MissingInternal   = "MISSING_INTERNAL"
	DuplicateExternal = "DUPLICATE_EXTERNAL"
)

type Record struct {
	ProviderRef     string `json:"provider_ref"`
	ClientReference string `json:"client_reference"`
	Amount          int64  `json:"amount"`
	Currency        string `json:"currency"`
	Status          string `json:"status"`
}

type Finding struct {
	Classification string   `json:"classification"`
	Internal       *Record  `json:"internal,omitempty"`
	External       []Record `json:"external"`
	Fields         []string `json:"fields,omitempty"`
}

func Compare(internal, external []Record) ([]Finding, error) {
	ours := make(map[string]Record, len(internal))
	owner := make(map[string]string, len(internal))
	bank := make(map[string][]Record)
	refCount := make(map[string]int)
	keys := make(map[string]bool)
	for _, row := range internal {
		if err := validate(row, false); err != nil {
			return nil, err
		}
		if _, exists := ours[row.ClientReference]; exists {
			return nil, errors.New("duplicate internal client reference")
		}
		if row.ProviderRef != "" {
			if _, exists := owner[row.ProviderRef]; exists {
				return nil, errors.New("duplicate internal provider reference")
			}
			owner[row.ProviderRef] = row.ClientReference
		}
		ours[row.ClientReference], keys[row.ClientReference] = row, true
	}
	for _, row := range external {
		if err := validate(row, true); err != nil {
			return nil, err
		}
		if client, known := owner[row.ProviderRef]; known && client != row.ClientReference {
			return nil, errors.New("conflicting reconciliation references")
		}
		bank[row.ClientReference] = append(bank[row.ClientReference], row)
		refCount[row.ProviderRef]++
		keys[row.ClientReference] = true
	}
	ordered := make([]string, 0, len(keys))
	for key := range keys {
		ordered = append(ordered, key)
	}
	sort.Strings(ordered)
	var findings []Finding
	for _, key := range ordered {
		row, present := ours[key]
		rows := bank[key]
		sort.Slice(rows, func(i, j int) bool {
			a, b := rows[i], rows[j]
			if a.ProviderRef != b.ProviderRef {
				return a.ProviderRef < b.ProviderRef
			}
			if a.Amount != b.Amount {
				return a.Amount < b.Amount
			}
			return a.Status < b.Status
		})
		finding := Finding{External: rows}
		if present {
			finding.Internal = &row
		} else {
			finding.Classification = MissingInternal
			findings = append(findings, finding)
		}
		duplicate := len(rows) > 1
		for _, externalRow := range rows {
			duplicate = duplicate || refCount[externalRow.ProviderRef] > 1
		}
		if duplicate {
			finding.Classification = DuplicateExternal
			findings = append(findings, finding)
			continue
		}
		if !present {
			continue
		}
		if len(rows) == 0 {
			finding.Classification = MissingExternal
			findings = append(findings, finding)
			continue
		}
		if row.ProviderRef != "" && row.ProviderRef != rows[0].ProviderRef {
			return nil, errors.New("conflicting reconciliation references")
		}
		if row.Amount != rows[0].Amount {
			finding.Classification, finding.Fields = AmountMismatch, []string{"amount"}
			findings = append(findings, finding)
		}
		if row.Status != rows[0].Status {
			finding.Classification, finding.Fields = StatusMismatch, []string{"status"}
			findings = append(findings, finding)
		}
		if row.Amount == rows[0].Amount && row.Status == rows[0].Status {
			finding.Classification = Matched
			findings = append(findings, finding)
		}
	}
	return findings, nil
}

func validate(row Record, external bool) error {
	if strings.TrimSpace(row.ClientReference) == "" || !utf8.ValidString(row.ClientReference) || !utf8.ValidString(row.ProviderRef) ||
		strings.ContainsRune(row.ClientReference, '\x00') || strings.ContainsRune(row.ProviderRef, '\x00') ||
		row.Amount <= 0 || row.Currency != "USD" || (external && strings.TrimSpace(row.ProviderRef) == "") {
		return errors.New("invalid reconciliation record")
	}
	if row.Status == "processing" || row.Status == "settled" || row.Status == "failed" || (!external && row.Status == "unresolved") {
		return nil
	}
	return errors.New("invalid reconciliation status")
}
