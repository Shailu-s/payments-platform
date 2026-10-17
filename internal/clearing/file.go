// Package clearing turns the card network's daily clearing file into final
// charges: each line settles a hold (or charges the account directly when there
// is none) and credits what we owe the network. The network's file is the
// record we must post; anything unexpected is flagged, never silently fixed.
package clearing

import (
	"bytes"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	// MaxFileBytes bounds a file held in memory and stored whole: about 100,000 lines.
	MaxFileBytes = 10 << 20

	DateLayout = "2006-01-02"

	KindPurchase = "purchase"
	KindRefund   = "refund"

	Currency = "USD"

	maxIDLength = 255
)

var ErrInvalidFile = errors.New("invalid clearing file")

var header = []string{"clearing_id", "auth_id", "card_id", "kind", "amount", "currency", "merchant_id", "cleared_at"}

// Line is one row of a clearing file.
type Line struct {
	ClearingID string    `json:"clearing_id"`
	AuthID     string    `json:"auth_id"`
	CardID     string    `json:"card_id"`
	Kind       string    `json:"kind"`
	Amount     int64     `json:"amount"`
	Currency   string    `json:"currency"`
	MerchantID string    `json:"merchant_id"`
	ClearedAt  time.Time `json:"cleared_at"`
}

// Parse validates a whole clearing file for fileDate before anything is
// written: one bad line rejects the file, so a file is never half understood.
// A clearing_id twice in one file is a broken file, not a duplicate to skip.
func Parse(raw []byte, fileDate time.Time) ([]Line, error) {
	if len(raw) > MaxFileBytes {
		return nil, fmt.Errorf("%w: exceeds %d bytes", ErrInvalidFile, MaxFileBytes)
	}
	if !utf8.Valid(raw) {
		return nil, fmt.Errorf("%w: not UTF-8", ErrInvalidFile)
	}
	date := fileDate.UTC().Format(DateLayout)
	reader := csv.NewReader(bytes.NewReader(raw))
	reader.FieldsPerRecord = -1
	first, err := reader.Read()
	if err != nil || !slices.Equal(first, header) {
		return nil, fmt.Errorf("%w: header must be %s", ErrInvalidFile, strings.Join(header, ","))
	}
	var lines []Line
	seen := map[string]bool{}
	for n := 2; ; n++ {
		fields, err := reader.Read()
		if errors.Is(err, io.EOF) {
			return lines, nil
		}
		if err != nil {
			return nil, fmt.Errorf("%w: line %d: %v", ErrInvalidFile, n, err)
		}
		l, err := parseLine(fields, date)
		if err != nil {
			return nil, fmt.Errorf("%w: line %d: %v", ErrInvalidFile, n, err)
		}
		if seen[l.ClearingID] {
			return nil, fmt.Errorf("%w: line %d: clearing_id %s appears twice", ErrInvalidFile, n, l.ClearingID)
		}
		seen[l.ClearingID] = true
		lines = append(lines, l)
	}
}

func parseLine(fields []string, date string) (Line, error) {
	if len(fields) != len(header) {
		return Line{}, fmt.Errorf("%d fields, want %d", len(fields), len(header))
	}
	l := Line{
		ClearingID: fields[0], AuthID: fields[1], CardID: fields[2], Kind: fields[3],
		Currency: fields[5], MerchantID: fields[6],
	}
	for _, f := range []struct {
		name, value string
		optional    bool
	}{
		{"clearing_id", l.ClearingID, false}, {"auth_id", l.AuthID, true},
		{"card_id", l.CardID, false}, {"merchant_id", l.MerchantID, false},
	} {
		if err := validID(f.value, f.optional); err != nil {
			return Line{}, fmt.Errorf("%s: %v", f.name, err)
		}
	}
	if l.Kind != KindPurchase && l.Kind != KindRefund {
		return Line{}, fmt.Errorf("kind %q", l.Kind)
	}
	amount, err := parseAmount(fields[4])
	if err != nil {
		return Line{}, err
	}
	l.Amount = amount
	if l.Currency != Currency {
		return Line{}, fmt.Errorf("currency %q, want %s", l.Currency, Currency)
	}
	at, err := time.Parse(time.RFC3339, fields[7])
	if err != nil || !strings.HasSuffix(fields[7], "Z") {
		return Line{}, fmt.Errorf("cleared_at %q is not an RFC 3339 UTC time", fields[7])
	}
	if at.Format(DateLayout) != date {
		return Line{}, fmt.Errorf("cleared_at %s is not on the file's date %s", fields[7], date)
	}
	l.ClearedAt = at
	return l, nil
}

func validID(v string, optional bool) error {
	switch {
	case v == "" && optional:
		return nil
	case v == "":
		return errors.New("required")
	case len(v) > maxIDLength:
		return fmt.Errorf("longer than %d bytes", maxIDLength)
	case strings.TrimSpace(v) != v || strings.ContainsRune(v, '\x00'):
		return errors.New("has surrounding space or NUL")
	}
	return nil
}

// parseAmount accepts positive integer minor units written plainly: no sign,
// no leading zero, no decimal point.
func parseAmount(s string) (int64, error) {
	if s == "" || s[0] < '1' || s[0] > '9' || strings.Trim(s, "0123456789") != "" {
		return 0, fmt.Errorf("amount %q is not positive integer minor units", s)
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("amount %q out of range", s)
	}
	return n, nil
}
