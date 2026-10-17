package cardrecon

import (
	"bytes"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	DateLayout = "2006-01-02"
	// MaxStatementBytes bounds a statement: one header and one row.
	MaxStatementBytes = 4 << 10
)

var ErrInvalidStatement = errors.New("invalid settlement statement")

var statementHeader = []string{"settlement_date", "purchases_total", "refunds_total", "net_amount", "line_count", "currency"}

// Totals is one side's view of a day: what the network says, what we
// recorded paying, or what our posted records add up to.
type Totals struct {
	PurchasesTotal int64 `json:"purchases_total"`
	RefundsTotal   int64 `json:"refunds_total"`
	NetAmount      int64 `json:"net_amount"`
	Count          int   `json:"count"`
}

// Statement is the network's settlement statement for one date, as received.
type Statement struct {
	Date string
	Totals
	Raw []byte
}

// ParseStatement validates the whole statement: a header and exactly one row
// whose net is its purchases minus its refunds.
func ParseStatement(raw []byte) (Statement, error) {
	if len(raw) > MaxStatementBytes {
		return Statement{}, fmt.Errorf("%w: exceeds %d bytes", ErrInvalidStatement, MaxStatementBytes)
	}
	if !utf8.Valid(raw) {
		return Statement{}, fmt.Errorf("%w: not UTF-8", ErrInvalidStatement)
	}
	records, err := csv.NewReader(bytes.NewReader(raw)).ReadAll()
	if err != nil {
		return Statement{}, fmt.Errorf("%w: %v", ErrInvalidStatement, err)
	}
	if len(records) != 2 || !slices.Equal(records[0], statementHeader) || len(records[1]) != len(statementHeader) {
		return Statement{}, fmt.Errorf("%w: want the header and one row", ErrInvalidStatement)
	}
	row := records[1]
	if _, err := time.Parse(DateLayout, row[0]); err != nil {
		return Statement{}, fmt.Errorf("%w: settlement_date must be YYYY-MM-DD", ErrInvalidStatement)
	}
	s := Statement{Date: row[0], Raw: raw}
	for i, into := range []*int64{&s.PurchasesTotal, &s.RefundsTotal, &s.NetAmount} {
		v, err := strconv.ParseInt(row[i+1], 10, 64)
		if err != nil || strings.HasPrefix(row[i+1], "+") {
			return Statement{}, fmt.Errorf("%w: %s is not an integer", ErrInvalidStatement, statementHeader[i+1])
		}
		*into = v
	}
	count, err := strconv.Atoi(row[4])
	switch {
	case err != nil || count < 0 || strings.HasPrefix(row[4], "+"):
		return Statement{}, fmt.Errorf("%w: line_count must be a whole number", ErrInvalidStatement)
	case s.PurchasesTotal < 0 || s.RefundsTotal < 0:
		return Statement{}, fmt.Errorf("%w: totals must not be negative", ErrInvalidStatement)
	case s.PurchasesTotal-s.RefundsTotal != s.NetAmount:
		// Both totals are non-negative, so the subtraction cannot overflow.
		return Statement{}, fmt.Errorf("%w: net_amount is not purchases_total - refunds_total", ErrInvalidStatement)
	case row[5] != "USD":
		return Statement{}, fmt.Errorf("%w: currency must be USD", ErrInvalidStatement)
	}
	s.Count = count
	return s, nil
}

// FetchStatement downloads the network's statement for date, bounded in size.
func FetchStatement(ctx context.Context, client *http.Client, baseURL, date string) ([]byte, error) {
	u := strings.TrimRight(baseURL, "/") + "/settlement?date=" + url.QueryEscape(date)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("download settlement statement: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("settlement statement download returned HTTP %d", resp.StatusCode)
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, MaxStatementBytes+1))
	if err != nil {
		return nil, fmt.Errorf("download settlement statement: %w", err)
	}
	return raw, nil
}

var errOverflow = errors.New("totals exceed int64 minor units")

// add is a + b, or errOverflow.
func add(a, b int64) (int64, error) {
	if (b > 0 && a > math.MaxInt64-b) || (b < 0 && a < math.MinInt64-b) {
		return 0, errOverflow
	}
	return a + b, nil
}
