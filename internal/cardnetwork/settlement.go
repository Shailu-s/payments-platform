package cardnetwork

import (
	"bytes"
	"encoding/csv"
	"strconv"
	"time"
)

var statementHeader = []string{"settlement_date", "purchases_total", "refunds_total", "net_amount", "line_count", "currency"}

// Statement is what the network says the issuer owes it for one UTC date:
// every line of that date's clearing file, purchases minus refunds. A
// negative net means the network owes the issuer.
type Statement struct {
	Date           string
	PurchasesTotal int64
	RefundsTotal   int64
	NetAmount      int64
	LineCount      int
	Currency       string
}

// StatementOf totals lines, the clearing lines of date.
func StatementOf(date string, lines []ClearingLine) Statement {
	s := Statement{Date: date, LineCount: len(lines), Currency: Currency}
	for _, l := range lines {
		if l.Kind == ClearingRefund {
			s.RefundsTotal += l.Amount
		} else {
			s.PurchasesTotal += l.Amount
		}
	}
	s.NetAmount = s.PurchasesTotal - s.RefundsTotal
	return s
}

// SettlementStatement builds the settlement CSV for one UTC date from the same
// lines as that date's clearing file, so the two always agree. The same
// journal always yields the same bytes.
func (n *Network) SettlementStatement(date string) ([]byte, error) {
	day, err := time.Parse(DateLayout, date)
	if err != nil {
		return nil, ErrInvalidDate
	}
	s := StatementOf(date, n.ClearingLines(day))

	var buf bytes.Buffer
	w := csv.NewWriter(&buf)
	_ = w.Write(statementHeader)
	_ = w.Write([]string{s.Date, strconv.FormatInt(s.PurchasesTotal, 10), strconv.FormatInt(s.RefundsTotal, 10),
		strconv.FormatInt(s.NetAmount, 10), strconv.Itoa(s.LineCount), s.Currency})
	w.Flush()
	if err := w.Error(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
