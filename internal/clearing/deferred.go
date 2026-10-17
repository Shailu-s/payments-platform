package clearing

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/jackc/pgx/v5"
)

// deferredPageSize bounds the lines held in memory per page of a run.
const deferredPageSize = 500

// DeferredSummary describes what one PostDeferred run posted.
type DeferredSummary struct {
	Processed int            `json:"processed"`
	Statuses  map[string]int `json:"statuses"`
	Flags     map[string]int `json:"flags"`
	Credited  int64          `json:"credited"`
}

// PostDeferred posts the refund lines that imports from before refunds were
// posted recorded as deferred, as Import would post them now.
//
// Each line is claimed in the same short transaction as its money, so
// concurrent or repeated runs credit it once and a run that stops part-way
// is finished by running it again.
func PostDeferred(ctx context.Context, db DB, cfg Config) (DeferredSummary, error) {
	if cfg.OverTolerancePct < 0 {
		return DeferredSummary{}, errors.New("over tolerance must not be negative")
	}
	s := DeferredSummary{Statuses: map[string]int{}, Flags: map[string]int{}}
	after := ""
	for {
		lines, err := deferredLines(ctx, db, after)
		if err != nil {
			return s, err
		}
		if len(lines) == 0 {
			break
		}
		var posted []string
		for _, l := range lines {
			ok, err := postWithRetry(ctx, func() (bool, error) { return postDeferredLine(ctx, db, l, cfg) })
			if err != nil {
				return s, fmt.Errorf("deferred clearing %s: %w", l.ClearingID, err)
			}
			if ok {
				posted = append(posted, l.ClearingID)
			}
		}
		if err := s.add(ctx, db, posted); err != nil {
			return s, err
		}
		after = lines[len(lines)-1].ClearingID
	}
	slog.Info("deferred clearing lines posted", "processed", s.Processed, "statuses", s.Statuses,
		"flags", s.Flags, "credited", s.Credited)
	return s, nil
}

// deferredLines reads the next page of deferred lines after the clearing_id after.
func deferredLines(ctx context.Context, db DB, after string) ([]Line, error) {
	rows, err := db.Query(ctx, `
		SELECT clearing_id, COALESCE(auth_id, ''), card_id, kind, amount, currency, merchant_id, cleared_at
		FROM clearing_records
		WHERE status = $1 AND clearing_id > $2
		ORDER BY clearing_id LIMIT $3`, StatusDeferred, after, deferredPageSize)
	if err != nil {
		return nil, fmt.Errorf("deferred clearing lines: %w", err)
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (Line, error) {
		var l Line
		err := row.Scan(&l.ClearingID, &l.AuthID, &l.CardID, &l.Kind, &l.Amount, &l.Currency, &l.MerchantID, &l.ClearedAt)
		l.ClearedAt = l.ClearedAt.UTC()
		return l, err
	})
}

// add counts the lines this run posted, as they now stand.
func (s *DeferredSummary) add(ctx context.Context, db DB, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	rows, err := db.Query(ctx, `
		SELECT status, COALESCE(flag, ''), count(*), sum(credited_amount)::bigint
		FROM clearing_records WHERE clearing_id = ANY($1) GROUP BY 1, 2`, ids)
	if err != nil {
		return fmt.Errorf("summarise deferred lines: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var status, flag string
		var n int
		var credited int64
		if err := rows.Scan(&status, &flag, &n, &credited); err != nil {
			return fmt.Errorf("summarise deferred lines: %w", err)
		}
		s.Processed += n
		s.Statuses[status] += n
		if flag != "" {
			for _, one := range strings.Split(flag, ",") {
				s.Flags[one] += n
			}
		}
		s.Credited += credited
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("summarise deferred lines: %w", err)
	}
	return nil
}
