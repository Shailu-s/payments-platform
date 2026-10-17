// Package cardsettlement records what we pay the card network each day: the
// net of the clearing lines we posted for that date, purchases minus refunds,
// recorded once per date with its ledger transaction.
package cardsettlement

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Shailu-s/payments-platform/internal/clearing"
	"github.com/Shailu-s/payments-platform/internal/ledger"
	"github.com/jackc/pgx/v5"
)

// NetworkCashAccountID is our cash leg toward the network.
const NetworkCashAccountID = "acc_network_cash_usd"

const DateLayout = "2006-01-02"

var (
	ErrNotFound       = errors.New("no settlement recorded for this date")
	ErrNoClearingFile = errors.New("no clearing file imported for this date")
	ErrDeferredLines  = errors.New("clearing lines of this date are still deferred; run clearing -post-deferred first")
)

// DB is a pool: Settle starts its own transaction.
type DB interface {
	Begin(ctx context.Context) (pgx.Tx, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Querier reads in the caller's transaction or on a pool.
type Querier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Settlement is one day's payment to the network.
type Settlement struct {
	Date           string    `json:"settlement_date"`
	PurchasesTotal int64     `json:"purchases_total"`
	RefundsTotal   int64     `json:"refunds_total"`
	NetAmount      int64     `json:"net_amount"`
	RecordCount    int       `json:"record_count"`
	LedgerTxnID    *string   `json:"ledger_txn_id"`
	CreatedAt      time.Time `json:"created_at"`
}

const columns = `to_char(settlement_date, 'YYYY-MM-DD'), purchases_total, refunds_total, net_amount,
	record_count, ledger_txn_id, created_at`

// Settle records the payment for date's posted clearing lines and reports
// whether this call recorded it. A date already settled returns its row and
// moves no money.
//
// The row is inserted before the money moves, in the same transaction: a
// concurrent Settle of the same date waits on the primary key until the first
// commits, then inserts nothing and returns the first's row. The ledger
// transaction id is chosen before the insert; the foreign key is checked at
// commit, after the ledger write.
//
// The totals are what was posted when the row is written. A line posted for
// the date afterwards is not paid by a rerun: reconciliation reports it.
func Settle(ctx context.Context, db DB, date time.Time) (Settlement, bool, error) {
	day := date.UTC().Format(DateLayout)
	if s, err := Get(ctx, db, date); err == nil {
		return s, false, nil
	} else if !errors.Is(err, ErrNotFound) {
		return Settlement{}, false, err
	}

	tx, err := db.Begin(ctx)
	if err != nil {
		return Settlement{}, false, fmt.Errorf("begin settlement %s: %w", day, err)
	}
	defer tx.Rollback(ctx)

	var files, deferred int
	if err := tx.QueryRow(ctx, `
		SELECT (SELECT count(*) FROM clearing_files WHERE file_date = $1),
			(SELECT count(*) FROM clearing_records r JOIN clearing_files f ON f.id = r.file_id
			 WHERE f.file_date = $1 AND r.status = $2)`, day, clearing.StatusDeferred).Scan(&files, &deferred); err != nil {
		return Settlement{}, false, fmt.Errorf("check clearing for %s: %w", day, err)
	}
	switch {
	case files == 0:
		return Settlement{}, false, ErrNoClearingFile
	case deferred > 0:
		return Settlement{}, false, ErrDeferredLines
	}

	// A line's record belongs to the file that recorded it first, so a line
	// repeated in a later date's file is paid once, on its first date.
	txnID := ledger.NewTxnID()
	s, err := scan(tx.QueryRow(ctx, `
		INSERT INTO network_settlements (settlement_date, purchases_total, refunds_total, net_amount,
			record_count, ledger_txn_id)
		SELECT $1, t.purchases, t.refunds, t.purchases - t.refunds, t.n,
			CASE WHEN t.purchases <> t.refunds THEN $2 END
		FROM (
			SELECT COALESCE(sum(r.amount) FILTER (WHERE r.kind = $4), 0)::bigint AS purchases,
				COALESCE(sum(r.amount) FILTER (WHERE r.kind = $5), 0)::bigint AS refunds,
				count(*)::int AS n
			FROM clearing_records r JOIN clearing_files f ON f.id = r.file_id
			WHERE f.file_date = $1 AND r.status = $3
		) t
		ON CONFLICT (settlement_date) DO NOTHING
		RETURNING `+columns, day, txnID, clearing.StatusPosted, clearing.KindPurchase, clearing.KindRefund))
	if errors.Is(err, pgx.ErrNoRows) {
		// Another Settle of this date committed first; this statement waited for it.
		existing, err := Get(ctx, tx, date)
		return existing, false, err
	}
	if err != nil {
		return Settlement{}, false, fmt.Errorf("record settlement %s: %w", day, err)
	}

	if s.LedgerTxnID != nil {
		if err := ledger.RecordWithID(ctx, tx, *s.LedgerTxnID, "network settlement "+day, entries(s.NetAmount)); err != nil {
			return Settlement{}, false, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return Settlement{}, false, fmt.Errorf("commit settlement %s: %w", day, err)
	}
	return s, true, nil
}

// entries pays a positive net from our cash, clearing the payable the day's
// lines credited; a negative net is the network paying us.
func entries(net int64) []ledger.Entry {
	from, to := clearing.NetworkPayableAccountID, NetworkCashAccountID
	if net < 0 {
		from, to, net = to, from, -net
	}
	return []ledger.Entry{
		{AccountID: from, Direction: ledger.DirectionDebit, Amount: net},
		{AccountID: to, Direction: ledger.DirectionCredit, Amount: net},
	}
}

// Get returns date's settlement, or ErrNotFound.
func Get(ctx context.Context, db Querier, date time.Time) (Settlement, error) {
	s, err := scan(db.QueryRow(ctx, `SELECT `+columns+` FROM network_settlements WHERE settlement_date = $1`,
		date.UTC().Format(DateLayout)))
	if errors.Is(err, pgx.ErrNoRows) {
		return Settlement{}, ErrNotFound
	}
	if err != nil {
		return Settlement{}, fmt.Errorf("get settlement: %w", err)
	}
	return s, nil
}

func scan(row pgx.Row) (Settlement, error) {
	var s Settlement
	err := row.Scan(&s.Date, &s.PurchasesTotal, &s.RefundsTotal, &s.NetAmount, &s.RecordCount, &s.LedgerTxnID, &s.CreatedAt)
	s.CreatedAt = s.CreatedAt.UTC()
	return s, err
}
