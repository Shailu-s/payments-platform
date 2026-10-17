package cardrecon

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/Shailu-s/payments-platform/internal/clearing"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrNotFound    = errors.New("card reconciliation run not found")
	ErrRunConflict = errors.New("card reconciliation run ID belongs to a different statement")
)

// Config is the operator's policy for what counts as never cleared.
type Config struct {
	// HoldExpiryDays: an approved hold still open after this many days is
	// reported as authorized but not cleared. The hold expirer's setting.
	HoldExpiryDays int
}

type Run struct {
	ID                 string         `json:"id"`
	SettlementDate     string         `json:"settlement_date"`
	StatementSHA256    string         `json:"statement_sha256"`
	CapturedAt         time.Time      `json:"captured_at"`
	CreatedAt          time.Time      `json:"created_at"`
	HoldExpiryDays     int            `json:"hold_expiry_days"`
	RecordCount        int            `json:"record_count"`
	AuthorizationCount int            `json:"authorization_count"`
	Counts             map[string]int `json:"counts"`
	PreparationMS      int64          `json:"preparation_ms"`
}

type Reader interface {
	QueryRow(context.Context, string, ...any) pgx.Row
	Query(context.Context, string, ...any) (pgx.Rows, error)
}

const runColumns = `id, to_char(settlement_date, 'YYYY-MM-DD'), statement_sha256, captured_at, created_at,
	hold_expiry_days, record_count, authorization_count, counts, preparation_ms`

// Reconcile compares the settlement date of the network's statement, read
// from source, with our records, and saves the run and its findings under
// id, append-only. The same id with the same statement bytes returns the
// saved run; with different bytes, ErrRunConflict. It reads our records in one
// read-only Repeatable Read snapshot and writes nothing but the run.
func Reconcile(ctx context.Context, pool *pgxpool.Pool, id string, source io.Reader, cfg Config) (Run, error) {
	started := time.Now()
	if strings.TrimSpace(id) == "" || len(id) > 255 {
		return Run{}, errors.New("run ID must contain 1–255 characters")
	}
	if cfg.HoldExpiryDays < 1 {
		return Run{}, errors.New("hold expiry must be at least one day")
	}
	raw, err := io.ReadAll(io.LimitReader(source, MaxStatementBytes+1))
	if err != nil {
		return Run{}, fmt.Errorf("read settlement statement: %w", err)
	}
	stmt, err := ParseStatement(raw)
	if err != nil {
		return Run{}, err
	}
	hash := sha256.Sum256(raw)
	digest := hex.EncodeToString(hash[:])
	if previous, err := GetRun(ctx, pool, id); err == nil {
		return replay(previous, digest)
	} else if !errors.Is(err, ErrNotFound) {
		return Run{}, err
	}

	snap, err := snapshot(ctx, pool, stmt.Date, cfg.HoldExpiryDays)
	if err != nil {
		return Run{}, err
	}
	findings, matched, err := Classify(snap, stmt)
	if err != nil {
		return Run{}, err
	}
	run := Run{ID: id, SettlementDate: stmt.Date, StatementSHA256: digest, CapturedAt: snap.CapturedAt,
		HoldExpiryDays: cfg.HoldExpiryDays, RecordCount: len(snap.Records),
		AuthorizationCount: len(snap.Authorizations), Counts: map[string]int{}}
	for _, class := range Classes {
		run.Counts[class] = 0
	}
	run.Counts[Matched] = matched
	for _, f := range findings {
		run.Counts[f.Class]++
	}
	counts, err := json.Marshal(run.Counts)
	if err != nil {
		return Run{}, err
	}
	rows := make([][]any, len(findings))
	for i, f := range findings {
		evidence, err := json.Marshal(f)
		if err != nil {
			return Run{}, err
		}
		rows[i] = []any{id, i, f.Class, nullable(f.AuthID), nullable(f.ClearingID), nullable(f.CardID), f.Amount, evidence}
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return Run{}, err
	}
	defer tx.Rollback(ctx)
	run.PreparationMS = time.Since(started).Milliseconds()
	inserted, err := tx.Exec(ctx, `INSERT INTO card_recon_runs
		(id, settlement_date, statement, statement_sha256, captured_at, hold_expiry_days, record_count,
			authorization_count, counts, preparation_ms)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) ON CONFLICT (id) DO NOTHING`,
		id, stmt.Date, raw, digest, snap.CapturedAt, cfg.HoldExpiryDays, len(snap.Records),
		len(snap.Authorizations), counts, run.PreparationMS)
	if err != nil {
		return Run{}, fmt.Errorf("save card reconciliation run: %w", err)
	}
	if inserted.RowsAffected() == 0 {
		// A concurrent run with this id committed first; this insert waited for it.
		previous, err := GetRun(ctx, tx, id)
		if err != nil {
			return Run{}, err
		}
		return replay(previous, digest)
	}
	if _, err := tx.CopyFrom(ctx, pgx.Identifier{"card_recon_findings"},
		[]string{"run_id", "ordinal", "class", "auth_id", "clearing_id", "card_id", "amount", "evidence"},
		pgx.CopyFromRows(rows)); err != nil {
		return Run{}, fmt.Errorf("save card reconciliation findings: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return Run{}, fmt.Errorf("commit card reconciliation run: %w", err)
	}
	return GetRun(ctx, pool, id)
}

func replay(previous Run, digest string) (Run, error) {
	if previous.StatementSHA256 != digest {
		return Run{}, ErrRunConflict
	}
	return previous, nil
}

func nullable(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// snapshot reads the date's clearing records, the authorizations relevant to
// them and to holds that should have cleared by now, and the recorded
// settlement, all as of one instant.
func snapshot(ctx context.Context, pool *pgxpool.Pool, date string, expiryDays int) (Snapshot, error) {
	day, err := time.Parse(DateLayout, date)
	if err != nil {
		return Snapshot{}, err
	}
	snap := Snapshot{Date: date, HoldExpiryDays: expiryDays}
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return Snapshot{}, err
	}
	defer tx.Rollback(ctx)
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&snap.CapturedAt); err != nil {
		return Snapshot{}, err
	}
	snap.CapturedAt = snap.CapturedAt.UTC()

	rows, err := tx.Query(ctx, `
		SELECT r.clearing_id, COALESCE(r.auth_id, ''), r.card_id, r.kind, r.amount, r.status, r.shortfall,
			r.credited_amount, COALESCE(r.flag, ''), r.cleared_at, r.created_at
		FROM clearing_records r JOIN clearing_files f ON f.id = r.file_id
		WHERE f.file_date = $1
		ORDER BY r.cleared_at, r.clearing_id`, date)
	if err != nil {
		return Snapshot{}, fmt.Errorf("read clearing records: %w", err)
	}
	snap.Records, err = pgx.CollectRows(rows, func(row pgx.CollectableRow) (Record, error) {
		var r Record
		err := row.Scan(&r.ClearingID, &r.AuthID, &r.CardID, &r.Kind, &r.Amount, &r.Status, &r.Shortfall,
			&r.Credited, &r.Flag, &r.ClearedAt, &r.CreatedAt)
		r.ClearedAt, r.CreatedAt = r.ClearedAt.UTC(), r.CreatedAt.UTC()
		return r, err
	})
	if err != nil {
		return Snapshot{}, fmt.Errorf("read clearing records: %w", err)
	}
	authIDs := make([]string, 0, len(snap.Records))
	for _, r := range snap.Records {
		if r.AuthID != "" {
			authIDs = append(authIDs, r.AuthID)
		}
	}

	// Each branch has its own index: the primary key, the incremental index,
	// the open-holds index and the expired index.
	rows, err = tx.Query(ctx, `
		WITH scope AS (
			SELECT auth_id FROM card_authorizations WHERE auth_id = ANY($1)
			UNION SELECT auth_id FROM card_authorizations WHERE original_auth_id = ANY($1)
			UNION SELECT auth_id FROM card_authorizations
				WHERE held_amount > 0 AND expired_at IS NULL AND created_at < $2
			UNION SELECT auth_id FROM card_authorizations WHERE expired_at >= $3 AND expired_at < $4
		)
		SELECT a.auth_id, a.card_id, a.kind, COALESCE(a.original_auth_id, ''), a.decision, a.approved_amount,
			a.held_amount, a.released_amount, a.cleared_amount, a.refunded_amount, a.hold_shortfall,
			a.stand_in, a.expired_at, a.created_at, t.created_at,
			(SELECT min(r.created_at) FROM clearing_records r
			 WHERE r.auth_id = COALESCE(a.original_auth_id, a.auth_id) AND r.kind = $5 AND r.status = $6)
		FROM scope JOIN card_authorizations a USING (auth_id)
		LEFT JOIN ledger_transactions t ON t.id = a.ledger_txn_id
		ORDER BY a.auth_id`,
		authIDs, snap.CapturedAt.AddDate(0, 0, -expiryDays), day, day.AddDate(0, 0, 1),
		clearing.KindPurchase, clearing.StatusPosted)
	if err != nil {
		return Snapshot{}, fmt.Errorf("read authorizations: %w", err)
	}
	snap.Authorizations, err = pgx.CollectRows(rows, func(row pgx.CollectableRow) (Authorization, error) {
		var a Authorization
		err := row.Scan(&a.AuthID, &a.CardID, &a.Kind, &a.OriginalAuthID, &a.Decision, &a.ApprovedAmount,
			&a.HeldAmount, &a.ReleasedAmount, &a.ClearedAmount, &a.RefundedAmount, &a.HoldShortfall,
			&a.StandIn, &a.ExpiredAt, &a.CreatedAt, &a.HoldPlacedAt, &a.ChainClearedAt)
		a.CreatedAt = a.CreatedAt.UTC()
		for _, t := range []*time.Time{a.ExpiredAt, a.HoldPlacedAt, a.ChainClearedAt} {
			if t != nil {
				*t = t.UTC()
			}
		}
		return a, err
	})
	if err != nil {
		return Snapshot{}, fmt.Errorf("read authorizations: %w", err)
	}

	var s Totals
	err = tx.QueryRow(ctx, `SELECT purchases_total, refunds_total, net_amount, record_count
		FROM network_settlements WHERE settlement_date = $1`, date).
		Scan(&s.PurchasesTotal, &s.RefundsTotal, &s.NetAmount, &s.Count)
	switch {
	case err == nil:
		snap.Settlement = &s
	case !errors.Is(err, pgx.ErrNoRows):
		return Snapshot{}, fmt.Errorf("read settlement: %w", err)
	}
	return snap, tx.Commit(ctx)
}

func GetRun(ctx context.Context, db Reader, id string) (Run, error) {
	run, err := scanRun(db.QueryRow(ctx, `SELECT `+runColumns+` FROM card_recon_runs WHERE id = $1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return Run{}, ErrNotFound
	}
	return run, err
}

func ListRuns(ctx context.Context, db Reader, limit, offset int) ([]Run, error) {
	if limit < 1 || limit > 100 || offset < 0 {
		return nil, errors.New("invalid card reconciliation page")
	}
	rows, err := db.Query(ctx, `SELECT `+runColumns+` FROM card_recon_runs
		ORDER BY created_at DESC, id DESC LIMIT $1 OFFSET $2`, limit, offset)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (Run, error) { return scanRun(row) })
}

// Findings lists a run's findings in order, optionally of one class.
func Findings(ctx context.Context, db Reader, id, class string, limit, offset int) ([]Finding, error) {
	if limit < 1 || limit > 100 || offset < 0 {
		return nil, errors.New("invalid card reconciliation page")
	}
	rows, err := db.Query(ctx, `SELECT evidence FROM card_recon_findings WHERE run_id = $1
		AND ($2 = '' OR class = $2) ORDER BY ordinal LIMIT $3 OFFSET $4`, id, class, limit, offset)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (Finding, error) {
		var f Finding
		var raw []byte
		if err := row.Scan(&raw); err != nil {
			return f, err
		}
		err := json.Unmarshal(raw, &f)
		return f, err
	})
}

// ValidClass reports whether class is one a finding can have.
func ValidClass(class string) bool {
	for _, c := range Classes {
		if c == class && c != Matched {
			return true
		}
	}
	return false
}

func scanRun(row interface{ Scan(...any) error }) (Run, error) {
	var run Run
	var counts []byte
	err := row.Scan(&run.ID, &run.SettlementDate, &run.StatementSHA256, &run.CapturedAt, &run.CreatedAt,
		&run.HoldExpiryDays, &run.RecordCount, &run.AuthorizationCount, &counts, &run.PreparationMS)
	if err != nil {
		return Run{}, err
	}
	run.CapturedAt, run.CreatedAt = run.CapturedAt.UTC(), run.CreatedAt.UTC()
	err = json.Unmarshal(counts, &run.Counts)
	return run, err
}
