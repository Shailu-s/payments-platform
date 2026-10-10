package reconciliation

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

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrNotFound    = errors.New("reconciliation run not found")
	ErrRunConflict = errors.New("reconciliation run ID belongs to a different report")
)

type Run struct {
	ID                 string         `json:"id"`
	ReportSHA256       string         `json:"report_sha256"`
	ProviderCapturedAt time.Time      `json:"provider_captured_at"`
	InternalCapturedAt time.Time      `json:"internal_captured_at"`
	CreatedAt          time.Time      `json:"created_at"`
	InternalRows       int            `json:"internal_rows"`
	ExternalRows       int            `json:"external_rows"`
	Counts             map[string]int `json:"counts"`
	PreparationMS      int64          `json:"preparation_ms"`
}

type Reader interface {
	QueryRow(context.Context, string, ...any) pgx.Row
	Query(context.Context, string, ...any) (pgx.Rows, error)
}

const runColumns = `id, report_sha256, provider_captured_at, internal_captured_at, created_at, internal_rows, external_rows, counts, preparation_ms`

func Reconcile(ctx context.Context, pool *pgxpool.Pool, id string, source io.Reader) (Run, error) {
	started := time.Now()
	if strings.TrimSpace(id) == "" || len(id) > 255 {
		return Run{}, errors.New("run ID must contain 1–255 characters")
	}
	report, err := ReadReport(source)
	if err != nil {
		return Run{}, err
	}
	hash := sha256.Sum256(report.Raw)
	digest := hex.EncodeToString(hash[:])
	if previous, err := GetRun(ctx, pool, id); err == nil {
		return replay(previous, digest)
	} else if !errors.Is(err, ErrNotFound) {
		return Run{}, err
	}
	internalAt, internal, err := snapshot(ctx, pool, report.CapturedAt, report.Records)
	if err != nil {
		return Run{}, err
	}
	findings, err := Compare(internal, report.Records)
	if err != nil {
		return Run{}, err
	}
	run := Run{ID: id, ReportSHA256: digest, ProviderCapturedAt: report.CapturedAt, InternalCapturedAt: internalAt,
		InternalRows: len(internal), ExternalRows: len(report.Records), Counts: map[string]int{Matched: 0, AmountMismatch: 0, StatusMismatch: 0, MissingExternal: 0, MissingInternal: 0, DuplicateExternal: 0}}
	for _, f := range findings {
		run.Counts[f.Classification]++
	}
	counts, err := json.Marshal(run.Counts)
	if err != nil {
		return Run{}, err
	}
	internalJSON, err := json.Marshal(internal)
	if err != nil {
		return Run{}, err
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return Run{}, err
	}
	defer tx.Rollback(ctx)
	run.PreparationMS = time.Since(started).Milliseconds()
	inserted, err := tx.Exec(ctx, `INSERT INTO reconciliation_runs
		(id,report_sha256,provider_captured_at,internal_captured_at,internal_rows,external_rows,counts,preparation_ms,provider_report,internal_snapshot)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) ON CONFLICT (id) DO NOTHING`,
		id, digest, report.CapturedAt, internalAt, len(internal), len(report.Records), counts, run.PreparationMS, report.Raw, internalJSON)
	if err != nil {
		return Run{}, fmt.Errorf("save reconciliation run: %w", err)
	}
	if inserted.RowsAffected() == 0 {
		previous, err := GetRun(ctx, tx, id)
		if err != nil {
			return Run{}, err
		}
		return replay(previous, digest)
	}
	rows := make([][]any, len(findings))
	for i, f := range findings {
		evidence, err := json.Marshal(f)
		if err != nil {
			return Run{}, err
		}
		client, ref := "", ""
		if f.Internal != nil {
			client, ref = f.Internal.ClientReference, f.Internal.ProviderRef
		} else if len(f.External) > 0 {
			client, ref = f.External[0].ClientReference, f.External[0].ProviderRef
		}
		rows[i] = []any{id, i, f.Classification, client, ref, evidence}
	}
	if _, err := tx.CopyFrom(ctx, pgx.Identifier{"reconciliation_findings"}, []string{"run_id", "ordinal", "classification", "client_reference", "provider_ref", "evidence"}, pgx.CopyFromRows(rows)); err != nil {
		return Run{}, fmt.Errorf("save reconciliation findings: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return Run{}, fmt.Errorf("commit reconciliation run: %w", err)
	}
	return GetRun(ctx, pool, id)
}

func replay(previous Run, digest string) (Run, error) {
	if previous.ReportSHA256 != digest {
		return Run{}, ErrRunConflict
	}
	return previous, nil
}

func snapshot(ctx context.Context, pool *pgxpool.Pool, cutoff time.Time, external []Record) (time.Time, []Record, error) {
	clients := make([]string, len(external))
	for i, row := range external {
		clients[i] = row.ClientReference
	}
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return time.Time{}, nil, err
	}
	defer tx.Rollback(ctx)
	var capturedAt time.Time
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&capturedAt); err != nil {
		return time.Time{}, nil, err
	}
	rows, err := tx.Query(ctx, `SELECT COALESCE(provider_ref,''),id,amount,currency,status FROM transfers
		WHERE created_at <= $1 AND (provider_ref IS NOT NULL OR status='unresolved'
		OR (status='processing' AND attempt_count>0) OR id=ANY($2::text[])) ORDER BY id`, cutoff, clients)
	if err != nil {
		return time.Time{}, nil, err
	}
	internal, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (Record, error) {
		var r Record
		err := row.Scan(&r.ProviderRef, &r.ClientReference, &r.Amount, &r.Currency, &r.Status)
		return r, err
	})
	if err != nil {
		return time.Time{}, nil, err
	}
	return capturedAt.UTC(), internal, tx.Commit(ctx)
}

func GetRun(ctx context.Context, db Reader, id string) (Run, error) {
	run, err := scanRun(db.QueryRow(ctx, `SELECT `+runColumns+` FROM reconciliation_runs WHERE id=$1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return Run{}, ErrNotFound
	}
	return run, err
}

func ListRuns(ctx context.Context, db Reader, limit, offset int) ([]Run, error) {
	if limit < 1 || limit > 100 || offset < 0 {
		return nil, errors.New("invalid reconciliation page")
	}
	rows, err := db.Query(ctx, `SELECT `+runColumns+` FROM reconciliation_runs ORDER BY created_at DESC,id DESC LIMIT $1 OFFSET $2`, limit, offset)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (Run, error) { return scanRun(row) })
}

func Findings(ctx context.Context, db Reader, id, classification, clientReference string, limit, offset int) ([]Finding, error) {
	if limit < 1 || limit > 100 || offset < 0 {
		return nil, errors.New("invalid reconciliation page")
	}
	rows, err := db.Query(ctx, `SELECT evidence FROM reconciliation_findings WHERE run_id=$1
		AND ($2='' OR classification=$2) AND ($3='' OR client_reference=$3) ORDER BY ordinal LIMIT $4 OFFSET $5`, id, classification, clientReference, limit, offset)
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

func scanRun(row interface{ Scan(...any) error }) (Run, error) {
	var run Run
	var counts []byte
	err := row.Scan(&run.ID, &run.ReportSHA256, &run.ProviderCapturedAt, &run.InternalCapturedAt, &run.CreatedAt, &run.InternalRows, &run.ExternalRows, &counts, &run.PreparationMS)
	if err != nil {
		return Run{}, err
	}
	run.ProviderCapturedAt = run.ProviderCapturedAt.UTC()
	run.InternalCapturedAt = run.InternalCapturedAt.UTC()
	run.CreatedAt = run.CreatedAt.UTC()
	err = json.Unmarshal(counts, &run.Counts)
	return run, err
}
