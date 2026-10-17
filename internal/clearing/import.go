package clearing

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

const (
	StatusPosted   = "posted"
	StatusDeferred = "deferred"
	StatusRejected = "rejected"
)

// DB is a pool: lines commit one by one, so Import needs to start transactions.
type DB interface {
	Begin(ctx context.Context) (pgx.Tx, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// Config is the operator's policy for lines that differ from their hold.
type Config struct {
	// OverTolerancePct flags a clearing above its hold by more than this
	// percentage of the authorized amount. It is still posted.
	OverTolerancePct int64
}

type File struct {
	ID         string    `json:"id"`
	FileDate   string    `json:"file_date"`
	SHA256     string    `json:"sha256"`
	LineCount  int       `json:"line_count"`
	ImportedAt time.Time `json:"imported_at"`
}

// Summary describes a file's lines as they stand in the database.
type Summary struct {
	File File `json:"file"`
	// Replayed means these exact bytes had been imported before.
	Replayed bool `json:"replayed"`
	// Processed is how many lines this call recorded; 0 on a complete replay.
	Processed int `json:"processed"`
	// FromOtherFiles are lines a different file had already recorded.
	FromOtherFiles int `json:"from_other_files"`
	// Unprocessed are lines with no record yet: an import that stopped early.
	Unprocessed          int            `json:"unprocessed"`
	Statuses             map[string]int `json:"statuses"`
	Flags                map[string]int `json:"flags"`
	Amount               int64          `json:"amount"`
	ChargedFromHeld      int64          `json:"charged_from_held"`
	ChargedFromAvailable int64          `json:"charged_from_available"`
	Released             int64          `json:"released"`
	Shortfall            int64          `json:"shortfall"`
	// Credited is what refund lines gave back to accounts.
	Credited int64 `json:"credited"`
}

// Import records a clearing file and posts each of its lines once.
//
// The same bytes for the same date change nothing: the file is found by its
// date and hash and, if every line already has a record, nothing is
// processed. Each line commits in its own short transaction, so a crash leaves
// the file partly posted; running it again posts the rest, because a line's
// clearing_id is claimed in the same transaction as its money.
func Import(ctx context.Context, db DB, raw []byte, fileDate time.Time, cfg Config) (Summary, error) {
	if cfg.OverTolerancePct < 0 {
		return Summary{}, errors.New("over tolerance must not be negative")
	}
	lines, err := Parse(raw, fileDate)
	if err != nil {
		return Summary{}, err
	}
	hash := sha256.Sum256(raw)
	file, inserted, err := insertFile(ctx, db, File{
		ID: newID("clf"), FileDate: fileDate.UTC().Format(DateLayout),
		SHA256: hex.EncodeToString(hash[:]), LineCount: len(lines),
	}, raw)
	if err != nil {
		return Summary{}, err
	}
	ids := clearingIDs(lines)
	if !inserted {
		summary, err := summarise(ctx, db, file, ids)
		if err != nil {
			return Summary{}, err
		}
		summary.Replayed = true
		if summary.Unprocessed == 0 {
			slog.Info("clearing file already imported", "file_id", file.ID, "lines", file.LineCount)
			return summary, nil
		}
	}

	processed := 0
	for _, l := range lines {
		ok, err := postWithRetry(ctx, db, file.ID, l, cfg)
		if err != nil {
			return Summary{}, fmt.Errorf("clearing %s: %w", l.ClearingID, err)
		}
		if ok {
			processed++
		}
	}
	summary, err := summarise(ctx, db, file, ids)
	if err != nil {
		return Summary{}, err
	}
	summary.Replayed, summary.Processed = !inserted, processed
	slog.Info("clearing file imported", "file_id", file.ID, "date", file.FileDate, "lines", file.LineCount,
		"processed", processed, "statuses", summary.Statuses, "flags", summary.Flags,
		"charged_from_held", summary.ChargedFromHeld, "charged_from_available", summary.ChargedFromAvailable,
		"shortfall", summary.Shortfall, "credited", summary.Credited)
	return summary, nil
}

// insertFile stores the file unless its bytes are already stored for its date,
// and returns the stored file. A concurrent insert of the same file waits for
// the first to commit and then finds it.
func insertFile(ctx context.Context, db DB, f File, raw []byte) (File, bool, error) {
	const insert = `
		INSERT INTO clearing_files (id, file_date, sha256, raw, line_count)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (file_date, sha256) DO NOTHING
		RETURNING ` + fileColumns
	stored, err := scanFile(db.QueryRow(ctx, insert, f.ID, f.FileDate, f.SHA256, raw, f.LineCount))
	if err == nil {
		return stored, true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return File{}, false, fmt.Errorf("save clearing file: %w", err)
	}
	stored, err = scanFile(db.QueryRow(ctx, `SELECT `+fileColumns+` FROM clearing_files
		WHERE file_date = $1 AND sha256 = $2`, f.FileDate, f.SHA256))
	if err != nil {
		return File{}, false, fmt.Errorf("find clearing file %s: %w", f.SHA256, err)
	}
	return stored, false, nil
}

// maxAttempts bounds retries of a line that lost a lock wait to a tap or to
// another import of the same line; neither holds its lock for long.
const maxAttempts = 5

func postWithRetry(ctx context.Context, db DB, fileID string, l Line, cfg Config) (bool, error) {
	for attempt := 1; ; attempt++ {
		ok, err := postLine(ctx, db, fileID, l, cfg)
		if err == nil || attempt == maxAttempts || !retryable(err) {
			return ok, err
		}
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-time.After(time.Duration(attempt) * 20 * time.Millisecond):
		}
	}
}

func retryable(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) &&
		(pgErr.Code == pgerrcode.LockNotAvailable || pgErr.Code == pgerrcode.DeadlockDetected)
}

// summarise reads the records of the file's lines, wherever they were
// recorded first, in one grouped query.
func summarise(ctx context.Context, db DB, f File, ids []string) (Summary, error) {
	const q = `
		SELECT file_id = $2, status, COALESCE(flag, ''), count(*), sum(amount)::bigint, sum(charged_from_held)::bigint,
			sum(charged_from_available)::bigint, sum(released_amount)::bigint, sum(shortfall)::bigint,
			sum(credited_amount)::bigint
		FROM clearing_records WHERE clearing_id = ANY($1)
		GROUP BY 1, 2, 3`
	rows, err := db.Query(ctx, q, ids, f.ID)
	if err != nil {
		return Summary{}, fmt.Errorf("summarise clearing file %s: %w", f.ID, err)
	}
	defer rows.Close()
	s := Summary{File: f, Statuses: map[string]int{}, Flags: map[string]int{}}
	recorded := 0
	for rows.Next() {
		var own bool
		var status, flag string
		var n int
		var amount, held, available, released, shortfall, credited int64
		if err := rows.Scan(&own, &status, &flag, &n, &amount, &held, &available, &released, &shortfall, &credited); err != nil {
			return Summary{}, fmt.Errorf("summarise clearing file %s: %w", f.ID, err)
		}
		recorded += n
		if !own {
			s.FromOtherFiles += n
		}
		s.Statuses[status] += n
		if flag != "" {
			for _, one := range strings.Split(flag, ",") {
				s.Flags[one] += n
			}
		}
		s.Amount += amount
		s.ChargedFromHeld += held
		s.ChargedFromAvailable += available
		s.Released += released
		s.Shortfall += shortfall
		s.Credited += credited
	}
	if err := rows.Err(); err != nil {
		return Summary{}, fmt.Errorf("summarise clearing file %s: %w", f.ID, err)
	}
	s.Unprocessed = len(ids) - recorded
	return s, nil
}

func clearingIDs(lines []Line) []string {
	ids := make([]string, len(lines))
	for i, l := range lines {
		ids[i] = l.ClearingID
	}
	return ids
}

const fileColumns = `id, to_char(file_date, 'YYYY-MM-DD'), sha256, line_count, imported_at`

func scanFile(row pgx.Row) (File, error) {
	var f File
	err := row.Scan(&f.ID, &f.FileDate, &f.SHA256, &f.LineCount, &f.ImportedAt)
	f.ImportedAt = f.ImportedAt.UTC()
	return f, err
}

func newID(prefix string) string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return prefix + "_" + hex.EncodeToString(b[:])
}
