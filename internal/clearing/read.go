package clearing

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

var ErrNotFound = errors.New("clearing file not found")

// Record is a clearing line as posted.
type Record struct {
	Line
	FileID               string  `json:"file_id"`
	Status               string  `json:"status"`
	ChargedFromHeld      int64   `json:"charged_from_held"`
	ChargedFromAvailable int64   `json:"charged_from_available"`
	Released             int64   `json:"released"`
	Shortfall            int64   `json:"shortfall"`
	Credited             int64   `json:"credited"`
	Flag                 *string `json:"flag"`
	LedgerTxnID          *string `json:"ledger_txn_id"`
}

func ListFiles(ctx context.Context, db DB, limit, offset int) ([]File, error) {
	if limit < 1 || limit > 100 || offset < 0 {
		return nil, errors.New("invalid clearing page")
	}
	rows, err := db.Query(ctx, `SELECT `+fileColumns+` FROM clearing_files
		ORDER BY file_date DESC, imported_at DESC, id LIMIT $1 OFFSET $2`, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("list clearing files: %w", err)
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (File, error) { return scanFile(row) })
}

// Inspect summarises a stored file from its own bytes, as Import would.
func Inspect(ctx context.Context, db DB, fileID string) (Summary, error) {
	var f File
	var raw []byte
	err := db.QueryRow(ctx, `SELECT `+fileColumns+`, raw FROM clearing_files WHERE id = $1`, fileID).
		Scan(&f.ID, &f.FileDate, &f.SHA256, &f.LineCount, &f.ImportedAt, &raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return Summary{}, ErrNotFound
	}
	if err != nil {
		return Summary{}, fmt.Errorf("get clearing file %s: %w", fileID, err)
	}
	f.ImportedAt = f.ImportedAt.UTC()
	day, err := time.Parse(DateLayout, f.FileDate)
	if err != nil {
		return Summary{}, err
	}
	lines, err := Parse(raw, day)
	if err != nil {
		return Summary{}, fmt.Errorf("stored clearing file %s: %w", fileID, err)
	}
	return summarise(ctx, db, f, clearingIDs(lines))
}

// Records lists the lines a file recorded, in file order, optionally only
// those carrying flag.
func Records(ctx context.Context, db DB, fileID, flag string, limit, offset int) ([]Record, error) {
	if limit < 1 || limit > 100 || offset < 0 {
		return nil, errors.New("invalid clearing page")
	}
	rows, err := db.Query(ctx, `
		SELECT clearing_id, COALESCE(auth_id, ''), card_id, kind, amount, currency, merchant_id, cleared_at,
			file_id, status, charged_from_held, charged_from_available, released_amount, shortfall,
			credited_amount, flag, ledger_txn_id
		FROM clearing_records
		WHERE file_id = $1 AND ($2 = '' OR $2 = ANY(string_to_array(flag, ',')))
		ORDER BY cleared_at, clearing_id LIMIT $3 OFFSET $4`, fileID, flag, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("clearing records of %s: %w", fileID, err)
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (Record, error) {
		var r Record
		err := row.Scan(&r.ClearingID, &r.AuthID, &r.CardID, &r.Kind, &r.Amount, &r.Currency, &r.MerchantID,
			&r.ClearedAt, &r.FileID, &r.Status, &r.ChargedFromHeld, &r.ChargedFromAvailable, &r.Released,
			&r.Shortfall, &r.Credited, &r.Flag, &r.LedgerTxnID)
		r.ClearedAt = r.ClearedAt.UTC()
		return r, err
	})
}

// Download fetches the network's clearing file for date, bounded in size.
func Download(ctx context.Context, client *http.Client, baseURL string, date time.Time) ([]byte, error) {
	u := strings.TrimRight(baseURL, "/") + "/clearing?date=" + url.QueryEscape(date.UTC().Format(DateLayout))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("download clearing file: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("clearing file download returned HTTP %d", resp.StatusCode)
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, MaxFileBytes+1))
	if err != nil {
		return nil, fmt.Errorf("download clearing file: %w", err)
	}
	if len(raw) > MaxFileBytes {
		return nil, fmt.Errorf("%w: exceeds %d bytes", ErrInvalidFile, MaxFileBytes)
	}
	return raw, nil
}

// ValidFlag reports whether flag is one the importer sets.
func ValidFlag(flag string) bool {
	switch flag {
	case FlagOverTolerance, FlagClearedAfterExpiry, FlagClearedAfterReversal, FlagDuplicateClearing,
		FlagUnauthorizedClearing, FlagUnknownCard, FlagShortfall, FlagNothingHeld, FlagCardMismatch,
		FlagUnmatchedRefund, FlagRefundExceedsPurchase:
		return true
	}
	return false
}
