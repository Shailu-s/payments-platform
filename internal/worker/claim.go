// Package worker sends accepted transfers to the payment rail.
package worker

import (
	"context"
	"fmt"
	"time"

	"github.com/Shailu-s/payments-platform/internal/transfers"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type DB interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Begin(ctx context.Context) (pgx.Tx, error)
}

// Claim reserves due transfers until their backoff expires. Row locks prevent
// concurrent claims; SKIP LOCKED lets other workers claim different rows.
// On a pool, locks end with the statement, before provider calls.
// headStart delays only first attempts so the event consumer gets priority.
func Claim(ctx context.Context, db DB, limit int, backoff, headStart time.Duration) ([]transfers.Transfer, error) {
	const q = `
		UPDATE transfers
		SET attempt_count   = attempt_count + 1,
		    next_attempt_at = now() + $2::interval,
		    updated_at      = now()
		WHERE id IN (
			SELECT id FROM transfers
			WHERE status = 'processing'
			  AND provider_ref IS NULL
			  AND (next_attempt_at <= now()
			       OR (next_attempt_at IS NULL AND created_at <= now() - $3::interval))
			ORDER BY next_attempt_at NULLS FIRST, created_at
			FOR UPDATE SKIP LOCKED
			LIMIT $1
		)
		RETURNING id, source_account, destination_account, amount, currency,
		          status, ledger_txn_id, api_key_id, idempotency_key,
		          request_fingerprint, provider_ref, attempt_count,
		          next_attempt_at, last_error, created_at, updated_at`

	// Persist backoff in the claim so a crash cannot cause an immediate retry loop.
	rows, err := db.Query(ctx, q, limit, backoff.String(), headStart.String())
	if err != nil {
		return nil, fmt.Errorf("claim transfers: %w", err)
	}
	defer rows.Close()

	var claimed []transfers.Transfer
	for rows.Next() {
		var t transfers.Transfer
		if err := rows.Scan(&t.ID, &t.SourceAccount, &t.DestinationAccount, &t.Amount,
			&t.Currency, &t.Status, &t.LedgerTxnID, &t.APIKeyID, &t.IdempotencyKey,
			&t.RequestFingerprint, &t.ProviderRef, &t.AttemptCount,
			&t.NextAttemptAt, &t.LastError, &t.CreatedAt, &t.UpdatedAt); err != nil {
			return nil, fmt.Errorf("claim transfers: %w", err)
		}
		claimed = append(claimed, t)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("claim transfers: %w", err)
	}
	return claimed, nil
}
