// Package worker sends accepted transfers to the payment rail.
//
// It is a separate process from the API, which is what makes it the first part
// of this system where two things can crash independently. Everything awkward
// here comes from that.
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

// Claim takes up to limit transfers that are due to be sent, marking them as
// attempted so no other worker picks them up.
//
// FOR UPDATE SKIP LOCKED is the whole trick, and it is worth being able to
// draw. Without SKIP LOCKED a second worker BLOCKS on the rows the first has
// locked: it waits, takes the same work, and you have one worker with extra
// steps and extra latency. With it, the second worker steps over the locked
// rows and claims different ones, so adding workers adds throughput.
//
// The lock is released by the COMMIT at the end of this function, not held for
// the length of the provider call. Holding a row lock across a network call to
// a third party is how one slow vendor stops your whole queue.
func Claim(ctx context.Context, db DB, limit int, backoff time.Duration) ([]transfers.Transfer, error) {
	const q = `
		UPDATE transfers
		SET attempt_count   = attempt_count + 1,
		    next_attempt_at = now() + $2::interval,
		    updated_at      = now()
		WHERE id IN (
			SELECT id FROM transfers
			WHERE status = 'processing'
			  AND provider_ref IS NULL
			  AND (next_attempt_at IS NULL OR next_attempt_at <= now())
			ORDER BY next_attempt_at NULLS FIRST, created_at
			FOR UPDATE SKIP LOCKED
			LIMIT $1
		)
		RETURNING id, source_account, destination_account, amount, currency,
		          status, ledger_txn_id, api_key_id, idempotency_key,
		          request_fingerprint, provider_ref, attempt_count,
		          next_attempt_at, last_error, created_at, updated_at`

	// next_attempt_at is pushed forward as part of claiming, so a worker that
	// dies mid-send does not leave the transfer claimable again immediately —
	// the row becomes available when the backoff expires, and the crash costs
	// one delay rather than a tight retry loop against the rail.
	rows, err := db.Query(ctx, q, limit, backoff.String())
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
