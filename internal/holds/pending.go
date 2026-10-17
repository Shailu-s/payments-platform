package holds

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// lockAuthIDSQL serialises a reversal with the approval of the auth_id it
// cancels. Without it each could miss the other: the reversal finds no row
// (the approval has not committed) and parks itself, while the approval finds
// no parked reversal (the reversal has not committed), and the money stays held.
// A row lock cannot do this: the row may not exist yet. Taken first in every
// transaction that takes it, before any row lock, so it cannot deadlock.
const lockAuthIDSQL = `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`

// QueueLockAuthID adds the auth_id lock to batch. It is held until commit.
func QueueLockAuthID(batch *pgx.Batch, authID string) {
	batch.Queue(lockAuthIDSQL, authID)
}

// LockAuthID takes the auth_id lock in tx, until commit.
func LockAuthID(ctx context.Context, tx pgx.Tx, authID string) error {
	if _, err := tx.Exec(ctx, lockAuthIDSQL, authID); err != nil {
		return fmt.Errorf("lock auth_id %s: %w", authID, err)
	}
	return nil
}

// Pending is a reversal that arrived before its authorization was approved.
type Pending struct {
	ReversalID string
	Amount     int64
}

// Park remembers a reversal for an authorization that holds nothing yet. It
// is applied if the authorization is approved later.
func Park(ctx context.Context, tx pgx.Tx, reversalID, authID string, amount int64) error {
	const q = `
		INSERT INTO pending_reversals (reversal_id, auth_id, amount) VALUES ($1, $2, $3)
		ON CONFLICT (reversal_id) DO NOTHING`
	if _, err := tx.Exec(ctx, q, reversalID, authID, amount); err != nil {
		return fmt.Errorf("park reversal %s: %w", reversalID, err)
	}
	return nil
}

// QueuePending adds the read of authID's parked reversals to batch, filling
// *into when the batch runs. Read under the auth_id lock, the answer cannot
// change before commit.
func QueuePending(batch *pgx.Batch, authID string, into *[]Pending) {
	const q = `
		SELECT reversal_id, amount FROM pending_reversals
		WHERE auth_id = $1 AND applied_at IS NULL
		ORDER BY received_at, reversal_id`
	batch.Queue(q, authID).Query(func(rows pgx.Rows) error {
		for rows.Next() {
			var p Pending
			if err := rows.Scan(&p.ReversalID, &p.Amount); err != nil {
				return fmt.Errorf("pending reversals for %s: %w", authID, err)
			}
			*into = append(*into, p)
		}
		if err := rows.Err(); err != nil {
			return fmt.Errorf("pending reversals for %s: %w", authID, err)
		}
		return nil
	})
}

// PendingFor reads authID's parked reversals; the caller holds the auth_id lock.
func PendingFor(ctx context.Context, tx pgx.Tx, authID string) ([]Pending, error) {
	var pending []Pending
	batch := &pgx.Batch{}
	QueuePending(batch, authID, &pending)
	if err := tx.SendBatch(ctx, batch).Close(); err != nil {
		return nil, err
	}
	return pending, nil
}

// ApplyPending releases parked reversals from holder, the row that holds the
// money (an incremental's initial authorization), and marks them applied, in
// the caller's transaction. Call it after the hold is placed.
func ApplyPending(ctx context.Context, tx pgx.Tx, holder string, pending []Pending) error {
	if len(pending) == 0 {
		return nil
	}
	ids := make([]string, 0, len(pending))
	for _, p := range pending {
		if _, err := Release(ctx, tx, holder, p.Amount, ReasonReversal, p.ReversalID); err != nil {
			return err
		}
		ids = append(ids, p.ReversalID)
	}
	const q = `UPDATE pending_reversals SET applied_at = now() WHERE reversal_id = ANY($1)`
	if _, err := tx.Exec(ctx, q, ids); err != nil {
		return fmt.Errorf("mark reversals applied: %w", err)
	}
	return nil
}
