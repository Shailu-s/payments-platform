package holds

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/jackc/pgx/v5"
)

// Beginner is a pool, or anything else that starts transactions.
type Beginner interface {
	Begin(ctx context.Context) (pgx.Tx, error)
}

// ExpireBatch releases in full up to limit holds created before
// now - olderThan and returns how many it expired. Holds already released
// (reversed or expired) are not candidates; one a reversal is releasing is
// skipped, not waited for. Concurrent runners expire disjoint holds.
//
// Each hold expires in its own short transaction, claimed with
// FOR UPDATE SKIP LOCKED. One transaction for the whole batch would hold an
// account's balance lock, which every tap on that account needs, until the
// last hold of the batch was done; and it would take several card-day rows
// while holding that balance lock, the opposite of the order a reversal
// takes them in, so the two could deadlock.
func ExpireBatch(ctx context.Context, db Beginner, now time.Time, olderThan time.Duration, limit int) (int, error) {
	cutoff := now.Add(-olderThan)
	expired := 0
	for expired < limit {
		ok, err := expireOne(ctx, db, cutoff)
		if err != nil {
			return expired, err
		}
		if !ok {
			break
		}
		expired++
	}
	return expired, nil
}

func expireOne(ctx context.Context, db Beginner, cutoff time.Time) (bool, error) {
	tx, err := db.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("begin expiry: %w", err)
	}
	defer tx.Rollback(ctx)

	// The partial index holds only open holds, so this stays cheap however
	// many authorizations have been released.
	const claim = `
		SELECT auth_id FROM card_authorizations
		WHERE held_amount > 0 AND expired_at IS NULL AND created_at < $1
		ORDER BY created_at
		LIMIT 1
		FOR UPDATE SKIP LOCKED`
	var authID string
	found := false
	batch := &pgx.Batch{}
	// A tap holding the account's balance lock delays the release; bounded.
	batch.Queue(`SET LOCAL lock_timeout = '1s'`)
	batch.Queue(claim, cutoff).QueryRow(func(row pgx.Row) error {
		err := row.Scan(&authID)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		found = err == nil
		return err
	})
	if err := tx.SendBatch(ctx, batch).Close(); err != nil {
		return false, fmt.Errorf("claim expired hold: %w", err)
	}
	if !found {
		return false, nil
	}
	if _, err := Release(ctx, tx, authID, math.MaxInt64, ReasonExpiry, ""); err != nil {
		return false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit expiry of %s: %w", authID, err)
	}
	return true, nil
}
