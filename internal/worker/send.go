package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/Shailu-s/payments-platform/internal/provider"
	"github.com/Shailu-s/payments-platform/internal/transfers"
)

// Send submits one transfer to the rail and records what happened.
//
// There are three outcomes, not two, and the third is the hard one:
//
//	accepted   → store provider_ref, stay 'processing', wait for the webhook
//	rejected   → 'failed' + a REVERSING ledger transaction
//	unknown    → 'unresolved'. Do not retry. Do not fail.
//
// The unknown case is the most expensive problem in the system. Our call timed
// out, so the rail may have moved the money or may not have, and we cannot tell
// which. Retrying risks paying twice; failing risks telling everyone a payment
// failed that actually succeeded, with the money gone and the books disagreeing.
// So it parks, and only a webhook or a lookup against the rail resolves it.
//
// It is tempting to retry anyway because MockBank deduplicates by
// client_reference. The contract warns against exactly that: a rail that loses
// its deduplication table, or dedupes only within a window, is a rail that pays
// twice. The guarantee cannot depend on the provider being well behaved.
func (w *Worker) Send(ctx context.Context, t transfers.Transfer) error {
	payment, err := w.provider.Submit(ctx, provider.SubmitRequest{
		// Our transfer id is the deduplication key. It is stable across
		// retries of the same transfer, which is what makes the rail's
		// deduplication reachable at all.
		ClientReference: t.ID,
		Amount:          t.Amount,
		Currency:        t.Currency,
		Source:          t.SourceAccount,
		Destination:     t.DestinationAccount,
	})

	switch {
	case err == nil:
		return w.recordAccepted(ctx, t, payment)

	case errors.Is(err, provider.ErrRejected), errors.Is(err, provider.ErrConflict):
		// The rail considered it and said no. Terminal: give the money back.
		return w.recordRejected(ctx, t, err)

	case errors.Is(err, provider.ErrUnknown):
		// We do not know. Say so, and stop touching it.
		return w.recordUnresolved(ctx, t, err)

	case errors.Is(err, provider.ErrRetryable):
		// The rail refused to look at it, so nothing happened and retrying is
		// safe. Left at 'processing' with its backoff already set by Claim.
		return w.recordRetryable(ctx, t, err)

	default:
		// An error the client did not classify. Treated as unknown rather than
		// retryable, because assuming "nothing happened" is a guess about money.
		return w.recordUnresolved(ctx, t, err)
	}
}

// recordAccepted stores the rail's reference and leaves the transfer
// processing. The money has left the source and sits in settlement; the
// destination is credited when the webhook confirms.
func (w *Worker) recordAccepted(ctx context.Context, t transfers.Transfer, p provider.Payment) error {
	// Deliberately NOT the caller's context.
	//
	// The rail has accepted the payment. If shutdown cancels this write, we
	// lose a reference we already hold: the transfer still has provider_ref
	// NULL, the next worker claims it and sends it again, and the vendor is
	// paid twice. Measured — a worker killed mid-batch produced exactly that,
	// intermittently, which is the worst kind.
	//
	// So once the rail has said yes, recording it is not optional. A shutdown
	// waits the extra few milliseconds.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()

	const q = `
		UPDATE transfers
		SET provider_ref = $1, last_error = NULL, updated_at = now()
		WHERE id = $2 AND provider_ref IS NULL`

	tag, err := w.db.Exec(ctx, q, p.ProviderRef, t.ID)
	if err != nil {
		return fmt.Errorf("record accepted %s: %w", t.ID, err)
	}
	if tag.RowsAffected() == 0 {
		// Another worker stored a reference first, or a webhook already
		// settled it. Not an error: the guard is what makes this idempotent.
		slog.WarnContext(ctx, "transfer already had a provider reference",
			"transfer_id", t.ID, "provider_ref", p.ProviderRef)
		return nil
	}

	slog.InfoContext(ctx, "transfer accepted by provider",
		"transfer_id", t.ID, "provider_ref", p.ProviderRef)
	return nil
}

// recordRejected marks the transfer failed and returns the money.
//
// The original entries are never touched. Guarantee 6: ledger entries are not
// modified or deleted, so putting money back means writing NEW entries that
// move it in the opposite direction. One transfer then has two ledger
// transactions, which is exactly why transfers and ledger_transactions are
// separate tables.
func (w *Worker) recordRejected(ctx context.Context, t transfers.Transfer, cause error) error {
	if err := transfers.Fail(ctx, w.db, t.ID, cause.Error()); err != nil {
		return err
	}
	return nil
}

// recordUnresolved parks a transfer whose outcome we do not know.
//
// next_attempt_at is cleared: this transfer is not waiting for a retry, it is
// waiting for the truth. Leaving a time on it would invite a future claim to
// send it again, which is the double payment we are avoiding.
func (w *Worker) recordUnresolved(ctx context.Context, t transfers.Transfer, cause error) error {
	const q = `
		UPDATE transfers
		SET status = 'unresolved', last_error = $1, next_attempt_at = NULL, updated_at = now()
		WHERE id = $2 AND status = 'processing'`

	if _, err := w.db.Exec(ctx, q, truncate(cause.Error(), 500), t.ID); err != nil {
		return fmt.Errorf("record unresolved %s: %w", t.ID, err)
	}

	slog.WarnContext(ctx, "transfer outcome unknown, parked as unresolved",
		"transfer_id", t.ID, "error", cause,
		"note", "not retried and not failed: the provider may or may not have moved the money")
	return nil
}

// recordRetryable leaves the transfer processing. Claim already pushed its
// next_attempt_at forward, so it comes back on its own.
func (w *Worker) recordRetryable(ctx context.Context, t transfers.Transfer, cause error) error {
	const q = `UPDATE transfers SET last_error = $1, updated_at = now() WHERE id = $2`

	if _, err := w.db.Exec(ctx, q, truncate(cause.Error(), 500), t.ID); err != nil {
		return fmt.Errorf("record retryable %s: %w", t.ID, err)
	}

	slog.InfoContext(ctx, "provider temporarily unavailable, will retry",
		"transfer_id", t.ID, "attempt", t.AttemptCount, "error", cause)
	return nil
}

// Resolve asks the rail what happened to a transfer we parked as unresolved.
//
// This is the other half of the unknown case: parking is only correct because
// something later comes back for it. A webhook usually arrives first; this is
// what rescues the transfers where it does not.
func (w *Worker) Resolve(ctx context.Context, t transfers.Transfer) error {
	var payment provider.Payment
	var err error

	if t.ProviderRef != nil && *t.ProviderRef != "" {
		payment, err = w.provider.Get(ctx, *t.ProviderRef)
	} else {
		// We never learned a reference, which is the whole reason lookup by
		// client_reference exists in the contract.
		payment, err = w.provider.GetByClientReference(ctx, t.ID)
	}

	if errors.Is(err, provider.ErrNotFound) {
		// The rail has no record, so the instruction never arrived. Safe to
		// send again: put it back in the queue rather than leaving it parked.
		const q = `
			UPDATE transfers
			SET status = 'processing', next_attempt_at = now(), updated_at = now()
			WHERE id = $1 AND status = 'unresolved'`
		if _, err := w.db.Exec(ctx, q, t.ID); err != nil {
			return fmt.Errorf("requeue %s: %w", t.ID, err)
		}
		slog.InfoContext(ctx, "provider has no record, transfer requeued", "transfer_id", t.ID)
		return nil
	}
	if err != nil {
		// Still cannot tell. Leave it parked; asking again later is free.
		slog.WarnContext(ctx, "could not resolve transfer", "transfer_id", t.ID, "error", err)
		return nil
	}

	switch payment.Status {
	case provider.StatusSettled:
		// It did happen. Store the reference and let the settlement path
		// credit the destination, exactly as a webhook would.
		return w.settleFromLookup(ctx, t, payment)

	case provider.StatusFailed:
		// It did not happen, and now we know. Return the money.
		reason := "provider reported failed"
		if payment.FailureReason != nil {
			reason = *payment.FailureReason
		}
		if err := w.markProcessingAgain(ctx, t); err != nil {
			return err
		}
		return w.recordRejected(ctx, t, fmt.Errorf("%w: %s", provider.ErrRejected, reason))

	default:
		// Still in flight at the rail. It was accepted, so record the
		// reference and put it back to waiting for the webhook.
		const q = `
			UPDATE transfers
			SET status = 'processing', provider_ref = COALESCE(provider_ref, $1),
			    next_attempt_at = NULL, updated_at = now()
			WHERE id = $2 AND status = 'unresolved'`
		if _, err := w.db.Exec(ctx, q, payment.ProviderRef, t.ID); err != nil {
			return fmt.Errorf("resolve %s to processing: %w", t.ID, err)
		}
		slog.InfoContext(ctx, "transfer is still processing at the provider",
			"transfer_id", t.ID, "provider_ref", payment.ProviderRef)
		return nil
	}
}

// markProcessingAgain moves an unresolved transfer back to processing so the
// terminal paths, which are all guarded on 'processing', can act on it.
func (w *Worker) markProcessingAgain(ctx context.Context, t transfers.Transfer) error {
	const q = `UPDATE transfers SET status = 'processing' WHERE id = $1 AND status = 'unresolved'`
	if _, err := w.db.Exec(ctx, q, t.ID); err != nil {
		return fmt.Errorf("unpark %s: %w", t.ID, err)
	}
	return nil
}

func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max]
}
