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

// Send submits a transfer and records its outcome. Unknown outcomes are parked:
// retrying could pay twice, while reversing could refund a successful payment.
// Provider deduplication alone is insufficient if its retention window expires.
func (w *Worker) Send(ctx context.Context, t transfers.Transfer) error {
	payment, err := w.provider.Submit(ctx, provider.SubmitRequest{
		// A stable reference lets the rail recognise retries.
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
		return w.recordRejected(ctx, t, err)

	case errors.Is(err, provider.ErrUnknown):
		return w.recordUnresolved(ctx, t, err)

	case errors.Is(err, provider.ErrRetryable):
		// The rail did not process the request; the claim's backoff permits retry.
		return w.recordRetryable(ctx, t, err)

	default:
		// Unclassified errors do not prove that no money moved.
		return w.recordUnresolved(ctx, t, err)
	}
}

func (w *Worker) recordAccepted(ctx context.Context, t transfers.Transfer, p provider.Payment) error {
	// Shutdown must not discard an accepted reference and make the payment
	// claimable again. This protects cancellation, not a process crash.
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
		// A concurrent sender or resolving lookup already stored the reference.
		slog.WarnContext(ctx, "transfer already had a provider reference",
			"transfer_id", t.ID, "provider_ref", p.ProviderRef)
		return nil
	}

	slog.InfoContext(ctx, "transfer accepted by provider",
		"transfer_id", t.ID, "provider_ref", p.ProviderRef)
	return nil
}

func (w *Worker) recordRejected(ctx context.Context, t transfers.Transfer, cause error) error {
	if err := transfers.Fail(ctx, w.db, t.ID, cause.Error()); err != nil {
		return err
	}
	return nil
}

// Unresolved transfers wait for a lookup or webhook, not another submission.
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

func (w *Worker) recordRetryable(ctx context.Context, t transfers.Transfer, cause error) error {
	const q = `UPDATE transfers SET last_error = $1, updated_at = now() WHERE id = $2`

	if _, err := w.db.Exec(ctx, q, truncate(cause.Error(), 500), t.ID); err != nil {
		return fmt.Errorf("record retryable %s: %w", t.ID, err)
	}

	slog.InfoContext(ctx, "provider temporarily unavailable, will retry",
		"transfer_id", t.ID, "attempt", t.AttemptCount, "error", cause)
	return nil
}

// Resolve looks up an unresolved transfer's outcome when no webhook has resolved it.
func (w *Worker) Resolve(ctx context.Context, t transfers.Transfer) error {
	var payment provider.Payment
	var err error

	if t.ProviderRef != nil && *t.ProviderRef != "" {
		payment, err = w.provider.Get(ctx, *t.ProviderRef)
	} else {
		// A timed-out submission may not have returned a provider reference.
		payment, err = w.provider.GetByClientReference(ctx, t.ID)
	}

	if errors.Is(err, provider.ErrNotFound) {
		// Requeue on this rail's not-found response; retain the same client reference.
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
		slog.WarnContext(ctx, "could not resolve transfer", "transfer_id", t.ID, "error", err)
		return nil
	}

	switch payment.Status {
	case provider.StatusSettled:
		return w.settleFromLookup(ctx, t, payment)

	case provider.StatusFailed:
		reason := "provider reported failed"
		if payment.FailureReason != nil {
			reason = *payment.FailureReason
		}
		if err := w.markProcessingAgain(ctx, t); err != nil {
			return err
		}
		return w.recordRejected(ctx, t, fmt.Errorf("%w: %s", provider.ErrRejected, reason))

	default:
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

// Restore processing status before applying the resolved failure.
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
