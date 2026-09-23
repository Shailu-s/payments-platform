package worker

import (
	"context"
	"log/slog"
	"time"

	"github.com/Shailu-s/payments-platform/internal/provider"
	"github.com/Shailu-s/payments-platform/internal/transfers"
)

// Provider is what the worker needs from the rail. An interface rather than the
// concrete client so tests can make it time out, reject, or vanish on demand —
// which is the only way to rehearse the outcomes that matter.
type Provider interface {
	Submit(ctx context.Context, req provider.SubmitRequest) (provider.Payment, error)
	Get(ctx context.Context, providerRef string) (provider.Payment, error)
	GetByClientReference(ctx context.Context, clientReference string) (provider.Payment, error)
}

type Config struct {
	// BatchSize is how many transfers one claim takes. Larger batches mean
	// fewer round trips and a longer tail if the process dies mid-batch.
	BatchSize int
	// PollInterval is how long to wait after finding nothing. Work arrives
	// continuously in production, so this only governs an idle queue.
	PollInterval time.Duration
	// RetryBackoff is how far forward a claim pushes next_attempt_at, and so
	// how long a crashed worker's transfers wait before anyone retries them.
	RetryBackoff time.Duration
	// ResolveInterval is how often to ask the rail about transfers parked as
	// unresolved. Parking is only correct because something comes back for it.
	ResolveInterval time.Duration
}

func DefaultConfig() Config {
	return Config{
		BatchSize:       10,
		PollInterval:    time.Second,
		RetryBackoff:    30 * time.Second,
		ResolveInterval: 30 * time.Second,
	}
}

type Worker struct {
	db       DB
	provider Provider
	cfg      Config
}

func New(db DB, p Provider, cfg Config) *Worker {
	return &Worker{db: db, provider: p, cfg: cfg}
}

// Run claims and sends until ctx is cancelled.
//
// Shutdown is graceful from the start: cancelling stops the worker taking new
// work, and the in-flight batch finishes. A worker that cannot stop cleanly
// makes every later test noisier, and phase 5 kills one deliberately.
func (w *Worker) Run(ctx context.Context) {
	slog.InfoContext(ctx, "worker started",
		"batch_size", w.cfg.BatchSize, "poll_interval", w.cfg.PollInterval.String())

	resolveTicker := time.NewTicker(w.cfg.ResolveInterval)
	defer resolveTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			slog.InfoContext(ctx, "worker stopped")
			return
		case <-resolveTicker.C:
			w.resolveParked(ctx)
		default:
		}

		sent, err := w.RunOnce(ctx)
		if err != nil {
			slog.ErrorContext(ctx, "claim failed", "error", err)
		}
		if sent > 0 {
			// More work is probably waiting; do not sleep on a busy queue.
			continue
		}

		select {
		case <-ctx.Done():
			slog.InfoContext(ctx, "worker stopped")
			return
		case <-time.After(w.cfg.PollInterval):
		}
	}
}

// RunOnce claims a batch and sends it, returning how many were sent. Separated
// from Run so tests can drive a single pass deterministically instead of
// racing a loop.
func (w *Worker) RunOnce(ctx context.Context) (int, error) {
	batch, err := Claim(ctx, w.db, w.cfg.BatchSize, w.cfg.RetryBackoff)
	if err != nil {
		return 0, err
	}

	for _, t := range batch {
		// Each send is independent: one transfer failing must not abandon the
		// rest of the batch.
		if err := w.Send(ctx, t); err != nil {
			slog.ErrorContext(ctx, "sending transfer", "transfer_id", t.ID, "error", err)
		}
	}
	return len(batch), nil
}

// resolveParked asks the rail about transfers whose outcome we never learned.
func (w *Worker) resolveParked(ctx context.Context) {
	parked, err := w.claimUnresolved(ctx, w.cfg.BatchSize)
	if err != nil {
		slog.ErrorContext(ctx, "loading unresolved transfers", "error", err)
		return
	}
	for _, t := range parked {
		if err := w.Resolve(ctx, t); err != nil {
			slog.ErrorContext(ctx, "resolving transfer", "transfer_id", t.ID, "error", err)
		}
	}
}

// claimUnresolved reads transfers parked as unresolved. It does not lock them:
// resolving is a read against the rail followed by a guarded update, so two
// workers doing it at once is wasteful rather than wrong.
func (w *Worker) claimUnresolved(ctx context.Context, limit int) ([]transfers.Transfer, error) {
	const q = `
		SELECT id, source_account, destination_account, amount, currency,
		       status, ledger_txn_id, api_key_id, idempotency_key,
		       request_fingerprint, provider_ref, attempt_count,
		       next_attempt_at, last_error, created_at, updated_at
		FROM transfers
		WHERE status = 'unresolved'
		ORDER BY updated_at
		LIMIT $1`

	rows, err := w.db.Query(ctx, q, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []transfers.Transfer
	for rows.Next() {
		var t transfers.Transfer
		if err := rows.Scan(&t.ID, &t.SourceAccount, &t.DestinationAccount, &t.Amount,
			&t.Currency, &t.Status, &t.LedgerTxnID, &t.APIKeyID, &t.IdempotencyKey,
			&t.RequestFingerprint, &t.ProviderRef, &t.AttemptCount,
			&t.NextAttemptAt, &t.LastError, &t.CreatedAt, &t.UpdatedAt); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}
