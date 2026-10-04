package worker

import (
	"context"
	"log/slog"
	"time"

	"github.com/Shailu-s/payments-platform/internal/provider"
	"github.com/Shailu-s/payments-platform/internal/transfers"
)

// Provider supports submission and lookup, including lookup after an unknown outcome.
type Provider interface {
	Submit(ctx context.Context, req provider.SubmitRequest) (provider.Payment, error)
	Get(ctx context.Context, providerRef string) (provider.Payment, error)
	GetByClientReference(ctx context.Context, clientReference string) (provider.Payment, error)
}

type Config struct {
	// BatchSize trades fewer claim queries for more reserved work on a crash.
	BatchSize int
	// PollInterval applies only when no transfers were claimed.
	PollInterval time.Duration
	// RetryBackoff is the reservation duration, including after a worker crash.
	RetryBackoff time.Duration
	// ResolveInterval controls lookups for unresolved transfers.
	ResolveInterval time.Duration
	// HeadStart is how long a new transfer is left to the event consumer before
	// the poller will take it. Zero when the poller is the only sender.
	HeadStart time.Duration
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

// Run claims and sends until ctx is cancelled. Accepted references are recorded
// independently of cancellation; other in-flight operations use ctx.
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

// RunOnce attempts one batch and returns the number claimed, not accepted.
func (w *Worker) RunOnce(ctx context.Context) (int, error) {
	batch, err := Claim(ctx, w.db, w.cfg.BatchSize, w.cfg.RetryBackoff, w.cfg.HeadStart)
	if err != nil {
		return 0, err
	}

	for _, t := range batch {
		// One failed transfer must not abandon the rest of the reserved batch.
		if err := w.Send(ctx, t); err != nil {
			slog.ErrorContext(ctx, "sending transfer", "transfer_id", t.ID, "error", err)
		}
	}
	return len(batch), nil
}

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

// No exclusive claim: duplicate lookups are harmless because transitions are guarded.
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
