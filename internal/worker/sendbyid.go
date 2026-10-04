package worker

import (
      "context"
      "errors"
      "fmt"

      "github.com/Shailu-s/payments-platform/internal/transfers"
      "github.com/jackc/pgx/v5"
)


func (w *Worker) SendByID(ctx context.Context, id string) (bool, error) {
      const q = `
              UPDATE transfers
              SET attempt_count   = attempt_count + 1,
                  next_attempt_at = now() + $2::interval,
                  updated_at      = now()
              WHERE id = (
                      SELECT id FROM transfers
                      WHERE id = $1
                        AND status = 'processing'
                        AND provider_ref IS NULL
                        AND (next_attempt_at IS NULL OR next_attempt_at <= now())
                      FOR UPDATE SKIP LOCKED
              )
              RETURNING id, source_account, destination_account, amount, currency,
                        status, ledger_txn_id, api_key_id, idempotency_key,
                        request_fingerprint, provider_ref, attempt_count,
                        next_attempt_at, last_error, created_at, updated_at`

      var t transfers.Transfer
      err := w.db.QueryRow(ctx, q, id, w.cfg.RetryBackoff.String()).Scan(
              &t.ID, &t.SourceAccount, &t.DestinationAccount, &t.Amount,
              &t.Currency, &t.Status, &t.LedgerTxnID, &t.APIKeyID, &t.IdempotencyKey,
              &t.RequestFingerprint, &t.ProviderRef, &t.AttemptCount,
              &t.NextAttemptAt, &t.LastError, &t.CreatedAt, &t.UpdatedAt)
      if errors.Is(err, pgx.ErrNoRows) {
              return false, nil
      }
      if err != nil {
              return false, fmt.Errorf("claim transfer %s: %w", id, err)
      }
      return true, w.Send(ctx, t)
}