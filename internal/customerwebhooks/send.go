package customerwebhooks

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/Shailu-s/payments-platform/internal/webhooks"
	"github.com/jackc/pgx/v5"
)

func (w *Worker) retryDelay(attempt int) time.Duration {
	delay := w.cfg.Backoff
	for i := 1; i < attempt; i++ {
		if delay >= w.cfg.MaxBackoff/2 {
			return w.cfg.MaxBackoff
		}
		delay *= 2
	}
	return min(delay, w.cfg.MaxBackoff)
}

func (w *Worker) RunOnce(ctx context.Context) (int, error) {
	if _, err := w.db.Exec(ctx, `
		UPDATE customer_webhook_deliveries
		SET status = 'dead', last_error = 'final delivery lease expired; outcome unknown', updated_at = $1
		WHERE status = 'pending' AND attempt_count >= $2 AND next_attempt_at <= $1`, w.cfg.Now(), w.cfg.MaxAttempts); err != nil {
		return 0, fmt.Errorf("expire customer delivery leases: %w", err)
	}
	processed := 0
	for processed < w.cfg.BatchSize {
		delivery, err := w.claim(ctx)
		if errors.Is(err, pgx.ErrNoRows) {
			return processed, nil
		}
		if err != nil {
			return processed, fmt.Errorf("claim customer delivery: %w", err)
		}
		processed++
		if err := w.send(ctx, delivery); err != nil {
			return processed, err
		}
	}
	return processed, nil
}

func (w *Worker) send(ctx context.Context, d Delivery) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, d.Endpoint, bytes.NewReader(d.Payload))
	if err != nil {
		return fmt.Errorf("build customer delivery %s", d.EventID)
	}
	timestamp := strconv.FormatInt(w.cfg.Now().Unix(), 10)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(EventIDHeader, d.EventID)
	req.Header.Set(TimestampHeader, timestamp)
	req.Header.Set(SignatureHeader, webhooks.Sign(w.secret, timestamp, d.Payload))
	status, reason := StatusPending, "transport failure"
	var httpStatus *int
	resp, err := w.client.Do(req)
	if err == nil {
		code := resp.StatusCode
		httpStatus = &code
		io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		resp.Body.Close()
		if code >= 200 && code < 300 {
			status, reason = StatusDelivered, ""
		} else {
			reason = fmt.Sprintf("HTTP status %d", code)
			if code < 500 && code != http.StatusRequestTimeout && code != http.StatusTooManyRequests {
				status = StatusDead
			}
		}
	}
	if status == StatusPending && d.AttemptCount >= w.cfg.MaxAttempts {
		status = StatusDead
		reason = "attempt limit reached: " + reason
	}
	now := w.cfg.Now()
	next := now
	if status == StatusPending {
		next = now.Add(w.retryDelay(d.AttemptCount))
	}
	_, err = w.db.Exec(ctx, `
		UPDATE customer_webhook_deliveries
		SET status = $4, last_http_status = $5, last_error = NULLIF($6, ''), updated_at = $3,
		    delivered_at = CASE WHEN $4 = 'delivered' THEN $3::timestamptz ELSE NULL END,
		    next_attempt_at = $7
		WHERE event_id = $1 AND attempt_count = $2 AND status = 'pending'`,
		d.EventID, d.AttemptCount, now, status, httpStatus, reason, next)
	if err != nil {
		return fmt.Errorf("save customer delivery %s: %w", d.EventID, err)
	}
	return nil
}
