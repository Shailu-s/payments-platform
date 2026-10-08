package customerwebhooks

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/url"
	"time"

	"github.com/Shailu-s/payments-platform/internal/webhooks"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

const (
	TimestampHeader = "X-Payments-Timestamp"
	SignatureHeader = "X-Payments-Signature"
	EventIDHeader   = "X-Payments-Event-Id"
	StatusPending   = "pending"
	StatusDelivered = "delivered"
	StatusDead      = "dead"
)

type DB interface {
	QueryRow(context.Context, string, ...any) pgx.Row
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
}

type Config struct {
	Timeout      time.Duration
	Lease        time.Duration
	Backoff      time.Duration
	MaxBackoff   time.Duration
	MaxAttempts  int
	BatchSize    int
	PollInterval time.Duration
	Now          func() time.Time
}

func DefaultConfig() Config {
	return Config{
		Timeout: 10 * time.Second, Lease: 30 * time.Second,
		Backoff: time.Second, MaxBackoff: 5 * time.Minute,
		MaxAttempts: 6, BatchSize: 20, PollInterval: time.Second,
		Now: time.Now,
	}
}

type Worker struct {
	db       DB
	endpoint string
	secret   []byte
	cfg      Config
	client   *http.Client
}

func (w *Worker) Run(ctx context.Context) {
	for ctx.Err() == nil {
		n, err := w.RunOnce(ctx)
		if err != nil {
			slog.ErrorContext(ctx, "customer delivery batch failed", "error", err)
		} else if n > 0 {
			continue
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(w.cfg.PollInterval):
		}
	}
}

func New(db DB, endpoint string, secret []byte, cfg Config) (*Worker, error) {
	u, err := url.Parse(endpoint)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return nil, errors.New("customer webhook endpoint must be an HTTP(S) URL without credentials, query or fragment")
	}
	if len(secret) < webhooks.MinSecretBytes {
		return nil, errors.New("customer webhook signing key must contain at least 32 bytes")
	}
	if cfg.Timeout <= 0 || cfg.Lease <= cfg.Timeout || cfg.Backoff <= 0 || cfg.MaxBackoff < cfg.Backoff ||
		cfg.MaxAttempts < 1 || cfg.MaxAttempts > 20 || cfg.BatchSize < 1 || cfg.PollInterval <= 0 || cfg.Now == nil {
		return nil, errors.New("invalid customer webhook worker configuration")
	}
	return &Worker{
		db: db, endpoint: endpoint, secret: append([]byte(nil), secret...), cfg: cfg,
		client: &http.Client{
			Timeout: cfg.Timeout,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}, nil
}
