// Package api provides HTTP routing, middleware and handlers.
package api

import (
	"context"
	"net/http"
	"time"

	"github.com/Shailu-s/payments-platform/internal/ratelimit"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// DB supports queries and atomic transfer, ledger and outbox writes.
type DB interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Begin(ctx context.Context) (pgx.Tx, error)
}

type Server struct {
	db            DB
	limiter       *ratelimit.Limiter
	webhookSecret []byte
}

const (
	DefaultRateLimit  = 100
	DefaultRateWindow = time.Minute

	// Windows are retained well past their expiry so a sweep can never race a
	// request that is still counting against one.
	sweepInterval  = 5 * time.Minute
	sweepRetention = time.Hour
)

func NewServer(pool *pgxpool.Pool, webhookSecret []byte) *Server {
	return &Server{
		db:            pool,
		limiter:       ratelimit.New(pool, DefaultRateLimit, DefaultRateWindow),
		webhookSecret: append([]byte(nil), webhookSecret...),
	}
}

// StartRateLimitSweeper bounds retained rate-limit windows until ctx is cancelled.
func (s *Server) StartRateLimitSweeper(ctx context.Context) {
	if s.limiter == nil {
		return
	}
	go s.limiter.RunSweeper(ctx, sweepInterval, sweepRetention)
}

// Handler builds the router and middleware stack.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	// Database failure must not make liveness restart a healthy process.
	mux.HandleFunc("GET /healthz", s.handleHealthz)

	// Readiness removes an instance from traffic when the database is unavailable.
	mux.HandleFunc("GET /readyz", s.handleReadyz)

	// Provider callbacks use their signing key, not a customer API key.
	mux.HandleFunc("POST /v1/webhooks/mockbank", s.handleProviderWebhook)

	authenticated := chain(s.routes(), s.withAuth, s.withRateLimit)
	mux.Handle("/v1/", authenticated)

	// Recovery wraps all routes; request IDs must be assigned before logging.
	return chain(mux, withRequestID, withRecovery, withLogging)
}

func (s *Server) routes() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /v1/accounts", s.handleCreateAccount)
	mux.HandleFunc("GET /v1/accounts/{id}", s.handleGetAccount)

	mux.HandleFunc("POST /v1/transfers", s.handleCreateTransfer)
	mux.HandleFunc("GET /v1/transfers/{id}", s.handleGetTransfer)
	mux.HandleFunc("GET /v1/transfers", s.handleListTransfers)
	mux.HandleFunc("GET /v1/reconciliation/runs", s.handleListReconciliationRuns)
	mux.HandleFunc("GET /v1/reconciliation/runs/{id}", s.handleGetReconciliationRun)
	mux.HandleFunc("GET /v1/reconciliation/runs/{id}/findings", s.handleReconciliationFindings)

	// Keep unknown-route errors JSON, unlike the mux's plain-text default.
	mux.HandleFunc("/v1/", func(w http.ResponseWriter, r *http.Request) {
		writeError(w, http.StatusNotFound, CodeNotFound, "no such endpoint: "+r.Method+" "+r.URL.Path)
	})

	return mux
}
