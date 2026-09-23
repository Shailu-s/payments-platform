// Package api is the HTTP layer: routing, middleware and handlers.
//
// net/http with Go 1.22 routing patterns rather than chi or gin. A framework
// would be defensible, but stdlib means every line of routing is ours and there
// is nothing to explain away.
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

// DB is what the api layer needs from a database: queries, and the ability to
// start a transaction. Narrow on purpose — POST /transfers writes a transfer
// and its ledger entries in one transaction, and phase 5 adds an outbox row to
// that same one.
type DB interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Begin(ctx context.Context) (pgx.Tx, error)
}

type Server struct {
	db      DB
	limiter *ratelimit.Limiter
}

// Requests per key per window. Small on purpose: this is a portfolio system,
// and a limit nobody can reach proves nothing.
const (
	DefaultRateLimit  = 100
	DefaultRateWindow = time.Minute

	// Windows are retained well past their expiry so a sweep can never race a
	// request that is still counting against one.
	sweepInterval  = 5 * time.Minute
	sweepRetention = time.Hour
)

func NewServer(pool *pgxpool.Pool) *Server {
	return &Server{
		db:      pool,
		limiter: ratelimit.New(pool, DefaultRateLimit, DefaultRateWindow),
	}
}

// StartRateLimitSweeper removes rolled-over rate limit windows in the
// background until ctx is cancelled. Without it the table grows by one row per
// key per window forever.
func (s *Server) StartRateLimitSweeper(ctx context.Context) {
	if s.limiter == nil {
		return
	}
	go s.limiter.RunSweeper(ctx, sweepInterval, sweepRetention)
}

// Handler builds the router. Routes are declared with their method, so a GET to
// a POST-only path is a 405 from the mux rather than a handler that has to
// check.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	// Liveness is deliberately unauthenticated and does not touch the database:
	// a health check that fails when Postgres is down tells an orchestrator to
	// restart a process that is working fine.
	mux.HandleFunc("GET /healthz", s.handleHealthz)

	// Readiness does check the database, because "ready to serve traffic" and
	// "alive" are different questions with different remedies.
	mux.HandleFunc("GET /readyz", s.handleReadyz)

	// Auth before the rate limit: the limit is per key, so there is nothing to
	// count against until the caller is known.
	// The provider calls this, not a customer, so it is outside the api-key
	// chain: the rail has no key of ours. Phase 6 authenticates it properly
	// with a signature.
	mux.HandleFunc("POST /v1/webhooks/mockbank", s.handleProviderWebhook)

	authenticated := chain(s.routes(), s.withAuth, s.withRateLimit)
	mux.Handle("/v1/", authenticated)

	// Applied outermost first. Recovery wraps everything so a panic anywhere
	// still produces a response; request id is outside logging so every line
	// carries it.
	return chain(mux, withRequestID, withRecovery, withLogging)
}

// routes holds everything under /v1, all of which requires a key.
func (s *Server) routes() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /v1/accounts", s.handleCreateAccount)
	mux.HandleFunc("GET /v1/accounts/{id}", s.handleGetAccount)

	mux.HandleFunc("POST /v1/transfers", s.handleCreateTransfer)
	mux.HandleFunc("GET /v1/transfers/{id}", s.handleGetTransfer)
	mux.HandleFunc("GET /v1/transfers", s.handleListTransfers)

	// Anything else under /v1 is a 404 in the house error shape rather than
	// net/http's plain-text default, so a caller parsing JSON does not get a
	// surprise content type on a typo.
	mux.HandleFunc("/v1/", func(w http.ResponseWriter, r *http.Request) {
		writeError(w, http.StatusNotFound, CodeNotFound, "no such endpoint: "+r.Method+" "+r.URL.Path)
	})

	return mux
}
