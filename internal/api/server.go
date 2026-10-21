// Package api provides HTTP routing, middleware and handlers.
package api

import (
	"context"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/Shailu-s/payments-platform/internal/auth"
	"github.com/Shailu-s/payments-platform/internal/cards"
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
	db DB
	// hot serves the money routes: it refuses rather than queue for a
	// connection. Nil falls back to db.
	hot DB
	// keys caches verified api keys; nil verifies every request against db.
	keys    *auth.Cache
	limiter *ratelimit.Limiter
	gate    *accountGate
	// admit holds one token per request in flight on the money routes; nil
	// admits everything.
	admit         chan struct{}
	webhookSecret []byte
	networkSecret []byte
	httpRequests  [5]atomic.Uint64
	// Time in the rate limiter's Allow, and in the whole request from Allow
	// on: their ratio is the limiter's share of a rate-limited request.
	rateLimitChecks  atomic.Uint64
	rateLimitNanos   atomic.Uint64
	rateLimitedNanos atomic.Uint64
	// Requests served unchecked because the limiter's store was unreachable.
	rateLimitFailOpen atomic.Uint64
}

const (
	DefaultRateLimit  = 100
	DefaultRateWindow = time.Minute

	// Windows are retained well past their expiry so a sweep can never race a
	// request that is still counting against one.
	sweepInterval  = 5 * time.Minute
	sweepRetention = time.Hour

	// Every /v1/ request, database calls included, gives up after this rather
	// than queueing without bound.
	requestTimeout = 5 * time.Second
)

func NewServer(pool *pgxpool.Pool, webhookSecret, networkSecret []byte) *Server {
	return &Server{
		db:            pool,
		hot:           newBoundedPool(pool, poolAcquireWait),
		keys:          auth.NewCache(pool, auth.DefaultCacheTTL, auth.DefaultCacheSize),
		limiter:       ratelimit.New(pool, DefaultRateLimit, DefaultRateWindow),
		gate:          newAccountGate(maxInFlightPerAccount, accountSlotWait),
		admit:         make(chan struct{}, admissionPerConn*int(pool.Config().MaxConns)),
		webhookSecret: append([]byte(nil), webhookSecret...),
		networkSecret: append([]byte(nil), networkSecret...),
	}
}

// hotDB is the database for taps, advices, reversals and transfer creations.
func (s *Server) hotDB() DB {
	if s.hot != nil {
		return s.hot
	}
	return s.db
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
	mux.HandleFunc("GET /metrics", s.handleMetrics)

	// Database failure must not make liveness restart a healthy process.
	mux.HandleFunc("GET /healthz", s.handleHealthz)

	// Readiness removes an instance from traffic when the database is unavailable.
	mux.HandleFunc("GET /readyz", s.handleReadyz)

	// Provider callbacks use their signing key, not a customer API key.
	mux.HandleFunc("POST /v1/webhooks/mockbank", s.handleProviderWebhook)

	// The card network signs with its own secret and answers to its own
	// deadline. Admission comes first, so excess is refused before the body
	// is even read.
	mux.Handle("POST /network/authorizations", s.withAdmission(http.HandlerFunc(s.handleNetworkAuthorization)))
	mux.Handle("POST /network/advices", s.withAdmission(http.HandlerFunc(s.handleNetworkAdvice)))
	mux.Handle("POST /network/reversals", s.withAdmission(http.HandlerFunc(s.handleNetworkReversal)))

	authenticated := chain(s.routes(), withDeadline, s.withAuth, s.withRateLimit)
	mux.Handle("/v1/", authenticated)
	// Admitted before auth and the rate limit, which both use the database.
	mux.Handle("POST /v1/transfers", s.withAdmission(authenticated))

	// Recovery wraps all routes; request IDs must be assigned before logging.
	return chain(mux, withRequestID, s.withMetrics, withRecovery, withLogging)
}

func (s *Server) routes() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /v1/accounts", s.handleCreateAccount)
	mux.HandleFunc("GET /v1/accounts/{id}", s.handleGetAccount)

	mux.HandleFunc("POST /v1/transfers", s.handleCreateTransfer)
	mux.HandleFunc("GET /v1/transfers/{id}", s.handleGetTransfer)
	mux.HandleFunc("GET /v1/transfers", s.handleListTransfers)

	mux.HandleFunc("POST /v1/cards", s.handleIssueCard)
	mux.HandleFunc("GET /v1/cards/{id}", s.handleGetCard)
	mux.HandleFunc("POST /v1/cards/{id}/freeze", s.handleCardTransition(cards.Freeze))
	mux.HandleFunc("POST /v1/cards/{id}/unfreeze", s.handleCardTransition(cards.Unfreeze))
	mux.HandleFunc("POST /v1/cards/{id}/close", s.handleCardTransition(cards.Close))

	mux.HandleFunc("GET /v1/reconciliation/runs", s.handleListReconciliationRuns)
	mux.HandleFunc("GET /v1/reconciliation/runs/{id}", s.handleGetReconciliationRun)
	mux.HandleFunc("GET /v1/reconciliation/runs/{id}/findings", s.handleReconciliationFindings)

	// Keep unknown-route errors JSON, unlike the mux's plain-text default.
	mux.HandleFunc("/v1/", func(w http.ResponseWriter, r *http.Request) {
		writeError(w, http.StatusNotFound, CodeNotFound, "no such endpoint: "+r.Method+" "+r.URL.Path)
	})

	return mux
}
