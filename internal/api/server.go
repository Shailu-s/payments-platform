// Package api is the HTTP layer: routing, middleware and handlers.
//
// net/http with Go 1.22 routing patterns rather than chi or gin. A framework
// would be defensible, but stdlib means every line of routing is ours and there
// is nothing to explain away.
package api

import (
	"context"
	"net/http"

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
	db DB
}

func NewServer(pool *pgxpool.Pool) *Server {
	return &Server{db: pool}
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

	authenticated := chain(s.routes(), s.withAuth)
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
