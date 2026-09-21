package api

import (
	"net/http"
	"time"
)

// Liveness. No database, no auth: this answers "is the process running", and
// nothing else. A liveness probe that fails when Postgres is unreachable tells
// an orchestrator to restart a process that is working perfectly.
func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// Readiness. This one does check the database, because "ready to take traffic"
// is a different question with a different remedy: remove the instance from the
// load balancer rather than restart it.
func (s *Server) handleReadyz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := contextWithTimeout(r, 2*time.Second)
	defer cancel()

	var one int
	if err := s.db.QueryRow(ctx, `SELECT 1`).Scan(&one); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{
			"status": "unavailable",
			"reason": "database is not reachable",
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}
