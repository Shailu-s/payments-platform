package api

import "net/http"

// The transfer endpoints land in 2.4. They are declared now so the routing,
// middleware and error shape can be exercised end to end, and they return 501
// rather than a plausible-looking fake: an endpoint that pretends to move money
// is worse than one that says it cannot yet.

func (s *Server) handleCreateTransfer(w http.ResponseWriter, r *http.Request) {
	writeError(w, http.StatusNotImplemented, CodeNotImplemented,
		"POST /v1/transfers arrives in phase 2.4")
}

func (s *Server) handleGetTransfer(w http.ResponseWriter, r *http.Request) {
	writeError(w, http.StatusNotImplemented, CodeNotImplemented,
		"GET /v1/transfers/{id} arrives in phase 2.4")
}

func (s *Server) handleListTransfers(w http.ResponseWriter, r *http.Request) {
	writeError(w, http.StatusNotImplemented, CodeNotImplemented,
		"GET /v1/transfers arrives in phase 2.4")
}
