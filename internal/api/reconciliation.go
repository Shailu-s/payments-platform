package api

import (
	"errors"
	"net/http"
	"strconv"

	"github.com/Shailu-s/payments-platform/internal/reconciliation"
)

func reconciliationPage(r *http.Request) (int, int, error) {
	limit, offset := 25, 0
	var err error
	if value := r.URL.Query().Get("limit"); value != "" {
		limit, err = strconv.Atoi(value)
	}
	if err != nil || limit < 1 || limit > 100 {
		return 0, 0, errors.New("limit must be between 1 and 100")
	}
	if value := r.URL.Query().Get("offset"); value != "" {
		offset, err = strconv.Atoi(value)
	}
	if err != nil || offset < 0 {
		return 0, 0, errors.New("offset must be a nonnegative integer")
	}
	return limit, offset, nil
}

func (s *Server) handleListReconciliationRuns(w http.ResponseWriter, r *http.Request) {
	limit, offset, err := reconciliationPage(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, CodeInvalidRequest, err.Error())
		return
	}
	runs, err := reconciliation.ListRuns(r.Context(), s.db, limit, offset)
	if err != nil {
		writeInternalError(w, r, err)
		return
	}
	if runs == nil {
		runs = []reconciliation.Run{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"data": runs, "limit": limit, "offset": offset})
}

func (s *Server) handleGetReconciliationRun(w http.ResponseWriter, r *http.Request) {
	run, err := reconciliation.GetRun(r.Context(), s.db, r.PathValue("id"))
	if errors.Is(err, reconciliation.ErrNotFound) {
		writeError(w, http.StatusNotFound, CodeNotFound, "reconciliation run not found")
		return
	}
	if err != nil {
		writeInternalError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, run)
}

func (s *Server) handleReconciliationFindings(w http.ResponseWriter, r *http.Request) {
	limit, offset, err := reconciliationPage(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, CodeInvalidRequest, err.Error())
		return
	}
	classification := r.URL.Query().Get("classification")
	switch classification {
	case "", reconciliation.Matched, reconciliation.AmountMismatch, reconciliation.StatusMismatch, reconciliation.MissingExternal, reconciliation.MissingInternal, reconciliation.DuplicateExternal:
	default:
		writeError(w, http.StatusBadRequest, CodeInvalidRequest, "unknown reconciliation classification")
		return
	}
	if _, err := reconciliation.GetRun(r.Context(), s.db, r.PathValue("id")); errors.Is(err, reconciliation.ErrNotFound) {
		writeError(w, http.StatusNotFound, CodeNotFound, "reconciliation run not found")
		return
	} else if err != nil {
		writeInternalError(w, r, err)
		return
	}
	findings, err := reconciliation.Findings(r.Context(), s.db, r.PathValue("id"), classification, r.URL.Query().Get("client_reference"), limit, offset)
	if err != nil {
		writeInternalError(w, r, err)
		return
	}
	if findings == nil {
		findings = []reconciliation.Finding{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"data": findings, "limit": limit, "offset": offset})
}
