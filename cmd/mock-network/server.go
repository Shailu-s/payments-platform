package main

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"

	"github.com/Shailu-s/payments-platform/internal/cardnetwork"
)

type Server struct {
	network *cardnetwork.Network
}

func NewServer(network *cardnetwork.Network) *Server {
	return &Server{network: network}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /clearing", s.handleClearing)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})

	// Operator endpoints. Not part of the issuer contract.
	mux.HandleFunc("POST /sandbox/scenarios", s.handleScenario)
	mux.HandleFunc("GET /sandbox/journal", s.handleJournal)
	mux.HandleFunc("POST /sandbox/reset", s.handleReset)

	return mux
}

func (s *Server) handleScenario(w http.ResponseWriter, r *http.Request) {
	var req cardnetwork.ScenarioRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10)).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "body is not valid json")
		return
	}

	result, err := s.network.RunScenario(r.Context(), req)
	switch {
	case errors.Is(err, cardnetwork.ErrInvalidMessage), errors.Is(err, cardnetwork.ErrUnknownScenario):
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	case errors.Is(err, cardnetwork.ErrIDReused):
		writeError(w, http.StatusConflict, "id_reused",
			"this seed reuses identifiers already in the journal; reset first or use another seed")
		return
	case err != nil:
		slog.Error("scenario failed", "scenario", req.Scenario, "error", err)
		writeError(w, http.StatusInternalServerError, "internal_error", "scenario did not complete")
		return
	}

	slog.Info("scenario played", "scenario", result.Scenario, "seed", result.Seed,
		"clearing_dates", result.ClearingDates)
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) handleClearing(w http.ResponseWriter, r *http.Request) {
	date := r.URL.Query().Get("date")
	file, err := s.network.ClearingFile(date)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "date is required as YYYY-MM-DD")
		return
	}
	w.Header().Set("Content-Type", "text/csv; charset=utf-8")
	w.Header().Set("Content-Disposition", `attachment; filename="`+clearingFileName(date)+`"`)
	w.Header().Set("Cache-Control", "no-store")
	if _, err := w.Write(file); err != nil {
		slog.Error("write clearing file", "error", err)
	}
}

func (s *Server) handleJournal(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"data": s.network.Journal()})
}

func (s *Server) handleReset(w http.ResponseWriter, r *http.Request) {
	s.network.Reset()
	writeJSON(w, http.StatusOK, map[string]string{"status": "reset"})
}

func clearingFileName(date string) string {
	return "clearing-" + date + ".csv"
}

// exportClearing writes one clearing file per date into dir and returns their paths.
func exportClearing(network *cardnetwork.Network, dir string, dates []string) ([]string, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	var paths []string
	for _, date := range dates {
		file, err := network.ClearingFile(date)
		if err != nil {
			return nil, err
		}
		path := filepath.Join(dir, clearingFileName(date))
		if err := os.WriteFile(path, file, 0o644); err != nil {
			return nil, err
		}
		paths = append(paths, path)
	}
	return paths, nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		slog.Error("writing response", "error", err)
	}
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, map[string]any{
		"error": map[string]string{"code": code, "message": message},
	})
}
