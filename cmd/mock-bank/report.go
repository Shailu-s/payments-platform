package main

import (
	"encoding/csv"
	"log/slog"
	"net/http"
	"sort"
	"strconv"
	"time"
)

func (s *Server) handleSettlements(w http.ResponseWriter, r *http.Request) {
	if r.URL.RawQuery != "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "settlements requires a complete snapshot without query parameters")
		return
	}
	capturedAt, payments := s.store.Snapshot()
	sort.Slice(payments, func(i, j int) bool { return payments[i].ProviderRef < payments[j].ProviderRef })
	w.Header().Set("Content-Type", "text/csv; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	writer := csv.NewWriter(w)
	writer.Write([]string{"snapshot_at", capturedAt.Format(time.RFC3339Nano)})
	writer.Write([]string{"provider_ref", "client_reference", "amount", "currency", "status"})
	for _, p := range payments {
		writer.Write([]string{p.ProviderRef, p.ClientReference, strconv.FormatInt(p.Amount, 10), p.Currency, p.Status})
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		slog.Error("write settlement report", "error", err)
	}
}
