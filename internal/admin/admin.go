// Package admin serves the sidecar listener: liveness, metrics, and
// runtime backend management (list, drain, undrain).
package admin

import (
	"encoding/json"
	"log/slog"
	"net/http"

	"overpass/internal/balancer"
	"overpass/internal/metrics"
)

// New wires the admin routes. The pool and registry are shared with the
// proxy listener; both are safe for concurrent use.
func New(pool *balancer.Pool, m *metrics.Registry, log *slog.Logger) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})

	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, m.Snapshot())
	})

	mux.HandleFunc("GET /metrics/prometheus", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		w.Write([]byte(m.Prometheus()))
	})

	mux.HandleFunc("GET /admin/backends", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, pool.Snapshot())
	})

	mux.HandleFunc("POST /admin/backends/{id}/drain", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		if !pool.SetDrained(id, true) {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "unknown backend " + id})
			return
		}
		log.Info("backend drained", "backend", id)
		writeJSON(w, http.StatusOK, map[string]string{"backend": id, "drained": "true"})
	})

	mux.HandleFunc("POST /admin/backends/{id}/undrain", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		if !pool.SetDrained(id, false) {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "unknown backend " + id})
			return
		}
		log.Info("backend restored", "backend", id)
		writeJSON(w, http.StatusOK, map[string]string{"backend": id, "drained": "false"})
	})

	return mux
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
