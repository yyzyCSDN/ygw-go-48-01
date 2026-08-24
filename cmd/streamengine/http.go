package main

import (
	"encoding/json"
	"net/http"
	"path/filepath"

	"streamengine/internal/topology"
)

type httpServer struct {
	addr     string
	webDir   string
	pipeline *topology.Pipeline
}

func newHTTPServer(addr string, webDir string, pipeline *topology.Pipeline) *http.Server {
	h := &httpServer{addr: addr, webDir: webDir, pipeline: pipeline}
	mux := http.NewServeMux()
	mux.HandleFunc("/health", h.handleHealth)
	mux.HandleFunc("/metrics", h.handleMetrics)
	mux.HandleFunc("/api/status", h.handleStatus)
	mux.HandleFunc("/topology", h.handlePage)
	mux.Handle("/", http.FileServer(http.Dir(filepath.Clean(webDir))))
	return &http.Server{Addr: addr, Handler: mux}
}

func (h *httpServer) handleHealth(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

func (h *httpServer) handleMetrics(w http.ResponseWriter, _ *http.Request) {
	payload, err := json.Marshal(h.pipeline.Metrics.Snapshot())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	_, _ = w.Write(payload)
}

func (h *httpServer) handleStatus(w http.ResponseWriter, _ *http.Request) {
	payload, err := json.Marshal(h.pipeline.Status())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	_, _ = w.Write(payload)
}

func (h *httpServer) handlePage(w http.ResponseWriter, r *http.Request) {
	http.ServeFile(w, r, filepath.Join(h.webDir, "topology.html"))
}
