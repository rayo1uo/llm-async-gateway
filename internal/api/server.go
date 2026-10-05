// Package api serves the OpenAI-compatible batch surface and the nearline request API.
package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/rayo1uo/llm-async-gateway/internal/clock"
	"github.com/rayo1uo/llm-async-gateway/internal/observe"
	"github.com/rayo1uo/llm-async-gateway/internal/store"
)

// Options configures API defaults.
type Options struct {
	MaxFileBytes            int64
	DefaultCompletionWindow time.Duration
	DefaultNearlineDeadline time.Duration
	IdempotencyTTL          time.Duration
	Observer                *observe.Observer
}

// Handler is the HTTP API.
type Handler struct {
	store *store.Store
	opts  Options
	log   *slog.Logger
	clk   clock.Clock
	mux   *http.ServeMux
}

// New registers routes on a Go 1.22 ServeMux.
func New(st *store.Store, opts Options, logger *slog.Logger, clk clock.Clock) *Handler {
	if clk == nil {
		clk = clock.Real{}
	}
	if logger == nil {
		logger = slog.Default()
	}
	h := &Handler{store: st, opts: opts, log: logger, clk: clk, mux: http.NewServeMux()}
	h.mux.HandleFunc("GET /healthz", h.health)
	h.mux.HandleFunc("GET /readyz", h.ready)
	h.mux.HandleFunc("POST /v1/files", h.uploadFile)
	h.mux.HandleFunc("GET /v1/files/{id}", h.getFile)
	h.mux.HandleFunc("GET /v1/files/{id}/content", h.getFileContent)
	h.mux.HandleFunc("DELETE /v1/files/{id}", h.deleteFile)
	h.mux.HandleFunc("POST /v1/batches", h.createBatch)
	h.mux.HandleFunc("GET /v1/batches", h.listBatches)
	h.mux.HandleFunc("GET /v1/batches/{id}", h.getBatch)
	h.mux.HandleFunc("POST /v1/batches/{id}/cancel", h.cancelBatch)
	h.mux.HandleFunc("POST /v1/requests", h.createRequest)
	h.mux.HandleFunc("GET /v1/requests/{id}", h.getRequest)
	h.mux.HandleFunc("POST /v1/requests/{id}/cancel", h.cancelRequest)
	return h
}

// Handle registers an extra route, such as /metrics, on the API mux.
func (h *Handler) Handle(pattern string, handler http.Handler) {
	h.mux.Handle(pattern, handler)
}

// HandleFunc registers an extra route on the API mux.
func (h *Handler) HandleFunc(pattern string, fn http.HandlerFunc) {
	h.mux.HandleFunc(pattern, fn)
}

// ServeHTTP implements http.Handler.
// Every request is an http.serve span so POST /v1/requests can be continued
// by the dispatcher via the W3C traceparent stored on the queue message.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if h.opts.Observer != nil {
		ctx, span := h.opts.Observer.Start(r.Context(), "http.serve")
		defer span.End()
		r = r.WithContext(ctx)
	}
	h.mux.ServeHTTP(w, r)
}

func (h *Handler) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handler) ready(w http.ResponseWriter, r *http.Request) {
	if err := h.store.Ping(r.Context()); err != nil {
		h.log.Error("ready", "err", err)
		writeError(w, http.StatusServiceUnavailable, "redis unavailable", "api_error")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

func (h *Handler) now() time.Time { return h.clk.Now() }

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return
	}
}

func writeError(w http.ResponseWriter, code int, message, typ string) {
	writeJSON(w, code, map[string]any{
		"error": map[string]any{
			"message": message,
			"type":    typ,
			"param":   nil,
			"code":    nil,
		},
	})
}

func mapStoreErr(w http.ResponseWriter, err error, what string) {
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, what+" not found", "not_found_error")
		return
	}
	writeError(w, http.StatusInternalServerError, "internal error", "api_error")
}

func decodeJSON(w http.ResponseWriter, r *http.Request, dest any) error {
	r.Body = http.MaxBytesReader(w, r.Body, 2<<20)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dest); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid json: %s", err), "invalid_request_error")
		return err
	}
	return nil
}

func validateMetadata(md map[string]string) error {
	if len(md) > 16 {
		return fmt.Errorf("metadata supports at most 16 keys")
	}
	for k, v := range md {
		if len(k) == 0 || len(k) > 64 {
			return fmt.Errorf("metadata keys must be 1 to 64 characters")
		}
		if len(v) > 512 {
			return fmt.Errorf("metadata values must be at most 512 characters")
		}
	}
	return nil
}
