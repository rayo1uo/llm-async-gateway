package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"time"
	"unicode"

	"github.com/rayo1uo/llm-async-gateway/internal/id"
	"github.com/rayo1uo/llm-async-gateway/internal/model"
	"github.com/rayo1uo/llm-async-gateway/internal/observe"
	"github.com/rayo1uo/llm-async-gateway/internal/store"
)

type createNearlineRequest struct {
	Endpoint        string            `json:"endpoint"`
	DeadlineSeconds int               `json:"deadline_seconds"`
	Body            json.RawMessage   `json:"body"`
	Metadata        map[string]string `json:"metadata"`
}

type requestView struct {
	ID          string                `json:"id"`
	Object      string                `json:"object"`
	Status      string                `json:"status"`
	Endpoint    string                `json:"endpoint"`
	CreatedAt   int64                 `json:"created_at"`
	Deadline    int64                 `json:"deadline"`
	CompletedAt *int64                `json:"completed_at,omitempty"`
	Metadata    map[string]string     `json:"metadata"`
	Response    *model.OutputResponse `json:"response"`
	Error       *model.OutputError    `json:"error"`
}

func (h *Handler) createRequest(w http.ResponseWriter, r *http.Request) {
	var req createNearlineRequest
	if err := decodeJSON(w, r, &req); err != nil {
		return
	}
	if req.Endpoint == "" {
		req.Endpoint = "/v1/chat/completions"
	}
	if !model.AllowedEndpoint(req.Endpoint) {
		writeError(w, http.StatusBadRequest, "endpoint is not supported", "invalid_request_error")
		return
	}
	if !jsonObject(req.Body) {
		writeError(w, http.StatusBadRequest, "body must be a JSON object", "invalid_request_error")
		return
	}
	if err := validateMetadata(req.Metadata); err != nil {
		writeError(w, http.StatusBadRequest, err.Error(), "invalid_request_error")
		return
	}
	if req.Metadata == nil {
		req.Metadata = map[string]string{}
	}
	deadline := h.opts.DefaultNearlineDeadline
	if req.DeadlineSeconds != 0 {
		if req.DeadlineSeconds < 1 || req.DeadlineSeconds > 86400 {
			writeError(w, http.StatusBadRequest, "deadline_seconds must be between 1 and 86400", "invalid_request_error")
			return
		}
		deadline = time.Duration(req.DeadlineSeconds) * time.Second
	}
	if deadline <= 0 {
		writeError(w, http.StatusBadRequest, "deadline must be positive", "invalid_request_error")
		return
	}

	idemKey := r.Header.Get("Idempotency-Key")
	if len(idemKey) > 200 {
		writeError(w, http.StatusBadRequest, "Idempotency-Key is too long", "invalid_request_error")
		return
	}
	reqID, err := id.New("req_")
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal error", "api_error")
		return
	}
	now := h.now()
	rec := &model.Nearline{
		ID:         reqID,
		Object:     model.ObjectRequest,
		Status:     model.StatusQueued,
		Endpoint:   req.Endpoint,
		CreatedAt:  now.Unix(),
		Deadline:   now.Add(deadline).Unix(),
		DeadlineMS: now.Add(deadline).UnixMilli(),
		Metadata:   req.Metadata,
	}
	unit := &model.Unit{
		ID:          reqID,
		Tier:        model.TierAsync,
		Endpoint:    req.Endpoint,
		Body:        req.Body,
		Deadline:    rec.Deadline,
		Created:     now.Unix(),
		TraceParent: observe.Traceparent(r.Context()),
	}
	id, created, err := h.store.AcceptNearline(r.Context(), idemKey, h.opts.IdempotencyTTL, rec, unit)
	if err != nil {
		h.log.Error("accept nearline", "err", err)
		writeError(w, http.StatusInternalServerError, "internal error", "api_error")
		return
	}
	if !created {
		h.writeExisting(w, r, id)
		return
	}
	writeJSON(w, http.StatusAccepted, viewFrom(rec, nil))
}

func (h *Handler) writeExisting(w http.ResponseWriter, r *http.Request, id string) {
	view, err := h.loadView(r, id)
	if err != nil {
		mapStoreErr(w, err, "request")
		return
	}
	code := http.StatusOK
	if !model.TerminalNearline(view.Status) {
		code = http.StatusAccepted
	}
	writeJSON(w, code, view)
}

func (h *Handler) getRequest(w http.ResponseWriter, r *http.Request) {
	view, err := h.loadView(r, r.PathValue("id"))
	if err != nil {
		mapStoreErr(w, err, "request")
		return
	}
	writeJSON(w, http.StatusOK, view)
}

func (h *Handler) cancelRequest(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	rec, err := h.store.GetNearline(r.Context(), id)
	if err != nil {
		mapStoreErr(w, err, "request")
		return
	}
	if model.TerminalNearline(rec.Status) {
		view, err := h.loadView(r, id)
		if err != nil {
			mapStoreErr(w, err, "request")
			return
		}
		writeJSON(w, http.StatusOK, view)
		return
	}
	if err := h.store.MarkCancelled(r.Context(), id); err != nil {
		h.log.Error("mark cancel", "err", err)
		writeError(w, http.StatusInternalServerError, "internal error", "api_error")
		return
	}
	if err := h.store.SetNearlineStatus(r.Context(), id, model.StatusCancelling, 0); err != nil {
		h.log.Error("set cancelling", "err", err)
		writeError(w, http.StatusInternalServerError, "internal error", "api_error")
		return
	}
	view, err := h.loadView(r, id)
	if err != nil {
		mapStoreErr(w, err, "request")
		return
	}
	writeJSON(w, http.StatusOK, view)
}

func (h *Handler) loadView(r *http.Request, id string) (*requestView, error) {
	rec, err := h.store.GetNearline(r.Context(), id)
	if err != nil {
		return nil, err
	}
	res, err := h.store.GetResult(r.Context(), id)
	if err != nil {
		// A missing result just means the request is still running.
		res = nil
	}
	// GetResult returns ErrNotFound. Anything else is a real failure.
	if err != nil && !isNotFound(err) {
		return nil, err
	}
	return viewFrom(rec, res), nil
}

func viewFrom(rec *model.Nearline, res *model.Result) *requestView {
	view := &requestView{
		ID:          rec.ID,
		Object:      model.ObjectRequest,
		Status:      rec.Status,
		Endpoint:    rec.Endpoint,
		CreatedAt:   rec.CreatedAt,
		Deadline:    rec.Deadline,
		CompletedAt: rec.CompletedAt,
		Metadata:    rec.Metadata,
	}
	if view.Metadata == nil {
		view.Metadata = map[string]string{}
	}
	if res == nil {
		return view
	}
	view.Status = res.Status
	if res.FinishedAt > 0 {
		view.CompletedAt = &res.FinishedAt
	}
	body := res.Body
	if len(body) == 0 {
		body = json.RawMessage(`null`)
	}
	if res.StatusCode > 0 {
		reqID := res.RequestID
		if reqID == "" {
			reqID = rec.ID
		}
		view.Response = &model.OutputResponse{StatusCode: res.StatusCode, RequestID: reqID, Body: body}
	}
	if res.Status != model.StatusCompleted {
		code := res.ErrorCode
		if code == "" {
			code = model.ErrUpstream
		}
		view.Error = &model.OutputError{Code: code, Message: res.ErrorMessage}
	}
	return view
}

func jsonObject(raw json.RawMessage) bool {
	trimmed := bytes.TrimLeftFunc(raw, unicode.IsSpace)
	return len(trimmed) > 0 && trimmed[0] == '{' && json.Valid(raw)
}

func isNotFound(err error) bool {
	return errors.Is(err, store.ErrNotFound)
}
