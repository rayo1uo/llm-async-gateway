package api

import (
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/rayo1uo/llm-async-gateway/internal/id"
	"github.com/rayo1uo/llm-async-gateway/internal/model"
)

type createBatchRequest struct {
	InputFileID      string            `json:"input_file_id"`
	Endpoint         string            `json:"endpoint"`
	CompletionWindow string            `json:"completion_window"`
	Metadata         map[string]string `json:"metadata"`
}

func (h *Handler) createBatch(w http.ResponseWriter, r *http.Request) {
	var req createBatchRequest
	if err := decodeJSON(w, r, &req); err != nil {
		return
	}
	if req.InputFileID == "" {
		writeError(w, http.StatusBadRequest, "input_file_id is required", "invalid_request_error")
		return
	}
	if req.Endpoint == "" {
		writeError(w, http.StatusBadRequest, "endpoint is required", "invalid_request_error")
		return
	}
	if !model.AllowedEndpoint(req.Endpoint) {
		writeError(w, http.StatusBadRequest, "endpoint is not supported", "invalid_request_error")
		return
	}
	window, windowText, err := parseWindow(req.CompletionWindow, h.opts.DefaultCompletionWindow)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error(), "invalid_request_error")
		return
	}
	if err := validateMetadata(req.Metadata); err != nil {
		writeError(w, http.StatusBadRequest, err.Error(), "invalid_request_error")
		return
	}
	if req.Metadata == nil {
		req.Metadata = map[string]string{}
	}
	file, err := h.store.GetFile(r.Context(), req.InputFileID)
	if err != nil {
		mapStoreErr(w, err, "file")
		return
	}
	if file.Purpose != model.PurposeBatch {
		writeError(w, http.StatusBadRequest, "input file purpose must be batch", "invalid_request_error")
		return
	}
	bid, err := id.New("batch_")
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal error", "api_error")
		return
	}
	now := h.now()
	deadline := now.Add(window)
	expires := deadline.Unix()
	if deadline.After(time.Unix(expires, 0)) {
		expires++
	}
	batch := &model.Batch{
		ID:               bid,
		Object:           model.ObjectBatch,
		Endpoint:         req.Endpoint,
		InputFileID:      req.InputFileID,
		CompletionWindow: windowText,
		Status:           model.StatusValidating,
		CreatedAt:        now.Unix(),
		ExpiresAt:        &expires,
		RequestCounts:    model.RequestCounts{},
		Metadata:         req.Metadata,
	}
	if err := h.store.CreateBatch(r.Context(), batch, deadline.UnixMilli()); err != nil {
		h.log.Error("create batch", "err", err)
		writeError(w, http.StatusInternalServerError, "internal error", "api_error")
		return
	}
	writeJSON(w, http.StatusOK, batch)
}

func (h *Handler) getBatch(w http.ResponseWriter, r *http.Request) {
	b, err := h.store.GetBatch(r.Context(), r.PathValue("id"))
	if err != nil {
		mapStoreErr(w, err, "batch")
		return
	}
	writeJSON(w, http.StatusOK, b)
}

func (h *Handler) listBatches(w http.ResponseWriter, r *http.Request) {
	limit := 20
	if raw := r.URL.Query().Get("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 {
			writeError(w, http.StatusBadRequest, "limit must be a positive integer", "invalid_request_error")
			return
		}
		if n > 100 {
			n = 100
		}
		limit = n
	}
	after := r.URL.Query().Get("after")
	ids, err := h.store.ListBatchIDs(r.Context())
	if err != nil {
		h.log.Error("list batches", "err", err)
		writeError(w, http.StatusInternalServerError, "internal error", "api_error")
		return
	}
	start := 0
	if after != "" {
		found := false
		for i, id := range ids {
			if id == after {
				start = i + 1
				found = true
				break
			}
		}
		if !found {
			writeError(w, http.StatusBadRequest, "unknown pagination cursor", "invalid_request_error")
			return
		}
	}
	end := start + limit
	hasMore := false
	if end < len(ids) {
		hasMore = true
	} else {
		end = len(ids)
	}
	page := ids[start:end]
	data := make([]*model.Batch, 0, len(page))
	for _, id := range page {
		b, err := h.store.GetBatch(r.Context(), id)
		if err != nil {
			h.log.Error("get batch in list", "id", id, "err", err)
			writeError(w, http.StatusInternalServerError, "internal error", "api_error")
			return
		}
		data = append(data, b)
	}
	resp := map[string]any{
		"object":   model.ObjectList,
		"data":     data,
		"has_more": hasMore,
	}
	if len(data) > 0 {
		resp["first_id"] = data[0].ID
		resp["last_id"] = data[len(data)-1].ID
	}
	writeJSON(w, http.StatusOK, resp)
}

func (h *Handler) cancelBatch(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	current, err := h.store.GetBatch(r.Context(), id)
	if err != nil {
		mapStoreErr(w, err, "batch")
		return
	}
	if model.TerminalBatch(current.Status) || current.Status == model.StatusCancelling {
		writeJSON(w, http.StatusOK, current)
		return
	}
	if err := h.store.MarkBatchCancel(r.Context(), id); err != nil {
		h.log.Error("mark batch cancel", "err", err)
		writeError(w, http.StatusInternalServerError, "internal error", "api_error")
		return
	}
	updated, err := h.store.MutateBatch(r.Context(), id, func(b *model.Batch) error {
		if model.TerminalBatch(b.Status) || b.Status == model.StatusCancelling {
			return nil
		}
		now := h.now().Unix()
		b.Status = model.StatusCancelling
		b.CancellingAt = &now
		return nil
	})
	if err != nil {
		mapStoreErr(w, err, "batch")
		return
	}
	writeJSON(w, http.StatusOK, updated)
}

func formatWindow(d time.Duration) string {
	if d > 0 && d%time.Hour == 0 {
		return fmt.Sprintf("%dh", int(d/time.Hour))
	}
	return d.String()
}

func parseWindow(raw string, def time.Duration) (time.Duration, string, error) {
	if raw == "" {
		return def, formatWindow(def), nil
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d <= 0 {
		return 0, "", errWindow
	}
	if d > 7*24*time.Hour {
		return 0, "", errWindow
	}
	return d, raw, nil
}

var errWindow = errString("completion_window must be a positive duration up to 168h, for example 24h")

type errString string

func (e errString) Error() string { return string(e) }
