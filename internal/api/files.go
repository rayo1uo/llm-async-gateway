package api

import (
	"errors"
	"io"
	"net/http"
	"path"

	"github.com/rayo1uo/llm-async-gateway/internal/id"
	"github.com/rayo1uo/llm-async-gateway/internal/model"
	"github.com/rayo1uo/llm-async-gateway/internal/store"
)

func (h *Handler) uploadFile(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, h.opts.MaxFileBytes+4096)
	if err := r.ParseMultipartForm(h.opts.MaxFileBytes); err != nil {
		writeError(w, http.StatusBadRequest, "invalid multipart form or file too large", "invalid_request_error")
		return
	}
	if r.FormValue("purpose") != model.PurposeBatch {
		writeError(w, http.StatusBadRequest, "purpose must be batch", "invalid_request_error")
		return
	}
	file, hdr, err := r.FormFile("file")
	if err != nil {
		writeError(w, http.StatusBadRequest, "file is required", "invalid_request_error")
		return
	}
	defer func() { _ = file.Close() }()
	body, err := io.ReadAll(io.LimitReader(file, h.opts.MaxFileBytes+1))
	if err != nil {
		writeError(w, http.StatusBadRequest, "could not read file", "invalid_request_error")
		return
	}
	if int64(len(body)) > h.opts.MaxFileBytes {
		writeError(w, http.StatusBadRequest, "file exceeds max size", "invalid_request_error")
		return
	}
	fid, err := id.New("file_")
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal error", "api_error")
		return
	}
	name := "batch.jsonl"
	if hdr != nil && hdr.Filename != "" {
		name = path.Base(hdr.Filename)
	}
	doc := &model.File{
		ID:        fid,
		Object:    model.ObjectFile,
		Bytes:     len(body),
		CreatedAt: h.now().Unix(),
		Filename:  name,
		Purpose:   model.PurposeBatch,
	}
	if err := h.store.PutFile(r.Context(), doc, body); err != nil {
		h.log.Error("put file", "err", err)
		writeError(w, http.StatusInternalServerError, "internal error", "api_error")
		return
	}
	writeJSON(w, http.StatusOK, doc)
}

func (h *Handler) getFile(w http.ResponseWriter, r *http.Request) {
	f, err := h.store.GetFile(r.Context(), r.PathValue("id"))
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, "file not found", "not_found_error")
			return
		}
		h.log.Error("get file", "err", err)
		writeError(w, http.StatusInternalServerError, "internal error", "api_error")
		return
	}
	writeJSON(w, http.StatusOK, f)
}

func (h *Handler) getFileContent(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if _, err := h.store.GetFile(r.Context(), id); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, "file not found", "not_found_error")
			return
		}
		h.log.Error("get file", "err", err)
		writeError(w, http.StatusInternalServerError, "internal error", "api_error")
		return
	}
	body, err := h.store.GetFileContent(r.Context(), id)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, "file not found", "not_found_error")
			return
		}
		h.log.Error("get file content", "err", err)
		writeError(w, http.StatusInternalServerError, "internal error", "api_error")
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}

func (h *Handler) deleteFile(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := h.store.DeleteFile(r.Context(), id); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, "file not found", "not_found_error")
			return
		}
		h.log.Error("delete file", "err", err)
		writeError(w, http.StatusInternalServerError, "internal error", "api_error")
		return
	}
	writeJSON(w, http.StatusOK, model.DeletedFile{ID: id, Object: model.ObjectFile, Deleted: true})
}
