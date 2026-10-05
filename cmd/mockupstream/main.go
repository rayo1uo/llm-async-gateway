// Command mockupstream is a tiny OpenAI-compatible inference server for demos without GPUs.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	addr := flag.String("addr", ":8090", "listen address")
	delay := flag.Duration("delay", 20*time.Millisecond, "artificial inference delay")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("POST /v1/chat/completions", func(w http.ResponseWriter, r *http.Request) {
		handleChat(w, r, *delay, logger)
	})
	mux.HandleFunc("POST /v1/responses", func(w http.ResponseWriter, r *http.Request) {
		handleChat(w, r, *delay, logger)
	})
	mux.HandleFunc("POST /v1/completions", func(w http.ResponseWriter, r *http.Request) {
		handleCompletion(w, r, *delay, logger)
	})
	mux.HandleFunc("POST /v1/embeddings", func(w http.ResponseWriter, r *http.Request) {
		handleEmbedding(w, r, *delay, logger)
	})

	srv := &http.Server{Addr: *addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go func() {
		logger.Info("mock upstream listening", "addr", *addr, "delay", delay.String())
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("server", "err", err)
			os.Exit(1)
		}
	}()
	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("shutdown", "err", err)
	}
}

func handleChat(w http.ResponseWriter, r *http.Request, delay time.Duration, logger *slog.Logger) {
	if !sleep(r, delay) {
		return
	}
	var body struct {
		Model    string `json:"model"`
		Messages []struct {
			Content string `json:"content"`
		} `json:"messages"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, errBody("invalid json"))
		return
	}
	content := "mock reply"
	if n := len(body.Messages); n > 0 && body.Messages[n-1].Content != "" {
		content = "mock: " + body.Messages[n-1].Content
	}
	modelName := body.Model
	if modelName == "" {
		modelName = "mock"
	}
	logger.Info("chat", "model", modelName, "tier", r.Header.Get("X-Async-Tier"), "request_id", r.Header.Get("X-Request-Id"))
	w.Header().Set("X-Request-Id", "chatcmpl_"+r.Header.Get("X-Request-Id"))
	writeJSON(w, http.StatusOK, map[string]any{
		"id":      "chatcmpl_mock",
		"object":  "chat.completion",
		"model":   modelName,
		"choices": []any{map[string]any{"index": 0, "message": map[string]string{"role": "assistant", "content": content}, "finish_reason": "stop"}},
		"usage":   map[string]int{"prompt_tokens": 8, "completion_tokens": 4, "total_tokens": 12},
	})
}

func handleCompletion(w http.ResponseWriter, r *http.Request, delay time.Duration, logger *slog.Logger) {
	if !sleep(r, delay) {
		return
	}
	var body struct {
		Model  string `json:"model"`
		Prompt string `json:"prompt"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, errBody("invalid json"))
		return
	}
	logger.Info("completion", "request_id", r.Header.Get("X-Request-Id"))
	text := "mock: " + body.Prompt
	writeJSON(w, http.StatusOK, map[string]any{
		"id":      "cmpl_mock",
		"object":  "text_completion",
		"model":   body.Model,
		"choices": []any{map[string]any{"index": 0, "text": text, "finish_reason": "stop"}},
		"usage":   map[string]int{"prompt_tokens": 4, "completion_tokens": 4, "total_tokens": 8},
	})
}

func handleEmbedding(w http.ResponseWriter, r *http.Request, delay time.Duration, logger *slog.Logger) {
	if !sleep(r, delay) {
		return
	}
	logger.Info("embedding", "request_id", r.Header.Get("X-Request-Id"))
	writeJSON(w, http.StatusOK, map[string]any{
		"object": "list",
		"model":  "mock-embed",
		"data":   []any{map[string]any{"object": "embedding", "index": 0, "embedding": []float64{0.1, 0.2, 0.3}}},
		"usage":  map[string]int{"prompt_tokens": 2, "total_tokens": 2},
	})
}

func sleep(r *http.Request, delay time.Duration) bool {
	if delay <= 0 {
		return true
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-r.Context().Done():
		return false
	case <-timer.C:
		return true
	}
}

func errBody(message string) map[string]any {
	return map[string]any{"error": map[string]any{"message": message, "type": "invalid_request_error"}}
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return
	}
}
