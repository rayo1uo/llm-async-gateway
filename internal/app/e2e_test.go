package app_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/rayo1uo/llm-async-gateway/internal/app"
	"github.com/rayo1uo/llm-async-gateway/internal/config"
	"github.com/rayo1uo/llm-async-gateway/internal/model"
)

type scriptedUpstream struct {
	mu          sync.Mutex
	hits        int
	inflight    int
	maxInflight int
	failFirst   int
	hold        <-chan struct{}
}

func (s *scriptedUpstream) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	s.hits++
	n := s.hits
	s.inflight++
	if s.inflight > s.maxInflight {
		s.maxInflight = s.inflight
	}
	fail := n <= s.failFirst
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		s.inflight--
		s.mu.Unlock()
	}()
	if s.hold != nil {
		select {
		case <-s.hold:
		case <-r.Context().Done():
			return
		}
	}
	if fail {
		w.Header().Set("Retry-After", "0")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"error":{"message":"busy"}}`))
		return
	}
	var body struct {
		Messages []struct {
			Content string `json:"content"`
		} `json:"messages"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)
	content := "mock"
	if len(body.Messages) > 0 {
		content = "mock: " + body.Messages[len(body.Messages)-1].Content
	}
	w.Header().Set("X-Request-Id", "up_"+r.Header.Get("X-Request-Id"))
	writeJSON(w, map[string]any{
		"id":      "chatcmpl_test",
		"object":  "chat.completion",
		"choices": []any{map[string]any{"message": map[string]string{"role": "assistant", "content": content}}},
		"usage":   map[string]int{"prompt_tokens": 8, "completion_tokens": 4, "total_tokens": 12},
	})
}

func (s *scriptedUpstream) snapshot() (hits, inflight, maxInflight int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.hits, s.inflight, s.maxInflight
}

type harness struct {
	base   string
	up     *scriptedUpstream
	cancel context.CancelFunc
	app    *app.App
}

func startHarness(t *testing.T, mutate func(*config.Config), up *scriptedUpstream) *harness {
	t.Helper()
	mr := miniredis.RunT(t)
	srv := httptest.NewServer(up)
	t.Cleanup(srv.Close)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	cfg := config.Default()
	cfg.UpstreamURL = srv.URL
	cfg.PollInterval = 10 * time.Millisecond
	cfg.MaxConcurrency = 4
	cfg.ReservedBatchSlots = 1
	cfg.RateLimitRPS = 1000
	cfg.RateBurst = 100
	cfg.LeaseTTL = 300 * time.Millisecond
	cfg.RequestTimeout = 2 * time.Second
	cfg.RetryBase = 15 * time.Millisecond
	cfg.RetryMax = 100 * time.Millisecond
	cfg.BatchEnqueueWindow = 8
	cfg.BatchReserveEvery = 4
	cfg.AgingSlack = time.Minute
	cfg.DefaultNearlineDeadline = 20 * time.Second
	cfg.DefaultCompletionWindow = time.Hour
	cfg.ResultTTL = time.Hour
	cfg.MaxAttempts = 6
	if mutate != nil {
		mutate(&cfg)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelDebug}))
	ctx, cancel := context.WithCancel(context.Background())
	application := app.New(cfg, rdb, logger, nil)
	application.Start(ctx)
	apiSrv := httptest.NewServer(application.Handler)
	t.Cleanup(func() {
		apiSrv.Close()
		cancel()
		application.Wait()
	})
	return &harness{base: apiSrv.URL, up: up, cancel: cancel, app: application}
}

func TestE2ENearlineAndBatch(t *testing.T) {
	h := startHarness(t, nil, &scriptedUpstream{})

	view := postJSON(t, h.base+"/v1/requests", map[string]any{
		"endpoint":         "/v1/chat/completions",
		"deadline_seconds": 30,
		"metadata":         map[string]string{"tenant": "demo"},
		"body": map[string]any{
			"model":    "mock",
			"messages": []any{map[string]string{"role": "user", "content": "hello"}},
		},
	}, map[string]string{"Idempotency-Key": "demo-1"})
	if view["status"] != model.StatusQueued {
		t.Fatalf("create status=%v", view["status"])
	}
	id, _ := view["id"].(string)
	again := postJSON(t, h.base+"/v1/requests", map[string]any{
		"body": map[string]any{"model": "mock", "messages": []any{map[string]string{"role": "user", "content": "other"}}},
	}, map[string]string{"Idempotency-Key": "demo-1"})
	if again["id"] != id {
		t.Fatalf("idempotency id=%v want %s", again["id"], id)
	}

	done := pollRequest(t, h.base, id)
	if done["status"] != model.StatusCompleted {
		t.Fatalf("nearline status=%v error=%v", done["status"], done["error"])
	}
	raw, _ := json.Marshal(done["response"])
	var resp model.OutputResponse
	if err := json.Unmarshal(raw, &resp); err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 || !bytes.Contains(resp.Body, []byte("mock: hello")) {
		t.Fatalf("response=%s", resp.Body)
	}

	file := upload(t, h.base, batchJSONL())
	batch := postJSON(t, h.base+"/v1/batches", map[string]any{
		"input_file_id":     file["id"],
		"endpoint":          "/v1/chat/completions",
		"completion_window": "1h",
		"metadata":          map[string]string{"job": "demo"},
	}, nil)
	bid, _ := batch["id"].(string)
	if batch["status"] != model.StatusValidating {
		t.Fatalf("batch create status=%v", batch["status"])
	}
	final := pollBatch(t, h.base, bid)
	if final.Status != model.StatusCompleted {
		t.Fatalf("batch status=%s errors=%v counts=%+v", final.Status, final.Errors, final.RequestCounts)
	}
	if final.RequestCounts.Total != 3 || final.RequestCounts.Completed != 3 || final.RequestCounts.Failed != 0 {
		t.Fatalf("counts=%+v", final.RequestCounts)
	}
	if final.OutputFileID == nil || final.Usage == nil || final.Usage.TotalTokens != 36 {
		t.Fatalf("output=%v usage=%+v", final.OutputFileID, final.Usage)
	}
	if final.FinalizingAt == nil || final.CompletedAt == nil || final.InProgressAt == nil {
		t.Fatalf("timestamps missing: %+v", final)
	}
	lines := fetchLines(t, h.base, *final.OutputFileID)
	if len(lines) != 3 {
		t.Fatalf("output lines=%d", len(lines))
	}
	got := map[string]string{}
	for _, ln := range lines {
		if ln.Error != nil || ln.Response == nil {
			t.Fatalf("line %+v", ln)
		}
		got[ln.CustomID] = string(ln.Response.Body)
	}
	for _, id := range []string{"c1", "c2", "c3"} {
		if !bytes.Contains([]byte(got[id]), []byte("mock: "+id)) {
			t.Fatalf("custom_id %s body=%s", id, got[id])
		}
	}

	_ = postJSON(t, h.base+"/v1/batches", map[string]any{
		"input_file_id":     file["id"],
		"endpoint":          "/v1/chat/completions",
		"completion_window": "30m",
	}, nil)
	page := getJSON(t, h.base+"/v1/batches?limit=1")
	if page["has_more"] != true {
		t.Fatalf("expected another page: %#v", page)
	}
	last, _ := page["last_id"].(string)
	next := getJSON(t, h.base+"/v1/batches?limit=1&after="+last)
	nextData, _ := next["data"].([]any)
	if len(nextData) != 1 {
		t.Fatalf("second page: %#v", next)
	}
}

func TestE2EBatchValidationFailure(t *testing.T) {
	h := startHarness(t, nil, &scriptedUpstream{})
	file := upload(t, h.base, "{\"custom_id\":\"x\",\"method\":\"GET\",\"url\":\"/v1/chat/completions\",\"body\":{}}\n")
	batch := postJSON(t, h.base+"/v1/batches", map[string]any{
		"input_file_id":     file["id"],
		"endpoint":          "/v1/chat/completions",
		"completion_window": "1h",
	}, nil)
	final := pollBatch(t, h.base, batch["id"].(string))
	if final.Status != model.StatusFailed || final.Errors == nil || len(final.Errors.Data) == 0 {
		t.Fatalf("status=%s errors=%v", final.Status, final.Errors)
	}
}

func TestE2EIncrementalEnqueue(t *testing.T) {
	hold := make(chan struct{})
	up := &scriptedUpstream{hold: hold}
	h := startHarness(t, func(cfg *config.Config) {
		cfg.BatchEnqueueWindow = 1
		cfg.MaxConcurrency = 4
	}, up)
	file := upload(t, h.base, batchJSONL())
	batch := postJSON(t, h.base+"/v1/batches", map[string]any{
		"input_file_id":     file["id"],
		"endpoint":          "/v1/chat/completions",
		"completion_window": "1h",
	}, nil)
	waitUntil(t, func() bool {
		_, inflight, maxInflight := up.snapshot()
		return inflight == 1 && maxInflight == 1
	})
	time.Sleep(120 * time.Millisecond)
	_, _, maxInflight := up.snapshot()
	if maxInflight != 1 {
		t.Fatalf("window did not hold the batch to one in-flight request (max=%d)", maxInflight)
	}
	close(hold)
	final := pollBatch(t, h.base, batch["id"].(string))
	if final.Status != model.StatusCompleted || final.RequestCounts.Completed != 3 {
		t.Fatalf("status=%s counts=%+v", final.Status, final.RequestCounts)
	}
}

func TestE2EBatchCancelAndExpiry(t *testing.T) {
	t.Run("cancel", func(t *testing.T) {
		hold := make(chan struct{})
		h := startHarness(t, func(cfg *config.Config) {
			cfg.BatchEnqueueWindow = 8
		}, &scriptedUpstream{hold: hold})
		file := upload(t, h.base, batchJSONL())
		batch := postJSON(t, h.base+"/v1/batches", map[string]any{
			"input_file_id":     file["id"],
			"endpoint":          "/v1/chat/completions",
			"completion_window": "1h",
		}, nil)
		waitUntil(t, func() bool {
			_, inflight, _ := h.up.snapshot()
			return inflight >= 1
		})
		cancelled := postJSON(t, h.base+"/v1/batches/"+batch["id"].(string)+"/cancel", map[string]any{}, nil)
		if cancelled["status"] != model.StatusCancelling && cancelled["status"] != model.StatusCancelled {
			t.Fatalf("cancel status=%v", cancelled["status"])
		}
		final := pollBatch(t, h.base, batch["id"].(string))
		if final.Status != model.StatusCancelled {
			t.Fatalf("status=%s counts=%+v", final.Status, final.RequestCounts)
		}
		if final.RequestCounts.Completed+final.RequestCounts.Failed != 3 {
			t.Fatalf("counts=%+v", final.RequestCounts)
		}
		close(hold)
	})

	t.Run("expire", func(t *testing.T) {
		hold := make(chan struct{})
		defer close(hold)
		h := startHarness(t, func(cfg *config.Config) {
			cfg.RequestTimeout = 5 * time.Second
			cfg.MaxAttempts = 2
		}, &scriptedUpstream{hold: hold})
		file := upload(t, h.base, batchJSONL())
		batch := postJSON(t, h.base+"/v1/batches", map[string]any{
			"input_file_id":     file["id"],
			"endpoint":          "/v1/chat/completions",
			"completion_window": "400ms",
		}, nil)
		final := pollBatch(t, h.base, batch["id"].(string))
		if final.Status != model.StatusExpired {
			t.Fatalf("status=%s counts=%+v errors=%v", final.Status, final.RequestCounts, final.Errors)
		}
		if final.ErrorFileID == nil {
			t.Fatal("expected an error file for unexecuted requests")
		}
		lines := fetchLines(t, h.base, *final.ErrorFileID)
		if len(lines) == 0 {
			t.Fatal("error file was empty")
		}
		for _, ln := range lines {
			if ln.Error == nil || ln.Error.Code != model.ErrBatchExpired {
				t.Fatalf("line error=%+v", ln.Error)
			}
		}
	})
}

func TestE2ERetryAndCancel(t *testing.T) {
	t.Run("retry", func(t *testing.T) {
		h := startHarness(t, nil, &scriptedUpstream{failFirst: 1})
		view := postJSON(t, h.base+"/v1/requests", map[string]any{
			"deadline_seconds": 20,
			"body":             map[string]any{"model": "mock", "messages": []any{map[string]string{"role": "user", "content": "retry"}}},
		}, nil)
		done := pollRequest(t, h.base, view["id"].(string))
		if done["status"] != model.StatusCompleted {
			t.Fatalf("status=%v error=%v", done["status"], done["error"])
		}
		hits, _, _ := h.up.snapshot()
		if hits < 2 {
			t.Fatalf("hits=%d want a retry", hits)
		}
	})
	t.Run("cancel", func(t *testing.T) {
		hold := make(chan struct{})
		h := startHarness(t, nil, &scriptedUpstream{hold: hold})
		view := postJSON(t, h.base+"/v1/requests", map[string]any{
			"deadline_seconds": 30,
			"body":             map[string]any{"model": "mock", "messages": []any{map[string]string{"role": "user", "content": "stop"}}},
		}, nil)
		id := view["id"].(string)
		waitUntil(t, func() bool {
			_, inflight, _ := h.up.snapshot()
			return inflight == 1
		})
		cancelled := postJSON(t, h.base+"/v1/requests/"+id+"/cancel", map[string]any{}, nil)
		if cancelled["status"] != model.StatusCancelling && cancelled["status"] != model.StatusCancelled {
			t.Fatalf("cancel response=%v", cancelled["status"])
		}
		done := pollRequest(t, h.base, id)
		if done["status"] != model.StatusCancelled {
			t.Fatalf("status=%v error=%v", done["status"], done["error"])
		}
		close(hold)
	})
}

func batchJSONL() string {
	lines := []string{
		`{"custom_id":"c1","method":"POST","url":"/v1/chat/completions","body":{"model":"mock","messages":[{"role":"user","content":"c1"}]}}`,
		`{"custom_id":"c2","method":"POST","url":"/v1/chat/completions","body":{"model":"mock","messages":[{"role":"user","content":"c2"}]}}`,
		`{"custom_id":"c3","method":"POST","url":"/v1/chat/completions","body":{"model":"mock","messages":[{"role":"user","content":"c3"}]}}`,
	}
	return lines[0] + "\n" + lines[1] + "\n" + lines[2] + "\n"
}

func upload(t *testing.T, base, body string) map[string]any {
	t.Helper()
	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)
	if err := w.WriteField("purpose", "batch"); err != nil {
		t.Fatal(err)
	}
	part, err := w.CreateFormFile("file", "batch.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := part.Write([]byte(body)); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequest(http.MethodPost, base+"/v1/files", &buf)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", w.FormDataContentType())
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("upload %d %s", resp.StatusCode, b)
	}
	var out map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	return out
}

func postJSON(t *testing.T, url string, body any, headers map[string]string) map[string]any {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	payload, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		t.Fatalf("POST %s -> %d %s", url, resp.StatusCode, payload)
	}
	var out map[string]any
	if err := json.Unmarshal(payload, &out); err != nil {
		t.Fatalf("decode %s: %v", payload, err)
	}
	return out
}

func getJSON(t *testing.T, url string) map[string]any {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	var out map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode >= 300 {
		t.Fatalf("GET %s -> %d %#v", url, resp.StatusCode, out)
	}
	return out
}

func pollRequest(t *testing.T, base, id string) map[string]any {
	t.Helper()
	deadline := time.Now().Add(8 * time.Second)
	var last map[string]any
	for time.Now().Before(deadline) {
		last = getJSON(t, base+"/v1/requests/"+id)
		status, _ := last["status"].(string)
		if model.TerminalNearline(status) {
			return last
		}
		time.Sleep(15 * time.Millisecond)
	}
	t.Fatalf("request %s did not finish: %#v", id, last)
	return nil
}

func pollBatch(t *testing.T, base, id string) model.Batch {
	t.Helper()
	deadline := time.Now().Add(8 * time.Second)
	var last model.Batch
	for time.Now().Before(deadline) {
		resp, err := http.Get(base + "/v1/batches/" + id)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if err := json.Unmarshal(body, &last); err != nil {
			t.Fatalf("decode batch: %v %s", err, body)
		}
		if model.TerminalBatch(last.Status) {
			return last
		}
		time.Sleep(15 * time.Millisecond)
	}
	t.Fatalf("batch %s did not finish: %+v", id, last)
	return last
}

func fetchLines(t *testing.T, base, fileID string) []model.OutputLine {
	t.Helper()
	resp, err := http.Get(base + "/v1/files/" + fileID + "/content")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	var lines []model.OutputLine
	dec := json.NewDecoder(bytes.NewReader(body))
	for dec.More() {
		var ln model.OutputLine
		if err := dec.Decode(&ln); err != nil {
			t.Fatal(err)
		}
		lines = append(lines, ln)
	}
	return lines
}

func waitUntil(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition was not met")
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(v)
}
