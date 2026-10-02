package api

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/rayo1uo/llm-async-gateway/internal/model"
	"github.com/rayo1uo/llm-async-gateway/internal/store"
)

func TestConcurrentIdempotencySubmitsOneRequest(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	st := store.New(rdb, "lag")
	h := New(st, Options{
		MaxFileBytes:            1 << 20,
		DefaultCompletionWindow: time.Hour,
		DefaultNearlineDeadline: time.Minute,
		IdempotencyTTL:          time.Hour,
	}, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	body := []byte(`{"endpoint":"/v1/chat/completions","deadline_seconds":30,"body":{"model":"m","messages":[{"role":"user","content":"hi"}]}}`)
	const n = 8
	ids := make([]string, n)
	codes := make([]int, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			req, err := http.NewRequest(http.MethodPost, srv.URL+"/v1/requests", bytes.NewReader(body))
			if err != nil {
				t.Errorf("request: %v", err)
				return
			}
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Idempotency-Key", "same")
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Errorf("do: %v", err)
				return
			}
			defer func() { _ = resp.Body.Close() }()
			raw, _ := io.ReadAll(resp.Body)
			var view requestView
			if err := json.Unmarshal(raw, &view); err != nil {
				t.Errorf("decode %s: %v", raw, err)
				return
			}
			codes[i] = resp.StatusCode
			ids[i] = view.ID
		}(i)
	}
	wg.Wait()
	for i, id := range ids {
		if id == "" || id != ids[0] {
			t.Fatalf("ids=%v codes=%v", ids, codes)
		}
		if codes[i] != http.StatusAccepted && codes[i] != http.StatusOK {
			t.Fatalf("status=%d body id=%s", codes[i], id)
		}
	}
	if nq, err := st.QueueLen(context.Background(), model.TierNearline); err != nil || nq != 1 {
		t.Fatalf("queue=%d err=%v", nq, err)
	}
	if _, err := st.GetNearline(context.Background(), ids[0]); err != nil {
		t.Fatal(err)
	}
	resp, err := http.Get(srv.URL + "/v1/requests/" + ids[0])
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("get status=%d", resp.StatusCode)
	}
}
