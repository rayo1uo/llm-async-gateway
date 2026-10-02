package batch

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/rayo1uo/llm-async-gateway/internal/model"
	"github.com/rayo1uo/llm-async-gateway/internal/store"
)

func TestSumUsage(t *testing.T) {
	tests := []struct {
		name string
		body string
		in   int
		out  int
		all  int
	}{
		{
			name: "chat completions",
			body: `{"usage":{"prompt_tokens":8,"completion_tokens":4,"total_tokens":12}}`,
			in:   8, out: 4, all: 12,
		},
		{
			name: "responses api",
			body: `{"usage":{"input_tokens":10,"output_tokens":5,"total_tokens":15}}`,
			in:   10, out: 5, all: 15,
		},
		{
			name: "embeddings",
			body: `{"usage":{"prompt_tokens":2,"total_tokens":2}}`,
			in:   2, out: 0, all: 2,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := sumUsage([]model.OutputLine{{
				Response: &model.OutputResponse{StatusCode: 200, Body: json.RawMessage(tt.body)},
			}})
			if got == nil || got.InputTokens != tt.in || got.OutputTokens != tt.out || got.TotalTokens != tt.all {
				t.Fatalf("usage=%+v", got)
			}
		})
	}
}

func TestFillWindowIsSharedAcrossControllers(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	st := store.New(rdb, "lag")
	ctx := context.Background()
	now := time.Now()
	b := &model.Batch{
		ID: "batch_win", Object: model.ObjectBatch, Endpoint: "/v1/chat/completions",
		Status: model.StatusInProgress, CompletionWindow: "1h", CreatedAt: now.Unix(),
		Metadata: map[string]string{},
	}
	if err := st.CreateBatch(ctx, b, now.Add(time.Hour).UnixMilli()); err != nil {
		t.Fatal(err)
	}
	var lines []model.InputLine
	for i, custom := range []string{"a", "b", "c", "d"} {
		lines = append(lines, model.InputLine{
			CustomID: custom, Method: "POST", URL: "/v1/chat/completions",
			Body: []byte(`{"model":"m"}`), Index: i + 1,
		})
	}
	if err := st.ReplaceBatchInputs(ctx, b.ID, lines); err != nil {
		t.Fatal(err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	opts := Options{Window: 1, PollInterval: time.Millisecond, ResultTTL: time.Hour}
	left := New(st, opts, logger, nil)
	right := New(st, opts, logger, nil)

	var wg sync.WaitGroup
	errCh := make(chan error, 2)
	for _, ctl := range []*Controller{left, right} {
		wg.Add(1)
		go func(ctl *Controller) {
			defer wg.Done()
			errCh <- ctl.fill(ctx, b, now.Add(time.Hour).UnixMilli())
		}(ctl)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}
	outstanding, err := st.Outstanding(ctx, b.ID)
	if err != nil {
		t.Fatal(err)
	}
	if outstanding != 1 {
		t.Fatalf("outstanding=%d, window is 1", outstanding)
	}
	leftN, err := st.BatchInputLen(ctx, b.ID)
	if err != nil {
		t.Fatal(err)
	}
	if leftN != 3 {
		t.Fatalf("inputs left=%d", leftN)
	}
}
