package dispatch

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/rayo1uo/llm-async-gateway/internal/budget"
	"github.com/rayo1uo/llm-async-gateway/internal/model"
	"github.com/rayo1uo/llm-async-gateway/internal/store"
)

type recordingUpstream struct {
	mu  sync.Mutex
	ids []string
}

func (r *recordingUpstream) Do(_ context.Context, u *model.Unit) (*UpstreamResponse, error) {
	r.mu.Lock()
	r.ids = append(r.ids, u.ID)
	r.mu.Unlock()
	return &UpstreamResponse{StatusCode: 200, Body: []byte(`{"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}`), RequestID: "up_" + u.ID}, nil
}

func TestDispatcherPrefersNearlineOverSoonerBatch(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	st := store.New(rdb, "lag")
	ctx := context.Background()
	now := time.Now()

	batchUnit := &model.Unit{
		ID: "batch_req_early", Tier: model.TierBatch, Endpoint: "/v1/chat/completions",
		Body: []byte(`{"model":"m"}`), Deadline: now.Add(time.Hour).UnixMilli(), Created: now.UnixMilli(),
		BatchID: "batch_1", CustomID: "c",
	}
	near := &model.Unit{
		ID: "req_later", Tier: model.TierNearline, Endpoint: "/v1/chat/completions",
		Body: []byte(`{"model":"m"}`), Deadline: now.Add(2 * time.Hour).UnixMilli(), Created: now.UnixMilli(),
	}
	if err := st.PutNearline(ctx, &model.Nearline{
		ID: near.ID, Object: model.ObjectRequest, Status: model.StatusQueued,
		Endpoint: near.Endpoint, CreatedAt: now.Unix(), Deadline: now.Add(2 * time.Hour).Unix(), DeadlineMS: near.Deadline,
		Metadata: map[string]string{},
	}); err != nil {
		t.Fatal(err)
	}
	if err := st.Enqueue(ctx, batchUnit); err != nil {
		t.Fatal(err)
	}
	if err := st.Enqueue(ctx, near); err != nil {
		t.Fatal(err)
	}

	up := &recordingUpstream{}
	lim := budget.NewLocal(budget.LocalConfig{MaxConcurrency: 1, ReservedBatch: 0, RatePerSec: 1000, Burst: 10})
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	d := New(st, lim, up, Options{
		LeaseTTL: time.Second, PollInterval: 5 * time.Millisecond, RequestTimeout: time.Second,
		ReserveEvery: 0, AgingSlack: time.Millisecond, ResultTTL: time.Hour,
		RetryBase: time.Millisecond, RetryMax: 10 * time.Millisecond, MaxAttempts: 3,
	}, logger, nil)

	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go d.Run(runCtx)

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		up.mu.Lock()
		n := len(up.ids)
		first := ""
		if n > 0 {
			first = up.ids[0]
		}
		up.mu.Unlock()
		if n > 0 {
			if first != near.ID {
				t.Fatalf("first dispatch = %s, want nearline %s", first, near.ID)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("dispatcher did not claim a request")
}
