package store

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/rayo1uo/llm-async-gateway/internal/model"
)

func newStore(t *testing.T) *Store {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return New(rdb, "lag")
}

func TestEnqueueClaimFinish(t *testing.T) {
	st := newStore(t)
	ctx := context.Background()
	now := time.Now()
	u := &model.Unit{
		ID:       "req_1",
		Tier:     model.TierNearline,
		Endpoint: "/v1/chat/completions",
		Body:     []byte(`{"model":"m"}`),
		Deadline: now.Add(time.Hour).UnixMilli(),
		Created:  now.UnixMilli(),
	}
	if err := st.Enqueue(ctx, u); err != nil {
		t.Fatal(err)
	}
	if n, err := st.QueueLen(ctx, model.TierNearline); err != nil || n != 1 {
		t.Fatalf("len=%d err=%v", n, err)
	}
	got, err := st.Claim(ctx, model.TierNearline, now.Add(time.Minute), "own_1")
	if err != nil || got == nil || got.ID != u.ID {
		t.Fatalf("claim got=%v err=%v", got, err)
	}
	empty, err := st.Claim(ctx, model.TierNearline, now.Add(time.Minute), "own_2")
	if err != nil || empty != nil {
		t.Fatalf("second claim got=%v err=%v", empty, err)
	}
	ok, err := st.ExtendLease(ctx, u.ID, "own_1", now.Add(2*time.Minute))
	if err != nil || !ok {
		t.Fatalf("extend ok=%v err=%v", ok, err)
	}
	res := &model.Result{ID: u.ID, Status: model.StatusCompleted, StatusCode: 200, Body: []byte(`{"ok":true}`), FinishedAt: now.Unix()}
	created, err := st.Finish(ctx, got, "own_1", res, nil, model.CountCompleted, false, time.Hour)
	if err != nil || !created {
		t.Fatalf("finish created=%v err=%v", created, err)
	}
	created, err = st.Finish(ctx, got, "own_1", res, nil, model.CountCompleted, false, time.Hour)
	if err != nil || created {
		t.Fatalf("second finish created=%v err=%v", created, err)
	}
	loaded, err := st.GetResult(ctx, u.ID)
	if err != nil || loaded.Status != model.StatusCompleted {
		t.Fatalf("result=%v err=%v", loaded, err)
	}
}

func TestReclaimAndFence(t *testing.T) {
	st := newStore(t)
	ctx := context.Background()
	now := time.Now()
	u := &model.Unit{
		ID:       "batch_req_1",
		Tier:     model.TierBatch,
		Endpoint: "/v1/chat/completions",
		Body:     []byte(`{"model":"m"}`),
		Deadline: now.Add(time.Hour).UnixMilli(),
		Created:  now.UnixMilli(),
		BatchID:  "batch_1",
		CustomID: "c1",
	}
	if err := st.SetBatchTotal(ctx, u.BatchID, 1); err != nil {
		t.Fatal(err)
	}
	if err := st.Enqueue(ctx, u); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Claim(ctx, model.TierBatch, now.Add(-time.Second), "own_old"); err != nil {
		t.Fatal(err)
	}
	n, err := st.Reclaim(ctx, now)
	if err != nil || n != 1 {
		t.Fatalf("reclaim n=%d err=%v", n, err)
	}
	line := &model.OutputLine{
		ID:       u.ID,
		CustomID: u.CustomID,
		Response: &model.OutputResponse{StatusCode: 200, RequestID: u.ID, Body: []byte(`{"ok":true}`)},
	}
	res := &model.Result{ID: u.ID, Status: model.StatusCompleted, StatusCode: 200, Body: []byte(`{"ok":true}`), FinishedAt: now.Unix()}
	created, err := st.Finish(ctx, u, "own_old", res, line, model.CountCompleted, true, time.Hour)
	if err != nil || !created {
		t.Fatalf("old finish created=%v err=%v", created, err)
	}
	again, err := st.Claim(ctx, model.TierBatch, now.Add(time.Minute), "own_new")
	if err != nil || again == nil {
		t.Fatalf("reclaim should have restored the unit, got=%v err=%v", again, err)
	}
	created, err = st.Finish(ctx, again, "own_new", res, line, model.CountCompleted, true, time.Hour)
	if err != nil || created {
		t.Fatalf("duplicate finish created=%v err=%v", created, err)
	}
	counts, err := st.BatchCounts(ctx, u.BatchID)
	if err != nil {
		t.Fatal(err)
	}
	if counts.Completed != 1 {
		t.Fatalf("completed=%d", counts.Completed)
	}
	outstanding, err := st.Outstanding(ctx, u.BatchID)
	if err != nil || outstanding != 0 {
		t.Fatalf("outstanding=%d err=%v", outstanding, err)
	}
}

func TestParkAndPromote(t *testing.T) {
	st := newStore(t)
	ctx := context.Background()
	now := time.Now()
	u := &model.Unit{
		ID:       "req_retry",
		Tier:     model.TierNearline,
		Endpoint: "/v1/embeddings",
		Body:     []byte(`{"model":"m","input":"x"}`),
		Deadline: now.Add(time.Hour).UnixMilli(),
		Created:  now.UnixMilli(),
		Attempts: 1,
	}
	if err := st.Enqueue(ctx, u); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Claim(ctx, model.TierNearline, now.Add(time.Minute), "own"); err != nil {
		t.Fatal(err)
	}
	u.Attempts = 2
	if err := st.ParkRetry(ctx, u, "own", now.Add(-time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if n, err := st.QueueLen(ctx, model.TierNearline); err != nil || n != 0 {
		t.Fatalf("queued while parked: %d %v", n, err)
	}
	if _, err := st.PromoteRetries(ctx, now); err != nil {
		t.Fatal(err)
	}
	got, err := st.Claim(ctx, model.TierNearline, now.Add(time.Minute), "own_2")
	if err != nil || got == nil || got.Attempts != 2 {
		t.Fatalf("promoted unit=%v err=%v", got, err)
	}
}

func TestExpiredLeaseMovesToList(t *testing.T) {
	st := newStore(t)
	ctx := context.Background()
	now := time.Now()
	u := &model.Unit{
		ID:       "req_late",
		Tier:     model.TierBatch,
		Endpoint: "/v1/completions",
		Body:     []byte(`{"model":"m"}`),
		Deadline: now.Add(-time.Second).UnixMilli(),
		Created:  now.UnixMilli(),
		BatchID:  "batch_x",
		CustomID: "z",
	}
	if err := st.Enqueue(ctx, u); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Claim(ctx, model.TierBatch, now.Add(-time.Millisecond), "own"); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Reclaim(ctx, now); err != nil {
		t.Fatal(err)
	}
	id, err := st.PopExpired(ctx)
	if err != nil || id != u.ID {
		t.Fatalf("expired id=%q err=%v", id, err)
	}
}
