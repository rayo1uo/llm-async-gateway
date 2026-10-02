package store

import (
	"context"
	"errors"
	"fmt"
	"sync"
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

func TestCommitBatchInputWindowAndMismatch(t *testing.T) {
	st := newStore(t)
	ctx := context.Background()
	now := time.Now()
	lines := []model.InputLine{
		{CustomID: "a", Method: "POST", URL: "/v1/chat/completions", Body: []byte(`{"model":"m"}`), Index: 1},
		{CustomID: "b", Method: "POST", URL: "/v1/chat/completions", Body: []byte(`{"model":"m"}`), Index: 2},
	}
	if err := st.ReplaceBatchInputs(ctx, "batch_w", lines); err != nil {
		t.Fatal(err)
	}
	raw, line, err := st.PeekBatchInput(ctx, "batch_w")
	if err != nil || line == nil || line.CustomID != "a" {
		t.Fatalf("peek=%v err=%v", line, err)
	}
	unit := &model.Unit{
		ID: "batch_req_a", Tier: model.TierBatch, Endpoint: line.URL, Body: line.Body,
		Deadline: now.Add(time.Hour).UnixMilli(), Created: now.UnixMilli(),
		BatchID: "batch_w", CustomID: line.CustomID,
	}
	status, err := st.CommitBatchInput(ctx, "batch_w", raw+"nope", unit, 1)
	if err != nil || status != "retry" {
		t.Fatalf("mismatch status=%s err=%v", status, err)
	}
	if n, err := st.BatchInputLen(ctx, "batch_w"); err != nil || n != 2 {
		t.Fatalf("input len after mismatch=%d err=%v", n, err)
	}
	status, err = st.CommitBatchInput(ctx, "batch_w", raw, unit, 1)
	if err != nil || status != "ok" {
		t.Fatalf("commit status=%s err=%v", status, err)
	}
	second := *unit
	second.ID = "batch_req_b"
	second.CustomID = "b"
	raw2, _, err := st.PeekBatchInput(ctx, "batch_w")
	if err != nil {
		t.Fatal(err)
	}
	status, err = st.CommitBatchInput(ctx, "batch_w", raw2, &second, 1)
	if err != nil || status != "full" {
		t.Fatalf("window status=%s err=%v", status, err)
	}
	if n, err := st.BatchInputLen(ctx, "batch_w"); err != nil || n != 1 {
		t.Fatalf("input len=%d err=%v", n, err)
	}
	if out, err := st.Outstanding(ctx, "batch_w"); err != nil || out != 1 {
		t.Fatalf("outstanding=%d err=%v", out, err)
	}
	if n, err := st.QueueLen(ctx, model.TierBatch); err != nil || n != 1 {
		t.Fatalf("queued=%d err=%v", n, err)
	}
}

func TestCommitBatchTerminalRemovesHead(t *testing.T) {
	st := newStore(t)
	ctx := context.Background()
	line := model.InputLine{CustomID: "c", Method: "POST", URL: "/v1/chat/completions", Body: []byte(`{"model":"m"}`), Index: 1}
	if err := st.ReplaceBatchInputs(ctx, "batch_t", []model.InputLine{line}); err != nil {
		t.Fatal(err)
	}
	raw, got, err := st.PeekBatchInput(ctx, "batch_t")
	if err != nil || got == nil {
		t.Fatal(err)
	}
	unit := &model.Unit{ID: "batch_req_c", BatchID: "batch_t", CustomID: "c", Tier: model.TierBatch}
	res := &model.Result{ID: unit.ID, Status: model.StatusExpired, ErrorCode: model.ErrBatchExpired, ErrorMessage: "expired", FinishedAt: time.Now().Unix()}
	out := &model.OutputLine{ID: unit.ID, CustomID: "c", Error: &model.OutputError{Code: model.ErrBatchExpired, Message: "expired"}}
	status, err := st.CommitBatchTerminal(ctx, "batch_t", raw, unit, res, out, time.Hour)
	if err != nil || status != "ok" {
		t.Fatalf("status=%s err=%v", status, err)
	}
	if n, err := st.BatchInputLen(ctx, "batch_t"); err != nil || n != 0 {
		t.Fatalf("inputs left=%d err=%v", n, err)
	}
	counts, err := st.BatchCounts(ctx, "batch_t")
	if err != nil || counts.Failed != 1 {
		t.Fatalf("counts=%+v err=%v", counts, err)
	}
}

func TestAcceptNearlineIsIdempotent(t *testing.T) {
	st := newStore(t)
	ctx := context.Background()
	now := time.Now()
	first := nearlinePair("req_a", now)
	id, created, err := st.AcceptNearline(ctx, "same-key", time.Hour, first.rec, first.unit)
	if err != nil || !created || id != "req_a" {
		t.Fatalf("id=%s created=%v err=%v", id, created, err)
	}
	second := nearlinePair("req_b", now)
	id, created, err = st.AcceptNearline(ctx, "same-key", time.Hour, second.rec, second.unit)
	if err != nil || created || id != "req_a" {
		t.Fatalf("replay id=%s created=%v err=%v", id, created, err)
	}
	if n, err := st.QueueLen(ctx, model.TierNearline); err != nil || n != 1 {
		t.Fatalf("queue=%d err=%v", n, err)
	}
	if _, err := st.GetNearline(ctx, "req_a"); err != nil {
		t.Fatal(err)
	}
	if _, err := st.GetUnit(ctx, "req_b"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("duplicate unit err=%v", err)
	}

	var wg sync.WaitGroup
	ids := make([]string, 8)
	for i := range ids {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			pair := nearlinePair(fmt.Sprintf("req_race_%d", i), now)
			got, _, err := st.AcceptNearline(ctx, "race-key", time.Hour, pair.rec, pair.unit)
			if err != nil {
				t.Errorf("accept: %v", err)
				return
			}
			ids[i] = got
		}(i)
	}
	wg.Wait()
	for _, id := range ids[1:] {
		if id == "" || id != ids[0] {
			t.Fatalf("concurrent ids=%v", ids)
		}
	}
	if _, err := st.GetNearline(ctx, ids[0]); err != nil {
		t.Fatal(err)
	}
	if n, err := st.QueueLen(ctx, model.TierNearline); err != nil || n != 2 {
		t.Fatalf("queue after race=%d err=%v (first key still queued)", n, err)
	}
}

func TestExpireReadyDoesNotNeedALease(t *testing.T) {
	st := newStore(t)
	ctx := context.Background()
	now := time.Now()
	late := &model.Unit{
		ID: "req_late", Tier: model.TierNearline, Endpoint: "/v1/chat/completions",
		Body: []byte(`{"model":"m"}`), Deadline: now.Add(-time.Second).UnixMilli(), Created: now.UnixMilli(),
	}
	fresh := &model.Unit{
		ID: "req_fresh", Tier: model.TierNearline, Endpoint: "/v1/chat/completions",
		Body: []byte(`{"model":"m"}`), Deadline: now.Add(time.Hour).UnixMilli(), Created: now.UnixMilli(),
	}
	if err := st.Enqueue(ctx, late); err != nil {
		t.Fatal(err)
	}
	if err := st.Enqueue(ctx, fresh); err != nil {
		t.Fatal(err)
	}
	n, err := st.ExpireReady(ctx, model.TierNearline, now)
	if err != nil || n != 1 {
		t.Fatalf("expired=%d err=%v", n, err)
	}
	id, err := st.PopExpired(ctx)
	if err != nil || id != late.ID {
		t.Fatalf("popped=%s err=%v", id, err)
	}
	if q, err := st.QueueLen(ctx, model.TierNearline); err != nil || q != 1 {
		t.Fatalf("remaining=%d err=%v", q, err)
	}
}

type nearlinePairSet struct {
	rec  *model.Nearline
	unit *model.Unit
}

func nearlinePair(id string, now time.Time) nearlinePairSet {
	rec := &model.Nearline{
		ID: id, Object: model.ObjectRequest, Status: model.StatusQueued,
		Endpoint: "/v1/chat/completions", CreatedAt: now.Unix(), Deadline: now.Add(time.Minute).Unix(),
		DeadlineMS: now.Add(time.Minute).UnixMilli(), Metadata: map[string]string{},
	}
	unit := &model.Unit{
		ID: id, Tier: model.TierNearline, Endpoint: rec.Endpoint,
		Body: []byte(`{"model":"m"}`), Deadline: rec.DeadlineMS, Created: now.UnixMilli(),
	}
	return nearlinePairSet{rec: rec, unit: unit}
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
