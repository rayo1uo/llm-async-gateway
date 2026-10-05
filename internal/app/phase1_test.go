package app_test

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/rayo1uo/llm-async-gateway/internal/app"
	"github.com/rayo1uo/llm-async-gateway/internal/config"
	"github.com/rayo1uo/llm-async-gateway/internal/model"
	"github.com/rayo1uo/llm-async-gateway/internal/observe"
)

func phase1Config(t *testing.T, upstream string) config.Config {
	t.Helper()
	cfg := config.Default()
	cfg.UpstreamURL = upstream
	cfg.PollInterval = 10 * time.Millisecond
	cfg.MaxConcurrency = 2
	cfg.ReservedBatchSlots = 0
	cfg.RateLimitRPS = 1000
	cfg.RateBurst = 100
	cfg.LeaseTTL = time.Second
	cfg.ControllerLockTTL = 250 * time.Millisecond
	cfg.RequestTimeout = 2 * time.Second
	cfg.RetryBase = 15 * time.Millisecond
	cfg.RetryMax = 100 * time.Millisecond
	cfg.BatchEnqueueWindow = 8
	cfg.BatchReserveEvery = 4
	cfg.AgingSlack = time.Minute
	cfg.DefaultNearlineDeadline = 20 * time.Second
	cfg.DefaultCompletionWindow = time.Hour
	cfg.ResultTTL = time.Hour
	cfg.MaxAttempts = 4
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	return cfg
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

func TestTwoDispatchersShareInflightCap(t *testing.T) {
	mr := miniredis.RunT(t)
	hold := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(hold) }) }
	up := &scriptedUpstream{hold: hold}
	upstream := httptest.NewServer(up)
	t.Cleanup(upstream.Close)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	cfg := phase1Config(t, upstream.URL)
	logger := discardLogger()
	apiApp := app.NewRole(cfg, rdb, logger, nil, app.RoleAPI)
	apiSrv := httptest.NewServer(apiApp.Handler)
	t.Cleanup(apiSrv.Close)

	ctx1, cancel1 := context.WithCancel(context.Background())
	ctx2, cancel2 := context.WithCancel(context.Background())
	d1 := app.NewRole(cfg, rdb, logger, nil, app.RoleDispatch)
	d2 := app.NewRole(cfg, rdb, logger, nil, app.RoleDispatch)
	d1.Start(ctx1)
	d2.Start(ctx2)
	t.Cleanup(func() {
		release()
		cancel1()
		cancel2()
		d1.Wait()
		d2.Wait()
	})

	for i := 0; i < 6; i++ {
		postJSON(t, apiSrv.URL+"/v1/requests", map[string]any{
			"deadline_seconds": 30,
			"body": map[string]any{
				"model":    "mock",
				"messages": []any{map[string]string{"role": "user", "content": "hold"}},
			},
		}, nil)
	}
	waitUntil(t, func() bool {
		_, inflight, _ := up.snapshot()
		return inflight == 2
	})
	time.Sleep(150 * time.Millisecond)
	_, inflight, maxInflight := up.snapshot()
	if inflight > 2 || maxInflight > 2 {
		t.Fatalf("global inflight=%d max=%d, cap is 2", inflight, maxInflight)
	}
	total, _, err := apiApp.Store.SlotCounts(context.Background(), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if total > 2 {
		t.Fatalf("redis inflight holders=%d", total)
	}

	metrics1 := httptest.NewServer(d1.Handler)
	metrics2 := httptest.NewServer(d2.Handler)
	t.Cleanup(metrics1.Close)
	t.Cleanup(metrics2.Close)
	body := getText(t, metrics1.URL+"/metrics") + getText(t, metrics2.URL+"/metrics")
	for _, name := range []string{
		observe.MetricQueueDepth,
		observe.MetricInflight,
		observe.MetricSlack,
		observe.MetricBudget,
		observe.MetricGateDecision,
	} {
		if !strings.Contains(body, name) {
			t.Fatalf("metrics missing %s\n%s", name, body)
		}
	}
	if !samplePositive(body, observe.MetricInflight) {
		t.Fatalf("inflight gauge stayed at zero\n%s", body)
	}
	release()
	waitUntil(t, func() bool {
		body = getText(t, metrics1.URL+"/metrics") + getText(t, metrics2.URL+"/metrics")
		return samplePositive(body, observe.MetricAttempts) && strings.Contains(body, `result="ok"`)
	})
	if !samplePositive(body, observe.MetricSlack) {
		t.Fatalf("deadline slack was not observed\n%s", body)
	}
}

func samplePositive(body, name string) bool {
	for _, line := range strings.Split(body, "\n") {
		if strings.HasPrefix(line, "#") || !strings.Contains(line, name) {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		switch fields[len(fields)-1] {
		case "0", "0.0", "0.00":
			continue
		default:
			return true
		}
	}
	return false
}

func TestControllerFailoverDoesNotDoubleCount(t *testing.T) {
	mr := miniredis.RunT(t)
	hold := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(hold) }) }
	t.Cleanup(release)
	up := &scriptedUpstream{hold: hold}
	upstream := httptest.NewServer(up)
	t.Cleanup(upstream.Close)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	cfg := phase1Config(t, upstream.URL)
	logger := discardLogger()
	apiA := app.NewRole(cfg, rdb, logger, nil, app.RoleAPI)
	apiB := app.NewRole(cfg, rdb, logger, nil, app.RoleAPI)
	srvA := httptest.NewServer(apiA.Handler)
	srvB := httptest.NewServer(apiB.Handler)
	t.Cleanup(srvA.Close)
	t.Cleanup(srvB.Close)

	dctx, dcancel := context.WithCancel(context.Background())
	disp := app.NewRole(cfg, rdb, logger, nil, app.RoleDispatch)
	disp.Start(dctx)
	t.Cleanup(func() {
		dcancel()
		disp.Wait()
	})

	c1ctx, c1cancel := context.WithCancel(context.Background())
	c2ctx, c2cancel := context.WithCancel(context.Background())
	c1 := app.NewRole(cfg, rdb, logger, nil, app.RoleControl)
	c2 := app.NewRole(cfg, rdb, logger, nil, app.RoleControl)
	c1.Start(c1ctx)
	c2.Start(c2ctx)
	t.Cleanup(func() {
		c1cancel()
		c2cancel()
		c1.Wait()
		c2.Wait()
	})

	waitUntil(t, func() bool {
		_, _, ok1 := c1.Leader(context.Background())
		_, _, ok2 := c2.Leader(context.Background())
		return ok1 != ok2
	})

	file := upload(t, srvA.URL, batchJSONL())
	batch := postJSON(t, srvB.URL+"/v1/batches", map[string]any{
		"input_file_id":     file["id"],
		"endpoint":          "/v1/chat/completions",
		"completion_window": "1h",
	}, nil)
	bid, _ := batch["id"].(string)
	waitUntil(t, func() bool {
		_, inflight, _ := up.snapshot()
		return inflight >= 1
	})

	_, _, lead1 := c1.Leader(context.Background())
	if lead1 {
		c1cancel()
	} else {
		c2cancel()
	}
	waitUntil(t, func() bool {
		_, _, ok1 := c1.Leader(context.Background())
		_, _, ok2 := c2.Leader(context.Background())
		if lead1 {
			return ok2 && !ok1
		}
		return ok1 && !ok2
	})
	release()

	final := pollBatch(t, srvA.URL, bid)
	if final.Status != model.StatusCompleted {
		t.Fatalf("status=%s counts=%+v errors=%v", final.Status, final.RequestCounts, final.Errors)
	}
	if final.RequestCounts.Total != 3 || final.RequestCounts.Completed != 3 || final.RequestCounts.Failed != 0 {
		t.Fatalf("counts=%+v", final.RequestCounts)
	}
	if final.OutputFileID == nil {
		t.Fatal("missing output file")
	}
	lines := fetchLines(t, srvB.URL, *final.OutputFileID)
	seen := map[string]int{}
	for _, ln := range lines {
		seen[ln.CustomID]++
	}
	if len(lines) != 3 || len(seen) != 3 {
		t.Fatalf("lines=%d unique=%v", len(lines), seen)
	}
	for _, id := range []string{"c1", "c2", "c3"} {
		if seen[id] != 1 {
			t.Fatalf("custom_id %s counted %d times", id, seen[id])
		}
	}
}

func TestAPIRoleDoesNotReconcile(t *testing.T) {
	mr := miniredis.RunT(t)
	up := httptest.NewServer(&scriptedUpstream{})
	t.Cleanup(up.Close)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	cfg := phase1Config(t, up.URL)
	apiApp := app.NewRole(cfg, rdb, discardLogger(), nil, app.RoleAPI)
	srv := httptest.NewServer(apiApp.Handler)
	t.Cleanup(srv.Close)

	file := upload(t, srv.URL, batchJSONL())
	batch := postJSON(t, srv.URL+"/v1/batches", map[string]any{
		"input_file_id":     file["id"],
		"endpoint":          "/v1/chat/completions",
		"completion_window": "1h",
	}, nil)
	time.Sleep(200 * time.Millisecond)
	got := getJSON(t, srv.URL+"/v1/batches/"+batch["id"].(string))
	if got["status"] != model.StatusValidating {
		t.Fatalf("api replica advanced the batch to %v", got["status"])
	}
}

func TestTraceFromRequestToUpstream(t *testing.T) {
	h := startHarness(t, nil, &scriptedUpstream{})
	view := postJSON(t, h.base+"/v1/requests", map[string]any{
		"deadline_seconds": 30,
		"body": map[string]any{
			"model":    "mock",
			"messages": []any{map[string]string{"role": "user", "content": "traced"}},
		},
	}, nil)
	done := pollRequest(t, h.base, view["id"].(string))
	if done["status"] != model.StatusCompleted {
		t.Fatalf("status=%v", done["status"])
	}
	var httpSpan, upSpan observe.Snapshot
	for _, snap := range h.app.Observer.Snapshots() {
		switch snap.Name {
		case "http.serve":
			if httpSpan.TraceID == "" {
				httpSpan = snap
			}
		case "dispatch.upstream":
			upSpan = snap
		}
	}
	if httpSpan.TraceID == "" || upSpan.TraceID == "" {
		t.Fatalf("missing spans http=%+v upstream=%+v", httpSpan, upSpan)
	}
	if httpSpan.TraceID != upSpan.TraceID {
		t.Fatalf("trace ids differ http=%s upstream=%s", httpSpan.TraceID, upSpan.TraceID)
	}
	if upSpan.ParentID != httpSpan.SpanID {
		t.Fatalf("upstream parent=%s want http span %s", upSpan.ParentID, httpSpan.SpanID)
	}
}

func getText(t *testing.T, url string) string {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET %s -> %d %s", url, resp.StatusCode, body)
	}
	return string(body)
}
