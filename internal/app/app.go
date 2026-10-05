// Package app wires the HTTP API, batch controller, and dispatcher.
// Role selects which of those run in this process. The all-in-one role keeps
// `make demo` on a single binary. API replicas do not reconcile batches.
package app

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/rayo1uo/llm-async-gateway/internal/api"
	"github.com/rayo1uo/llm-async-gateway/internal/batch"
	"github.com/rayo1uo/llm-async-gateway/internal/budget"
	"github.com/rayo1uo/llm-async-gateway/internal/config"
	"github.com/rayo1uo/llm-async-gateway/internal/dispatch"
	"github.com/rayo1uo/llm-async-gateway/internal/model"
	"github.com/rayo1uo/llm-async-gateway/internal/observe"
	"github.com/rayo1uo/llm-async-gateway/internal/store"
)

// Role is the set of components a process runs.
type Role uint8

const (
	// RoleAPI serves HTTP and does not reconcile or dispatch.
	RoleAPI Role = 1 << iota
	// RoleDispatch claims queue messages and calls the upstream.
	RoleDispatch
	// RoleControl campaigns for the single-active batch controller.
	RoleControl
	// RoleAll is the demo process: API, dispatcher, and controller together.
	RoleAll = RoleAPI | RoleDispatch | RoleControl
)

// Has reports whether r includes bit.
func (r Role) Has(bit Role) bool { return r&bit != 0 }

// App is one gateway process. Components that this role does not include stay nil.
type App struct {
	Handler  http.Handler
	Store    *store.Store
	Observer *observe.Observer

	disp *dispatch.Dispatcher
	ctrl *batch.Controller
	role Role
	wg   sync.WaitGroup
}

// New connects every component. up may be nil to use cfg.UpstreamURL.
func New(cfg config.Config, rdb *redis.Client, logger *slog.Logger, up dispatch.Upstream) *App {
	return NewRole(cfg, rdb, logger, up, RoleAll)
}

// NewRole connects only the components in role.
func NewRole(cfg config.Config, rdb *redis.Client, logger *slog.Logger, up dispatch.Upstream, role Role) *App {
	if logger == nil {
		logger = slog.Default()
	}
	if role == 0 {
		role = RoleAll
	}
	st := store.NewInPool(rdb, cfg.KeyPrefix, cfg.Pool)
	lim := budget.NewLocal(budget.LocalConfig{
		MaxConcurrency: cfg.MaxConcurrency,
		ReservedBatch:  cfg.ReservedBatchSlots,
		RatePerSec:     cfg.RateLimitRPS,
		Burst:          cfg.RateBurst,
	})
	gate := budget.NewSharedGate(budget.SharedConfig{
		Local:    lim,
		Store:    st,
		Max:      cfg.MaxConcurrency,
		LeaseTTL: cfg.LeaseTTL,
		Pool:     st.Pool(),
		Metrics:  nil,
		Logger:   logger,
	})
	// Metrics are built after the gate so the scrape can sample Budget().
	metrics := observe.NewMetrics(func() observe.QueueStats {
		return scrapeStats(st, gate)
	})
	metrics.Prime(st.Pool())
	obs := observe.New(metrics, logger)
	gate = budget.NewSharedGate(budget.SharedConfig{
		Local:    lim,
		Store:    st,
		Max:      cfg.MaxConcurrency,
		LeaseTTL: cfg.LeaseTTL,
		Pool:     st.Pool(),
		Metrics:  metrics,
		Logger:   logger,
	})
	if _, err := cfg.GateList(); err != nil {
		logger.Error("refusing to start an open dispatcher", "err", err)
		gate = budget.NewSharedGate(budget.SharedConfig{Max: 0, Pool: st.Pool(), Logger: logger})
	}

	var disp *dispatch.Dispatcher
	if role.Has(RoleDispatch) {
		if up == nil {
			up = dispatch.NewHTTPClient(cfg.UpstreamURL)
		}
		disp = dispatch.New(st, gate, up, dispatch.Options{
			LeaseTTL:       cfg.LeaseTTL,
			PollInterval:   cfg.PollInterval,
			RequestTimeout: cfg.RequestTimeout,
			ReserveEvery:   cfg.BatchReserveEvery,
			AgingSlack:     cfg.AgingSlack,
			ResultTTL:      cfg.ResultTTL,
			RetryBase:      cfg.RetryBase,
			RetryMax:       cfg.RetryMax,
			MaxAttempts:    cfg.MaxAttempts,
			Observer:       obs,
		}, logger, nil)
	}
	var ctrl *batch.Controller
	if role.Has(RoleControl) {
		ctrl = batch.New(st, batch.Options{
			Window:       cfg.BatchEnqueueWindow,
			PollInterval: cfg.PollInterval,
			ResultTTL:    cfg.ResultTTL,
			LockTTL:      cfg.ControllerLockTTL,
			Observer:     obs,
		}, logger, nil)
	}

	application := &App{Store: st, Observer: obs, disp: disp, ctrl: ctrl, role: role}
	application.Handler = application.routes(st, cfg, logger)
	return application
}

func (a *App) routes(st *store.Store, cfg config.Config, logger *slog.Logger) http.Handler {
	if a.role.Has(RoleAPI) {
		h := api.New(st, api.Options{
			MaxFileBytes:            cfg.MaxFileBytes,
			DefaultCompletionWindow: cfg.DefaultCompletionWindow,
			DefaultNearlineDeadline: cfg.DefaultNearlineDeadline,
			IdempotencyTTL:          cfg.ResultTTL,
			Observer:                a.Observer,
		}, logger, nil)
		if a.Observer != nil && a.Observer.Metrics != nil {
			h.Handle("GET /metrics", a.Observer.Metrics.Handler())
		}
		if a.role.Has(RoleControl) {
			h.HandleFunc("GET /leaderz", a.leaderz)
		}
		return h
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		if err := st.Ping(r.Context()); err != nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "redis unavailable"})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
	})
	if a.Observer != nil && a.Observer.Metrics != nil {
		mux.Handle("GET /metrics", a.Observer.Metrics.Handler())
	}
	if a.role.Has(RoleControl) {
		mux.HandleFunc("GET /leaderz", a.leaderz)
	}
	return mux
}

func (a *App) leaderz(w http.ResponseWriter, r *http.Request) {
	if a.ctrl == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "standby"})
		return
	}
	owner, token, ok := a.ctrl.Leadership(r.Context())
	if !ok {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "standby", "owner": owner})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "leader", "owner": owner, "token": token})
}

// Leader reports the controller lock held by this process.
func (a *App) Leader(ctx context.Context) (owner, token string, ok bool) {
	if a.ctrl == nil {
		return "", "", false
	}
	return a.ctrl.Leadership(ctx)
}

// Start launches the workers selected by this process's role.
// Cancel ctx to stop them, then call Wait.
func (a *App) Start(ctx context.Context) {
	if a.disp != nil {
		a.wg.Add(1)
		go func() {
			defer a.wg.Done()
			a.disp.Run(ctx)
		}()
	}
	if a.ctrl != nil {
		a.wg.Add(1)
		go func() {
			defer a.wg.Done()
			a.ctrl.Run(ctx)
		}()
	}
}

// Wait blocks until Start's goroutines return.
func (a *App) Wait() { a.wg.Wait() }

func scrapeStats(st *store.Store, gate *budget.SharedGate) observe.QueueStats {
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	depth := map[string]float64{}
	for _, tier := range []model.Tier{model.TierInteractive, model.TierAsync, model.TierBatch} {
		n, err := st.QueueLen(ctx, tier)
		if err != nil {
			continue
		}
		depth[string(tier)] = float64(n)
	}
	claimed, _ := st.ClaimedLen(ctx)
	_, byTier, _ := st.SlotCounts(ctx, time.Now())
	inflight := map[string]float64{}
	for tier, n := range byTier {
		inflight[tier] = float64(n)
	}
	jobs, _ := st.BatchStatusCounts(ctx)
	stats := observe.QueueStats{
		Pool:      st.Pool(),
		Depth:     depth,
		Claimed:   float64(claimed),
		Inflight:  inflight,
		BatchJobs: jobs,
	}
	if gate != nil {
		stats.Budget = gate.Budget(ctx)
	}
	return stats
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(v)
}
