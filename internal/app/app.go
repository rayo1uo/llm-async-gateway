// Package app wires the HTTP API, batch controller, and dispatcher into one process.
package app

import (
	"context"
	"log/slog"
	"net/http"
	"sync"

	"github.com/redis/go-redis/v9"

	"github.com/rayo1uo/llm-async-gateway/internal/api"
	"github.com/rayo1uo/llm-async-gateway/internal/batch"
	"github.com/rayo1uo/llm-async-gateway/internal/budget"
	"github.com/rayo1uo/llm-async-gateway/internal/config"
	"github.com/rayo1uo/llm-async-gateway/internal/dispatch"
	"github.com/rayo1uo/llm-async-gateway/internal/store"
)

// App is the demo process: API and workers share one Redis store.
type App struct {
	Handler http.Handler
	Store   *store.Store
	disp    *dispatch.Dispatcher
	ctrl    *batch.Controller
	wg      sync.WaitGroup
}

// New connects the components. up may be nil to use cfg.UpstreamURL.
func New(cfg config.Config, rdb *redis.Client, logger *slog.Logger, up dispatch.Upstream) *App {
	if logger == nil {
		logger = slog.Default()
	}
	st := store.New(rdb, cfg.KeyPrefix)
	if up == nil {
		up = dispatch.NewHTTPClient(cfg.UpstreamURL)
	}
	lim := budget.NewLocal(budget.LocalConfig{
		MaxConcurrency: cfg.MaxConcurrency,
		ReservedBatch:  cfg.ReservedBatchSlots,
		RatePerSec:     cfg.RateLimitRPS,
		Burst:          cfg.RateBurst,
	})
	disp := dispatch.New(st, lim, up, dispatch.Options{
		LeaseTTL:       cfg.LeaseTTL,
		PollInterval:   cfg.PollInterval,
		RequestTimeout: cfg.RequestTimeout,
		ReserveEvery:   cfg.BatchReserveEvery,
		AgingSlack:     cfg.AgingSlack,
		ResultTTL:      cfg.ResultTTL,
		RetryBase:      cfg.RetryBase,
		RetryMax:       cfg.RetryMax,
		MaxAttempts:    cfg.MaxAttempts,
	}, logger, nil)
	ctrl := batch.New(st, batch.Options{
		Window:       cfg.BatchEnqueueWindow,
		PollInterval: cfg.PollInterval,
		ResultTTL:    cfg.ResultTTL,
	}, logger, nil)
	handler := api.New(st, api.Options{
		MaxFileBytes:            cfg.MaxFileBytes,
		DefaultCompletionWindow: cfg.DefaultCompletionWindow,
		DefaultNearlineDeadline: cfg.DefaultNearlineDeadline,
		IdempotencyTTL:          cfg.ResultTTL,
	}, logger, nil)
	return &App{Handler: handler, Store: st, disp: disp, ctrl: ctrl}
}

// Start launches the dispatcher and batch controller. Cancel ctx to stop them,
// then call Wait to block until in-flight upstream calls finish.
func (a *App) Start(ctx context.Context) {
	a.wg.Add(2)
	go func() {
		defer a.wg.Done()
		a.disp.Run(ctx)
	}()
	go func() {
		defer a.wg.Done()
		a.ctrl.Run(ctx)
	}()
}

// Wait blocks until Start's goroutines return.
func (a *App) Wait() { a.wg.Wait() }
