// Package dispatch leases queued requests and sends them to an upstream inference server.
package dispatch

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/rayo1uo/llm-async-gateway/internal/clock"
	"github.com/rayo1uo/llm-async-gateway/internal/id"
	"github.com/rayo1uo/llm-async-gateway/internal/model"
	"github.com/rayo1uo/llm-async-gateway/internal/retry"
	"github.com/rayo1uo/llm-async-gateway/internal/schedule"
	"github.com/rayo1uo/llm-async-gateway/internal/store"
)

// Budget grants dispatch slots. release is called when the worker drops the slot.
// Implementations include the local concurrency/rate limiter and, later, a
// Prometheus saturation budget.
type Budget interface {
	Allow(ctx context.Context, tier model.Tier) (release func(), ok bool)
}

// UpstreamResponse is one attempt against the inference server.
type UpstreamResponse struct {
	StatusCode int
	Body       []byte
	RetryAfter time.Duration
	RequestID  string
}

// Upstream performs a single inference call.
type Upstream interface {
	Do(ctx context.Context, u *model.Unit) (*UpstreamResponse, error)
}

// Options controls leasing, retry, and tier selection.
type Options struct {
	LeaseTTL       time.Duration
	PollInterval   time.Duration
	RequestTimeout time.Duration
	ReserveEvery   int
	AgingSlack     time.Duration
	ResultTTL      time.Duration
	RetryBase      time.Duration
	RetryMax       time.Duration
	MaxAttempts    int
}

// Dispatcher is a single-process worker loop. Multiple goroutines share one queue
// through claim leases, so a second replica can run the same loop safely.
type Dispatcher struct {
	store  *store.Store
	budget Budget
	up     Upstream
	opts   Options
	log    *slog.Logger
	clk    clock.Clock

	mu     sync.Mutex
	consec int
	wg     sync.WaitGroup
}

// New builds a dispatcher. clk may be nil to use the wall clock.
func New(st *store.Store, budget Budget, up Upstream, opts Options, logger *slog.Logger, clk clock.Clock) *Dispatcher {
	if clk == nil {
		clk = clock.Real{}
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Dispatcher{
		store:  st,
		budget: budget,
		up:     up,
		opts:   opts,
		log:    logger,
		clk:    clk,
	}
}

// Run polls until ctx is cancelled, then waits for in-flight attempts.
func (d *Dispatcher) Run(ctx context.Context) {
	d.tick(ctx)
	ticker := time.NewTicker(d.opts.PollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			d.wg.Wait()
			return
		case <-ticker.C:
			d.tick(context.Background())
		}
	}
}

func (d *Dispatcher) tick(ctx context.Context) {
	if err := ctx.Err(); err != nil {
		return
	}
	now := d.clk.Now()
	if _, err := d.store.Reclaim(ctx, now); err != nil {
		d.log.Error("reclaim leases", "err", err)
	}
	if _, err := d.store.PromoteRetries(ctx, now); err != nil {
		d.log.Error("promote retries", "err", err)
	}
	d.expireReady(ctx, now)
	d.drainExpired(ctx)
	d.pump(ctx)
}

func (d *Dispatcher) expireReady(ctx context.Context, now time.Time) {
	for _, tier := range []model.Tier{model.TierNearline, model.TierBatch} {
		if _, err := d.store.ExpireReady(ctx, tier, now); err != nil {
			d.log.Error("expire ready", "tier", tier, "err", err)
		}
	}
}

func (d *Dispatcher) drainExpired(ctx context.Context) {
	for i := 0; i < 64; i++ {
		id, err := d.store.PopExpired(ctx)
		if err != nil {
			d.log.Error("pop expired", "err", err)
			return
		}
		if id == "" {
			return
		}
		u, err := d.store.GetUnit(ctx, id)
		if err != nil {
			d.log.Error("load expired unit", "id", id, "err", err)
			continue
		}
		done, err := d.store.HasResult(ctx, id)
		if err != nil {
			d.log.Error("expired result check", "id", id, "err", err)
			continue
		}
		if done {
			continue
		}
		if err := d.finishExpired(ctx, u, ""); err != nil {
			d.log.Error("finish expired", "id", id, "err", err)
		}
	}
}

func (d *Dispatcher) pump(ctx context.Context) {
	for {
		if err := ctx.Err(); err != nil {
			return
		}
		order, err := d.candidateTiers(ctx)
		if err != nil {
			d.log.Error("candidate tiers", "err", err)
			return
		}
		if len(order) == 0 {
			return
		}
		var (
			tier    model.Tier
			release func()
			granted bool
		)
		for _, candidate := range order {
			rel, ok := d.budget.Allow(ctx, candidate)
			if !ok {
				continue
			}
			tier = candidate
			release = rel
			granted = true
			break
		}
		if !granted {
			return
		}
		owner, err := id.New("own_")
		if err != nil {
			release()
			d.log.Error("owner id", "err", err)
			return
		}
		unit, err := d.store.Claim(ctx, tier, d.clk.Now().Add(d.opts.LeaseTTL), owner)
		if err != nil || unit == nil {
			release()
			if err != nil {
				d.log.Error("claim", "tier", tier, "err", err)
			}
			return
		}
		d.note(tier)
		d.wg.Add(1)
		go func(u *model.Unit, owner string, release func()) {
			defer d.wg.Done()
			defer release()
			d.process(u, owner)
		}(unit, owner, release)
	}
}

// candidateTiers returns the preferred tier, then the other tier when it also
// has ready work. A budget refusal for the first tier must not skip the second.
func (d *Dispatcher) candidateTiers(ctx context.Context) ([]model.Tier, error) {
	preferred, ok := d.choose(ctx)
	if !ok {
		return nil, nil
	}
	order := []model.Tier{preferred}
	other := model.TierBatch
	if preferred == model.TierBatch {
		other = model.TierNearline
	}
	n, err := d.store.QueueLen(ctx, other)
	if err != nil {
		return nil, err
	}
	if n > 0 {
		order = append(order, other)
	}
	return order, nil
}

func (d *Dispatcher) choose(ctx context.Context) (model.Tier, bool) {
	nl, err := d.store.QueueLen(ctx, model.TierNearline)
	if err != nil {
		d.log.Error("queue len", "err", err)
		return "", false
	}
	bl, err := d.store.QueueLen(ctx, model.TierBatch)
	if err != nil {
		d.log.Error("queue len", "err", err)
		return "", false
	}
	nld, _, err := d.store.EarliestDeadline(ctx, model.TierNearline)
	if err != nil {
		d.log.Error("earliest deadline", "err", err)
		return "", false
	}
	bd, _, err := d.store.EarliestDeadline(ctx, model.TierBatch)
	if err != nil {
		d.log.Error("earliest deadline", "err", err)
		return "", false
	}
	d.mu.Lock()
	consec := d.consec
	d.mu.Unlock()
	return schedule.Pick(schedule.Input{
		NearlineReady:       int(nl),
		BatchReady:          int(bl),
		NearlineDeadline:    nld,
		BatchDeadline:       bd,
		ConsecutiveNearline: consec,
		ReserveEvery:        d.opts.ReserveEvery,
		Now:                 d.clk.Now(),
		AgingSlack:          d.opts.AgingSlack,
	})
}

func (d *Dispatcher) note(tier model.Tier) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if tier == model.TierNearline {
		d.consec++
		return
	}
	d.consec = 0
}

func (d *Dispatcher) process(u *model.Unit, owner string) {
	ctx := context.Background()
	done, err := d.store.HasResult(ctx, u.ID)
	if err != nil {
		d.log.Error("result check", "id", u.ID, "err", err)
		return
	}
	if done {
		return
	}
	cancelled, err := d.cancellation(ctx, u)
	if err != nil {
		d.log.Error("cancel check", "id", u.ID, "err", err)
		return
	}
	if cancelled {
		if err := d.finishCancelled(ctx, u, owner); err != nil {
			d.log.Error("finish cancelled", "id", u.ID, "err", err)
		}
		return
	}
	if d.clk.Now().UnixMilli() >= u.Deadline {
		if err := d.finishExpired(ctx, u, owner); err != nil {
			d.log.Error("finish expired", "id", u.ID, "err", err)
		}
		return
	}
	if u.Tier == model.TierNearline {
		if err := d.store.SetNearlineStatus(ctx, u.ID, model.StatusInProgress, 0); err != nil {
			d.log.Error("mark in progress", "id", u.ID, "err", err)
		}
	}

	u.Attempts++
	if err := d.store.SaveUnit(ctx, u); err != nil {
		d.log.Error("save attempt", "id", u.ID, "err", err)
		return
	}

	timeout := d.opts.RequestTimeout
	remain := time.UnixMilli(u.Deadline).Sub(d.clk.Now())
	if remain < timeout {
		timeout = remain
	}
	if timeout <= 0 {
		if err := d.finishExpired(ctx, u, owner); err != nil {
			d.log.Error("finish expired", "id", u.ID, "err", err)
		}
		return
	}

	reqCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	stopWatch := d.watch(reqCtx, cancel, u, owner)
	defer stopWatch()

	resp, callErr := d.up.Do(reqCtx, u)
	if callErr != nil && errors.Is(callErr, context.Canceled) {
		lost, lerr := d.leaseLost(ctx, u.ID, owner)
		if lerr != nil {
			d.log.Error("lease check", "id", u.ID, "err", lerr)
			return
		}
		if lost {
			d.log.Info("dropped attempt after lease loss", "id", u.ID)
			return
		}
		cancelled, err = d.cancellation(ctx, u)
		if err != nil {
			d.log.Error("cancel check", "id", u.ID, "err", err)
			return
		}
		if cancelled {
			if err := d.finishCancelled(ctx, u, owner); err != nil {
				d.log.Error("finish cancelled", "id", u.ID, "err", err)
			}
			return
		}
	}

	if callErr == nil && resp != nil && resp.StatusCode >= 200 && resp.StatusCode < 300 {
		if err := d.finishUpstream(ctx, u, owner, resp, model.StatusCompleted, "", ""); err != nil {
			d.log.Error("finish success", "id", u.ID, "err", err)
		}
		return
	}

	retryAfter := time.Duration(0)
	status := 0
	var body []byte
	var reqID string
	if resp != nil {
		retryAfter = resp.RetryAfter
		status = resp.StatusCode
		body = resp.Body
		reqID = resp.RequestID
	}
	retryable := callErr != nil || retry.RetryableStatus(status)
	if callErr != nil && errors.Is(callErr, context.Canceled) {
		retryable = false
	}
	if !retryable {
		msg := "upstream rejected the request"
		if callErr != nil {
			msg = callErr.Error()
		}
		if err := d.finishUpstream(ctx, u, owner, &UpstreamResponse{StatusCode: status, Body: body, RequestID: reqID}, model.StatusFailed, model.ErrUpstream, msg); err != nil {
			d.log.Error("finish failure", "id", u.ID, "err", err)
		}
		return
	}
	if u.Attempts >= d.opts.MaxAttempts {
		if err := d.finishUpstream(ctx, u, owner, &UpstreamResponse{StatusCode: status, Body: body, RequestID: reqID}, model.StatusFailed, model.ErrMaxAttempts, "exceeded max attempts before the deadline"); err != nil {
			d.log.Error("finish max attempts", "id", u.ID, "err", err)
		}
		return
	}
	remain = time.UnixMilli(u.Deadline).Sub(d.clk.Now())
	delay, ok := retry.NextDelay(u.Attempts, d.opts.RetryBase, d.opts.RetryMax, retryAfter, remain)
	if !ok {
		if err := d.finishExpired(ctx, u, owner); err != nil {
			d.log.Error("finish expired", "id", u.ID, "err", err)
		}
		return
	}
	if err := d.store.ParkRetry(ctx, u, owner, d.clk.Now().Add(delay)); err != nil {
		d.log.Error("park retry", "id", u.ID, "err", err)
		return
	}
	d.log.Info("retry scheduled", "id", u.ID, "attempt", u.Attempts, "delay", delay.String(), "status", status)
}

func (d *Dispatcher) watch(reqCtx context.Context, cancel context.CancelFunc, u *model.Unit, owner string) func() {
	ctx, stop := context.WithCancel(context.Background())
	go func() {
		interval := d.opts.LeaseTTL / 3
		if interval < 20*time.Millisecond {
			interval = 20 * time.Millisecond
		}
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-reqCtx.Done():
				return
			case <-ticker.C:
				ok, err := d.store.ExtendLease(context.Background(), u.ID, owner, d.clk.Now().Add(d.opts.LeaseTTL))
				if err != nil || !ok {
					cancel()
					return
				}
				cancelled, err := d.cancellation(context.Background(), u)
				if err == nil && cancelled {
					cancel()
					return
				}
			}
		}
	}()
	return stop
}

func (d *Dispatcher) leaseLost(ctx context.Context, id, owner string) (bool, error) {
	ok, err := d.store.ExtendLease(ctx, id, owner, d.clk.Now().Add(d.opts.LeaseTTL))
	if err != nil {
		return false, err
	}
	return !ok, nil
}

func (d *Dispatcher) cancellation(ctx context.Context, u *model.Unit) (bool, error) {
	hit, err := d.store.IsCancelled(ctx, u.ID)
	if err != nil {
		return false, err
	}
	if hit {
		return true, nil
	}
	if u.BatchID == "" {
		return false, nil
	}
	return d.store.IsBatchCancelled(ctx, u.BatchID)
}

func (d *Dispatcher) finishExpired(ctx context.Context, u *model.Unit, owner string) error {
	code := model.ErrDeadlineExceeded
	msg := "deadline exceeded before the request completed"
	status := model.StatusExpired
	if u.BatchID != "" {
		code = model.ErrBatchExpired
		msg = "This request was not executed before the completion window expired."
	}
	res := &model.Result{
		ID:           u.ID,
		Status:       status,
		ErrorCode:    code,
		ErrorMessage: msg,
		Attempts:     u.Attempts,
		FinishedAt:   d.clk.Now().Unix(),
	}
	return d.finish(ctx, u, owner, res, model.CountFailed)
}

func (d *Dispatcher) finishCancelled(ctx context.Context, u *model.Unit, owner string) error {
	code := model.ErrCancelled
	msg := "request was cancelled"
	if u.BatchID != "" {
		code = model.ErrBatchCancelled
		msg = "This request was cancelled before it executed."
	}
	res := &model.Result{
		ID:           u.ID,
		Status:       model.StatusCancelled,
		ErrorCode:    code,
		ErrorMessage: msg,
		Attempts:     u.Attempts,
		FinishedAt:   d.clk.Now().Unix(),
	}
	return d.finish(ctx, u, owner, res, model.CountFailed)
}

func (d *Dispatcher) finishUpstream(ctx context.Context, u *model.Unit, owner string, resp *UpstreamResponse, status, errCode, errMsg string) error {
	if resp == nil {
		resp = &UpstreamResponse{}
	}
	res := &model.Result{
		ID:           u.ID,
		Status:       status,
		StatusCode:   resp.StatusCode,
		Body:         asJSON(resp.Body),
		ErrorCode:    errCode,
		ErrorMessage: trim(errMsg, 512),
		RequestID:    resp.RequestID,
		Attempts:     u.Attempts,
		FinishedAt:   d.clk.Now().Unix(),
	}
	if res.RequestID == "" {
		res.RequestID = u.ID
	}
	field := model.CountFailed
	if status == model.StatusCompleted {
		field = model.CountCompleted
	}
	return d.finish(ctx, u, owner, res, field)
}

func (d *Dispatcher) finish(ctx context.Context, u *model.Unit, owner string, res *model.Result, countField string) error {
	var line *model.OutputLine
	if u.BatchID != "" {
		line = outputLine(u, res)
	}
	created, err := d.store.Finish(ctx, u, owner, res, line, countField, true, d.opts.ResultTTL)
	if err != nil {
		return err
	}
	if created && u.Tier == model.TierNearline {
		if err := d.store.SetNearlineStatus(ctx, u.ID, res.Status, res.FinishedAt); err != nil {
			return err
		}
	}
	d.log.Info("request finished", "id", u.ID, "tier", u.Tier, "status", res.Status, "attempts", res.Attempts, "created", created)
	return nil
}
