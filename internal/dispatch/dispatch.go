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
	"github.com/rayo1uo/llm-async-gateway/internal/observe"
	"github.com/rayo1uo/llm-async-gateway/internal/pipeline"
	"github.com/rayo1uo/llm-async-gateway/internal/retry"
	"github.com/rayo1uo/llm-async-gateway/internal/store"
)

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
	// Observer receives traces and metrics. Nil disables both.
	Observer *observe.Observer
}

// Dispatcher leases queued requests. It is assembled as a pipeline.Flow:
// merge selects a lane, the gate chain admits it, then a worker claims and calls upstream.
// Several processes can run the loop. The shared gate caps global in-flight work.
type Dispatcher struct {
	store *store.Store
	gate  pipeline.Gate
	up    Upstream
	opts  Options
	log   *slog.Logger
	clk   clock.Clock
	flow  *redisFlow
	pool  string

	wg sync.WaitGroup
}

// New builds a dispatcher. clk may be nil to use the wall clock.
// A nil gate fails closed: Apply refuses every request.
func New(st *store.Store, gate pipeline.Gate, up Upstream, opts Options, logger *slog.Logger, clk clock.Clock) *Dispatcher {
	if clk == nil {
		clk = clock.Real{}
	}
	if logger == nil {
		logger = slog.Default()
	}
	if gate == nil {
		gate = refuseGate{}
	}
	flow := newFlow(st, gate, opts, logger, clk)
	pool := "default"
	if st != nil && st.Pool() != "" {
		pool = st.Pool()
	}
	return &Dispatcher{
		store: st,
		gate:  gate,
		up:    up,
		opts:  opts,
		log:   logger,
		clk:   clk,
		flow:  flow,
		pool:  pool,
	}
}

// Flow exposes the assembled pipeline. Tests and the process wiring use it.
func (d *Dispatcher) Flow() pipeline.Flow { return d.flow }

type refuseGate struct{}

func (refuseGate) Budget(context.Context) float64 { return 0 }

func (refuseGate) Apply(context.Context, *pipeline.Request, *[]pipeline.ReleaseFunc) (pipeline.Verdict, error) {
	return pipeline.VerdictRefuse, nil
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
	for _, tier := range []model.Tier{model.TierInteractive, model.TierAsync, model.TierBatch} {
		if _, err := d.store.ExpireReady(ctx, tier, now); err != nil {
			d.log.Error("expire ready", "tier", tier, "err", err)
		}
	}
	if d.opts.Observer != nil && d.opts.Observer.Metrics != nil {
		budget := d.gate.Budget(ctx)
		for _, tier := range []string{"interactive", "async", "batch"} {
			d.opts.Observer.Metrics.SetBudget(d.pool, tier, "local", budget)
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
	channels := append([]pipeline.Channel(nil), d.flow.Channels()...)
	for {
		if err := ctx.Err(); err != nil {
			return
		}
		_, req, ok := d.flow.Merge().Next(ctx, channels)
		if !ok || req == nil {
			return
		}
		tier := model.Tier(req.Message.Tier)
		ch := channelFor(channels, req.Message.Tier)
		owner, err := id.New("own_")
		if err != nil {
			d.log.Error("owner id", "err", err)
			return
		}
		admitCtx := pipeline.WithAdmission(ctx, owner)
		var releases []pipeline.ReleaseFunc
		verdict, err := pipeline.ApplyChain(admitCtx, req, ch.Gates, &releases)
		if err != nil {
			pipeline.ReleaseAll(releases)
			d.log.Error("gate", "tier", tier, "err", err)
			return
		}
		if verdict != pipeline.VerdictContinue {
			pipeline.ReleaseAll(releases)
			channels = withoutTier(channels, req.Message.Tier)
			continue
		}
		_, claimSpan := d.startSpan(context.Background(), req.Message.Metadata[pipeline.MetaTraceparent], "dispatch.claim")
		unit, err := d.store.Claim(ctx, tier, d.clk.Now().Add(d.opts.LeaseTTL), owner)
		claimSpan.End()
		if err != nil || unit == nil {
			pipeline.ReleaseAll(releases)
			if err != nil {
				d.log.Error("claim", "tier", tier, "pool", d.pool, "owner", owner, "err", err)
			}
			return
		}
		if unit.TraceParent == "" {
			unit.TraceParent = req.Message.Metadata[pipeline.MetaTraceparent]
		}
		d.flow.merge.Note(unit.Tier)
		if d.opts.Observer != nil && d.opts.Observer.Metrics != nil {
			d.opts.Observer.Metrics.ObserveClaim(d.pool, string(unit.Tier), unit.Created, unit.Deadline, d.clk.Now())
		}
		d.wg.Add(1)
		go func(u *model.Unit, owner string, releases []pipeline.ReleaseFunc) {
			defer d.wg.Done()
			defer pipeline.ReleaseAll(releases)
			d.process(u, owner)
		}(unit, owner, releases)
	}
}

func channelFor(channels []pipeline.Channel, tier pipeline.Tier) pipeline.Channel {
	for _, ch := range channels {
		if ch.Tier == tier {
			return ch
		}
	}
	if len(channels) == 0 {
		return pipeline.Channel{}
	}
	return channels[0]
}

func withoutTier(channels []pipeline.Channel, tier pipeline.Tier) []pipeline.Channel {
	out := make([]pipeline.Channel, 0, len(channels))
	for _, ch := range channels {
		if ch.Tier != tier {
			out = append(out, ch)
		}
	}
	return out
}

func (d *Dispatcher) startSpan(ctx context.Context, traceparent, name string) (context.Context, *observe.Span) {
	if d.opts.Observer == nil {
		return ctx, nil
	}
	if traceparent != "" {
		ctx = observe.WithTraceparent(ctx, traceparent)
	}
	return d.opts.Observer.Start(ctx, name)
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
	if d.clk.Now().Unix() >= u.Deadline {
		if err := d.finishExpired(ctx, u, owner); err != nil {
			d.log.Error("finish expired", "id", u.ID, "err", err)
		}
		return
	}
	if u.Tier == model.TierAsync {
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
	remain := time.Unix(u.Deadline, 0).Sub(d.clk.Now())
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

	upCtx, upSpan := d.startSpan(reqCtx, u.TraceParent, "dispatch.upstream")
	started := d.clk.Now()
	resp, callErr := d.up.Do(upCtx, u)
	upSpan.End()
	if d.opts.Observer != nil && d.opts.Observer.Metrics != nil {
		code := 0
		if resp != nil {
			code = resp.StatusCode
		}
		d.opts.Observer.Metrics.ObserveUpstream(d.pool, string(u.Tier), code, d.clk.Now().Sub(started))
	}
	if callErr != nil && errors.Is(callErr, context.Canceled) {
		lost, lerr := d.leaseLost(ctx, u.ID, u.Token, owner)
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
	remain = time.Unix(u.Deadline, 0).Sub(d.clk.Now())
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
	d.countAttempt(u, "retry")
	d.log.Info("retry scheduled", "id", u.ID, "pool", d.pool, "owner", owner, "attempt", u.Attempts, "delay", delay.String(), "status", status)
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
				until := d.clk.Now().Add(d.opts.LeaseTTL)
				ok, err := d.store.ExtendLease(context.Background(), u.ID, u.Token, owner, until)
				if err == nil && ok {
					if _, serr := d.store.ExtendSlot(context.Background(), u.Tier, owner, until); serr != nil {
						d.log.Warn("extend inflight slot", "id", u.ID, "owner", owner, "err", serr)
					}
				}
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

func (d *Dispatcher) leaseLost(ctx context.Context, id, token, owner string) (bool, error) {
	ok, err := d.store.ExtendLease(ctx, id, token, owner, d.clk.Now().Add(d.opts.LeaseTTL))
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
	_, ackSpan := d.startSpan(context.Background(), u.TraceParent, "dispatch.ack")
	created, err := d.store.Finish(ctx, u, owner, res, line, countField, true, d.opts.ResultTTL)
	ackSpan.End()
	if err != nil {
		return err
	}
	if created && u.Tier == model.TierAsync {
		if err := d.store.SetNearlineStatus(ctx, u.ID, res.Status, res.FinishedAt); err != nil {
			return err
		}
	}
	if created {
		d.countAttempt(u, attemptResult(res.Status))
		d.observeTokens(u, res)
		d.flow.emit(pipeline.Result{
			ID:           u.ID,
			RequestToken: u.Token,
			StatusCode:   res.StatusCode,
			Payload:      append([]byte(nil), res.Body...),
			ErrorCode:    res.ErrorCode,
			ErrorMessage: res.ErrorMessage,
		})
	}
	d.log.Info("request finished", "id", u.ID, "tier", u.Tier, "pool", d.pool, "owner", owner, "status", res.Status, "attempts", res.Attempts, "created", created)
	return nil
}

func (d *Dispatcher) countAttempt(u *model.Unit, result string) {
	if d.opts.Observer == nil || d.opts.Observer.Metrics == nil || u == nil {
		return
	}
	d.opts.Observer.Metrics.Attempt(d.pool, string(u.Tier), result)
}

func attemptResult(status string) string {
	switch status {
	case model.StatusCompleted:
		return "ok"
	case model.StatusExpired:
		return "expired"
	case model.StatusCancelled:
		return "cancelled"
	default:
		return "failed"
	}
}

func (d *Dispatcher) observeTokens(u *model.Unit, res *model.Result) {
	if d.opts.Observer == nil || d.opts.Observer.Metrics == nil || res == nil || res.Status != model.StatusCompleted {
		return
	}
	in, out := usageTokens(res.Body)
	d.opts.Observer.Metrics.Tokens(d.pool, string(u.Tier), "input", in)
	d.opts.Observer.Metrics.Tokens(d.pool, string(u.Tier), "output", out)
}
