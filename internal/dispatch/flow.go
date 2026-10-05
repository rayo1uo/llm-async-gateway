package dispatch

import (
	"context"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/rayo1uo/llm-async-gateway/internal/clock"
	"github.com/rayo1uo/llm-async-gateway/internal/model"
	"github.com/rayo1uo/llm-async-gateway/internal/pipeline"
	"github.com/rayo1uo/llm-async-gateway/internal/schedule"
	"github.com/rayo1uo/llm-async-gateway/internal/store"
)

// redisFlow is the Phase 1 pipeline.Flow. Start runs one consume loop that
// admits and claims in priority order, plus the retry and result workers.
// Inference workers live in Dispatcher and read the merge policy output.
type redisFlow struct {
	d        *Dispatcher
	merge    *priorityMerge
	sources  []pipeline.RequestChannel
	retryCh  chan pipeline.RetryMessage
	resultCh chan pipeline.Result

	startOnce sync.Once
	stopOnce  sync.Once
	shutOnce  sync.Once

	consumeCancel context.CancelFunc
	consumeWg     sync.WaitGroup
	drainCancel   context.CancelFunc
	drainWg       sync.WaitGroup
	fanWG         sync.WaitGroup
}

var _ pipeline.Flow = (*redisFlow)(nil)
var _ pipeline.RequestMergePolicy = (*priorityMerge)(nil)

func (f *redisFlow) Characteristics() pipeline.Characteristics {
	// Retries are parked in Redis with a future score. Workers pass the
	// duration and do not sleep it. The broker message has no separate
	// ingestion timestamp beyond Message.Created.
	return pipeline.Characteristics{HasExternalBackoff: true, SupportsMessageLatency: false}
}

func (f *redisFlow) RequestChannels() []pipeline.RequestChannel { return f.sources }

func (f *redisFlow) RetryChannel() chan pipeline.RetryMessage { return f.retryCh }

func (f *redisFlow) ResultChannel() chan pipeline.Result { return f.resultCh }

// Start launches the consume loop and the workers that own retries and results.
// Canceling ctx stops the consume loop. StopConsuming still closes channels
// and waits for that loop to leave.
func (f *redisFlow) Start(ctx context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}
	f.startOnce.Do(func() {
		consumeCtx, cancel := context.WithCancel(ctx)
		f.consumeCancel = cancel
		drainCtx, drainCancel := context.WithCancel(context.Background())
		f.drainCancel = drainCancel

		f.drainWg.Add(2)
		go func() {
			defer f.drainWg.Done()
			f.retryWorker(drainCtx)
		}()
		go func() {
			defer f.drainWg.Done()
			f.resultWorker(drainCtx)
		}()

		f.consumeWg.Add(1)
		go func() {
			defer f.consumeWg.Done()
			f.loop(consumeCtx)
		}()
	})
}

// StopConsuming stops new claims and closes request channels after the
// consume loop returns. The merge policy then closes its output.
func (f *redisFlow) StopConsuming() {
	f.stopOnce.Do(func() {
		if f.consumeCancel != nil {
			f.consumeCancel()
		}
		f.consumeWg.Wait()
		for _, src := range f.sources {
			if src.Channel != nil {
				close(src.Channel)
			}
		}
		f.fanWG.Wait()
	})
}

// Shutdown stops the retry and result workers. Call it after inference
// workers have finished sending on those channels.
func (f *redisFlow) Shutdown() {
	f.shutOnce.Do(func() {
		if f.drainCancel != nil {
			f.drainCancel()
		}
		f.drainWg.Wait()
	})
}

func (f *redisFlow) loop(ctx context.Context) {
	f.d.tick(ctx)
	interval := f.d.opts.PollInterval
	if interval <= 0 {
		interval = 50 * time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			f.d.tick(ctx)
		}
	}
}

func (f *redisFlow) retryWorker(ctx context.Context) {
	park := func(msg pipeline.RetryMessage) {
		if msg.Request == nil || f.d == nil {
			return
		}
		u := unitFromRequest(msg.Request)
		until := f.d.clk.Now().Add(msg.Backoff)
		if err := f.d.store.ParkRetry(context.Background(), u, msg.Owner, until); err != nil {
			f.d.log.Error("park retry", "id", u.ID, "owner", msg.Owner, "err", err)
		}
	}
	for {
		select {
		case <-ctx.Done():
			for {
				select {
				case msg := <-f.retryCh:
					park(msg)
				default:
					return
				}
			}
		case msg := <-f.retryCh:
			park(msg)
		}
	}
}

func (f *redisFlow) resultWorker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			for {
				select {
				case <-f.resultCh:
				default:
					return
				}
			}
		case <-f.resultCh:
		}
	}
}

type priorityMerge struct {
	flow  *redisFlow
	store *store.Store
	opts  Options
	clk   clock.Clock
	log   *slog.Logger
	pool  string

	mu     sync.Mutex
	consec int
}

func newFlow(d *Dispatcher) *redisFlow {
	logger := d.log
	if logger == nil {
		logger = slog.Default()
	}
	clk := d.clk
	if clk == nil {
		clk = clock.Real{}
	}
	pool := d.pool
	if pool == "" {
		pool = "default"
	}
	gate := d.gate
	if gate == nil {
		gate = refuseGate{}
	}
	flow := &redisFlow{
		d:        d,
		retryCh:  make(chan pipeline.RetryMessage, 64),
		resultCh: make(chan pipeline.Result, 64),
	}
	merge := &priorityMerge{flow: flow, store: d.store, opts: d.opts, clk: clk, log: logger, pool: pool}
	flow.merge = merge
	flow.sources = []pipeline.RequestChannel{
		newSource(d.store, model.TierInteractive, pipeline.TierInteractive, gate, pool),
		newSource(d.store, model.TierAsync, pipeline.TierAsync, gate, pool),
		newSource(d.store, model.TierBatch, pipeline.TierBatch, gate, pool),
	}
	return flow
}

func newSource(st *store.Store, tier model.Tier, pt pipeline.Tier, gate pipeline.Gate, pool string) pipeline.RequestChannel {
	queue := ""
	if st != nil {
		queue = st.QueueKey(tier)
	}
	return pipeline.RequestChannel{
		Queue:        queue,
		Tier:         pt,
		Gate:         gate,
		WorkerPoolID: pool,
		Channel:      make(chan pipeline.DispatchMessage),
	}
}

// MergeRequestChannels starts the fan-in from each tier channel into one
// pool channel. Admission order is decided by the consume loop before the
// send, so this fan-in keeps that order.
func (m *priorityMerge) MergeRequestChannels(channels []pipeline.RequestChannel) pipeline.PoolDispatch {
	out := make(chan pipeline.DispatchMessage)
	if m.flow != nil {
		m.flow.fanWG.Add(1)
	}
	go func() {
		if m.flow != nil {
			defer m.flow.fanWG.Done()
		}
		defer close(out)
		var wg sync.WaitGroup
		for _, src := range channels {
			if src.Channel == nil {
				continue
			}
			wg.Add(1)
			go func(ch <-chan pipeline.DispatchMessage) {
				defer wg.Done()
				for msg := range ch {
					out <- msg
				}
			}(src.Channel)
		}
		wg.Wait()
	}()
	pool := m.pool
	if pool == "" {
		pool = "default"
	}
	return pipeline.PoolDispatch{Channels: map[string]chan pipeline.DispatchMessage{pool: out}}
}

// Next peeks the preferred ready request. It does not claim.
// channels limits the lanes still eligible in this poll; a refused lane is omitted.
func (m *priorityMerge) Next(ctx context.Context, channels []pipeline.RequestChannel) (pipeline.RequestChannel, *pipeline.Request, bool) {
	allowed := map[model.Tier]bool{}
	for _, ch := range channels {
		allowed[model.Tier(ch.Tier)] = true
	}
	tier, ok := m.choose(ctx, allowed)
	if !ok {
		return pipeline.RequestChannel{}, nil, false
	}
	unit, err := m.store.Peek(ctx, tier)
	if err != nil {
		m.log.Error("peek queue", "tier", tier, "err", err)
		return pipeline.RequestChannel{}, nil, false
	}
	if unit == nil {
		return pipeline.RequestChannel{}, nil, false
	}
	req := store.PipelineRequest(unit, m.store.QueueKey(tier))
	return channelFor(channels, pipeline.Tier(tier)), &req, true
}

func (m *priorityMerge) Note(tier model.Tier) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if tier == model.TierBatch {
		m.consec = 0
		return
	}
	m.consec++
}

func (m *priorityMerge) choose(ctx context.Context, allowed map[model.Tier]bool) (model.Tier, bool) {
	ready := map[model.Tier]int{}
	deadline := map[model.Tier]time.Time{}
	for _, tier := range []model.Tier{model.TierInteractive, model.TierAsync, model.TierBatch} {
		if !allowed[tier] {
			continue
		}
		n, err := m.store.QueueLen(ctx, tier)
		if err != nil {
			m.log.Error("queue len", "tier", tier, "err", err)
			return "", false
		}
		ready[tier] = int(n)
		dl, ok, err := m.store.EarliestDeadline(ctx, tier)
		if err != nil {
			m.log.Error("earliest deadline", "tier", tier, "err", err)
			return "", false
		}
		if ok {
			deadline[tier] = dl
		}
	}
	m.mu.Lock()
	consec := m.consec
	m.mu.Unlock()
	return schedule.Pick(schedule.Input{
		InteractiveReady:    ready[model.TierInteractive],
		InteractiveDeadline: deadline[model.TierInteractive],
		NearlineReady:       ready[model.TierAsync],
		BatchReady:          ready[model.TierBatch],
		NearlineDeadline:    deadline[model.TierAsync],
		BatchDeadline:       deadline[model.TierBatch],
		ConsecutiveNearline: consec,
		ReserveEvery:        m.opts.ReserveEvery,
		Now:                 m.clk.Now(),
		AgingSlack:          m.opts.AgingSlack,
	})
}

func channelFor(channels []pipeline.RequestChannel, tier pipeline.Tier) pipeline.RequestChannel {
	for _, ch := range channels {
		if ch.Tier == tier {
			return ch
		}
	}
	if len(channels) == 0 {
		return pipeline.RequestChannel{}
	}
	return channels[0]
}

func withoutTier(channels []pipeline.RequestChannel, tier pipeline.Tier) []pipeline.RequestChannel {
	out := make([]pipeline.RequestChannel, 0, len(channels))
	for _, ch := range channels {
		if ch.Tier != tier {
			out = append(out, ch)
		}
	}
	return out
}

func unitFromRequest(req *pipeline.Request) *model.Unit {
	if req == nil {
		return &model.Unit{}
	}
	u := &model.Unit{
		ID:       req.Message.ID,
		Tier:     model.Tier(req.Message.Tier),
		Endpoint: req.Message.Endpoint,
		Body:     req.Message.Payload,
		Deadline: req.Message.Deadline,
		Created:  req.Message.Created,
		Attempts: req.RetryCount,
		Token:    req.RequestToken,
	}
	if req.Message.Metadata == nil {
		return u
	}
	u.TraceParent = req.Message.Metadata[pipeline.MetaTraceparent]
	u.BatchID = req.Message.Metadata[pipeline.MetaJobID]
	u.CustomID = req.Message.Metadata[pipeline.MetaCustomID]
	if idx := req.Message.Metadata[pipeline.MetaRequestIndex]; idx != "" {
		n, err := strconv.Atoi(idx)
		if err == nil {
			u.LineIndex = n
		}
	}
	return u
}
