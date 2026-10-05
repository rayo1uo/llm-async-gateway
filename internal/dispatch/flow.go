package dispatch

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/rayo1uo/llm-async-gateway/internal/clock"
	"github.com/rayo1uo/llm-async-gateway/internal/model"
	"github.com/rayo1uo/llm-async-gateway/internal/pipeline"
	"github.com/rayo1uo/llm-async-gateway/internal/schedule"
	"github.com/rayo1uo/llm-async-gateway/internal/store"
)

// redisFlow is the Phase 1 pipeline.Flow: one channel per tier, a shared gate
// chain, and the async-over-batch merge policy.
type redisFlow struct {
	channels []pipeline.Channel
	merge    *priorityMerge
	results  chan pipeline.Result
}

func (f *redisFlow) Channels() []pipeline.Channel { return f.channels }

func (f *redisFlow) Merge() pipeline.MergePolicy { return f.merge }

func (f *redisFlow) Results() <-chan pipeline.Result { return f.results }

func (f *redisFlow) emit(res pipeline.Result) {
	select {
	case f.results <- res:
	default:
	}
}

type priorityMerge struct {
	store *store.Store
	opts  Options
	clk   clock.Clock
	log   *slog.Logger
	pool  string

	mu     sync.Mutex
	consec int
}

func newFlow(st *store.Store, gate pipeline.Gate, opts Options, logger *slog.Logger, clk clock.Clock) *redisFlow {
	if logger == nil {
		logger = slog.Default()
	}
	if clk == nil {
		clk = clock.Real{}
	}
	pool := "default"
	if st != nil && st.Pool() != "" {
		pool = st.Pool()
	}
	gates := []pipeline.Gate{gate}
	merge := &priorityMerge{store: st, opts: opts, clk: clk, log: logger, pool: pool}
	return &redisFlow{
		merge:   merge,
		results: make(chan pipeline.Result, 64),
		channels: []pipeline.Channel{
			{Queue: st.QueueKey(model.TierInteractive), Tier: pipeline.TierInteractive, Gates: gates},
			{Queue: st.QueueKey(model.TierAsync), Tier: pipeline.TierAsync, Gates: gates},
			{Queue: st.QueueKey(model.TierBatch), Tier: pipeline.TierBatch, Gates: gates},
		},
	}
}

// Next peeks the preferred ready request. It does not claim.
// channels limits the lanes still eligible in this poll; a refused lane is omitted.
func (m *priorityMerge) Next(ctx context.Context, channels []pipeline.Channel) (string, *pipeline.Request, bool) {
	allowed := map[model.Tier]bool{}
	for _, ch := range channels {
		allowed[model.Tier(ch.Tier)] = true
	}
	tier, ok := m.choose(ctx, allowed)
	if !ok {
		return "", nil, false
	}
	unit, err := m.store.Peek(ctx, tier)
	if err != nil {
		m.log.Error("peek queue", "tier", tier, "err", err)
		return "", nil, false
	}
	if unit == nil {
		return "", nil, false
	}
	req := store.PipelineRequest(unit, m.store.QueueKey(tier))
	return m.pool, &req, true
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
