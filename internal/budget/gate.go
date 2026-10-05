package budget

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/rayo1uo/llm-async-gateway/internal/model"
	"github.com/rayo1uo/llm-async-gateway/internal/observe"
	"github.com/rayo1uo/llm-async-gateway/internal/pipeline"
	"github.com/rayo1uo/llm-async-gateway/internal/store"
)

// GateName is the only gate_type Phase 1 accepts. Anything else must fail startup.
const GateName = "local"

// SharedConfig builds the Phase 1 gate: local concurrency plus a Redis holder set.
type SharedConfig struct {
	Local    *Local
	Store    *store.Store
	Max      int
	LeaseTTL time.Duration
	Pool     string
	Metrics  *observe.Metrics
	Logger   *slog.Logger
}

// SharedGate implements pipeline.Gate.
// Apply refuses without claiming when either the local cap or the shared cap is full.
type SharedGate struct {
	local    *Local
	store    *store.Store
	max      int
	leaseTTL time.Duration
	pool     string
	metrics  *observe.Metrics
	log      *slog.Logger
}

// NewSharedGate returns the Phase 1 admission gate.
func NewSharedGate(cfg SharedConfig) *SharedGate {
	if cfg.LeaseTTL <= 0 {
		cfg.LeaseTTL = 30 * time.Second
	}
	if cfg.Pool == "" {
		cfg.Pool = "default"
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &SharedGate{
		local:    cfg.Local,
		store:    cfg.Store,
		max:      cfg.Max,
		leaseTTL: cfg.LeaseTTL,
		pool:     cfg.Pool,
		metrics:  cfg.Metrics,
		log:      cfg.Logger,
	}
}

// Budget reports the tighter of the local and shared remaining ratios, in [0, 1].
// A Redis error fails closed and returns 0.
func (g *SharedGate) Budget(ctx context.Context) float64 {
	if g == nil || g.max < 1 {
		return 0
	}
	localRatio := 1.0
	if g.local != nil {
		inUse, max := g.local.Snapshot()
		if max < 1 {
			return 0
		}
		localRatio = ratio(max-inUse, max)
	}
	if g.store == nil {
		return localRatio
	}
	total, _, err := g.store.SlotCounts(ctx, time.Now())
	if err != nil {
		g.log.Error("shared budget", "err", err)
		return 0
	}
	shared := ratio(g.max-total, g.max)
	if localRatio < shared {
		return localRatio
	}
	return shared
}

// Apply admits one request or refuses it.
// The holder id comes from pipeline.AdmissionID. A missing id fails closed.
func (g *SharedGate) Apply(ctx context.Context, req *pipeline.Request, releases *[]pipeline.ReleaseFunc) (pipeline.Verdict, error) {
	tier := model.TierAsync
	tierLabel := string(pipeline.TierAsync)
	if req != nil && req.Message.Tier != "" {
		tier = model.Tier(req.Message.Tier)
		tierLabel = string(req.Message.Tier)
	}
	owner := pipeline.AdmissionID(ctx)
	if owner == "" {
		g.decision(tierLabel, "refuse_local")
		return pipeline.VerdictRefuse, nil
	}
	var localRel func()
	if g.local != nil {
		rel, ok := g.local.Allow(ctx, tier)
		if !ok {
			g.decision(tierLabel, "refuse_local")
			return pipeline.VerdictRefuse, nil
		}
		localRel = rel
	}
	if g.store != nil {
		ok, err := g.store.TryAcquireSlot(ctx, tier, owner, time.Now().Add(g.leaseTTL), g.max)
		if err != nil {
			if localRel != nil {
				localRel()
			}
			g.decision(tierLabel, "refuse_shared")
			return pipeline.VerdictRefuse, err
		}
		if !ok {
			if localRel != nil {
				localRel()
			}
			g.decision(tierLabel, "refuse_shared")
			return pipeline.VerdictRefuse, nil
		}
	}
	g.decision(tierLabel, "continue")
	if g.metrics != nil {
		g.metrics.SetBudget(g.pool, tierLabel, GateName, g.Budget(ctx))
	}
	var once sync.Once
	*releases = append(*releases, func() {
		once.Do(func() {
			if localRel != nil {
				localRel()
			}
			if g.store != nil {
				if err := g.store.ReleaseSlot(context.Background(), tier, owner); err != nil {
					g.log.Error("release shared slot", "owner", owner, "err", err)
				}
			}
		})
	})
	return pipeline.VerdictContinue, nil
}

func (g *SharedGate) decision(tier, reason string) {
	if g.metrics != nil {
		g.metrics.GateDecision(g.pool, tier, GateName, reason)
	}
}

func ratio(remain, max int) float64 {
	if max < 1 || remain <= 0 {
		return 0
	}
	if remain >= max {
		return 1
	}
	return float64(remain) / float64(max)
}
