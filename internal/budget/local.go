// Package budget admits dispatch slots. Local is a concurrency and rate limiter.
// A later Prometheus implementation can satisfy the same method set and compute
// N = maxSYS * (D - baseline) from EPP or vLLM saturation, matching llm-d-async's
// dispatch budget, without changing the dispatcher.
package budget

import (
	"context"
	"sync"
	"time"

	"github.com/rayo1uo/llm-async-gateway/internal/model"
)

// LocalConfig parameterizes Local.
type LocalConfig struct {
	MaxConcurrency int
	// ReservedBatch is the number of concurrency slots nearline is not allowed
	// to occupy, so a busy nearline lane cannot consume the whole pool.
	ReservedBatch int
	RatePerSec    float64
	Burst         int
	// Now overrides the clock. Nil uses time.Now.
	Now func() time.Time
}

// Local limits in-flight calls and admissions per second.
type Local struct {
	max           int
	reserved      int
	rate          float64
	burst         float64
	now           func() time.Time
	mu            sync.Mutex
	tokens        float64
	last          time.Time
	inFlight      int
	nearlineInUse int
	batchInUse    int
}

// NewLocal builds a limiter. The token bucket starts full.
func NewLocal(cfg LocalConfig) *Local {
	nowFn := cfg.Now
	if nowFn == nil {
		nowFn = time.Now
	}
	burst := cfg.Burst
	if burst < 1 {
		burst = 1
	}
	return &Local{
		max:      cfg.MaxConcurrency,
		reserved: cfg.ReservedBatch,
		rate:     cfg.RatePerSec,
		burst:    float64(burst),
		now:      nowFn,
		tokens:   float64(burst),
		last:     nowFn(),
	}
}

// Allow grants one dispatch slot for tier. release must be called once when the
// attempt leaves the worker, including when the request is parked for retry.
// ok is false when the caller should wait for a later poll.
func (b *Local) Allow(ctx context.Context, tier model.Tier) (release func(), ok bool) {
	if err := ctx.Err(); err != nil {
		return nil, false
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.refill()
	if b.tokens < 1 || b.inFlight >= b.max {
		return nil, false
	}
	if tier == model.TierNearline && b.nearlineInUse >= b.max-b.reserved {
		return nil, false
	}
	b.tokens--
	b.inFlight++
	if tier == model.TierNearline {
		b.nearlineInUse++
	} else {
		b.batchInUse++
	}
	var once sync.Once
	return func() {
		once.Do(func() {
			b.mu.Lock()
			defer b.mu.Unlock()
			if b.inFlight > 0 {
				b.inFlight--
			}
			if tier == model.TierNearline {
				if b.nearlineInUse > 0 {
					b.nearlineInUse--
				}
			} else if b.batchInUse > 0 {
				b.batchInUse--
			}
		})
	}, true
}

func (b *Local) refill() {
	now := b.now()
	elapsed := now.Sub(b.last).Seconds()
	if elapsed < 0 {
		elapsed = 0
	}
	b.tokens += elapsed * b.rate
	if b.tokens > b.burst {
		b.tokens = b.burst
	}
	b.last = now
}
