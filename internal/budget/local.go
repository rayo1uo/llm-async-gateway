// Package budget admits dispatch slots.
// Local is the in-process half of the Phase 1 gate: a concurrency cap, a rate
// bucket, and slots held back for batch. SharedGate adds the Redis counter so
// replicas cannot multiply the cap.
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
	// ReservedBatch is the number of concurrency slots async and interactive
	// are not allowed to occupy, so a busy higher lane cannot consume the pool.
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
	nonBatchInUse int
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
	if tier != model.TierBatch && b.nonBatchInUse >= b.max-b.reserved {
		return nil, false
	}
	b.tokens--
	b.inFlight++
	if tier == model.TierBatch {
		b.batchInUse++
	} else {
		b.nonBatchInUse++
	}
	var once sync.Once
	return func() {
		once.Do(func() {
			b.mu.Lock()
			defer b.mu.Unlock()
			if b.inFlight > 0 {
				b.inFlight--
			}
			if tier == model.TierBatch {
				if b.batchInUse > 0 {
					b.batchInUse--
				}
			} else if b.nonBatchInUse > 0 {
				b.nonBatchInUse--
			}
		})
	}, true
}

// Snapshot reports the in-process in-flight count and the configured cap.
func (b *Local) Snapshot() (inFlight, max int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.inFlight, b.max
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
