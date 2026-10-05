package budget

import (
	"context"
	"testing"
	"time"

	"github.com/rayo1uo/llm-async-gateway/internal/model"
)

func TestLocalReserveAndRate(t *testing.T) {
	start := time.Unix(1_700_000_000, 0)
	now := start
	lim := NewLocal(LocalConfig{
		MaxConcurrency: 2,
		ReservedBatch:  1,
		RatePerSec:     1,
		Burst:          1,
		Now:            func() time.Time { return now },
	})
	ctx := context.Background()

	rel1, ok := lim.Allow(ctx, model.TierNearline)
	if !ok {
		t.Fatal("first nearline slot should be granted")
	}
	if _, ok := lim.Allow(ctx, model.TierNearline); ok {
		t.Fatal("second nearline slot should be refused by the rate bucket and the reserve")
	}

	now = now.Add(time.Second)
	if _, ok := lim.Allow(ctx, model.TierNearline); ok {
		t.Fatal("nearline must leave the reserved slot free")
	}
	relB, ok := lim.Allow(ctx, model.TierBatch)
	if !ok {
		t.Fatal("batch should take the reserved slot")
	}

	now = now.Add(time.Second)
	if _, ok := lim.Allow(ctx, model.TierBatch); ok {
		t.Fatal("concurrency cap should be exhausted")
	}

	rel1()
	relB()
	now = now.Add(time.Second)
	if _, ok := lim.Allow(ctx, model.TierNearline); !ok {
		t.Fatal("slot should be free after release")
	}
}

func TestLocalReleaseOnce(t *testing.T) {
	lim := NewLocal(LocalConfig{MaxConcurrency: 1, ReservedBatch: 0, RatePerSec: 100, Burst: 10})
	ctx := context.Background()
	rel, ok := lim.Allow(ctx, model.TierBatch)
	if !ok {
		t.Fatal("expected grant")
	}
	rel()
	rel()
	if _, ok := lim.Allow(ctx, model.TierBatch); !ok {
		t.Fatal("double release should not stick the counter")
	}
}

func TestLocalHonorsCanceledContext(t *testing.T) {
	lim := NewLocal(LocalConfig{MaxConcurrency: 1, RatePerSec: 100, Burst: 1})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, ok := lim.Allow(ctx, model.TierNearline); ok {
		t.Fatal("canceled context should not take a slot")
	}
}
