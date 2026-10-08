package supply

import (
	"math"
	"testing"
	"time"
)

func TestShortDeadlineDoesNotExplode(t *testing.T) {
	const tokens = 1e12
	resp := mustPlan(t, Request{
		Budgets: []GPUBudget{{Type: "H100", GPUs: 100}},
		Models: []Model{{
			ID:              "m",
			SoftMaxGPUs:     100,
			ReservedMinGPUs: 0,
			Backlog: []Bucket{{
				Name:      "30s",
				Remaining: 20 * time.Second,
				Tokens:    tokens,
			}},
			Profiles: []Profile{{
				Type:           "H100",
				GPUsPerReplica: 1,
				MuTokensPerSec: 100,
				ColdStart:      45 * time.Second,
			}},
		}},
	})
	p := resp.Models[0]
	if p.Replicas("H100") != 0 || p.TargetGPUs != 0 {
		t.Fatalf("short deadline allocated %d replicas (%d GPUs); want 0", p.Replicas("H100"), p.TargetGPUs)
	}
	near(t, p.UnsatisfiableTokens, tokens)
	near(t, p.DrainTokensPerSec, 0)
	near(t, p.DemandTokensPerSec, 0)
}

func TestHistogramDrainNotSingleDeadline(t *testing.T) {
	resp := mustPlan(t, Request{
		Budgets: []GPUBudget{{Type: "H100", GPUs: 8}},
		Models: []Model{{
			ID:          "m",
			SoftMaxGPUs: 8,
			Backlog: []Bucket{
				{Name: "30s", Remaining: 20 * time.Second, Tokens: 1e6},
				{Name: "5m", Remaining: 5 * time.Minute, Tokens: 12750},
				{Name: "later", Remaining: time.Hour, Tokens: 35550},
			},
			Profiles: []Profile{{
				Type:           "H100",
				GPUsPerReplica: 1,
				MuTokensPerSec: 60,
				ColdStart:      45 * time.Second,
			}},
		}},
	})
	p := resp.Models[0]
	// 5m window is 255s → 50 tok/s. Later window is 3555s → 10 tok/s.
	// The 30s bar is shorter than cold start and must not enter the sum.
	near(t, p.DrainTokensPerSec, 60)
	near(t, p.UnsatisfiableTokens, 1e6)
	near(t, p.DemandTokensPerSec, 60)
	if p.Replicas("H100") != 1 {
		t.Fatalf("replicas = %d, want 1 (a single tight deadline would ask for many)", p.Replicas("H100"))
	}
}

func TestDemandUsesTokens(t *testing.T) {
	resp := mustPlan(t, Request{
		Budgets: []GPUBudget{{Type: "H100", GPUs: 8}},
		Models: []Model{{
			ID:              "m",
			SoftMaxGPUs:     8,
			AvgInputTokens:  100,
			AvgOutputTokens: 100,
			Backlog: []Bucket{{
				Name:      "5m",
				Remaining: 110 * time.Second,
				Requests:  1000,
			}},
			Profiles: []Profile{{
				Type:           "H100",
				GPUsPerReplica: 1,
				MuTokensPerSec: 500,
				ColdStart:      10 * time.Second,
			}},
		}},
	})
	p := resp.Models[0]
	// 1000 requests * 200 tokens / (110s - 10s) = 2000 tok/s → 4 replicas.
	near(t, p.DemandTokensPerSec, 2000)
	near(t, p.DrainTokensPerSec, 2000)
	if p.Replicas("H100") != 4 {
		t.Fatalf("replicas = %d, want 4 from token mass, not the raw request count", p.Replicas("H100"))
	}
}

func TestUnboundedDemandUsesChosenType(t *testing.T) {
	resp := mustPlan(t, Request{
		Budgets: []GPUBudget{
			{Type: "H100", GPUs: 0},
			{Type: "A100", GPUs: 10},
		},
		Models: []Model{{
			ID:                  "m",
			SoftMaxGPUs:         10,
			ArrivalTokensPerSec: 100,
			Profiles: []Profile{
				{Type: "H100", GPUsPerReplica: 1, MuTokensPerSec: 100, ColdStart: time.Second},
				{Type: "A100", GPUsPerReplica: 1, MuTokensPerSec: 20, ColdStart: time.Second},
			},
		}},
	})
	p := resp.Models[0]
	if p.Replicas("H100") != 0 || p.Replicas("A100") != 5 {
		t.Fatalf("replicas H100=%d A100=%d, want 0 and 5 (not min GPU count across types)", p.Replicas("H100"), p.Replicas("A100"))
	}
}

func TestNewReplicaNotCountedReady(t *testing.T) {
	t.Run("cold start delays drain", func(t *testing.T) {
		resp := mustPlan(t, Request{
			Budgets: []GPUBudget{{Type: "H100", GPUs: 4}},
			Models: []Model{{
				ID:          "m",
				SoftMaxGPUs: 4,
				Backlog: []Bucket{{
					Remaining: 40 * time.Second,
					Tokens:    3500,
				}},
				Profiles: []Profile{{
					Type:           "H100",
					GPUsPerReplica: 1,
					MuTokensPerSec: 100,
					ColdStart:      10 * time.Second,
				}},
			}},
		})
		p := resp.Models[0]
		// Instant capacity would serve 4000 tokens with one replica and stop.
		// After a 10s cold start the first replica only serves 3000, so two are required.
		if p.Replicas("H100") != 2 {
			t.Fatalf("replicas = %d, want 2", p.Replicas("H100"))
		}
		near(t, p.ReadyTokensPerSec, 0)
		near(t, p.WarmingTokensPerSec, 200)
	})

	t.Run("existing ready stays ready", func(t *testing.T) {
		resp := mustPlan(t, Request{
			Budgets: []GPUBudget{{Type: "H100", GPUs: 4}},
			Models: []Model{{
				ID:          "m",
				SoftMaxGPUs: 2,
				Backlog: []Bucket{{
					Remaining: 100 * time.Second,
					Tokens:    18000,
				}},
				Supply: []GPUSupply{{Type: "H100", Ready: 1}},
				Profiles: []Profile{{
					Type:           "H100",
					GPUsPerReplica: 1,
					MuTokensPerSec: 100,
					ColdStart:      20 * time.Second,
				}},
			}},
		})
		p := resp.Models[0]
		if p.Replicas("H100") != 2 {
			t.Fatalf("replicas = %d, want 2", p.Replicas("H100"))
		}
		near(t, p.ReadyTokensPerSec, 100)
		near(t, p.WarmingTokensPerSec, 100)
	})
}

func TestReservedNotDoubleCountedAcrossGPUTypes(t *testing.T) {
	resp := mustPlan(t, Request{
		Budgets: []GPUBudget{
			{Type: "H100", GPUs: 10},
			{Type: "A100", GPUs: 10},
		},
		Models: []Model{{
			ID:              "m",
			ReservedMinGPUs: 4,
			SoftMaxGPUs:     10,
			Profiles: []Profile{
				{Type: "H100", GPUsPerReplica: 1, MuTokensPerSec: 100, ColdStart: time.Second},
				{Type: "A100", GPUsPerReplica: 1, MuTokensPerSec: 40, ColdStart: time.Second},
			},
		}},
	})
	p := resp.Models[0]
	if p.TargetGPUs != 4 || p.Replicas("H100") != 4 || p.Replicas("A100") != 0 {
		t.Fatalf("H100=%d A100=%d gpus=%d, want the 4 GPU reserve on H100 only", p.Replicas("H100"), p.Replicas("A100"), p.TargetGPUs)
	}
	if p.ReservedDeficitGPUs != 0 {
		t.Fatalf("deficit = %d, want 0", p.ReservedDeficitGPUs)
	}
}

func TestReservedRoundsUpOrReportsDeficit(t *testing.T) {
	profile := []Profile{{Type: "H100", GPUsPerReplica: 2, MuTokensPerSec: 50, ColdStart: time.Second}}

	t.Run("rounds up when budget allows", func(t *testing.T) {
		resp := mustPlan(t, Request{
			Budgets: []GPUBudget{{Type: "H100", GPUs: 4}},
			Models: []Model{{
				ID: "m", ReservedMinGPUs: 3, SoftMaxGPUs: 8, Profiles: profile,
			}},
		})
		p := resp.Models[0]
		if p.Replicas("H100") != 2 || p.TargetGPUs != 4 || p.ReservedDeficitGPUs != 0 {
			t.Fatalf("replicas=%d gpus=%d deficit=%d, want 2, 4, 0", p.Replicas("H100"), p.TargetGPUs, p.ReservedDeficitGPUs)
		}
	})

	t.Run("reports deficit when a replica does not fit", func(t *testing.T) {
		resp := mustPlan(t, Request{
			Budgets: []GPUBudget{{Type: "H100", GPUs: 2}},
			Models: []Model{{
				ID: "m", ReservedMinGPUs: 3, SoftMaxGPUs: 8, Profiles: profile,
			}},
		})
		p := resp.Models[0]
		if p.Replicas("H100") != 1 || p.TargetGPUs != 2 || p.ReservedDeficitGPUs != 1 {
			t.Fatalf("replicas=%d gpus=%d deficit=%d, want 1, 2, 1", p.Replicas("H100"), p.TargetGPUs, p.ReservedDeficitGPUs)
		}
	})

	t.Run("high tier takes the reserve first", func(t *testing.T) {
		resp := mustPlan(t, Request{
			Budgets: []GPUBudget{{Type: "H100", GPUs: 4}},
			Models: []Model{
				{ID: "low", Tier: 1, ReservedMinGPUs: 3, SoftMaxGPUs: 8, Profiles: []Profile{{Type: "H100", GPUsPerReplica: 1, MuTokensPerSec: 10, ColdStart: time.Second}}},
				{ID: "high", Tier: 0, ReservedMinGPUs: 3, SoftMaxGPUs: 8, Profiles: []Profile{{Type: "H100", GPUsPerReplica: 1, MuTokensPerSec: 10, ColdStart: time.Second}}},
			},
		})
		high := findPlan(t, resp, "high")
		low := findPlan(t, resp, "low")
		if high.TargetGPUs != 3 || high.ReservedDeficitGPUs != 0 {
			t.Fatalf("high gpus=%d deficit=%d", high.TargetGPUs, high.ReservedDeficitGPUs)
		}
		if low.TargetGPUs != 1 || low.ReservedDeficitGPUs != 2 {
			t.Fatalf("low gpus=%d deficit=%d, want 1 and 2", low.TargetGPUs, low.ReservedDeficitGPUs)
		}
	})
}

func TestSoftMaxClampsDemand(t *testing.T) {
	resp := mustPlan(t, Request{
		Budgets: []GPUBudget{{Type: "H100", GPUs: 50}},
		Models: []Model{{
			ID:                  "m",
			SoftMaxGPUs:         2,
			ArrivalTokensPerSec: 1e9,
			Profiles: []Profile{{
				Type: "H100", GPUsPerReplica: 1, MuTokensPerSec: 100, ColdStart: time.Second,
			}},
		}},
	})
	p := resp.Models[0]
	near(t, p.DemandTokensPerSec, 200)
	if p.Replicas("H100") != 2 || resp.BudgetUsed[0].GPUs != 2 {
		t.Fatalf("replicas=%d used=%d, want 2 and 2 (budget is 50)", p.Replicas("H100"), resp.BudgetUsed[0].GPUs)
	}
}

func TestTriageFundsFeasibleModel(t *testing.T) {
	// Budget covers one model fully. Splitting one replica each misses both.
	// B has the larger weight; A has the tighter deadline and must be saved.
	resp := mustPlan(t, Request{
		Budgets: []GPUBudget{{Type: "H100", GPUs: 2}},
		Models: []Model{
			{
				ID: "A", Tier: 0, Weight: 1, SoftMaxGPUs: 4,
				Backlog:  []Bucket{{Remaining: 40 * time.Second, Tokens: 6000}},
				Profiles: []Profile{{Type: "H100", GPUsPerReplica: 1, MuTokensPerSec: 100, ColdStart: 10 * time.Second}},
			},
			{
				ID: "B", Tier: 0, Weight: 100, SoftMaxGPUs: 4,
				Backlog:  []Bucket{{Remaining: 80 * time.Second, Tokens: 14000}},
				Profiles: []Profile{{Type: "H100", GPUsPerReplica: 1, MuTokensPerSec: 100, ColdStart: 10 * time.Second}},
			},
		},
	})
	a := findPlan(t, resp, "A")
	b := findPlan(t, resp, "B")
	if a.Replicas("H100") != 2 {
		t.Fatalf("A replicas = %d, want 2", a.Replicas("H100"))
	}
	if b.Replicas("H100") != 0 || b.TargetGPUs != 0 {
		t.Fatalf("B replicas = %d gpus = %d, want 0 (partial funding would miss both)", b.Replicas("H100"), b.TargetGPUs)
	}
}

func TestEvictionShrinksImmediately(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	prof := []Profile{{Type: "H100", GPUsPerReplica: 1, MuTokensPerSec: 100, ColdStart: 30 * time.Second}}
	resp := mustPlan(t, Request{
		Now:     now,
		Budgets: []GPUBudget{{Type: "H100", GPUs: 3}},
		Models: []Model{
			{
				ID: "high", Tier: 0, ReservedMinGPUs: 2, SoftMaxGPUs: 8, Profiles: prof,
				Supply:         []GPUSupply{{Type: "H100", Ready: 2}},
				Cooldown:       time.Hour,
				LastPlanChange: now,
			},
			{
				ID: "low", Tier: 1, ReservedMinGPUs: 1, SoftMaxGPUs: 8, Profiles: prof,
				Supply:         []GPUSupply{{Type: "H100", Ready: 6}},
				Cooldown:       time.Hour,
				LastPlanChange: now,
			},
		},
	})
	if !resp.EvictionReplan {
		t.Fatal("expected an eviction replan")
	}
	high := findPlan(t, resp, "high")
	low := findPlan(t, resp, "low")
	if high.Replicas("H100") != 2 || high.ReservedDeficitGPUs != 0 {
		t.Fatalf("high replicas=%d deficit=%d, want 2 and 0", high.Replicas("H100"), high.ReservedDeficitGPUs)
	}
	if low.Replicas("H100") != 1 || low.ReservedDeficitGPUs != 0 {
		t.Fatalf("low replicas=%d deficit=%d, want 1 and 0", low.Replicas("H100"), low.ReservedDeficitGPUs)
	}
}

func TestEvictionStripsReservedFromLowTier(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	prof := []Profile{{Type: "H100", GPUsPerReplica: 1, MuTokensPerSec: 100, ColdStart: 30 * time.Second}}
	resp := mustPlan(t, Request{
		Now:     now,
		Budgets: []GPUBudget{{Type: "H100", GPUs: 2}},
		Models: []Model{
			{
				ID: "high", Tier: 0, ReservedMinGPUs: 2, SoftMaxGPUs: 8, Profiles: prof,
				Supply: []GPUSupply{{Type: "H100", Ready: 2}},
			},
			{
				ID: "low", Tier: 1, ReservedMinGPUs: 1, SoftMaxGPUs: 8, Profiles: prof,
				Supply:         []GPUSupply{{Type: "H100", Ready: 6}},
				Cooldown:       time.Hour,
				LastPlanChange: now,
			},
		},
	})
	high := findPlan(t, resp, "high")
	low := findPlan(t, resp, "low")
	if high.Replicas("H100") != 2 || high.ReservedDeficitGPUs != 0 {
		t.Fatalf("high replicas=%d deficit=%d", high.Replicas("H100"), high.ReservedDeficitGPUs)
	}
	if low.Replicas("H100") != 0 || low.ReservedDeficitGPUs != 1 {
		t.Fatalf("low replicas=%d deficit=%d, want 0 and 1", low.Replicas("H100"), low.ReservedDeficitGPUs)
	}
}

func TestShrinkDropsHighestPhiWhenExcessTies(t *testing.T) {
	prof := []Profile{{Type: "H100", GPUsPerReplica: 1, MuTokensPerSec: 100, ColdStart: time.Second}}
	resp := mustPlan(t, Request{
		Budgets: []GPUBudget{{Type: "H100", GPUs: 4}},
		Models: []Model{
			{
				ID: "A", Tier: 1, Weight: 1, SoftMaxGPUs: 10, ArrivalTokensPerSec: 10, Profiles: prof,
				Supply: []GPUSupply{{Type: "H100", Ready: 3}},
			},
			{
				ID: "B", Tier: 1, Weight: 1, SoftMaxGPUs: 10, ArrivalTokensPerSec: 300, Profiles: prof,
				Supply: []GPUSupply{{Type: "H100", Ready: 3}},
			},
		},
	})
	a := findPlan(t, resp, "A")
	b := findPlan(t, resp, "B")
	// Equal excess. A has phi 30, B has phi 1, so A loses the first replica.
	// B then has more excess and loses the second. Both end at 2.
	if a.Replicas("H100") != 2 || b.Replicas("H100") != 2 {
		t.Fatalf("A=%d B=%d, want 2 and 2", a.Replicas("H100"), b.Replicas("H100"))
	}
}

func TestShrinkStripsLowTierBeforeHighPhi(t *testing.T) {
	prof := []Profile{{Type: "H100", GPUsPerReplica: 1, MuTokensPerSec: 100, ColdStart: time.Second}}
	resp := mustPlan(t, Request{
		Budgets: []GPUBudget{{Type: "H100", GPUs: 4}},
		Models: []Model{
			{
				ID: "high", Tier: 0, SoftMaxGPUs: 10, ArrivalTokensPerSec: 10, Profiles: prof,
				Supply: []GPUSupply{{Type: "H100", Ready: 3}},
			},
			{
				ID: "low", Tier: 1, SoftMaxGPUs: 10, ArrivalTokensPerSec: 1000, Profiles: prof,
				Supply: []GPUSupply{{Type: "H100", Ready: 3}},
			},
		},
	})
	high := findPlan(t, resp, "high")
	low := findPlan(t, resp, "low")
	if high.Replicas("H100") != 3 || low.Replicas("H100") != 1 {
		t.Fatalf("high=%d low=%d, want 3 and 1", high.Replicas("H100"), low.Replicas("H100"))
	}
}

func TestEvictionReclaimsReservedFromLowTier(t *testing.T) {
	prof := []Profile{{Type: "H100", GPUsPerReplica: 1, MuTokensPerSec: 100, ColdStart: time.Second}}
	resp := mustPlan(t, Request{
		Budgets: []GPUBudget{{Type: "H100", GPUs: 4}},
		Models: []Model{
			{
				ID: "high", Tier: 0, ReservedMinGPUs: 2, SoftMaxGPUs: 8, Profiles: prof,
			},
			{
				ID: "low", Tier: 1, ReservedMinGPUs: 1, SoftMaxGPUs: 8, Profiles: prof,
				Supply: []GPUSupply{{Type: "H100", Ready: 10}},
			},
		},
	})
	high := findPlan(t, resp, "high")
	low := findPlan(t, resp, "low")
	if high.Replicas("H100") != 2 || high.ReservedDeficitGPUs != 0 {
		t.Fatalf("high replicas=%d deficit=%d, want 2 and 0", high.Replicas("H100"), high.ReservedDeficitGPUs)
	}
	if low.Replicas("H100") != 2 || low.ReservedDeficitGPUs != 0 || high.TargetGPUs+low.TargetGPUs != 4 {
		t.Fatalf("high=%d low=%d, want the low tier held to the leftover after the high reserve", high.Replicas("H100"), low.Replicas("H100"))
	}
}

func TestBudgetShrinkBypassesCooldown(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	resp := mustPlan(t, Request{
		Now:                now,
		PreviousBudgetGPUs: 20,
		Budgets:            []GPUBudget{{Type: "H100", GPUs: 10}},
		Models: []Model{{
			ID: "m", ReservedMinGPUs: 1, SoftMaxGPUs: 10,
			Supply:         []GPUSupply{{Type: "H100", Ready: 5}},
			Cooldown:       time.Hour,
			LastPlanChange: now,
			Profiles:       []Profile{{Type: "H100", GPUsPerReplica: 1, MuTokensPerSec: 100, ColdStart: time.Second}},
		}},
	})
	if !resp.EvictionReplan {
		t.Fatal("expected eviction replan")
	}
	p := resp.Models[0]
	if p.Replicas("H100") != 1 {
		t.Fatalf("replicas = %d, want the reserved floor 1 rather than the cooled-down 5", p.Replicas("H100"))
	}
}

func TestHighTierFairnessPrecedesLowerTierTriage(t *testing.T) {
	prof := []Profile{{Type: "H100", GPUsPerReplica: 1, MuTokensPerSec: 100, ColdStart: 10 * time.Second}}
	resp := mustPlan(t, Request{
		Budgets: []GPUBudget{{Type: "H100", GPUs: 2}},
		Models: []Model{
			{
				ID: "online", Tier: 0, Weight: 1, SoftMaxGPUs: 4,
				ArrivalTokensPerSec: 500,
				Profiles:            prof,
			},
			{
				ID: "batch", Tier: 1, Weight: 1, SoftMaxGPUs: 4,
				Backlog:  []Bucket{{Remaining: 40 * time.Second, Tokens: 6000}},
				Profiles: prof,
			},
		},
	})
	online := findPlan(t, resp, "online")
	batch := findPlan(t, resp, "batch")
	if online.Replicas("H100") != 2 || batch.Replicas("H100") != 0 {
		t.Fatalf("online=%d batch=%d, want the higher tier to take the leftover before the lower tier is triaged", online.Replicas("H100"), batch.Replicas("H100"))
	}
}

func TestArrivalEWMADoesNotSmoothDrain(t *testing.T) {
	resp := mustPlan(t, Request{
		Budgets: []GPUBudget{{Type: "H100", GPUs: 4}},
		Models: []Model{{
			ID:                  "m",
			SoftMaxGPUs:         4,
			ArrivalTokensPerSec: 0,
			ArrivalEWMA:         100,
			EWMAAlpha:           0.5,
			Backlog: []Bucket{
				{Name: "30s", Remaining: 10 * time.Second, Tokens: 999},
				{Name: "5m", Remaining: 130 * time.Second, Tokens: 8000},
			},
			Profiles: []Profile{{
				Type: "H100", GPUsPerReplica: 1, MuTokensPerSec: 80, ColdStart: 30 * time.Second,
			}},
		}},
	})
	p := resp.Models[0]
	near(t, p.SmoothedArrivalTokensPerSec, 50)
	near(t, p.DrainTokensPerSec, 80)
	near(t, p.DemandTokensPerSec, 80)
	near(t, p.UnsatisfiableTokens, 999)
}

func TestCooldownHoldAndStepCap(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	prof := []Profile{{Type: "H100", GPUsPerReplica: 1, MuTokensPerSec: 100, ColdStart: time.Second}}

	t.Run("cooldown keeps current", func(t *testing.T) {
		resp := mustPlan(t, Request{
			Now:     now,
			Budgets: []GPUBudget{{Type: "H100", GPUs: 10}},
			Models: []Model{{
				ID: "m", SoftMaxGPUs: 10, ArrivalTokensPerSec: 1000, Profiles: prof,
				Supply:         []GPUSupply{{Type: "H100", Ready: 1}},
				Cooldown:       time.Minute,
				LastPlanChange: now,
			}},
		})
		if resp.Models[0].Replicas("H100") != 1 {
			t.Fatalf("replicas = %d, want 1", resp.Models[0].Replicas("H100"))
		}
	})

	t.Run("max step", func(t *testing.T) {
		resp := mustPlan(t, Request{
			Now:     now,
			Budgets: []GPUBudget{{Type: "H100", GPUs: 10}},
			Models: []Model{{
				ID: "m", SoftMaxGPUs: 10, ArrivalTokensPerSec: 1000, MaxStep: 1, Profiles: prof,
				Supply: []GPUSupply{{Type: "H100", Ready: 1}},
			}},
		})
		if resp.Models[0].Replicas("H100") != 2 {
			t.Fatalf("replicas = %d, want 2", resp.Models[0].Replicas("H100"))
		}
	})
}

func TestDeadlineBypassesCooldown(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	resp := mustPlan(t, Request{
		Now:     now,
		Budgets: []GPUBudget{{Type: "H100", GPUs: 8}},
		Models: []Model{{
			ID: "m", SoftMaxGPUs: 8,
			Backlog:        []Bucket{{Remaining: 100 * time.Second, Tokens: 50000}},
			Supply:         []GPUSupply{{Type: "H100", Ready: 1}},
			Cooldown:       time.Hour,
			LastPlanChange: now,
			Profiles:       []Profile{{Type: "H100", GPUsPerReplica: 1, MuTokensPerSec: 100, ColdStart: 10 * time.Second}},
		}},
	})
	p := resp.Models[0]
	if p.Replicas("H100") != 6 {
		t.Fatalf("replicas = %d, want 6 despite cooldown", p.Replicas("H100"))
	}
	near(t, p.ReadyTokensPerSec, 100)
	near(t, p.WarmingTokensPerSec, 500)
}

func TestInfeasibleProfileSkipped(t *testing.T) {
	resp := mustPlan(t, Request{
		Budgets: []GPUBudget{
			{Type: "H100", GPUs: 4},
			{Type: "A100", GPUs: 4},
		},
		Models: []Model{{
			ID: "m", SoftMaxGPUs: 4, ArrivalTokensPerSec: 40,
			Profiles: []Profile{
				{Type: "H100", GPUsPerReplica: 1, MuTokensPerSec: 100, ColdStart: time.Second, Infeasible: true},
				{Type: "A100", GPUsPerReplica: 1, MuTokensPerSec: 0, MuRequestsPerSec: 0, ColdStart: time.Second},
				{Type: "L40", GPUsPerReplica: 1, MuTokensPerSec: 20, ColdStart: time.Second},
			},
		}},
	})
	// L40 is not in the budget, so demand cannot be placed. H100 is infeasible
	// and A100 has mu 0. The zero-mu profile must not be selected.
	p := resp.Models[0]
	if p.Replicas("H100") != 0 || p.Replicas("A100") != 0 || p.TargetGPUs != 0 {
		t.Fatalf("placed H100=%d A100=%d gpus=%d on infeasible types", p.Replicas("H100"), p.Replicas("A100"), p.TargetGPUs)
	}
}

func TestInfeasibleDoesNotBlockOtherType(t *testing.T) {
	resp := mustPlan(t, Request{
		Budgets: []GPUBudget{
			{Type: "H100", GPUs: 4},
			{Type: "A100", GPUs: 4},
		},
		Models: []Model{{
			ID: "m", SoftMaxGPUs: 4, ArrivalTokensPerSec: 40,
			Profiles: []Profile{
				{Type: "H100", GPUsPerReplica: 1, MuTokensPerSec: 100, Infeasible: true, ColdStart: time.Second},
				{Type: "A100", GPUsPerReplica: 1, MuTokensPerSec: 20, ColdStart: time.Second},
			},
		}},
	})
	p := resp.Models[0]
	if p.Replicas("A100") != 2 || p.Replicas("H100") != 0 {
		t.Fatalf("H100=%d A100=%d, want 0 and 2", p.Replicas("H100"), p.Replicas("A100"))
	}
}

func TestRejectsDuplicateModel(t *testing.T) {
	_, err := Plan(Request{
		Budgets: []GPUBudget{{Type: "H100", GPUs: 1}},
		Models: []Model{
			{ID: "m", SoftMaxGPUs: 1, Profiles: []Profile{{Type: "H100", GPUsPerReplica: 1, MuTokensPerSec: 1}}},
			{ID: "m", SoftMaxGPUs: 1, Profiles: []Profile{{Type: "H100", GPUsPerReplica: 1, MuTokensPerSec: 1}}},
		},
	})
	if err == nil {
		t.Fatal("expected duplicate model to fail")
	}
}

func TestPlanIsDeterministic(t *testing.T) {
	req := Request{
		Budgets: []GPUBudget{{Type: "H100", GPUs: 3}, {Type: "A100", GPUs: 3}},
		Models: []Model{
			{
				ID: "b", Tier: 1, Weight: 2, ReservedMinGPUs: 1, SoftMaxGPUs: 4,
				ArrivalTokensPerSec: 30,
				Profiles: []Profile{
					{Type: "A100", GPUsPerReplica: 1, MuTokensPerSec: 15, ColdStart: 5 * time.Second},
					{Type: "H100", GPUsPerReplica: 1, MuTokensPerSec: 40, ColdStart: 5 * time.Second},
				},
			},
			{
				ID: "a", Tier: 0, Weight: 1, SoftMaxGPUs: 4,
				Backlog: []Bucket{{Name: "5m", Remaining: 2 * time.Minute, Tokens: 4000}},
				Profiles: []Profile{
					{Type: "H100", GPUsPerReplica: 1, MuTokensPerSec: 50, ColdStart: 5 * time.Second},
				},
			},
		},
	}
	first, err := Plan(req)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		got, err := Plan(req)
		if err != nil {
			t.Fatal(err)
		}
		if len(got.Models) != len(first.Models) {
			t.Fatalf("model count changed")
		}
		for mi := range got.Models {
			a, b := got.Models[mi], first.Models[mi]
			if a.ModelID != b.ModelID || a.TargetGPUs != b.TargetGPUs || len(a.TargetReplicas) != len(b.TargetReplicas) {
				t.Fatalf("plan changed at iter %d: %+v vs %+v", i, a, b)
			}
			for ri := range a.TargetReplicas {
				if a.TargetReplicas[ri] != b.TargetReplicas[ri] {
					t.Fatalf("replicas changed: %+v vs %+v", a.TargetReplicas[ri], b.TargetReplicas[ri])
				}
			}
		}
	}
}

func mustPlan(t *testing.T, req Request) Response {
	t.Helper()
	resp, err := Plan(req)
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	return resp
}

func findPlan(t *testing.T, resp Response, id string) ModelPlan {
	t.Helper()
	for _, p := range resp.Models {
		if p.ModelID == id {
			return p
		}
	}
	t.Fatalf("missing plan %s", id)
	return ModelPlan{}
}

func near(t *testing.T, got, want float64) {
	t.Helper()
	scale := math.Abs(want)
	if scale < 1 {
		scale = 1
	}
	if math.Abs(got-want) > 1e-6*scale {
		t.Fatalf("got %v want %v", got, want)
	}
}
