package supply_test

import (
	"fmt"
	"time"

	"github.com/rayo1uo/llm-async-gateway/internal/supply"
)

func ExamplePlan() {
	resp, err := supply.Plan(supply.Request{
		Budgets: []supply.GPUBudget{{Type: "H100", GPUs: 4}},
		Models: []supply.Model{{
			ID:              "model-a",
			Tier:            0,
			Weight:          1,
			ReservedMinGPUs: 1,
			SoftMaxGPUs:     4,
			Backlog: []supply.Bucket{{
				Name:      "5m",
				Remaining: 5 * time.Minute,
				Tokens:    12000,
			}},
			Profiles: []supply.Profile{{
				Type:           "H100",
				GPUsPerReplica: 1,
				MuTokensPerSec: 100,
				ColdStart:      30 * time.Second,
			}},
		}},
	})
	if err != nil {
		panic(err)
	}
	p := resp.Models[0]
	fmt.Printf("%s replicas=%d ready=%.0f warming=%.0f unsat=%.0f\n",
		p.ModelID, p.TargetReplicas[0].Replicas, p.ReadyTokensPerSec, p.WarmingTokensPerSec, p.UnsatisfiableTokens)
	// Output:
	// model-a replicas=1 ready=0 warming=100 unsat=0
}
