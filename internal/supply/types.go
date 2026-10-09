// Package supply plans target replica counts per model and GPU type.
//
// Plan is a pure function: the same request always yields the same response.
// It does not talk to Redis, HTTP, or Kubernetes. Actuators and placement
// live outside this process and are not invoked here.
package supply

import (
	"fmt"
	"time"
)

// Request is one planning tick. Zero Epsilon selects 1ms. Zero Horizon
// evaluates phi at each model's longest feasible cold start, so a replica
// counts only once that cold start would have elapsed.
type Request struct {
	Now time.Time
	// Epsilon floors (T - cold_start) when a backlog bucket is satisfiable.
	Epsilon time.Duration
	// Horizon is the instant at which phi's projected rate is measured.
	// Zero selects each model's longest feasible cold start.
	Horizon time.Duration
	// Eviction bypasses cooldown and the step cap even when the observed
	// footprint already fits in the new budget.
	Eviction bool
	// PreviousBudgetGPUs, when positive and greater than the sum of Budgets,
	// is a budget shrink and forces the same bypass.
	PreviousBudgetGPUs int
	Budgets            []GPUBudget
	Models             []Model
}

// GPUBudget is the GPU count still available for type Type after higher
// priority occupants have been subtracted by the caller.
type GPUBudget struct {
	Type string `json:"type"`
	GPUs int    `json:"gpus"`
}

// Model is one supply target. Demand is tokens per second. Request-shaped
// inputs are converted with AvgInputTokens + AvgOutputTokens.
type Model struct {
	ID     string
	Tier   int // 0 is the highest priority
	Weight float64
	// ReservedMinGPUs is a floor in GPUs, shared across types. It is not
	// applied once per GPU type.
	ReservedMinGPUs int
	// SoftMaxGPUs clamps demand before allocation. Negative means no cap.
	// Zero means the model may occupy no GPUs.
	SoftMaxGPUs           int
	ArrivalTokensPerSec   float64
	ArrivalRequestsPerSec float64
	// ArrivalEWMA is the previous smoothed arrival, in tokens/s.
	// EWMAAlpha in (0, 1] updates it from this tick's arrival. Drain is never
	// mixed into this average.
	ArrivalEWMA          float64
	EWMAAlpha            float64
	EngineTokensPerSec   float64
	EngineRequestsPerSec float64
	AvgInputTokens       float64
	AvgOutputTokens      float64
	Backlog              []Bucket
	Supply               []GPUSupply
	Profiles             []Profile
	LastPlanChange       time.Time
	Cooldown             time.Duration
	// MaxStep caps how many replicas an ordinary tick may add or remove.
	// Zero means no cap. Deadline pressure and budget shrinks ignore it.
	MaxStep int
}

// Bucket is one bar of the backlog histogram. Remaining is the time left
// until that bar's deadline. Tokens is input plus output tokens; when Tokens
// is zero and Requests is set, tokens = Requests * (avg input + avg output).
type Bucket struct {
	Name      string
	Remaining time.Duration
	Tokens    float64
	Requests  float64
}

// GPUSupply is the observed footprint on one GPU type.
type GPUSupply struct {
	Type    string
	Ready   int
	Warming []Warming
}

// Warming is a cohort that is not serving yet.
type Warming struct {
	Count int
	ETA   time.Duration
}

// Profile is the measured shape on one GPU type. Mu is tokens per second per
// replica at that shape. MuRequestsPerSec is converted with the model's
// average input plus output tokens. Infeasible or a zero mu drops the type.
type Profile struct {
	Type             string
	GPUsPerReplica   int
	MuTokensPerSec   float64
	MuRequestsPerSec float64
	ColdStart        time.Duration
	Infeasible       bool
}

// Response is the plan for this tick.
type Response struct {
	Models         []ModelPlan
	BudgetUsed     []GPUBudget
	EvictionReplan bool
}

// ModelPlan is one model's target. ReadyTokensPerSec counts only replicas
// that are ready now. WarmingTokensPerSec counts observed warming cohorts
// that the plan keeps, plus replicas this plan just decided to add.
type ModelPlan struct {
	ModelID                     string
	TargetReplicas              []ReplicaCount
	TargetGPUs                  int
	DemandTokensPerSec          float64
	SmoothedArrivalTokensPerSec float64
	DrainTokensPerSec           float64
	ReadyTokensPerSec           float64
	WarmingTokensPerSec         float64
	UnsatisfiableTokens         float64
	Phi                         float64
	ReservedDeficitGPUs         int
	Notes                       []string
}

// ReplicaCount is the target on one GPU type.
type ReplicaCount struct {
	Type     string `json:"type"`
	Replicas int    `json:"replicas"`
}

// Replicas returns the planned count for a GPU type.
func (p ModelPlan) Replicas(gpuType string) int {
	for _, r := range p.TargetReplicas {
		if r.Type == gpuType {
			return r.Replicas
		}
	}
	return 0
}

func validate(req Request) error {
	seenBudget := map[string]struct{}{}
	for _, b := range req.Budgets {
		if b.Type == "" {
			return fmt.Errorf("supply: budget is missing a GPU type")
		}
		if b.GPUs < 0 {
			return fmt.Errorf("supply: budget for %s is negative", b.Type)
		}
		if _, ok := seenBudget[b.Type]; ok {
			return fmt.Errorf("supply: duplicate budget for %s", b.Type)
		}
		seenBudget[b.Type] = struct{}{}
	}
	if req.PreviousBudgetGPUs < 0 {
		return fmt.Errorf("supply: previous budget is negative")
	}
	seenModel := map[string]struct{}{}
	for _, m := range req.Models {
		if m.ID == "" {
			return fmt.Errorf("supply: model is missing an id")
		}
		if _, ok := seenModel[m.ID]; ok {
			return fmt.Errorf("supply: duplicate model %s", m.ID)
		}
		seenModel[m.ID] = struct{}{}
		if err := validateModel(m); err != nil {
			return err
		}
	}
	return nil
}

func validateModel(m Model) error {
	if m.ReservedMinGPUs < 0 {
		return fmt.Errorf("supply: model %s has a negative reserved minimum", m.ID)
	}
	if m.EWMAAlpha < 0 || m.EWMAAlpha > 1 {
		return fmt.Errorf("supply: model %s EWMA alpha must be in [0, 1]", m.ID)
	}
	if m.MaxStep < 0 {
		return fmt.Errorf("supply: model %s has a negative max step", m.ID)
	}
	for _, name := range []struct {
		label string
		v     float64
	}{
		{"arrival tokens/s", m.ArrivalTokensPerSec},
		{"arrival requests/s", m.ArrivalRequestsPerSec},
		{"arrival EWMA", m.ArrivalEWMA},
		{"engine tokens/s", m.EngineTokensPerSec},
		{"engine requests/s", m.EngineRequestsPerSec},
		{"avg input tokens", m.AvgInputTokens},
		{"avg output tokens", m.AvgOutputTokens},
	} {
		if name.v < 0 {
			return fmt.Errorf("supply: model %s has negative %s", m.ID, name.label)
		}
	}
	for _, b := range m.Backlog {
		if b.Remaining < 0 {
			return fmt.Errorf("supply: model %s bucket %s has a negative remaining time", m.ID, bucketName(b))
		}
		if b.Tokens < 0 || b.Requests < 0 {
			return fmt.Errorf("supply: model %s bucket %s has a negative token mass", m.ID, bucketName(b))
		}
	}
	for _, s := range m.Supply {
		if s.Type == "" {
			return fmt.Errorf("supply: model %s supply is missing a GPU type", m.ID)
		}
		if s.Ready < 0 {
			return fmt.Errorf("supply: model %s has a negative ready count on %s", m.ID, s.Type)
		}
		for _, w := range s.Warming {
			if w.Count < 0 || w.ETA < 0 {
				return fmt.Errorf("supply: model %s has an invalid warming cohort on %s", m.ID, s.Type)
			}
		}
	}
	seenProf := map[string]struct{}{}
	for _, p := range m.Profiles {
		if p.Type == "" {
			return fmt.Errorf("supply: model %s profile is missing a GPU type", m.ID)
		}
		if _, ok := seenProf[p.Type]; ok {
			return fmt.Errorf("supply: model %s has a duplicate profile for %s", m.ID, p.Type)
		}
		seenProf[p.Type] = struct{}{}
		if p.GPUsPerReplica <= 0 {
			return fmt.Errorf("supply: model %s profile %s needs a positive GPU count per replica", m.ID, p.Type)
		}
		if p.MuTokensPerSec < 0 || p.MuRequestsPerSec < 0 || p.ColdStart < 0 {
			return fmt.Errorf("supply: model %s profile %s has a negative field", m.ID, p.Type)
		}
	}
	return nil
}

func bucketName(b Bucket) string {
	if b.Name != "" {
		return b.Name
	}
	return b.Remaining.String()
}
