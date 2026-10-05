// Package schedule chooses which priority tier to claim from.
package schedule

import (
	"time"

	"github.com/rayo1uo/llm-async-gateway/internal/model"
)

// Input is a point-in-time view of the two ready queues.
type Input struct {
	NearlineReady       int
	BatchReady          int
	NearlineDeadline    time.Time
	BatchDeadline       time.Time
	ConsecutiveNearline int
	// ReserveEvery forces a batch pick after this many consecutive nearline
	// picks when both lanes have work. Zero disables the rotation.
	ReserveEvery int
	Now          time.Time
	// AgingSlack promotes a batch request when its deadline is this close and
	// it is sooner than the oldest nearline request.
	AgingSlack time.Duration
}

// Pick selects the tier to claim. Nearline is preferred. Batch is chosen when
// it is the only ready lane, when it is closer to its deadline than nearline
// (aging), or when the reserved-share rotation is due.
func Pick(in Input) (model.Tier, bool) {
	if in.NearlineReady <= 0 && in.BatchReady <= 0 {
		return "", false
	}
	if in.NearlineReady <= 0 {
		return model.TierBatch, true
	}
	if in.BatchReady <= 0 {
		return model.TierNearline, true
	}

	if batchAgesAhead(in) {
		return model.TierBatch, true
	}
	if in.ReserveEvery > 0 && in.ConsecutiveNearline >= in.ReserveEvery-1 {
		return model.TierBatch, true
	}
	return model.TierNearline, true
}

func batchAgesAhead(in Input) bool {
	if in.BatchDeadline.IsZero() {
		return false
	}
	slack := in.BatchDeadline.Sub(in.Now)
	if slack > in.AgingSlack {
		return false
	}
	// Nearline still wins when its own deadline is sooner or equal.
	if !in.NearlineDeadline.IsZero() && !in.NearlineDeadline.After(in.BatchDeadline) {
		return false
	}
	return true
}
