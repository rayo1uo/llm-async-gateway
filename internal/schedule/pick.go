// Package schedule chooses which priority tier to claim from.
package schedule

import (
	"time"

	"github.com/rayo1uo/llm-async-gateway/internal/model"
)

// Input is a point-in-time view of the ready queues.
// Nearline fields are the async lane. The name is kept so the aging rules stay
// readable next to the tests that pinned them down.
type Input struct {
	InteractiveReady    int
	InteractiveDeadline time.Time
	NearlineReady       int
	BatchReady          int
	NearlineDeadline    time.Time
	BatchDeadline       time.Time
	ConsecutiveNearline int
	// ReserveEvery forces a batch pick after this many consecutive async
	// picks when both lanes have work. Zero disables the rotation.
	ReserveEvery int
	Now          time.Time
	// AgingSlack promotes a batch request when its deadline is this close and
	// it is sooner than the oldest async request.
	AgingSlack time.Duration
}

// Pick selects the tier to claim. Interactive is strict highest when it has
// work. Async is preferred over batch. Batch is chosen when it is the only
// ready lane, when it is closer to its deadline than async (aging), or when
// the reserved-share rotation is due.
func Pick(in Input) (model.Tier, bool) {
	if in.InteractiveReady <= 0 && in.NearlineReady <= 0 && in.BatchReady <= 0 {
		return "", false
	}
	if in.InteractiveReady > 0 && !lowerAgesAhead(in.InteractiveDeadline, soonestLower(in), in) {
		return model.TierInteractive, true
	}
	if in.NearlineReady <= 0 && in.BatchReady <= 0 {
		return model.TierInteractive, true
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

// soonestLower is the earlier deadline among async and batch, when that lane has work.
func soonestLower(in Input) time.Time {
	switch {
	case in.NearlineReady > 0 && in.BatchReady > 0:
		if in.BatchDeadline.Before(in.NearlineDeadline) {
			return in.BatchDeadline
		}
		return in.NearlineDeadline
	case in.NearlineReady > 0:
		return in.NearlineDeadline
	case in.BatchReady > 0:
		return in.BatchDeadline
	default:
		return time.Time{}
	}
}

// lowerAgesAhead reports whether a lower lane should preempt higher because it
// is inside the aging slack and strictly sooner.
func lowerAgesAhead(higher, lower time.Time, in Input) bool {
	if lower.IsZero() || in.AgingSlack < 0 {
		return false
	}
	if lower.Sub(in.Now) > in.AgingSlack {
		return false
	}
	if !higher.IsZero() && !higher.After(lower) {
		return false
	}
	return true
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
