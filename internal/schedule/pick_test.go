package schedule

import (
	"testing"
	"time"

	"github.com/rayo1uo/llm-async-gateway/internal/model"
)

func TestPick(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	soon := now.Add(30 * time.Second)
	later := now.Add(time.Hour)
	tests := []struct {
		name string
		in   Input
		want model.Tier
		ok   bool
	}{
		{name: "empty", in: Input{Now: now}, ok: false},
		{
			name: "interactive outranks async",
			in: Input{
				InteractiveReady: 1, InteractiveDeadline: later,
				NearlineReady: 2, NearlineDeadline: later,
				Now: now, AgingSlack: time.Minute,
			},
			want: model.TierInteractive, ok: true,
		},
		{
			name: "only nearline",
			in:   Input{NearlineReady: 2, NearlineDeadline: later, Now: now, AgingSlack: time.Minute},
			want: model.TierNearline, ok: true,
		},
		{
			name: "only batch",
			in:   Input{BatchReady: 3, BatchDeadline: later, Now: now},
			want: model.TierBatch, ok: true,
		},
		{
			name: "nearline preferred",
			in: Input{
				NearlineReady: 1, BatchReady: 4,
				NearlineDeadline: later, BatchDeadline: later.Add(time.Hour),
				Now: now, AgingSlack: time.Minute, ReserveEvery: 5, ConsecutiveNearline: 0,
			},
			want: model.TierNearline, ok: true,
		},
		{
			name: "rotation reserves a batch turn",
			in: Input{
				NearlineReady: 1, BatchReady: 1,
				NearlineDeadline: later, BatchDeadline: later,
				Now: now, AgingSlack: time.Minute, ReserveEvery: 5, ConsecutiveNearline: 4,
			},
			want: model.TierBatch, ok: true,
		},
		{
			name: "aging when batch deadline is sooner",
			in: Input{
				NearlineReady: 1, BatchReady: 1,
				NearlineDeadline: later, BatchDeadline: soon,
				Now: now, AgingSlack: time.Minute, ReserveEvery: 5, ConsecutiveNearline: 0,
			},
			want: model.TierBatch, ok: true,
		},
		{
			name: "more overdue nearline still beats batch",
			in: Input{
				NearlineReady: 1, BatchReady: 1,
				NearlineDeadline: now.Add(-time.Minute), BatchDeadline: now.Add(-time.Second),
				Now: now, AgingSlack: time.Minute, ReserveEvery: 5,
			},
			want: model.TierNearline, ok: true,
		},
		{
			name: "more overdue batch ages ahead of nearline",
			in: Input{
				NearlineReady: 1, BatchReady: 1,
				NearlineDeadline: now.Add(time.Hour), BatchDeadline: now.Add(-time.Second),
				Now: now, AgingSlack: time.Minute, ReserveEvery: 5, ConsecutiveNearline: 0,
			},
			want: model.TierBatch, ok: true,
		},
		{
			name: "aging does not jump a sooner nearline deadline",
			in: Input{
				NearlineReady: 1, BatchReady: 1,
				NearlineDeadline: soon, BatchDeadline: soon.Add(10 * time.Second),
				Now: now, AgingSlack: time.Minute, ReserveEvery: 0,
			},
			want: model.TierNearline, ok: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := Pick(tt.in)
			if ok != tt.ok || got != tt.want {
				t.Fatalf("Pick() = (%s, %v), want (%s, %v)", got, ok, tt.want, tt.ok)
			}
		})
	}
}
