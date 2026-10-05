package store

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/rayo1uo/llm-async-gateway/internal/model"
)

func (s *Store) slotKey() string { return s.key("slots") }

func slotField(tier model.Tier, owner string) string {
	return string(tier) + "|" + owner
}

// TryAcquireSlot takes one shared in-flight slot when the live count is below max.
// Expired holders are swept inside the script so a crashed dispatcher does not leak capacity.
func (s *Store) TryAcquireSlot(ctx context.Context, tier model.Tier, owner string, until time.Time, max int) (bool, error) {
	if max < 1 {
		return false, nil
	}
	n, err := acquireSlotScript.Run(ctx, s.rdb, []string{s.slotKey()},
		strconv.FormatInt(time.Now().UnixMilli(), 10),
		strconv.Itoa(max),
		slotField(tier, owner),
		strconv.FormatInt(until.UnixMilli(), 10),
	).Int()
	if err != nil {
		return false, fmt.Errorf("acquire slot: %w", err)
	}
	return n == 1, nil
}

// ExtendSlot pushes a holder's expiry forward. ok is false when the holder was swept.
func (s *Store) ExtendSlot(ctx context.Context, tier model.Tier, owner string, until time.Time) (bool, error) {
	n, err := extendSlotScript.Run(ctx, s.rdb, []string{s.slotKey()},
		slotField(tier, owner),
		strconv.FormatInt(until.UnixMilli(), 10),
	).Int()
	if err != nil {
		return false, fmt.Errorf("extend slot: %w", err)
	}
	return n == 1, nil
}

// ReleaseSlot drops a shared in-flight holder.
func (s *Store) ReleaseSlot(ctx context.Context, tier model.Tier, owner string) error {
	if err := releaseSlotScript.Run(ctx, s.rdb, []string{s.slotKey()}, slotField(tier, owner)).Err(); err != nil {
		return fmt.Errorf("release slot: %w", err)
	}
	return nil
}

// SlotCounts returns live shared in-flight holders, ignoring expired fields.
func (s *Store) SlotCounts(ctx context.Context, now time.Time) (int, map[string]int, error) {
	all, err := s.rdb.HGetAll(ctx, s.slotKey()).Result()
	if err != nil {
		return 0, nil, fmt.Errorf("slot counts: %w", err)
	}
	byTier := map[string]int{}
	total := 0
	nowMS := now.UnixMilli()
	for field, raw := range all {
		exp, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || exp <= nowMS {
			continue
		}
		tier, _, ok := strings.Cut(field, "|")
		if !ok || tier == "" {
			continue
		}
		byTier[tier]++
		total++
	}
	return total, byTier, nil
}
