package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/rayo1uo/llm-async-gateway/internal/model"
)

func (s *Store) unitKey(id string) string        { return s.key("unit", id) }
func (s *Store) queueKey(tier model.Tier) string { return s.key("q", string(tier)) }
func (s *Store) claimedKey() string              { return s.key("claimed") }
func (s *Store) retryKey() string                { return s.key("retry") }
func (s *Store) expiredKey() string              { return s.key("expired") }
func (s *Store) deadlineHash() string            { return s.key("meta", "deadline") }
func (s *Store) tierHash() string                { return s.key("meta", "tier") }
func (s *Store) tokenHash() string               { return s.key("meta", "token") }
func (s *Store) tokenSeqKey() string             { return s.key("meta", "token-seq") }
func (s *Store) finishedKey() string             { return s.key("finished") }
func (s *Store) resultKey(id string) string      { return s.key("result", id) }
func (s *Store) cancelKey(id string) string      { return s.key("cancel", id) }

// QueueKey is the ready-queue key for tier, including the pool hash tag.
func (s *Store) QueueKey(tier model.Tier) string { return s.queueKey(tier) }

func (s *Store) laneKeys() []string {
	return []string{
		s.deadlineHash(),
		s.tierHash(),
		s.finishedKey(),
		s.tokenHash(),
		s.tokenSeqKey(),
		s.queueKey(model.TierInteractive),
		s.queueKey(model.TierAsync),
		s.queueKey(model.TierBatch),
	}
}

// Enqueue persists a pipeline.Request and inserts it into its tier sorted set.
// The score is the deadline in Unix seconds. Batch units also increment the
// batch outstanding counter.
func (s *Store) Enqueue(ctx context.Context, u *model.Unit) error {
	if !u.Tier.Valid() {
		return fmt.Errorf("invalid tier %q", u.Tier)
	}
	if err := s.ensureToken(ctx, u); err != nil {
		return err
	}
	raw, err := encodeRequest(u, s.queueKey(u.Tier))
	if err != nil {
		return err
	}
	incr := "0"
	outstanding := s.key("noop")
	if u.BatchID != "" {
		incr = "1"
		outstanding = s.outstandingKey(u.BatchID)
	}
	_, err = enqueueScript.Run(ctx, s.rdb, []string{
		s.unitKey(u.ID),
		s.queueKey(u.Tier),
		outstanding,
		s.deadlineHash(),
		s.tierHash(),
		s.tokenHash(),
	}, string(raw), strconv.FormatInt(u.Deadline, 10), u.ID, string(u.Tier), incr, u.Token).Result()
	if err != nil {
		return fmt.Errorf("enqueue: %w", err)
	}
	return nil
}

// GetUnit loads a queued request.
func (s *Store) GetUnit(ctx context.Context, id string) (*model.Unit, error) {
	b, err := s.rdb.Get(ctx, s.unitKey(id)).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get unit: %w", err)
	}
	u, err := decodeRequest(b)
	if err != nil {
		return nil, err
	}
	return u, nil
}

// SaveUnit overwrites the request document without moving it between queues.
func (s *Store) SaveUnit(ctx context.Context, u *model.Unit) error {
	raw, err := encodeRequest(u, s.queueKey(u.Tier))
	if err != nil {
		return err
	}
	if err := s.rdb.Set(ctx, s.unitKey(u.ID), raw, 0).Err(); err != nil {
		return fmt.Errorf("save unit: %w", err)
	}
	return nil
}

// Claim leases the earliest-deadline unit in tier. A nil unit means the queue is empty.
// The returned unit's Token is the fencing generation stored with the claim.
func (s *Store) Claim(ctx context.Context, tier model.Tier, leaseUntil time.Time, owner string) (*model.Unit, error) {
	raw, err := claimScript.Run(ctx, s.rdb, []string{
		s.queueKey(tier),
		s.claimedKey(),
		s.tokenHash(),
		s.tokenSeqKey(),
	}, strconv.FormatInt(leaseUntil.UnixMilli(), 10), owner).Text()
	if errors.Is(err, redis.Nil) || raw == "" {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("claim: %w", err)
	}
	id, token, ok := splitClaim(raw)
	if !ok {
		return nil, fmt.Errorf("claim reply %q", raw)
	}
	u, err := s.GetUnit(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("claim load unit: %w", err)
	}
	u.Token = token
	return u, nil
}

func splitClaim(raw string) (id, token string, ok bool) {
	id, rest, found := strings.Cut(raw, "|")
	if !found || id == "" || rest == "" || strings.Contains(rest, "|") {
		return "", "", false
	}
	return id, rest, true
}

// ExtendLease pushes a claim's visibility timeout forward.
// ok is false when this owner and token no longer hold the lease.
func (s *Store) ExtendLease(ctx context.Context, id, token, owner string, until time.Time) (bool, error) {
	n, err := extendScript.Run(ctx, s.rdb, []string{s.claimedKey()},
		id, token, owner, strconv.FormatInt(until.UnixMilli(), 10)).Int()
	if err != nil {
		return false, fmt.Errorf("extend lease: %w", err)
	}
	return n == 1, nil
}

// ParkRetry releases the claim and hides the unit until until.
func (s *Store) ParkRetry(ctx context.Context, u *model.Unit, owner string, until time.Time) error {
	raw, err := encodeRequest(u, s.queueKey(u.Tier))
	if err != nil {
		return err
	}
	_, err = parkScript.Run(ctx, s.rdb, []string{s.claimedKey(), s.unitKey(u.ID), s.retryKey()},
		u.ID, u.Token, owner, string(raw), strconv.FormatInt(until.UnixMilli(), 10)).Result()
	if err != nil {
		return fmt.Errorf("park retry: %w", err)
	}
	return nil
}

// QueueLen returns ready (not claimed, not parked) units in a tier.
func (s *Store) QueueLen(ctx context.Context, tier model.Tier) (int64, error) {
	n, err := s.rdb.ZCard(ctx, s.queueKey(tier)).Result()
	if err != nil {
		return 0, fmt.Errorf("queue len: %w", err)
	}
	return n, nil
}

// EarliestDeadline returns the smallest deadline currently ready in tier.
func (s *Store) EarliestDeadline(ctx context.Context, tier model.Tier) (time.Time, bool, error) {
	zs, err := s.rdb.ZRangeWithScores(ctx, s.queueKey(tier), 0, 0).Result()
	if err != nil {
		return time.Time{}, false, fmt.Errorf("earliest deadline: %w", err)
	}
	if len(zs) == 0 {
		return time.Time{}, false, nil
	}
	return time.Unix(int64(zs[0].Score), 0), true, nil
}

// Peek returns the earliest ready unit without leasing it.
func (s *Store) Peek(ctx context.Context, tier model.Tier) (*model.Unit, error) {
	zs, err := s.rdb.ZRange(ctx, s.queueKey(tier), 0, 0).Result()
	if err != nil {
		return nil, fmt.Errorf("peek queue: %w", err)
	}
	if len(zs) == 0 {
		return nil, nil
	}
	return s.GetUnit(ctx, zs[0])
}

// ClaimedLen returns the number of leased members.
func (s *Store) ClaimedLen(ctx context.Context) (int64, error) {
	n, err := s.rdb.ZCard(ctx, s.claimedKey()).Result()
	if err != nil {
		return 0, fmt.Errorf("claimed len: %w", err)
	}
	return n, nil
}

// Reclaim returns expired leases to their tier queue, or to the expired list when the deadline has passed.
// A requeued request receives a new fencing token.
func (s *Store) Reclaim(ctx context.Context, now time.Time) (int, error) {
	keys := append([]string{s.claimedKey(), s.expiredKey()}, s.laneKeys()...)
	n, err := reclaimScript.Run(ctx, s.rdb, keys,
		strconv.FormatInt(now.UnixMilli(), 10),
		strconv.FormatInt(now.Unix(), 10),
	).Int()
	if err != nil {
		return 0, fmt.Errorf("reclaim: %w", err)
	}
	return n, nil
}

// ExpireReady moves up to 32 ready units in tier whose deadline is at or before now
// onto the expired list. It does not acquire a dispatch budget.
func (s *Store) ExpireReady(ctx context.Context, tier model.Tier, now time.Time) (int, error) {
	n, err := expireReadyScript.Run(ctx, s.rdb, []string{s.queueKey(tier), s.expiredKey(), s.finishedKey()},
		strconv.FormatInt(now.Unix(), 10),
	).Int()
	if err != nil {
		return 0, fmt.Errorf("expire ready: %w", err)
	}
	return n, nil
}

// PromoteRetries moves parked retries whose wait has elapsed back onto the ready queue.
// Each promoted request receives a new fencing token.
func (s *Store) PromoteRetries(ctx context.Context, now time.Time) (int, error) {
	keys := append([]string{s.retryKey(), s.expiredKey()}, s.laneKeys()...)
	n, err := promoteScript.Run(ctx, s.rdb, keys,
		strconv.FormatInt(now.UnixMilli(), 10),
		strconv.FormatInt(now.Unix(), 10),
	).Int()
	if err != nil {
		return 0, fmt.Errorf("promote retries: %w", err)
	}
	return n, nil
}

// PopExpired removes one id whose deadline passed while it was leased or parked.
// An empty id means the list is empty.
func (s *Store) PopExpired(ctx context.Context) (string, error) {
	id, err := s.rdb.RPop(ctx, s.expiredKey()).Result()
	if errors.Is(err, redis.Nil) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("pop expired: %w", err)
	}
	return id, nil
}

// Finish records a terminal result exactly once and releases the claim.
// created is false when a result already existed or when owner no longer holds the fencing token.
// decrOutstanding should be true only for units that passed through Enqueue.
func (s *Store) Finish(ctx context.Context, u *model.Unit, owner string, res *model.Result, line *model.OutputLine, countField string, decrOutstanding bool, ttl time.Duration) (bool, error) {
	resRaw, err := json.Marshal(res)
	if err != nil {
		return false, fmt.Errorf("encode result: %w", err)
	}
	isBatch := "0"
	customID := ""
	lineRaw := ""
	linesKey := s.key("noop")
	countsKey := s.key("noop")
	outstanding := s.key("noop")
	if u.BatchID != "" && line != nil {
		isBatch = "1"
		customID = line.CustomID
		b, err := json.Marshal(line)
		if err != nil {
			return false, fmt.Errorf("encode output line: %w", err)
		}
		lineRaw = string(b)
		linesKey = s.linesKey(u.BatchID)
		countsKey = s.countsKey(u.BatchID)
		outstanding = s.outstandingKey(u.BatchID)
	}
	decr := "0"
	if decrOutstanding {
		decr = "1"
	}
	ttlSec := int(ttl.Seconds())
	if ttlSec < 1 {
		ttlSec = 1
	}
	member := ""
	if owner != "" {
		if u.Token == "" {
			return false, fmt.Errorf("finish %s: missing request token", u.ID)
		}
		member = u.ID + "|" + u.Token + "|" + owner
	}
	n, err := finishScript.Run(ctx, s.rdb, []string{
		s.resultKey(u.ID),
		linesKey,
		countsKey,
		outstanding,
		s.claimedKey(),
		s.finishedKey(),
	}, string(resRaw), strconv.Itoa(ttlSec), isBatch, customID, lineRaw, countField, member, decr, u.ID).Int()
	if err != nil {
		return false, fmt.Errorf("finish: %w", err)
	}
	return n == 1, nil
}

// HasResult reports whether a terminal result is already stored.
func (s *Store) HasResult(ctx context.Context, id string) (bool, error) {
	n, err := s.rdb.Exists(ctx, s.resultKey(id)).Result()
	if err != nil {
		return false, fmt.Errorf("has result: %w", err)
	}
	return n == 1, nil
}

// GetResult loads a terminal result.
func (s *Store) GetResult(ctx context.Context, id string) (*model.Result, error) {
	var res model.Result
	if err := s.getJSON(ctx, s.resultKey(id), &res); err != nil {
		return nil, err
	}
	return &res, nil
}

// MarkCancelled sets the per-request cancel flag.
func (s *Store) MarkCancelled(ctx context.Context, id string) error {
	if err := s.rdb.Set(ctx, s.cancelKey(id), "1", 0).Err(); err != nil {
		return fmt.Errorf("mark cancelled: %w", err)
	}
	return nil
}

// IsCancelled reports the per-request cancel flag.
func (s *Store) IsCancelled(ctx context.Context, id string) (bool, error) {
	n, err := s.rdb.Exists(ctx, s.cancelKey(id)).Result()
	if err != nil {
		return false, fmt.Errorf("cancel flag: %w", err)
	}
	return n == 1, nil
}

// BatchStatusCounts groups stored batch documents by status.
func (s *Store) BatchStatusCounts(ctx context.Context) (map[string]float64, error) {
	ids, err := s.ListBatchIDs(ctx)
	if err != nil {
		return nil, err
	}
	out := map[string]float64{}
	for _, id := range ids {
		b, err := s.GetBatch(ctx, id)
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				continue
			}
			return nil, err
		}
		out[b.Status]++
	}
	return out, nil
}
