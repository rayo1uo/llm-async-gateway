package store

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

func (s *Store) controllerLockKey() string  { return s.key("controller", "lock") }
func (s *Store) controllerFenceKey() string { return s.key("controller", "fence") }

// TryLead campaigns for the single-active batch controller.
// The returned token is a monotonic fencing token plus the owner id.
// A lost campaign returns ok=false and no error.
func (s *Store) TryLead(ctx context.Context, owner string, ttl time.Duration) (string, bool, error) {
	if ttl <= 0 {
		return "", false, fmt.Errorf("controller lock ttl must be > 0")
	}
	n, err := s.rdb.Incr(ctx, s.controllerFenceKey()).Result()
	if err != nil {
		return "", false, fmt.Errorf("controller fence: %w", err)
	}
	token := strconv.FormatInt(n, 10) + ":" + owner
	ok, err := s.rdb.SetNX(ctx, s.controllerLockKey(), token, ttl).Result()
	if err != nil {
		return "", false, fmt.Errorf("controller lock: %w", err)
	}
	if !ok {
		return "", false, nil
	}
	return token, true, nil
}

// RenewLead extends the lock when this token still owns it.
func (s *Store) RenewLead(ctx context.Context, token string, ttl time.Duration) (bool, error) {
	ms := ttl.Milliseconds()
	if ms < 1 {
		ms = 1
	}
	n, err := renewLockScript.Run(ctx, s.rdb, []string{s.controllerLockKey()}, token, strconv.FormatInt(ms, 10)).Int()
	if err != nil {
		return false, fmt.Errorf("renew controller lock: %w", err)
	}
	return n == 1, nil
}

// HasLead reports whether token still owns the controller lock, without extending it.
func (s *Store) HasLead(ctx context.Context, token string) (bool, error) {
	cur, err := s.rdb.Get(ctx, s.controllerLockKey()).Result()
	if errors.Is(err, redis.Nil) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("read controller lock: %w", err)
	}
	return cur == token, nil
}

// ReleaseLead deletes the lock when token still owns it.
func (s *Store) ReleaseLead(ctx context.Context, token string) error {
	if _, err := releaseLockScript.Run(ctx, s.rdb, []string{s.controllerLockKey()}, token).Result(); err != nil {
		return fmt.Errorf("release controller lock: %w", err)
	}
	return nil
}

// LeadToken returns the current lock value. An empty token means there is no leader.
func (s *Store) LeadToken(ctx context.Context) (string, error) {
	cur, err := s.rdb.Get(ctx, s.controllerLockKey()).Result()
	if errors.Is(err, redis.Nil) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("read controller lock: %w", err)
	}
	return cur, nil
}
