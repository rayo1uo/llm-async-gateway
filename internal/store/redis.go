// Package store persists files, batches, and the deadline queue in Redis.
package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/rayo1uo/llm-async-gateway/internal/id"
)

// ErrNotFound is returned when a file, batch, or request does not exist.
var ErrNotFound = errors.New("not found")

// Store is a Redis-backed repository. It is safe for concurrent use.
type Store struct {
	rdb    *redis.Client
	prefix string
}

// New returns a store that prefixes every key.
func New(rdb *redis.Client, prefix string) *Store {
	return &Store{rdb: rdb, prefix: prefix}
}

// Ping checks the Redis connection.
func (s *Store) Ping(ctx context.Context) error {
	if err := s.rdb.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("redis ping: %w", err)
	}
	return nil
}

func (s *Store) key(parts ...string) string {
	return s.prefix + ":" + strings.Join(parts, ":")
}

func (s *Store) setJSON(ctx context.Context, key string, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("encode %s: %w", key, err)
	}
	if err := s.rdb.Set(ctx, key, b, 0).Err(); err != nil {
		return fmt.Errorf("set %s: %w", key, err)
	}
	return nil
}

func (s *Store) getJSON(ctx context.Context, key string, dest any) error {
	b, err := s.rdb.Get(ctx, key).Bytes()
	if errors.Is(err, redis.Nil) {
		return ErrNotFound
	}
	if err != nil {
		return fmt.Errorf("get %s: %w", key, err)
	}
	if err := json.Unmarshal(b, dest); err != nil {
		return fmt.Errorf("decode %s: %w", key, err)
	}
	return nil
}

func (s *Store) lock(ctx context.Context, key string) (func(), error) {
	token, err := id.New("lk_")
	if err != nil {
		return nil, err
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		ok, err := s.rdb.SetNX(ctx, key, token, 5*time.Second).Result()
		if err != nil {
			return nil, fmt.Errorf("lock %s: %w", key, err)
		}
		if ok {
			return func() {
				_, _ = releaseLockScript.Run(context.Background(), s.rdb, []string{key}, token).Result()
			}, nil
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("lock timeout: %s", key)
		}
		timer := time.NewTimer(15 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, fmt.Errorf("lock %s: %w", key, ctx.Err())
		case <-timer.C:
		}
	}
}
