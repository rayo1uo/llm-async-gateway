package store

import (
	"context"
	"fmt"
	"time"

	"github.com/rayo1uo/llm-async-gateway/internal/model"
)

func (s *Store) nearlineKey(id string) string { return s.key("nearline", id) }
func (s *Store) idemKey(key string) string    { return s.key("idem", key) }

// PutNearline stores a new nearline record.
func (s *Store) PutNearline(ctx context.Context, n *model.Nearline) error {
	if err := s.setJSON(ctx, s.nearlineKey(n.ID), n); err != nil {
		return err
	}
	return nil
}

// GetNearline loads a nearline record.
func (s *Store) GetNearline(ctx context.Context, id string) (*model.Nearline, error) {
	var n model.Nearline
	if err := s.getJSON(ctx, s.nearlineKey(id), &n); err != nil {
		return nil, err
	}
	return &n, nil
}

// SetNearlineStatus moves a record forward. A terminal status is not overwritten.
func (s *Store) SetNearlineStatus(ctx context.Context, id, status string, atUnix int64) error {
	unlock, err := s.lock(ctx, s.key("lock", "nl", id))
	if err != nil {
		return err
	}
	defer unlock()
	var n model.Nearline
	if err := s.getJSON(ctx, s.nearlineKey(id), &n); err != nil {
		return err
	}
	if model.TerminalNearline(n.Status) {
		return nil
	}
	n.Status = status
	if model.TerminalNearline(status) && atUnix > 0 {
		n.CompletedAt = &atUnix
	}
	return s.setJSON(ctx, s.nearlineKey(id), &n)
}

// ReserveIdempotency claims key for id. When created is false, existing is the prior id.
func (s *Store) ReserveIdempotency(ctx context.Context, key, requestID string, ttl time.Duration) (existing string, created bool, err error) {
	ok, err := s.rdb.SetNX(ctx, s.idemKey(key), requestID, ttl).Result()
	if err != nil {
		return "", false, fmt.Errorf("idempotency: %w", err)
	}
	if ok {
		return requestID, true, nil
	}
	prev, err := s.rdb.Get(ctx, s.idemKey(key)).Result()
	if err != nil {
		return "", false, fmt.Errorf("idempotency read: %w", err)
	}
	return prev, false, nil
}
