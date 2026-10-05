package store

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
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

// AcceptNearline stores the idempotency key, the nearline record, and the queued unit
// in one script. When the key was already claimed, created is false and id is the
// original request id; the record and unit from that first call are already present.
// idemKey empty skips the idempotency check.
func (s *Store) AcceptNearline(ctx context.Context, idemKey string, ttl time.Duration, rec *model.Nearline, unit *model.Unit) (id string, created bool, err error) {
	if err := s.ensureToken(ctx, unit); err != nil {
		return "", false, err
	}
	recRaw, err := json.Marshal(rec)
	if err != nil {
		return "", false, fmt.Errorf("encode nearline: %w", err)
	}
	unitRaw, err := encodeRequest(unit, s.queueKey(unit.Tier))
	if err != nil {
		return "", false, err
	}
	use := "0"
	key := s.key("noop")
	if idemKey != "" {
		use = "1"
		key = s.idemKey(idemKey)
	}
	ttlSec := int(ttl.Seconds())
	if ttlSec < 1 {
		ttlSec = 1
	}
	raw, err := acceptNearlineScript.Run(ctx, s.rdb, []string{
		key,
		s.nearlineKey(rec.ID),
		s.unitKey(unit.ID),
		s.queueKey(unit.Tier),
		s.deadlineHash(),
		s.tierHash(),
		s.tokenHash(),
	}, use, rec.ID, string(recRaw), string(unitRaw), strconv.FormatInt(unit.Deadline, 10), string(unit.Tier), strconv.Itoa(ttlSec), unit.Token).Text()
	if err != nil {
		return "", false, fmt.Errorf("accept nearline: %w", err)
	}
	switch {
	case strings.HasPrefix(raw, "created:"):
		return strings.TrimPrefix(raw, "created:"), true, nil
	case strings.HasPrefix(raw, "exists:"):
		return strings.TrimPrefix(raw, "exists:"), false, nil
	default:
		return "", false, fmt.Errorf("accept nearline: unexpected reply %q", raw)
	}
}
