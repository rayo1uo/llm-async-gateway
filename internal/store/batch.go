package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"github.com/redis/go-redis/v9"

	"github.com/rayo1uo/llm-async-gateway/internal/model"
)

func (s *Store) batchKey(id string) string         { return s.key("batch", id) }
func (s *Store) batchIndexKey() string             { return s.key("batchindex") }
func (s *Store) activeKey() string                 { return s.key("batches", "active") }
func (s *Store) inputsKey(id string) string        { return s.key("batch", id, "inputs") }
func (s *Store) linesKey(id string) string         { return s.key("batch", id, "lines") }
func (s *Store) countsKey(id string) string        { return s.key("batch", id, "counts") }
func (s *Store) outstandingKey(id string) string   { return s.key("batch", id, "outstanding") }
func (s *Store) batchCancelKey(id string) string   { return s.key("batch", id, "cancel") }
func (s *Store) batchDeadlineKey(id string) string { return s.key("batch", id, "dl") }

// CreateBatch inserts a batch, indexes it for listing, and marks it active.
func (s *Store) CreateBatch(ctx context.Context, b *model.Batch, deadlineMS int64) error {
	raw, err := mustJSON(b)
	if err != nil {
		return err
	}
	pipe := s.rdb.TxPipeline()
	pipe.Set(ctx, s.batchKey(b.ID), raw, 0)
	pipe.ZAdd(ctx, s.batchIndexKey(), redis.Z{Score: float64(b.CreatedAt), Member: b.ID})
	pipe.SAdd(ctx, s.activeKey(), b.ID)
	pipe.Set(ctx, s.batchDeadlineKey(b.ID), strconv.FormatInt(deadlineMS, 10), 0)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("create batch: %w", err)
	}
	return nil
}

// GetBatch loads a batch document.
func (s *Store) GetBatch(ctx context.Context, id string) (*model.Batch, error) {
	var b model.Batch
	if err := s.getJSON(ctx, s.batchKey(id), &b); err != nil {
		return nil, err
	}
	return &b, nil
}

// BatchDeadline returns the precise expiry in Unix milliseconds.
func (s *Store) BatchDeadline(ctx context.Context, id string) (int64, error) {
	v, err := s.rdb.Get(ctx, s.batchDeadlineKey(id)).Result()
	if errors.Is(err, redis.Nil) {
		return 0, ErrNotFound
	}
	if err != nil {
		return 0, fmt.Errorf("batch deadline: %w", err)
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("batch deadline: %w", err)
	}
	return n, nil
}

// MutateBatch read-modify-writes a batch under a short lock.
// fn must not call MutateBatch on the same id.
func (s *Store) MutateBatch(ctx context.Context, id string, fn func(*model.Batch) error) (*model.Batch, error) {
	unlock, err := s.lock(ctx, s.key("lock", "batch", id))
	if err != nil {
		return nil, err
	}
	defer unlock()
	var b model.Batch
	if err := s.getJSON(ctx, s.batchKey(id), &b); err != nil {
		return nil, err
	}
	if err := fn(&b); err != nil {
		return nil, err
	}
	if err := s.setJSON(ctx, s.batchKey(id), &b); err != nil {
		return nil, err
	}
	return &b, nil
}

// ListBatchIDs returns batch ids newest-first.
func (s *Store) ListBatchIDs(ctx context.Context) ([]string, error) {
	ids, err := s.rdb.ZRevRange(ctx, s.batchIndexKey(), 0, -1).Result()
	if err != nil {
		return nil, fmt.Errorf("list batches: %w", err)
	}
	if ids == nil {
		ids = []string{}
	}
	return ids, nil
}

// ActiveBatchIDs returns batches the controller should reconcile.
func (s *Store) ActiveBatchIDs(ctx context.Context) ([]string, error) {
	ids, err := s.rdb.SMembers(ctx, s.activeKey()).Result()
	if err != nil {
		return nil, fmt.Errorf("active batches: %w", err)
	}
	if ids == nil {
		ids = []string{}
	}
	return ids, nil
}

// RemoveActiveBatch drops a terminal batch from the reconcile set.
func (s *Store) RemoveActiveBatch(ctx context.Context, id string) error {
	if err := s.rdb.SRem(ctx, s.activeKey(), id).Err(); err != nil {
		return fmt.Errorf("remove active batch: %w", err)
	}
	return nil
}

// ReplaceBatchInputs replaces the pending input list.
func (s *Store) ReplaceBatchInputs(ctx context.Context, batchID string, lines []model.InputLine) error {
	key := s.inputsKey(batchID)
	pipe := s.rdb.TxPipeline()
	pipe.Del(ctx, key)
	for i := range lines {
		raw, err := json.Marshal(&lines[i])
		if err != nil {
			return fmt.Errorf("encode input: %w", err)
		}
		pipe.RPush(ctx, key, raw)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("replace inputs: %w", err)
	}
	return nil
}

// BatchInputLen returns how many lines are waiting to be enqueued.
func (s *Store) BatchInputLen(ctx context.Context, batchID string) (int64, error) {
	n, err := s.rdb.LLen(ctx, s.inputsKey(batchID)).Result()
	if err != nil {
		return 0, fmt.Errorf("input len: %w", err)
	}
	return n, nil
}

// PopBatchInput removes the next input line. A nil line means the list is empty.
func (s *Store) PopBatchInput(ctx context.Context, batchID string) (*model.InputLine, error) {
	raw, err := s.rdb.LPop(ctx, s.inputsKey(batchID)).Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("pop input: %w", err)
	}
	var line model.InputLine
	if err := json.Unmarshal([]byte(raw), &line); err != nil {
		return nil, fmt.Errorf("decode input: %w", err)
	}
	return &line, nil
}

// PushBatchInputFront puts a line back at the head after a failed enqueue.
func (s *Store) PushBatchInputFront(ctx context.Context, batchID string, line *model.InputLine) error {
	raw, err := json.Marshal(line)
	if err != nil {
		return fmt.Errorf("encode input: %w", err)
	}
	if err := s.rdb.LPush(ctx, s.inputsKey(batchID), raw).Err(); err != nil {
		return fmt.Errorf("push input: %w", err)
	}
	return nil
}

// SetBatchTotal records the validated request count.
func (s *Store) SetBatchTotal(ctx context.Context, batchID string, total int) error {
	if err := s.rdb.HSet(ctx, s.countsKey(batchID), "total", total).Err(); err != nil {
		return fmt.Errorf("set batch total: %w", err)
	}
	return nil
}

// BatchCounts reads request counters.
func (s *Store) BatchCounts(ctx context.Context, batchID string) (model.RequestCounts, error) {
	vals, err := s.rdb.HGetAll(ctx, s.countsKey(batchID)).Result()
	if err != nil {
		return model.RequestCounts{}, fmt.Errorf("batch counts: %w", err)
	}
	return model.RequestCounts{
		Total:     atoi(vals["total"]),
		Completed: atoi(vals["completed"]),
		Failed:    atoi(vals["failed"]),
	}, nil
}

// Outstanding returns queued plus in-flight lines for a batch.
func (s *Store) Outstanding(ctx context.Context, batchID string) (int64, error) {
	v, err := s.rdb.Get(ctx, s.outstandingKey(batchID)).Result()
	if errors.Is(err, redis.Nil) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("outstanding: %w", err)
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("outstanding: %w", err)
	}
	if n < 0 {
		return 0, nil
	}
	return n, nil
}

// BatchLines returns every recorded output line.
func (s *Store) BatchLines(ctx context.Context, batchID string) ([]model.OutputLine, error) {
	vals, err := s.rdb.HGetAll(ctx, s.linesKey(batchID)).Result()
	if err != nil {
		return nil, fmt.Errorf("batch lines: %w", err)
	}
	out := make([]model.OutputLine, 0, len(vals))
	for _, raw := range vals {
		var line model.OutputLine
		if err := json.Unmarshal([]byte(raw), &line); err != nil {
			return nil, fmt.Errorf("decode batch line: %w", err)
		}
		out = append(out, line)
	}
	return out, nil
}

// MarkBatchCancel records a batch-level cancel flag checked at dispatch time.
func (s *Store) MarkBatchCancel(ctx context.Context, batchID string) error {
	if err := s.rdb.Set(ctx, s.batchCancelKey(batchID), "1", 0).Err(); err != nil {
		return fmt.Errorf("mark batch cancel: %w", err)
	}
	return nil
}

// IsBatchCancelled reports the batch cancel flag.
func (s *Store) IsBatchCancelled(ctx context.Context, batchID string) (bool, error) {
	n, err := s.rdb.Exists(ctx, s.batchCancelKey(batchID)).Result()
	if err != nil {
		return false, fmt.Errorf("batch cancel flag: %w", err)
	}
	return n == 1, nil
}

func atoi(s string) int {
	if s == "" {
		return 0
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return 0
	}
	return n
}
