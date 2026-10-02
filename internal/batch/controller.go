// Package batch validates input files, window-enqueues lines, and finalizes results.
package batch

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/rayo1uo/llm-async-gateway/internal/clock"
	"github.com/rayo1uo/llm-async-gateway/internal/id"
	"github.com/rayo1uo/llm-async-gateway/internal/jsonl"
	"github.com/rayo1uo/llm-async-gateway/internal/model"
	"github.com/rayo1uo/llm-async-gateway/internal/store"
)

// Options controls the per-batch enqueue window.
type Options struct {
	Window       int
	PollInterval time.Duration
	ResultTTL    time.Duration
}

// Controller advances batch state machines.
type Controller struct {
	store *store.Store
	opts  Options
	log   *slog.Logger
	clk   clock.Clock
}

// New builds a controller. clk may be nil.
func New(st *store.Store, opts Options, logger *slog.Logger, clk clock.Clock) *Controller {
	if clk == nil {
		clk = clock.Real{}
	}
	if logger == nil {
		logger = slog.Default()
	}
	if opts.Window < 1 {
		opts.Window = 1
	}
	return &Controller{store: st, opts: opts, log: logger, clk: clk}
}

// Run reconciles active batches until ctx is cancelled.
func (c *Controller) Run(ctx context.Context) {
	c.reconcileAll(ctx)
	ticker := time.NewTicker(c.opts.PollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.reconcileAll(context.Background())
		}
	}
}

func (c *Controller) reconcileAll(ctx context.Context) {
	if err := ctx.Err(); err != nil {
		return
	}
	ids, err := c.store.ActiveBatchIDs(ctx)
	if err != nil {
		c.log.Error("list active batches", "err", err)
		return
	}
	sort.Slice(ids, func(i, j int) bool {
		di, ei := c.store.BatchDeadline(ctx, ids[i])
		dj, ej := c.store.BatchDeadline(ctx, ids[j])
		if ei != nil {
			return false
		}
		if ej != nil {
			return true
		}
		return di < dj
	})
	for _, id := range ids {
		if err := c.reconcile(ctx, id); err != nil {
			c.log.Error("reconcile batch", "batch_id", id, "err", err)
		}
	}
}

func (c *Controller) reconcile(ctx context.Context, id string) error {
	b, err := c.store.GetBatch(ctx, id)
	if err != nil {
		return err
	}
	switch b.Status {
	case model.StatusValidating:
		return c.validate(ctx, b)
	case model.StatusInProgress:
		if err := c.progress(ctx, b); err != nil {
			return err
		}
		return c.maybeFinalize(ctx, id)
	case model.StatusCancelling:
		if err := c.skipRemaining(ctx, b.ID, model.ErrBatchCancelled, model.StatusCancelled, "This request was cancelled before it executed."); err != nil {
			return err
		}
		return c.maybeFinalize(ctx, id)
	case model.StatusFinalizing:
		return c.maybeFinalize(ctx, id)
	default:
		if model.TerminalBatch(b.Status) {
			return c.store.RemoveActiveBatch(ctx, id)
		}
		return nil
	}
}

func (c *Controller) validate(ctx context.Context, b *model.Batch) error {
	body, err := c.store.GetFileContent(ctx, b.InputFileID)
	if err != nil {
		return c.fail(ctx, b.ID, model.BatchError{Code: "invalid_file", Message: "input file could not be read", Param: "input_file_id"})
	}
	lines, errs, err := jsonl.Parse(bytes.NewReader(body), b.Endpoint)
	if err != nil {
		return c.fail(ctx, b.ID, model.BatchError{Code: "invalid_file", Message: err.Error(), Param: "input_file_id"})
	}
	if len(errs) > 20 {
		errs = errs[:20]
	}
	updated, err := c.store.MutateBatch(ctx, b.ID, func(cur *model.Batch) error {
		if cur.Status != model.StatusValidating {
			return nil
		}
		if len(errs) > 0 {
			now := c.clk.Now().Unix()
			cur.Status = model.StatusFailed
			cur.FailedAt = &now
			cur.Errors = &model.BatchErrors{Object: model.ObjectList, Data: errs}
			return nil
		}
		if err := c.store.ReplaceBatchInputs(ctx, cur.ID, lines); err != nil {
			return err
		}
		if err := c.store.SetBatchTotal(ctx, cur.ID, len(lines)); err != nil {
			return err
		}
		now := c.clk.Now().Unix()
		cur.Status = model.StatusInProgress
		cur.InProgressAt = &now
		cur.RequestCounts = model.RequestCounts{Total: len(lines)}
		return nil
	})
	if err != nil {
		return err
	}
	c.log.Info("batch validated", "batch_id", b.ID, "status", updated.Status, "lines", len(lines))
	if updated.Status == model.StatusFailed {
		return c.store.RemoveActiveBatch(ctx, b.ID)
	}
	return nil
}

func (c *Controller) fail(ctx context.Context, id string, e model.BatchError) error {
	_, err := c.store.MutateBatch(ctx, id, func(cur *model.Batch) error {
		if cur.Status != model.StatusValidating {
			return nil
		}
		now := c.clk.Now().Unix()
		cur.Status = model.StatusFailed
		cur.FailedAt = &now
		cur.Errors = &model.BatchErrors{Object: model.ObjectList, Data: []model.BatchError{e}}
		return nil
	})
	if err != nil {
		return err
	}
	return c.store.RemoveActiveBatch(ctx, id)
}

func (c *Controller) progress(ctx context.Context, b *model.Batch) error {
	dl, err := c.store.BatchDeadline(ctx, b.ID)
	if err != nil {
		return err
	}
	if c.clk.Now().UnixMilli() >= dl {
		return c.skipRemaining(ctx, b.ID, model.ErrBatchExpired, model.StatusExpired, "This request was not executed before the completion window expired.")
	}
	return c.fill(ctx, b, dl)
}

func (c *Controller) fill(ctx context.Context, b *model.Batch, deadlineMS int64) error {
	outstanding, err := c.store.Outstanding(ctx, b.ID)
	if err != nil {
		return err
	}
	room := c.opts.Window - int(outstanding)
	for i := 0; i < room; i++ {
		line, err := c.store.PopBatchInput(ctx, b.ID)
		if err != nil {
			return err
		}
		if line == nil {
			break
		}
		uid, err := id.New("batch_req_")
		if err != nil {
			_ = c.store.PushBatchInputFront(ctx, b.ID, line)
			return err
		}
		unit := &model.Unit{
			ID:        uid,
			Tier:      model.TierBatch,
			Endpoint:  line.URL,
			Body:      line.Body,
			Deadline:  deadlineMS,
			Created:   c.clk.Now().UnixMilli(),
			BatchID:   b.ID,
			CustomID:  line.CustomID,
			LineIndex: line.Index,
		}
		if err := c.store.Enqueue(ctx, unit); err != nil {
			if pushErr := c.store.PushBatchInputFront(ctx, b.ID, line); pushErr != nil {
				return errors.Join(err, pushErr)
			}
			return err
		}
	}
	return c.publishCounts(ctx, b.ID)
}

func (c *Controller) skipRemaining(ctx context.Context, batchID, code, status, message string) error {
	for {
		line, err := c.store.PopBatchInput(ctx, batchID)
		if err != nil {
			return err
		}
		if line == nil {
			break
		}
		if err := c.recordSkipped(ctx, batchID, line, code, status, message); err != nil {
			if pushErr := c.store.PushBatchInputFront(ctx, batchID, line); pushErr != nil {
				return errors.Join(err, pushErr)
			}
			return err
		}
	}
	return c.publishCounts(ctx, batchID)
}

func (c *Controller) recordSkipped(ctx context.Context, batchID string, line *model.InputLine, code, status, message string) error {
	uid, err := id.New("batch_req_")
	if err != nil {
		return err
	}
	now := c.clk.Now().Unix()
	res := &model.Result{
		ID:           uid,
		Status:       status,
		ErrorCode:    code,
		ErrorMessage: message,
		FinishedAt:   now,
	}
	out := &model.OutputLine{
		ID:       uid,
		CustomID: line.CustomID,
		Error:    &model.OutputError{Code: code, Message: message},
	}
	unit := &model.Unit{ID: uid, BatchID: batchID, CustomID: line.CustomID, Tier: model.TierBatch}
	_, err = c.store.Finish(ctx, unit, "", res, out, model.CountFailed, false, c.opts.ResultTTL)
	return err
}

func (c *Controller) publishCounts(ctx context.Context, id string) error {
	counts, err := c.store.BatchCounts(ctx, id)
	if err != nil {
		return err
	}
	_, err = c.store.MutateBatch(ctx, id, func(cur *model.Batch) error {
		if model.TerminalBatch(cur.Status) || cur.Status == model.StatusValidating {
			return nil
		}
		cur.RequestCounts = counts
		return nil
	})
	return err
}

func (c *Controller) maybeFinalize(ctx context.Context, id string) error {
	b, err := c.store.GetBatch(ctx, id)
	if err != nil {
		return err
	}
	if model.TerminalBatch(b.Status) {
		return c.store.RemoveActiveBatch(ctx, id)
	}
	inLen, err := c.store.BatchInputLen(ctx, id)
	if err != nil {
		return err
	}
	outstanding, err := c.store.Outstanding(ctx, id)
	if err != nil {
		return err
	}
	counts, err := c.store.BatchCounts(ctx, id)
	if err != nil {
		return err
	}
	// A cancel that lands during validation never sets a total. Finalize it as
	// cancelled instead of waiting for counts that will not appear.
	earlyCancel := b.Status == model.StatusCancelling && inLen == 0 && outstanding == 0 && counts.Total == 0
	if !earlyCancel && (inLen > 0 || outstanding > 0 || counts.Total == 0 || counts.Completed+counts.Failed < counts.Total) {
		return c.publishCounts(ctx, id)
	}
	lines, err := c.store.BatchLines(ctx, id)
	if err != nil {
		return err
	}
	good, bad := splitLines(lines)
	var outputID, errorID string
	if len(good) > 0 {
		body, err := encodeJSONL(good)
		if err != nil {
			return err
		}
		outputID, err = c.putDerived(ctx, id, "output", model.PurposeBatchOutput, body)
		if err != nil {
			return err
		}
	}
	if len(bad) > 0 {
		body, err := encodeJSONL(bad)
		if err != nil {
			return err
		}
		errorID, err = c.putDerived(ctx, id, "error", model.PurposeBatchError, body)
		if err != nil {
			return err
		}
	}
	usage := sumUsage(good)
	cancelled, err := c.store.IsBatchCancelled(ctx, id)
	if err != nil {
		return err
	}
	updated, err := c.store.MutateBatch(ctx, id, func(cur *model.Batch) error {
		if model.TerminalBatch(cur.Status) {
			return nil
		}
		now := c.clk.Now().Unix()
		if cur.FinalizingAt == nil {
			cur.FinalizingAt = &now
		}
		cur.RequestCounts = counts
		cur.Usage = usage
		if outputID != "" {
			cur.OutputFileID = &outputID
		}
		if errorID != "" {
			cur.ErrorFileID = &errorID
		}
		switch {
		case cancelled || cur.Status == model.StatusCancelling:
			cur.Status = model.StatusCancelled
			cur.CancelledAt = &now
		case hasCode(lines, model.ErrBatchExpired):
			cur.Status = model.StatusExpired
			cur.ExpiredAt = &now
		default:
			cur.Status = model.StatusCompleted
			cur.CompletedAt = &now
		}
		return nil
	})
	if err != nil {
		return err
	}
	c.log.Info("batch finalized", "batch_id", id, "status", updated.Status, "completed", counts.Completed, "failed", counts.Failed)
	if model.TerminalBatch(updated.Status) {
		return c.store.RemoveActiveBatch(ctx, id)
	}
	return nil
}

func (c *Controller) putDerived(ctx context.Context, batchID, kind, purpose string, body []byte) (string, error) {
	fid, err := id.New("file_")
	if err != nil {
		return "", err
	}
	f := &model.File{
		ID:        fid,
		Object:    model.ObjectFile,
		Bytes:     len(body),
		CreatedAt: c.clk.Now().Unix(),
		Filename:  fmt.Sprintf("%s_%s.jsonl", batchID, kind),
		Purpose:   purpose,
	}
	if err := c.store.PutFile(ctx, f, body); err != nil {
		return "", err
	}
	return fid, nil
}

func splitLines(lines []model.OutputLine) (good, bad []model.OutputLine) {
	for _, ln := range lines {
		if ln.Error == nil && ln.Response != nil && ln.Response.StatusCode >= 200 && ln.Response.StatusCode < 300 {
			good = append(good, ln)
			continue
		}
		bad = append(bad, ln)
	}
	return good, bad
}

func encodeJSONL(lines []model.OutputLine) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	for i := range lines {
		if err := enc.Encode(&lines[i]); err != nil {
			return nil, fmt.Errorf("encode jsonl: %w", err)
		}
	}
	return buf.Bytes(), nil
}

func sumUsage(lines []model.OutputLine) *model.Usage {
	var usage model.Usage
	found := false
	for _, ln := range lines {
		if ln.Response == nil {
			continue
		}
		var body struct {
			Usage struct {
				PromptTokens     int `json:"prompt_tokens"`
				CompletionTokens int `json:"completion_tokens"`
				TotalTokens      int `json:"total_tokens"`
			} `json:"usage"`
		}
		if err := json.Unmarshal(ln.Response.Body, &body); err != nil {
			continue
		}
		if body.Usage.PromptTokens == 0 && body.Usage.CompletionTokens == 0 && body.Usage.TotalTokens == 0 {
			continue
		}
		found = true
		usage.InputTokens += body.Usage.PromptTokens
		usage.OutputTokens += body.Usage.CompletionTokens
		usage.TotalTokens += body.Usage.TotalTokens
	}
	if !found {
		return nil
	}
	return &usage
}

func hasCode(lines []model.OutputLine, code string) bool {
	for _, ln := range lines {
		if ln.Error != nil && ln.Error.Code == code {
			return true
		}
	}
	return false
}
