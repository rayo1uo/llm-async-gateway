// Package batch validates input files, window-enqueues lines, and finalizes results.
package batch

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/rayo1uo/llm-async-gateway/internal/clock"
	"github.com/rayo1uo/llm-async-gateway/internal/id"
	"github.com/rayo1uo/llm-async-gateway/internal/jsonl"
	"github.com/rayo1uo/llm-async-gateway/internal/model"
	"github.com/rayo1uo/llm-async-gateway/internal/observe"
	"github.com/rayo1uo/llm-async-gateway/internal/store"
)

// Options controls the per-batch enqueue window and the single-active lock.
type Options struct {
	Window       int
	PollInterval time.Duration
	ResultTTL    time.Duration
	LockTTL      time.Duration
	Observer     *observe.Observer
}

// Controller advances batch state machines. Run campaigns for a Redis lock
// with a fencing token so only one replica reconciles.
type Controller struct {
	store *store.Store
	opts  Options
	log   *slog.Logger
	clk   clock.Clock

	mu    sync.RWMutex
	owner string
	token string
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
	owner, err := id.New("ctl_")
	if err != nil {
		owner = "ctl_unknown"
	}
	return &Controller{store: st, opts: opts, log: logger, clk: clk, owner: owner}
}

// Leadership reports whether this process currently holds the controller lock.
func (c *Controller) Leadership(ctx context.Context) (owner, token string, ok bool) {
	c.mu.RLock()
	owner, token = c.owner, c.token
	c.mu.RUnlock()
	if token == "" {
		return owner, "", false
	}
	held, err := c.store.HasLead(ctx, token)
	if err != nil || !held {
		return owner, token, false
	}
	return owner, token, true
}

func (c *Controller) setToken(token string) {
	c.mu.Lock()
	c.token = token
	c.mu.Unlock()
}

// Run campaigns for leadership and reconciles only while this process holds
// the fencing token. A failed renewal stops the loop immediately.
func (c *Controller) Run(ctx context.Context) {
	ttl := c.opts.LockTTL
	if ttl <= 0 {
		ttl = 10 * time.Second
	}
	interval := c.opts.PollInterval
	if interval <= 0 {
		interval = 50 * time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if err := ctx.Err(); err != nil {
			return
		}
		token, ok, err := c.store.TryLead(ctx, c.owner, ttl)
		if err != nil {
			c.log.Error("controller campaign", "owner", c.owner, "err", err)
		} else if ok {
			c.lead(ctx, token, ttl)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (c *Controller) lead(ctx context.Context, token string, ttl time.Duration) {
	c.setToken(token)
	defer c.setToken("")
	lost := false
	defer func() {
		if lost {
			return
		}
		relCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := c.store.ReleaseLead(relCtx, token); err != nil {
			c.log.Error("release controller lock", "owner", c.owner, "err", err)
		}
	}()
	c.log.Info("controller elected", "owner", c.owner, "token", token)
	renewEvery := ttl / 3
	if renewEvery < 10*time.Millisecond {
		renewEvery = 10 * time.Millisecond
	}
	renew := time.NewTicker(renewEvery)
	defer renew.Stop()
	work := time.NewTicker(c.opts.PollInterval)
	defer work.Stop()
	if !c.renew(ctx, token, ttl) {
		lost = true
		return
	}
	c.reconcileAll(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-renew.C:
			if !c.renew(ctx, token, ttl) {
				lost = true
				c.log.Warn("controller lost leadership", "owner", c.owner, "token", token)
				return
			}
		case <-work.C:
			if !c.renew(ctx, token, ttl) {
				lost = true
				c.log.Warn("controller lost leadership", "owner", c.owner, "token", token)
				return
			}
			c.reconcileAll(ctx)
		}
	}
}

func (c *Controller) renew(ctx context.Context, token string, ttl time.Duration) bool {
	ok, err := c.store.RenewLead(ctx, token, ttl)
	if err != nil {
		c.log.Error("renew controller lock", "owner", c.owner, "err", err)
		return false
	}
	return ok
}

func (c *Controller) stillLeader(ctx context.Context) bool {
	c.mu.RLock()
	token := c.token
	c.mu.RUnlock()
	if token == "" {
		// Direct calls from tests are not inside a campaign.
		return true
	}
	ok, err := c.store.HasLead(ctx, token)
	return err == nil && ok
}

func (c *Controller) reconcileAll(ctx context.Context) {
	if err := ctx.Err(); err != nil {
		return
	}
	if !c.stillLeader(ctx) {
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
	if !c.stillLeader(ctx) {
		return nil
	}
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
	ctx, span := c.span(ctx, "batch.validate")
	defer span.End()
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
	retries := 0
	for {
		status, err := c.enqueueHead(ctx, b.ID, deadlineMS)
		if err != nil {
			return err
		}
		switch status {
		case "ok":
			retries = 0
		case "retry":
			retries++
			if retries > 8 {
				return fmt.Errorf("batch %s input head kept changing", b.ID)
			}
		case "lost":
			return nil
		case "full", "empty":
			return c.publishCounts(ctx, b.ID)
		default:
			return fmt.Errorf("batch %s enqueue status %q", b.ID, status)
		}
	}
}

func (c *Controller) enqueueHead(ctx context.Context, batchID string, deadlineMS int64) (string, error) {
	if !c.stillLeader(ctx) {
		return "lost", nil
	}
	raw, line, err := c.store.PeekBatchInput(ctx, batchID)
	if err != nil {
		return "", err
	}
	if line == nil {
		return "empty", nil
	}
	uid, err := id.New("batch_req_")
	if err != nil {
		return "", err
	}
	unit := &model.Unit{
		ID:        uid,
		Tier:      model.TierBatch,
		Endpoint:  line.URL,
		Body:      line.Body,
		Deadline:  deadlineMS / 1000,
		Created:   c.clk.Now().Unix(),
		BatchID:   batchID,
		CustomID:  line.CustomID,
		LineIndex: line.Index,
	}
	ctx, span := c.span(ctx, "batch.enqueue")
	defer span.End()
	return c.store.CommitBatchInput(ctx, batchID, raw, unit, c.opts.Window)
}

func (c *Controller) skipRemaining(ctx context.Context, batchID, code, status, message string) error {
	retries := 0
	for {
		raw, line, err := c.store.PeekBatchInput(ctx, batchID)
		if err != nil {
			return err
		}
		if line == nil {
			break
		}
		got, err := c.recordSkipped(ctx, batchID, raw, line, code, status, message)
		if err != nil {
			return err
		}
		switch got {
		case "ok":
			retries = 0
		case "retry":
			retries++
			if retries > 8 {
				return fmt.Errorf("batch %s input head kept changing", batchID)
			}
		case "empty":
			retries = 0
		default:
			return fmt.Errorf("batch %s terminal status %q", batchID, got)
		}
		if got == "empty" {
			break
		}
	}
	return c.publishCounts(ctx, batchID)
}

func (c *Controller) recordSkipped(ctx context.Context, batchID, raw string, line *model.InputLine, code, status, message string) (string, error) {
	uid, err := id.New("batch_req_")
	if err != nil {
		return "", err
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
	return c.store.CommitBatchTerminal(ctx, batchID, raw, unit, res, out, c.opts.ResultTTL)
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
	if !c.stillLeader(ctx) {
		return nil
	}
	ctx, span := c.span(ctx, "batch.finalize")
	defer span.End()
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
				InputTokens      int `json:"input_tokens"`
				OutputTokens     int `json:"output_tokens"`
				TotalTokens      int `json:"total_tokens"`
			} `json:"usage"`
		}
		if err := json.Unmarshal(ln.Response.Body, &body); err != nil {
			continue
		}
		// Chat Completions reports prompt/completion tokens. The Responses API
		// reports input/output tokens. Embeddings often sets only prompt and total.
		in := body.Usage.PromptTokens
		if in == 0 {
			in = body.Usage.InputTokens
		}
		out := body.Usage.CompletionTokens
		if out == 0 {
			out = body.Usage.OutputTokens
		}
		total := body.Usage.TotalTokens
		if total == 0 {
			total = in + out
		}
		if in == 0 && out == 0 && total == 0 {
			continue
		}
		found = true
		usage.InputTokens += in
		usage.OutputTokens += out
		usage.TotalTokens += total
	}
	if !found {
		return nil
	}
	return &usage
}

func (c *Controller) span(ctx context.Context, name string) (context.Context, *observe.Span) {
	if c.opts.Observer == nil {
		return ctx, nil
	}
	return c.opts.Observer.Start(ctx, name)
}

func hasCode(lines []model.OutputLine, code string) bool {
	for _, ln := range lines {
		if ln.Error != nil && ln.Error.Code == code {
			return true
		}
	}
	return false
}
