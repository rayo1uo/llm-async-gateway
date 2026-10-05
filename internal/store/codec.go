package store

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/rayo1uo/llm-async-gateway/internal/model"
	"github.com/rayo1uo/llm-async-gateway/internal/pipeline"
)

// PipelineRequest is the queue document for u.
func PipelineRequest(u *model.Unit, queue string) pipeline.Request {
	md := map[string]string{}
	if u.BatchID != "" {
		md[pipeline.MetaJobID] = u.BatchID
		if u.CustomID != "" {
			md[pipeline.MetaCustomID] = u.CustomID
		}
		md[pipeline.MetaRequestIndex] = strconv.Itoa(u.LineIndex)
	}
	if u.TraceParent != "" {
		md[pipeline.MetaTraceparent] = u.TraceParent
	}
	return pipeline.Request{
		Message: pipeline.Message{
			ID:       u.ID,
			Created:  u.Created,
			Deadline: u.Deadline,
			Tier:     pipeline.Tier(u.Tier),
			Endpoint: u.Endpoint,
			Payload:  append(json.RawMessage(nil), u.Body...),
			Metadata: md,
		},
		RequestToken: u.Token,
		Queue:        queue,
		RetryCount:   u.Attempts,
	}
}

func encodeRequest(u *model.Unit, queue string) ([]byte, error) {
	req := PipelineRequest(u, queue)
	raw, err := json.Marshal(&req)
	if err != nil {
		return nil, fmt.Errorf("encode request: %w", err)
	}
	return raw, nil
}

func decodeRequest(raw []byte) (*model.Unit, error) {
	var req pipeline.Request
	if err := json.Unmarshal(raw, &req); err != nil {
		return nil, fmt.Errorf("decode request: %w", err)
	}
	u := &model.Unit{
		ID:          req.Message.ID,
		Tier:        model.Tier(req.Message.Tier),
		Endpoint:    req.Message.Endpoint,
		Body:        req.Message.Payload,
		Deadline:    req.Message.Deadline,
		Created:     req.Message.Created,
		Attempts:    req.RetryCount,
		Token:       req.RequestToken,
		TraceParent: req.Message.Metadata[pipeline.MetaTraceparent],
	}
	if req.Message.Metadata != nil {
		u.BatchID = req.Message.Metadata[pipeline.MetaJobID]
		u.CustomID = req.Message.Metadata[pipeline.MetaCustomID]
		if idx := req.Message.Metadata[pipeline.MetaRequestIndex]; idx != "" {
			n, err := strconv.Atoi(idx)
			if err != nil {
				return nil, fmt.Errorf("decode request_index: %w", err)
			}
			u.LineIndex = n
		}
	}
	return u, nil
}

func (s *Store) ensureToken(ctx context.Context, u *model.Unit) error {
	if u.Token != "" {
		return nil
	}
	n, err := s.rdb.Incr(ctx, s.tokenSeqKey()).Result()
	if err != nil {
		return fmt.Errorf("request token: %w", err)
	}
	u.Token = strconv.FormatInt(n, 10)
	return nil
}
