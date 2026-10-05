package dispatch

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/rayo1uo/llm-async-gateway/internal/model"
	"github.com/rayo1uo/llm-async-gateway/internal/observe"
	"github.com/rayo1uo/llm-async-gateway/internal/retry"
)

const maxUpstreamBody = 8 << 20

// HTTPClient forwards a unit to an OpenAI-compatible base URL.
type HTTPClient struct {
	base string
	http *http.Client
	now  func() time.Time
}

// NewHTTPClient builds a client. The request context carries the attempt timeout.
func NewHTTPClient(baseURL string) *HTTPClient {
	return &HTTPClient{
		base: strings.TrimRight(baseURL, "/"),
		http: &http.Client{Transport: &http.Transport{
			MaxIdleConns:        128,
			MaxIdleConnsPerHost: 128,
			IdleConnTimeout:     90 * time.Second,
		}},
		now: time.Now,
	}
}

// Do posts the unit body to baseURL + endpoint.
func (c *HTTPClient) Do(ctx context.Context, u *model.Unit) (*UpstreamResponse, error) {
	endpoint := u.Endpoint
	if !strings.HasPrefix(endpoint, "/") {
		endpoint = "/" + endpoint
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base+endpoint, bytes.NewReader(u.Body))
	if err != nil {
		return nil, fmt.Errorf("build upstream request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Request-Id", u.ID)
	req.Header.Set("X-Async-Tier", string(u.Tier))
	if tp := observe.Traceparent(ctx); tp != "" {
		req.Header.Set("traceparent", tp)
	} else if u.TraceParent != "" {
		req.Header.Set("traceparent", u.TraceParent)
	}
	if u.BatchID != "" {
		req.Header.Set("X-Batch-Id", u.BatchID)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("upstream: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxUpstreamBody))
	if err != nil {
		return nil, fmt.Errorf("read upstream body: %w", err)
	}
	return &UpstreamResponse{
		StatusCode: resp.StatusCode,
		Body:       body,
		RetryAfter: retry.ParseRetryAfter(resp.Header.Get("Retry-After"), c.now()),
		RequestID:  resp.Header.Get("X-Request-Id"),
	}, nil
}

func outputLine(u *model.Unit, res *model.Result) *model.OutputLine {
	line := &model.OutputLine{ID: u.ID, CustomID: u.CustomID}
	if res.ErrorCode == model.ErrBatchExpired || res.ErrorCode == model.ErrBatchCancelled {
		line.Error = &model.OutputError{Code: res.ErrorCode, Message: res.ErrorMessage}
		return line
	}
	body := res.Body
	if len(body) == 0 {
		body = json.RawMessage(`null`)
	}
	if res.Status == model.StatusCompleted {
		line.Response = &model.OutputResponse{
			StatusCode: res.StatusCode,
			RequestID:  res.RequestID,
			Body:       body,
		}
		return line
	}
	if res.StatusCode > 0 {
		line.Response = &model.OutputResponse{
			StatusCode: res.StatusCode,
			RequestID:  res.RequestID,
			Body:       body,
		}
	}
	code := res.ErrorCode
	if code == "" {
		code = model.ErrUpstream
	}
	line.Error = &model.OutputError{Code: code, Message: res.ErrorMessage}
	return line
}

func asJSON(b []byte) json.RawMessage {
	if len(bytes.TrimSpace(b)) == 0 {
		return nil
	}
	if json.Valid(b) {
		return json.RawMessage(append([]byte(nil), b...))
	}
	enc, err := json.Marshal(string(b))
	if err != nil {
		return json.RawMessage(`""`)
	}
	return enc
}

func usageTokens(body []byte) (input, output int) {
	var parsed struct {
		Usage struct {
			PromptTokens     int `json:"prompt_tokens"`
			CompletionTokens int `json:"completion_tokens"`
			InputTokens      int `json:"input_tokens"`
			OutputTokens     int `json:"output_tokens"`
		} `json:"usage"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		return 0, 0
	}
	input = parsed.Usage.PromptTokens
	if input == 0 {
		input = parsed.Usage.InputTokens
	}
	output = parsed.Usage.CompletionTokens
	if output == 0 {
		output = parsed.Usage.OutputTokens
	}
	return input, output
}

func trim(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
