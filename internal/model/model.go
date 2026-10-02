// Package model holds the JSON documents shared by the API, queue, and batch controller.
package model

import "encoding/json"

const (
	ObjectFile    = "file"
	ObjectBatch   = "batch"
	ObjectList    = "list"
	ObjectRequest = "request"

	PurposeBatch       = "batch"
	PurposeBatchOutput = "batch_output"
	PurposeBatchError  = "batch_error"

	StatusValidating = "validating"
	StatusInProgress = "in_progress"
	StatusFinalizing = "finalizing"
	StatusCompleted  = "completed"
	StatusFailed     = "failed"
	StatusExpired    = "expired"
	StatusCancelling = "cancelling"
	StatusCancelled  = "cancelled"
	StatusQueued     = "queued"

	CountCompleted = "completed"
	CountFailed    = "failed"

	// Error codes written into batch error files and nearline error objects.
	ErrBatchExpired     = "batch_expired"
	ErrBatchCancelled   = "batch_cancelled"
	ErrDeadlineExceeded = "deadline_exceeded"
	ErrCancelled        = "cancelled"
	ErrUpstream         = "upstream_error"
	ErrMaxAttempts      = "max_attempts_exceeded"
	ErrInvalidRequest   = "invalid_request"
)

// Tier is a strict priority lane. Nearline is dispatched ahead of batch,
// except for the starvation guards in the scheduler.
type Tier string

const (
	TierNearline Tier = "nearline"
	TierBatch    Tier = "batch"
)

// Valid reports whether the tier is one of the known lanes.
func (t Tier) Valid() bool {
	return t == TierNearline || t == TierBatch
}

// File is an OpenAI Files API object. Bytes live in Redis for this demo.
type File struct {
	ID        string `json:"id"`
	Object    string `json:"object"`
	Bytes     int    `json:"bytes"`
	CreatedAt int64  `json:"created_at"`
	Filename  string `json:"filename"`
	Purpose   string `json:"purpose"`
}

// DeletedFile is the DELETE /v1/files response.
type DeletedFile struct {
	ID      string `json:"id"`
	Object  string `json:"object"`
	Deleted bool   `json:"deleted"`
}

// RequestCounts matches the OpenAI batch progress block.
type RequestCounts struct {
	Total     int `json:"total"`
	Completed int `json:"completed"`
	Failed    int `json:"failed"`
}

// BatchError is one validation or input error attached to a batch.
type BatchError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Param   string `json:"param,omitempty"`
	Line    *int   `json:"line,omitempty"`
}

// BatchErrors is the OpenAI list wrapper on batch.errors.
type BatchErrors struct {
	Object string       `json:"object"`
	Data   []BatchError `json:"data"`
}

// Usage aggregates token counts from successful upstream bodies.
type Usage struct {
	InputTokens  int `json:"input_tokens"`
	OutputTokens int `json:"output_tokens"`
	TotalTokens  int `json:"total_tokens"`
}

// Batch is an OpenAI Batch object.
type Batch struct {
	ID               string            `json:"id"`
	Object           string            `json:"object"`
	Endpoint         string            `json:"endpoint"`
	Errors           *BatchErrors      `json:"errors"`
	InputFileID      string            `json:"input_file_id"`
	CompletionWindow string            `json:"completion_window"`
	Status           string            `json:"status"`
	OutputFileID     *string           `json:"output_file_id"`
	ErrorFileID      *string           `json:"error_file_id"`
	CreatedAt        int64             `json:"created_at"`
	InProgressAt     *int64            `json:"in_progress_at"`
	ExpiresAt        *int64            `json:"expires_at"`
	FinalizingAt     *int64            `json:"finalizing_at"`
	CompletedAt      *int64            `json:"completed_at"`
	FailedAt         *int64            `json:"failed_at"`
	ExpiredAt        *int64            `json:"expired_at"`
	CancellingAt     *int64            `json:"cancelling_at"`
	CancelledAt      *int64            `json:"cancelled_at"`
	RequestCounts    RequestCounts     `json:"request_counts"`
	Metadata         map[string]string `json:"metadata"`
	Usage            *Usage            `json:"usage,omitempty"`
}

// TerminalBatch reports whether the batch status will not change again.
func TerminalBatch(status string) bool {
	switch status {
	case StatusCompleted, StatusFailed, StatusExpired, StatusCancelled:
		return true
	default:
		return false
	}
}

// InputLine is one JSONL request inside a batch input file.
type InputLine struct {
	CustomID string          `json:"custom_id"`
	Method   string          `json:"method"`
	URL      string          `json:"url"`
	Body     json.RawMessage `json:"body"`
	Index    int             `json:"index"`
}

// Unit is one queued inference request. Deadline is Unix milliseconds and is the
// sorted-set score (earliest deadline first within a tier).
type Unit struct {
	ID        string          `json:"id"`
	Tier      Tier            `json:"tier"`
	Endpoint  string          `json:"endpoint"`
	Body      json.RawMessage `json:"body"`
	Deadline  int64           `json:"deadline"`
	Created   int64           `json:"created"`
	Attempts  int             `json:"attempts"`
	BatchID   string          `json:"batch_id,omitempty"`
	CustomID  string          `json:"custom_id,omitempty"`
	LineIndex int             `json:"line_index,omitempty"`
}

// Result is the terminal outcome of a unit.
type Result struct {
	ID           string          `json:"id"`
	Status       string          `json:"status"`
	StatusCode   int             `json:"status_code,omitempty"`
	Body         json.RawMessage `json:"body,omitempty"`
	ErrorCode    string          `json:"error_code,omitempty"`
	ErrorMessage string          `json:"error_message,omitempty"`
	RequestID    string          `json:"request_id,omitempty"`
	Attempts     int             `json:"attempts"`
	FinishedAt   int64           `json:"finished_at"`
}

// OutputResponse is the successful or upstream-error response embedded in a JSONL line.
type OutputResponse struct {
	StatusCode int             `json:"status_code"`
	RequestID  string          `json:"request_id"`
	Body       json.RawMessage `json:"body"`
}

// OutputError is the error object on a batch output line.
type OutputError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// OutputLine is one JSONL row in a batch output or error file.
type OutputLine struct {
	ID       string          `json:"id"`
	CustomID string          `json:"custom_id"`
	Response *OutputResponse `json:"response"`
	Error    *OutputError    `json:"error"`
}

// Nearline is the durable record for a single async request.
type Nearline struct {
	ID          string            `json:"id"`
	Object      string            `json:"object"`
	Status      string            `json:"status"`
	Endpoint    string            `json:"endpoint"`
	CreatedAt   int64             `json:"created_at"`
	Deadline    int64             `json:"deadline"`
	DeadlineMS  int64             `json:"deadline_ms"`
	Metadata    map[string]string `json:"metadata"`
	CompletedAt *int64            `json:"completed_at,omitempty"`
}

// TerminalNearline reports whether a nearline status is final.
func TerminalNearline(status string) bool {
	switch status {
	case StatusCompleted, StatusFailed, StatusExpired, StatusCancelled:
		return true
	default:
		return false
	}
}

// AllowedEndpoint reports whether path is an inference route this gateway will forward.
func AllowedEndpoint(path string) bool {
	switch path {
	case "/v1/chat/completions", "/v1/completions", "/v1/embeddings", "/v1/responses":
		return true
	default:
		return false
	}
}
