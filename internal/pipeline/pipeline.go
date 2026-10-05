// Package pipeline holds the first-party queue, gate, and flow types.
//
// The shapes follow the concepts in RFC 0001 §3.5 and §3.6. They are defined
// here so this repository does not import llm-d-async or any other llm-d module.
package pipeline

import "encoding/json"

// Tier is a dispatch lane. Clients cannot set it.
type Tier string

const (
	TierInteractive Tier = "interactive"
	TierAsync       Tier = "async"
	TierBatch       Tier = "batch"
)

// Valid reports whether t is one of the known lanes.
func (t Tier) Valid() bool {
	switch t {
	case TierInteractive, TierAsync, TierBatch:
		return true
	default:
		return false
	}
}

// Message is one inference. Small bodies live in Payload.
// Batch rows and oversized bodies leave Payload empty and put the location in Metadata.
type Message struct {
	ID       string            `json:"id"`
	Created  int64             `json:"created"`  // Unix seconds
	Deadline int64             `json:"deadline"` // Unix seconds, also the sorted-set score
	Tier     Tier              `json:"tier"`
	Tenant   string            `json:"tenant,omitempty"` // written by the server
	Endpoint string            `json:"endpoint"`
	Payload  json.RawMessage   `json:"payload,omitempty"`
	Headers  map[string]string `json:"headers,omitempty"` // server-applied
	Metadata map[string]string `json:"metadata,omitempty"`
}

// Request is the internal envelope after a message is queued.
// RequestToken is the fencing generation.
type Request struct {
	Message      Message           `json:"message"`
	RequestToken string            `json:"request_token"`
	Queue        string            `json:"queue"`
	ResultQueue  string            `json:"result_queue,omitempty"`
	RetryCount   int               `json:"retry_count,omitempty"`
	Labels       map[string]string `json:"labels,omitempty"` // reserved / overflow, Phase 4
}

// Result is the terminal outcome of one inference.
// The HTTP layer translates it into the public JSON.
type Result struct {
	ID           string `json:"id"`
	RequestToken string `json:"request_token"`
	StatusCode   int    `json:"status_code,omitempty"`
	Payload      []byte `json:"payload,omitempty"`
	ErrorCode    string `json:"error_code,omitempty"`
	ErrorMessage string `json:"error_message,omitempty"`
}

// Metadata keys written by producers. Dispatchers do not change them.
const (
	MetaJobID         = "job_id"
	MetaCustomID      = "custom_id"
	MetaRequestIndex  = "request_index"
	MetaPayloadBucket = "payload_bucket"
	MetaPayloadKey    = "payload_key"
	MetaPayloadOffset = "payload_offset"
	MetaPayloadLength = "payload_length"
	MetaTraceparent   = "traceparent"
)
