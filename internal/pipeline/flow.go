package pipeline

import (
	"context"
	"time"
)

// Characteristics describes what a Flow implementation already does for the
// worker loop. Workers still fill Backoff on a retry; they do not sleep it
// when HasExternalBackoff is set.
type Characteristics struct {
	// HasExternalBackoff is true when the flow parks retries until Backoff
	// elapses. The worker supplies the duration and keeps running.
	HasExternalBackoff bool
	// SupportsMessageLatency is true when the broker stamps an ingestion
	// time the worker can turn into a latency histogram.
	SupportsMessageLatency bool
}

// RequestChannel is one tier queue and the gate that admits work from it.
// Channel carries requests the flow has already claimed. The merge policy
// reads it; the flow is the only sender and closes it from StopConsuming.
type RequestChannel struct {
	Queue        string
	Tier         Tier
	Gate         Gate
	WorkerPoolID string
	Channel      chan DispatchMessage
}

// DispatchMessage is one claimed request handed to a worker.
// The worker calls every release when the attempt finishes, including when
// it asks the flow to retry.
type DispatchMessage struct {
	Request  *Request
	Owner    string
	Releases []ReleaseFunc
}

// PoolDispatch is the merge policy output: one channel per worker pool.
// Backpressure on one pool does not block another pool's channel.
type PoolDispatch struct {
	Channels map[string]chan DispatchMessage
}

// RequestMergePolicy fans request channels into per-pool channels.
// The returned channels stay open until the flow closes its sources.
type RequestMergePolicy interface {
	MergeRequestChannels(channels []RequestChannel) PoolDispatch
}

// RetryMessage asks the flow to park a claimed request and try again later.
// The flow's retry worker outlives the inference workers: Shutdown runs only
// after those workers have finished sending.
type RetryMessage struct {
	Request *Request
	Owner   string
	Backoff time.Duration
}

// Flow is the dispatcher orchestrator.
// Start begins queue consumption and the retry and result workers.
// StopConsuming stops new claims, waits for the consume loop, and closes
// request channels so the merge policy can finish. Shutdown stops the retry
// and result workers and must run after inference workers have drained.
// Redis, MySQL, object storage, and HTTP stay outside this package.
type Flow interface {
	Characteristics() Characteristics
	Start(ctx context.Context)
	StopConsuming()
	Shutdown()
	RequestChannels() []RequestChannel
	RetryChannel() chan RetryMessage
	ResultChannel() chan Result
}
