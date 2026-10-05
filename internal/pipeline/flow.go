package pipeline

import "context"

// Channel is one tier queue plus the gates that admit work from it.
type Channel struct {
	Queue string
	Tier  Tier
	Gates []Gate
}

// MergePolicy picks the next request from several channels.
// Priority, aging, and the minimum share are the Phase 1 policy.
// The pool name separates output by inference pool.
type MergePolicy interface {
	Next(ctx context.Context, channels []Channel) (pool string, req *Request, ok bool)
}

// Flow wires channels, the merge policy, and a result stream.
// Redis, MySQL, object storage, and HTTP are injected by cmd.
type Flow interface {
	Channels() []Channel
	Merge() MergePolicy
	Results() <-chan Result
}
