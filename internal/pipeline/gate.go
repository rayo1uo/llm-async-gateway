package pipeline

import (
	"context"
	"fmt"
)

// Verdict is the admission decision for one request.
type Verdict int

const (
	// VerdictContinue admits the request.
	VerdictContinue Verdict = iota
	// VerdictRefuse leaves the message on the queue and does not occupy a worker.
	VerdictRefuse
	// VerdictWait parks the worker in memory. Phase 1 does not use it.
	VerdictWait
	// VerdictDrop writes a terminal outcome and does not retry.
	VerdictDrop
)

// String returns a stable label for metrics and logs.
func (v Verdict) String() string {
	switch v {
	case VerdictContinue:
		return "continue"
	case VerdictRefuse:
		return "refuse"
	case VerdictWait:
		return "wait"
	case VerdictDrop:
		return "drop"
	default:
		return "unknown"
	}
}

// ReleaseFunc returns one admission that Apply granted.
type ReleaseFunc func()

type admissionKey struct{}

// WithAdmission attaches the dispatcher-generated holder id used by a shared
// in-flight gate. The id is not a client label.
func WithAdmission(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, admissionKey{}, id)
}

// AdmissionID returns the holder id stored by WithAdmission.
func AdmissionID(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	id, _ := ctx.Value(admissionKey{}).(string)
	return id
}

// Gate reports remaining capacity and admits a single request.
// Budget returns a value in [0, 1]. 0 means no room, 1 means idle.
type Gate interface {
	Budget(ctx context.Context) float64
	Apply(ctx context.Context, req *Request, releases *[]ReleaseFunc) (Verdict, error)
}

// ApplyChain runs gates in order. The first verdict that is not Continue stops
// the chain and rolls back every release acquired in this call.
// A nil gate fails closed: it refuses and returns an error.
func ApplyChain(ctx context.Context, req *Request, gates []Gate, releases *[]ReleaseFunc) (Verdict, error) {
	if releases == nil {
		tmp := []ReleaseFunc{}
		releases = &tmp
	}
	start := len(*releases)
	for i, g := range gates {
		if g == nil {
			rollback(releases, start)
			return VerdictRefuse, fmt.Errorf("gate %d is nil", i)
		}
		verdict, err := g.Apply(ctx, req, releases)
		if err != nil {
			rollback(releases, start)
			return VerdictRefuse, err
		}
		if verdict != VerdictContinue {
			rollback(releases, start)
			return verdict, nil
		}
	}
	return VerdictContinue, nil
}

// ReleaseAll runs every release, last granted first.
func ReleaseAll(releases []ReleaseFunc) {
	for i := len(releases) - 1; i >= 0; i-- {
		if releases[i] != nil {
			releases[i]()
		}
	}
}

func rollback(releases *[]ReleaseFunc, start int) {
	ReleaseAll((*releases)[start:])
	*releases = (*releases)[:start]
}
