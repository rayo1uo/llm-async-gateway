package observe

import (
	"context"
	"testing"
)

func TestTraceparentRoundTrip(t *testing.T) {
	obs := Nop()
	ctx, span := obs.Start(context.Background(), "http.serve")
	defer span.End()
	header := Traceparent(ctx)
	if header == "" {
		t.Fatal("missing traceparent")
	}
	childCtx := WithTraceparent(context.Background(), header)
	childCtx, child := obs.Start(childCtx, "dispatch.upstream")
	child.End()
	span.End()

	snaps := obs.Snapshots()
	if len(snaps) != 2 {
		t.Fatalf("spans=%d", len(snaps))
	}
	var httpSpan, upSpan Snapshot
	for _, s := range snaps {
		switch s.Name {
		case "http.serve":
			httpSpan = s
		case "dispatch.upstream":
			upSpan = s
		}
	}
	if httpSpan.TraceID == "" || upSpan.TraceID != httpSpan.TraceID {
		t.Fatalf("trace ids http=%s up=%s", httpSpan.TraceID, upSpan.TraceID)
	}
	if upSpan.ParentID != httpSpan.SpanID {
		t.Fatalf("parent=%s want %s", upSpan.ParentID, httpSpan.SpanID)
	}
	if Traceparent(childCtx) == "" {
		t.Fatal("child context lost traceparent")
	}
}

func TestMalformedTraceparentIgnored(t *testing.T) {
	ctx := WithTraceparent(context.Background(), "not-a-header")
	if Traceparent(ctx) != "" {
		t.Fatal("malformed header should not create a span context")
	}
}
