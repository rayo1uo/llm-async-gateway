// Package observe records Prometheus metrics and W3C traces for the gateway.
package observe

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"io"
	"log/slog"
	"strings"
	"sync"
	"time"
)

type spanContextKey struct{}

type spanParent struct {
	traceID string
	spanID  string
}

// Snapshot is one finished span. Tests use it to prove a single trace
// crosses from the HTTP handler into the upstream call.
type Snapshot struct {
	Name     string
	TraceID  string
	SpanID   string
	ParentID string
	Start    time.Time
	End      time.Time
}

// Span is one in-progress trace span.
type Span struct {
	Name     string
	TraceID  string
	SpanID   string
	ParentID string
	start    time.Time
	obs      *Observer
	once     sync.Once
}

// End records the span. It is safe to call more than once, and on a nil span.
func (s *Span) End() {
	if s == nil || s.obs == nil {
		return
	}
	s.once.Do(func() {
		snap := Snapshot{
			Name:     s.Name,
			TraceID:  s.TraceID,
			SpanID:   s.SpanID,
			ParentID: s.ParentID,
			Start:    s.start,
			End:      time.Now(),
		}
		s.obs.record(snap)
	})
}

// Observer collects spans and owns the process metrics registry.
type Observer struct {
	Metrics *Metrics
	log     *slog.Logger

	mu    sync.Mutex
	spans []Snapshot
}

// New builds an observer. reg may be nil when metrics are not scraped from this process.
func New(m *Metrics, logger *slog.Logger) *Observer {
	if logger == nil {
		logger = slog.Default()
	}
	return &Observer{Metrics: m, log: logger}
}

// Nop returns an observer that still propagates trace context but keeps no registry.
func Nop() *Observer {
	return New(nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

// Start opens a child span. The trace id is inherited from ctx when one is present.
func (o *Observer) Start(ctx context.Context, name string) (context.Context, *Span) {
	if ctx == nil {
		ctx = context.Background()
	}
	if o == nil {
		return ctx, nil
	}
	parent, _ := ctx.Value(spanContextKey{}).(spanParent)
	traceID := parent.traceID
	if traceID == "" {
		traceID = randomHex(16)
	}
	sp := &Span{
		Name:     name,
		TraceID:  traceID,
		SpanID:   randomHex(8),
		ParentID: parent.spanID,
		start:    time.Now(),
		obs:      o,
	}
	ctx = context.WithValue(ctx, spanContextKey{}, spanParent{traceID: sp.TraceID, spanID: sp.SpanID})
	return ctx, sp
}

func (o *Observer) record(s Snapshot) {
	o.mu.Lock()
	o.spans = append(o.spans, s)
	o.mu.Unlock()
	o.log.Info("trace",
		"span", s.Name,
		"trace_id", s.TraceID,
		"span_id", s.SpanID,
		"parent_span_id", s.ParentID,
	)
}

// Snapshots returns a copy of finished spans in completion order.
func (o *Observer) Snapshots() []Snapshot {
	if o == nil {
		return nil
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	out := make([]Snapshot, len(o.spans))
	copy(out, o.spans)
	return out
}

// Traceparent returns the W3C traceparent header for the current span.
func Traceparent(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	parent, ok := ctx.Value(spanContextKey{}).(spanParent)
	if !ok || parent.traceID == "" || parent.spanID == "" {
		return ""
	}
	return "00-" + parent.traceID + "-" + parent.spanID + "-01"
}

// WithTraceparent continues a trace from a W3C traceparent header.
// Malformed headers are ignored and ctx is returned unchanged.
func WithTraceparent(ctx context.Context, header string) context.Context {
	traceID, spanID, ok := parseTraceparent(header)
	if !ok {
		return ctx
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, spanContextKey{}, spanParent{traceID: traceID, spanID: spanID})
}

func parseTraceparent(header string) (traceID, spanID string, ok bool) {
	parts := strings.Split(header, "-")
	if len(parts) != 4 || parts[0] != "00" {
		return "", "", false
	}
	if len(parts[1]) != 32 || len(parts[2]) != 16 {
		return "", "", false
	}
	if _, err := hex.DecodeString(parts[1]); err != nil {
		return "", "", false
	}
	if _, err := hex.DecodeString(parts[2]); err != nil {
		return "", "", false
	}
	if parts[1] == "00000000000000000000000000000000" || parts[2] == "0000000000000000" {
		return "", "", false
	}
	return parts[1], parts[2], true
}

func randomHex(n int) string {
	buf := make([]byte, n)
	if _, err := rand.Read(buf); err != nil {
		// A clock fallback keeps traces connected even if the RNG fails.
		now := time.Now().UnixNano()
		for i := range buf {
			buf[i] = byte(now >> (uint(i%8) * 8))
		}
	}
	return hex.EncodeToString(buf)
}
