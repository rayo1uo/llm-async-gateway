package pipeline

import (
	"context"
	"errors"
	"testing"
)

type scriptedGate struct {
	budget  float64
	verdict Verdict
	err     error
	got     int
}

func (g *scriptedGate) Budget(context.Context) float64 { return g.budget }

func (g *scriptedGate) Apply(_ context.Context, _ *Request, releases *[]ReleaseFunc) (Verdict, error) {
	g.got++
	if g.err != nil || g.verdict != VerdictContinue {
		return g.verdict, g.err
	}
	*releases = append(*releases, func() { g.got += 10 })
	return VerdictContinue, nil
}

func TestApplyChainRollsBackOnRefuse(t *testing.T) {
	first := &scriptedGate{budget: 1, verdict: VerdictContinue}
	second := &scriptedGate{budget: 0, verdict: VerdictRefuse}
	var rels []ReleaseFunc
	got, err := ApplyChain(context.Background(), &Request{}, []Gate{first, second}, &rels)
	if err != nil {
		t.Fatal(err)
	}
	if got != VerdictRefuse {
		t.Fatalf("verdict=%s", got)
	}
	if len(rels) != 0 {
		t.Fatalf("releases leaked: %d", len(rels))
	}
	if first.got != 11 {
		t.Fatalf("first gate calls=%d, want apply + rollback release", first.got)
	}
	if second.got != 1 {
		t.Fatalf("second gate calls=%d", second.got)
	}
}

func TestApplyChainNilGateFailsClosed(t *testing.T) {
	ok := &scriptedGate{verdict: VerdictContinue}
	_, err := ApplyChain(context.Background(), &Request{}, []Gate{ok, nil}, nil)
	if err == nil {
		t.Fatal("nil gate should fail closed")
	}
	if ok.got != 11 {
		t.Fatalf("release after nil gate=%d", ok.got)
	}
}

func TestApplyChainError(t *testing.T) {
	boom := errors.New("budget source down")
	g := &scriptedGate{err: boom, verdict: VerdictRefuse}
	_, err := ApplyChain(context.Background(), &Request{}, []Gate{g}, nil)
	if !errors.Is(err, boom) {
		t.Fatalf("err=%v", err)
	}
}

func TestApplyChainEmptyContinues(t *testing.T) {
	got, err := ApplyChain(context.Background(), &Request{}, nil, nil)
	if err != nil || got != VerdictContinue {
		t.Fatalf("got=%s err=%v", got, err)
	}
}
