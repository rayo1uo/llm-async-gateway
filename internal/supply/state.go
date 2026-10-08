package supply

import (
	"fmt"
	"math"
	"sort"
	"time"
)

const (
	defaultEpsilon = time.Millisecond
	tokenTol       = 1e-6
)

type world struct {
	req     Request
	now     time.Time
	eps     float64
	horizon float64
	budget  map[string]int
	models  []*mstate
	evict   bool
}

type mstate struct {
	in       Model
	id       string
	weight   float64
	avgTok   float64
	demand   float64
	drain    float64
	arrival  float64
	unsat    float64
	minCold  float64
	feasible []gprof
	byType   map[string]gprof
	ready    map[string]int
	warm     map[string][]warmCohort
	alloc    map[string]int
	notes    []string
}

type gprof struct {
	typ  string
	u    int
	mu   float64
	cold float64
}

type warmCohort struct {
	n   int
	eta float64
}

type timedRep struct {
	eta float64
	mu  float64
}

type tokenBucket struct {
	name   string
	t      float64
	tokens float64
}

func newWorld(req Request) *world {
	eps := req.Epsilon
	if eps <= 0 {
		eps = defaultEpsilon
	}
	w := &world{
		req:     req,
		now:     req.Now,
		eps:     eps.Seconds(),
		horizon: req.Horizon.Seconds(),
		budget:  map[string]int{},
	}
	if w.eps <= 0 {
		w.eps = defaultEpsilon.Seconds()
	}
	for _, b := range req.Budgets {
		w.budget[b.Type] = b.GPUs
	}
	for _, m := range req.Models {
		w.models = append(w.models, newModel(m))
	}
	return w
}

func newModel(m Model) *mstate {
	st := &mstate{
		in:     m,
		id:     m.ID,
		weight: m.Weight,
		avgTok: m.AvgInputTokens + m.AvgOutputTokens,
		byType: map[string]gprof{},
		ready:  map[string]int{},
		warm:   map[string][]warmCohort{},
		alloc:  map[string]int{},
	}
	if st.weight <= 0 {
		st.weight = 1
	}
	for _, p := range m.Profiles {
		g := gprof{typ: p.Type, u: p.GPUsPerReplica, cold: p.ColdStart.Seconds()}
		switch {
		case p.MuTokensPerSec > 0:
			g.mu = p.MuTokensPerSec
		case p.MuRequestsPerSec > 0 && st.avgTok > 0:
			g.mu = p.MuRequestsPerSec * st.avgTok
		}
		st.byType[p.Type] = g
		if p.Infeasible || g.mu <= 0 {
			continue
		}
		st.feasible = append(st.feasible, g)
	}
	sort.Slice(st.feasible, func(i, j int) bool { return st.feasible[i].typ < st.feasible[j].typ })
	if len(st.feasible) > 0 {
		st.minCold = st.feasible[0].cold
		for _, g := range st.feasible[1:] {
			if g.cold < st.minCold {
				st.minCold = g.cold
			}
		}
	}
	for _, s := range m.Supply {
		if s.Ready > 0 {
			st.ready[s.Type] += s.Ready
		}
		for _, c := range s.Warming {
			if c.Count <= 0 {
				continue
			}
			st.warm[s.Type] = append(st.warm[s.Type], warmCohort{n: c.Count, eta: c.ETA.Seconds()})
		}
		sort.Slice(st.warm[s.Type], func(i, j int) bool { return st.warm[s.Type][i].eta < st.warm[s.Type][j].eta })
	}
	return st
}

func (w *world) computeDemand() error {
	for _, m := range w.models {
		if err := m.computeDemand(w.eps); err != nil {
			return err
		}
	}
	return nil
}

func (m *mstate) computeDemand(eps float64) error {
	var drain float64
	var unsat float64
	for _, b := range m.in.Backlog {
		tokens, err := m.bucketTokens(b)
		if err != nil {
			return err
		}
		if tokens <= 0 {
			continue
		}
		// A bucket whose deadline is not strictly after the fastest cold start
		// cannot be saved by scale-up. Leave it out of demand so it cannot
		// divide by a tiny epsilon and water-fill the cluster.
		if len(m.feasible) == 0 || b.Remaining.Seconds() <= m.minCold {
			unsat += tokens
			continue
		}
		window := b.Remaining.Seconds() - m.minCold
		if window < eps {
			window = eps
		}
		drain += tokens / window
	}
	arrival, err := m.arrivalTokens()
	if err != nil {
		return err
	}
	smoothed := arrival
	if m.in.EWMAAlpha > 0 {
		smoothed = m.in.EWMAAlpha*arrival + (1-m.in.EWMAAlpha)*m.in.ArrivalEWMA
	}
	engine, err := m.engineTokens()
	if err != nil {
		return err
	}
	d := smoothed
	if drain > d {
		d = drain
	}
	if engine > d {
		d = engine
	}
	d = m.clamp(d)
	m.demand = d
	m.drain = drain
	m.arrival = smoothed
	m.unsat = unsat
	if unsat > 0 {
		m.note("unsatisfiable_backlog")
	}
	return nil
}

func (m *mstate) bucketTokens(b Bucket) (float64, error) {
	if b.Tokens > 0 {
		return b.Tokens, nil
	}
	if b.Requests <= 0 {
		return 0, nil
	}
	if m.avgTok <= 0 {
		return 0, errf("model %s bucket %s counts requests but has no average tokens", m.id, bucketName(b))
	}
	return b.Requests * m.avgTok, nil
}

func (m *mstate) arrivalTokens() (float64, error) {
	if m.in.ArrivalTokensPerSec > 0 {
		return m.in.ArrivalTokensPerSec, nil
	}
	if m.in.ArrivalRequestsPerSec <= 0 {
		return 0, nil
	}
	if m.avgTok <= 0 {
		return 0, errf("model %s arrival is in requests but has no average tokens", m.id)
	}
	return m.in.ArrivalRequestsPerSec * m.avgTok, nil
}

func (m *mstate) engineTokens() (float64, error) {
	if m.in.EngineTokensPerSec > 0 {
		return m.in.EngineTokensPerSec, nil
	}
	if m.in.EngineRequestsPerSec <= 0 {
		return 0, nil
	}
	if m.avgTok <= 0 {
		return 0, errf("model %s engine signal is in requests but has no average tokens", m.id)
	}
	return m.in.EngineRequestsPerSec * m.avgTok, nil
}

// clamp limits demand to the tokens/s SoftMaxGPUs can serve on the fastest
// feasible GPU type. Reservation and triage then cannot be dragged by one
// bad signal.
func (m *mstate) clamp(d float64) float64 {
	if m.in.SoftMaxGPUs < 0 {
		return d
	}
	capTPS := 0.0
	if m.in.SoftMaxGPUs > 0 && len(m.feasible) > 0 {
		best := m.feasible[0]
		for _, g := range m.feasible[1:] {
			if faster(g, best) {
				best = g
			}
		}
		n := m.in.SoftMaxGPUs / best.u
		capTPS = float64(n) * best.mu
	}
	if d > capTPS {
		m.note("clamped_soft_max")
		return capTPS
	}
	return d
}

func faster(a, b gprof) bool {
	if a.mu != b.mu {
		return a.mu > b.mu
	}
	au := a.mu / float64(a.u)
	bu := b.mu / float64(b.u)
	if au != bu {
		return au > bu
	}
	return a.typ < b.typ
}

func (m *mstate) satisfiable() []tokenBucket {
	var out []tokenBucket
	for _, b := range m.in.Backlog {
		tokens, err := m.bucketTokens(b)
		if err != nil || tokens <= 0 {
			continue
		}
		if len(m.feasible) == 0 || b.Remaining.Seconds() <= m.minCold {
			continue
		}
		out = append(out, tokenBucket{name: bucketName(b), t: b.Remaining.Seconds(), tokens: tokens})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].t != out[j].t {
			return out[i].t < out[j].t
		}
		return out[i].name < out[j].name
	})
	return out
}

func (m *mstate) hasSatisfiable() bool {
	return len(m.satisfiable()) > 0
}

func (m *mstate) tightest() float64 {
	bs := m.satisfiable()
	if len(bs) == 0 {
		return math.Inf(1)
	}
	return bs[0].t
}

func (m *mstate) deadlinesMet() bool {
	reps := m.timedReplicas()
	var cum float64
	for _, b := range m.satisfiable() {
		cum += b.tokens
		if !enough(produced(reps, b.t), cum) {
			return false
		}
	}
	return true
}

func (m *mstate) earliestUnmet() (float64, bool) {
	reps := m.timedReplicas()
	var cum float64
	for _, b := range m.satisfiable() {
		cum += b.tokens
		if !enough(produced(reps, b.t), cum) {
			return b.t, true
		}
	}
	return 0, false
}

func produced(reps []timedRep, until float64) float64 {
	var sum float64
	for _, r := range reps {
		if until > r.eta {
			sum += r.mu * (until - r.eta)
		}
	}
	return sum
}

func enough(have, need float64) bool {
	if have >= need {
		return true
	}
	scale := math.Abs(need)
	if scale < 1 {
		scale = 1
	}
	return need-have <= tokenTol*scale
}

func (m *mstate) timedReplicas() []timedRep {
	types := allocTypes(m.alloc)
	var out []timedRep
	for _, typ := range types {
		g, ok := m.byType[typ]
		if !ok || g.mu <= 0 {
			continue
		}
		n := m.alloc[typ]
		rdy := n
		if rdy > m.ready[typ] {
			rdy = m.ready[typ]
		}
		for i := 0; i < rdy; i++ {
			out = append(out, timedRep{mu: g.mu})
		}
		left := n - rdy
		for _, c := range m.warm[typ] {
			take := left
			if take > c.n {
				take = c.n
			}
			for i := 0; i < take; i++ {
				out = append(out, timedRep{eta: c.eta, mu: g.mu})
			}
			left -= take
		}
		for i := 0; i < left; i++ {
			out = append(out, timedRep{eta: g.cold, mu: g.mu})
		}
	}
	return out
}

func (m *mstate) readyTPS() float64 {
	var sum float64
	for _, r := range m.timedReplicas() {
		if r.eta <= tokenTol {
			sum += r.mu
		}
	}
	return sum
}

func (m *mstate) warmingTPS() float64 {
	var sum float64
	for _, r := range m.timedReplicas() {
		if r.eta > tokenTol {
			sum += r.mu
		}
	}
	return sum
}

func (m *mstate) horizon(global float64) float64 {
	if global > 0 {
		return global
	}
	h := 0.0
	for _, g := range m.feasible {
		if g.cold > h {
			h = g.cold
		}
	}
	for _, cs := range m.warm {
		for _, c := range cs {
			if c.eta > h {
				h = c.eta
			}
		}
	}
	return h
}

func (m *mstate) rateAt(h float64) float64 {
	var sum float64
	for _, r := range m.timedReplicas() {
		if r.eta <= h+tokenTol {
			sum += r.mu
		}
	}
	return sum
}

func (m *mstate) phi(globalH float64) float64 {
	if m.demand <= tokenTol {
		return 0
	}
	return m.rateAt(m.horizon(globalH)) / m.demand
}

// shrinkScore treats a model with no demand as infinitely over-served so
// idle extras are removed before models that still need tokens.
func (m *mstate) shrinkScore(globalH float64) float64 {
	if m.demand <= tokenTol {
		return math.Inf(1)
	}
	return m.rateAt(m.horizon(globalH)) / m.demand
}

func (m *mstate) gpusUsed() int {
	sum := 0
	for typ, n := range m.alloc {
		if n > 0 {
			sum += n * m.uOf(typ)
		}
	}
	return sum
}

func (m *mstate) uOf(typ string) int {
	g, ok := m.byType[typ]
	if !ok {
		return 0
	}
	return g.u
}

func (m *mstate) spareReady(typ string) int {
	n := m.ready[typ] - m.alloc[typ]
	if n < 0 {
		return 0
	}
	return n
}

func (m *mstate) nextSpareWarming(typ string) (float64, bool) {
	n := m.alloc[typ]
	usedWarm := 0
	if n > m.ready[typ] {
		usedWarm = n - m.ready[typ]
	}
	seen := 0
	for _, c := range m.warm[typ] {
		if seen+c.n > usedWarm {
			return c.eta, true
		}
		seen += c.n
	}
	return 0, false
}

func (m *mstate) marginalTokens(g gprof, until float64) float64 {
	if until <= 0 || g.mu <= 0 {
		return 0
	}
	if m.spareReady(g.typ) > 0 {
		return g.mu * until
	}
	if eta, ok := m.nextSpareWarming(g.typ); ok && eta < until {
		return g.mu * (until - eta)
	}
	if g.cold < until {
		return g.mu * (until - g.cold)
	}
	return 0
}

func (m *mstate) currentMap() map[string]int {
	out := map[string]int{}
	for typ, n := range m.ready {
		if n > 0 && m.uOf(typ) > 0 {
			out[typ] += n
		}
	}
	for typ, cs := range m.warm {
		if m.uOf(typ) <= 0 {
			continue
		}
		for _, c := range cs {
			if c.n > 0 {
				out[typ] += c.n
			}
		}
	}
	return out
}

func (m *mstate) note(s string) {
	for _, n := range m.notes {
		if n == s {
			return
		}
	}
	m.notes = append(m.notes, s)
}

func (m *mstate) inCooldown(now time.Time) bool {
	if m.in.Cooldown <= 0 || m.in.LastPlanChange.IsZero() || now.IsZero() {
		return false
	}
	return now.Sub(m.in.LastPlanChange) < m.in.Cooldown
}

func (w *world) budgetOf(typ string) int {
	return w.budget[typ]
}

func (w *world) usedByType() map[string]int {
	used := map[string]int{}
	for _, m := range w.models {
		for typ, n := range m.alloc {
			if n > 0 {
				used[typ] += n * m.uOf(typ)
			}
		}
	}
	return used
}

func (w *world) remaining(typ string) int {
	return w.budgetOf(typ) - w.usedByType()[typ]
}

func (w *world) overTypes() []string {
	used := w.usedByType()
	var over []string
	for typ, n := range used {
		if n > w.budgetOf(typ) {
			over = append(over, typ)
		}
	}
	sort.Strings(over)
	return over
}

func (w *world) overBudget() bool {
	return len(w.overTypes()) > 0
}

func (w *world) currentOverBudget() bool {
	used := map[string]int{}
	for _, m := range w.models {
		for typ, n := range m.currentMap() {
			used[typ] += n * m.uOf(typ)
		}
	}
	for typ, n := range used {
		if n > w.budgetOf(typ) {
			return true
		}
	}
	return false
}

func (w *world) totalBudget() int {
	sum := 0
	for _, n := range w.budget {
		sum += n
	}
	return sum
}

func (w *world) evictionRequested() bool {
	if w.req.Eviction {
		return true
	}
	if w.req.PreviousBudgetGPUs > 0 && w.totalBudget() < w.req.PreviousBudgetGPUs {
		return true
	}
	return false
}

func (w *world) canFit(m *mstate, g gprof) bool {
	if g.u <= 0 || g.mu <= 0 {
		return false
	}
	if w.remaining(g.typ) < g.u {
		return false
	}
	if m.in.SoftMaxGPUs >= 0 && m.gpusUsed()+g.u > m.in.SoftMaxGPUs {
		return false
	}
	return true
}

func (w *world) tiers() []int {
	seen := map[int]bool{}
	var tiers []int
	for _, m := range w.models {
		if !seen[m.in.Tier] {
			seen[m.in.Tier] = true
			tiers = append(tiers, m.in.Tier)
		}
	}
	sort.Ints(tiers)
	return tiers
}

func (w *world) inTier(tier int) []*mstate {
	var out []*mstate
	for _, m := range w.models {
		if m.in.Tier == tier {
			out = append(out, m)
		}
	}
	return out
}

func (w *world) byPriority() []*mstate {
	out := append([]*mstate{}, w.models...)
	sort.Slice(out, func(i, j int) bool { return out[i].before(out[j]) })
	return out
}

func (m *mstate) before(o *mstate) bool {
	if m.in.Tier != o.in.Tier {
		return m.in.Tier < o.in.Tier
	}
	if m.weight != o.weight {
		return m.weight > o.weight
	}
	if m.tightest() != o.tightest() {
		return m.tightest() < o.tightest()
	}
	return m.id < o.id
}

func allocTypes(alloc map[string]int) []string {
	types := make([]string, 0, len(alloc))
	for typ, n := range alloc {
		if n > 0 {
			types = append(types, typ)
		}
	}
	sort.Strings(types)
	return types
}

func cloneAlloc(in map[string]int) map[string]int {
	out := make(map[string]int, len(in))
	for k, v := range in {
		if v > 0 {
			out[k] = v
		}
	}
	return out
}

func sameAlloc(a, b map[string]int) bool {
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	for k, v := range b {
		if a[k] != v {
			return false
		}
	}
	return true
}

func errf(format string, args ...any) error {
	return &planError{msg: fmt.Sprintf(format, args...)}
}

type planError struct{ msg string }

func (e *planError) Error() string { return "supply: " + e.msg }
