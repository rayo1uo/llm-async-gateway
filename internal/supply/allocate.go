package supply

import (
	"math"
	"sort"
)

// ensureReserved places ReservedMinGPUs once per model, highest tier first.
// GPUs already allocated to the model count toward that floor, so the same
// reservation is not repeated on every GPU type. When the per-replica GPU
// count does not divide the remainder, one extra replica is taken only if
// budget and the soft max both allow it; otherwise the shortfall is a deficit.
func (w *world) ensureReserved() {
	for _, m := range w.byPriority() {
		if m.in.ReservedMinGPUs <= 0 {
			continue
		}
		m.note("reserved")
		for m.gpusUsed() < m.in.ReservedMinGPUs {
			g, ok := w.pickReservedType(m)
			if !ok {
				break
			}
			w.addReplica(m, g)
		}
		if m.gpusUsed() < m.in.ReservedMinGPUs {
			m.note("reserved_deficit")
		} else if m.gpusUsed() > m.in.ReservedMinGPUs {
			m.note("reserved_rounded_up")
		}
	}
}

func (w *world) addReplica(m *mstate, g gprof) {
	m.alloc[g.typ]++
}

func (w *world) pickReservedType(m *mstate) (gprof, bool) {
	var cands []gprof
	for _, g := range m.feasible {
		if w.canFit(m, g) {
			cands = append(cands, g)
		}
	}
	if len(cands) == 0 {
		return gprof{}, false
	}
	sort.Slice(cands, func(i, j int) bool { return betterPlace(m, cands[i], cands[j]) })
	return cands[0], true
}

// betterPlace prefers a type that already has a ready replica, then one that
// already has a warming replica, then higher tokens per GPU. Using supply that
// is already up avoids replacing a serving replica with a cold start.
func betterPlace(m *mstate, a, b gprof) bool {
	ar := m.spareReady(a.typ) > 0
	br := m.spareReady(b.typ) > 0
	if ar != br {
		return ar
	}
	_, aw := m.nextSpareWarming(a.typ)
	_, bw := m.nextSpareWarming(b.typ)
	if aw != bw {
		return aw
	}
	au := a.mu / float64(a.u)
	bu := b.mu / float64(b.u)
	if au != bu {
		return au > bu
	}
	if a.mu != b.mu {
		return a.mu > b.mu
	}
	return a.typ < b.typ
}

func (w *world) triage(tier int) {
	blocked := map[string]bool{}
	for {
		var saveable []*mstate
		for _, m := range w.inTier(tier) {
			if blocked[m.id] || m.deadlinesMet() {
				continue
			}
			if w.canSave(m) {
				saveable = append(saveable, m)
			} else {
				blocked[m.id] = true
			}
		}
		if len(saveable) == 0 {
			break
		}
		sort.Slice(saveable, func(i, j int) bool {
			a, b := saveable[i], saveable[j]
			if a.tightest() != b.tightest() {
				return a.tightest() < b.tightest()
			}
			if a.weight != b.weight {
				return a.weight > b.weight
			}
			return a.id < b.id
		})
		best := saveable[0]
		if !w.fund(best) {
			blocked[best.id] = true
			continue
		}
		best.note("triage")
	}
	for _, m := range w.inTier(tier) {
		if m.hasSatisfiable() && !m.deadlinesMet() {
			m.note("triage_unsavable")
		}
	}
}

// canSave reports whether some allocation of the budget still free can drain
// every satisfiable bucket. Models that fail this test get nothing above the
// reserved floor: a partial share would miss the deadline anyway.
func (w *world) canSave(m *mstate) bool {
	snap := w.snapshot()
	defer w.restore(snap)
	if m.deadlinesMet() {
		return true
	}
	for n := 0; n < w.stepLimit(); n++ {
		g, ok := w.pickTriageType(m)
		if !ok {
			return false
		}
		w.addReplica(m, g)
		if m.deadlinesMet() {
			return true
		}
	}
	return false
}

func (w *world) fund(m *mstate) bool {
	if m.deadlinesMet() {
		return true
	}
	for n := 0; n < w.stepLimit(); n++ {
		g, ok := w.pickTriageType(m)
		if !ok {
			return false
		}
		w.addReplica(m, g)
		if m.deadlinesMet() {
			return true
		}
	}
	return false
}

func (w *world) stepLimit() int {
	sum := 1
	for _, n := range w.budget {
		sum += n
	}
	if sum < 64 {
		return 64
	}
	return sum
}

// pickTriageType chooses the GPU type that adds the most tokens before the
// earliest unmet deadline, per GPU. The replica's cold start is part of that
// score: a replica that is not ready by the deadline adds nothing.
func (w *world) pickTriageType(m *mstate) (gprof, bool) {
	until, ok := m.earliestUnmet()
	if !ok {
		return gprof{}, false
	}
	bestScore := -1.0
	var best gprof
	found := false
	for _, g := range m.feasible {
		if !w.canFit(m, g) {
			continue
		}
		score := m.marginalTokens(g, until) / float64(g.u)
		if score <= 0 {
			continue
		}
		if !found || score > bestScore+tokenTol || (math.Abs(score-bestScore) <= tokenTol && g.typ < best.typ) {
			best = g
			bestScore = score
			found = true
		}
	}
	return best, found
}

// fairness raises phi = rate(H) / D inside one tier after that tier's triage
// commitments. Unsaved models are excluded. One replica goes to the smallest
// phi/weight each time, which is weighted max-min. Rate(H) includes a new
// replica only after its cold start.
func (w *world) fairness(tier int) {
	for n := 0; n < w.stepLimit()*len(w.models)+1; n++ {
		var best *mstate
		bestRatio := math.Inf(1)
		for _, m := range w.inTier(tier) {
			if !m.deadlinesMet() || m.demand <= tokenTol {
				continue
			}
			h := m.horizon(w.horizon)
			if enough(m.rateAt(h), m.demand) {
				continue
			}
			if _, ok := w.pickFairType(m); !ok {
				continue
			}
			ratio := m.phi(w.horizon) / m.weight
			if best == nil || ratio < bestRatio-tokenTol || (math.Abs(ratio-bestRatio) <= tokenTol && m.id < best.id) {
				best = m
				bestRatio = ratio
			}
		}
		if best == nil {
			return
		}
		g, ok := w.pickFairType(best)
		if !ok {
			return
		}
		w.addReplica(best, g)
		best.note("fairness")
	}
}

func (w *world) pickFairType(m *mstate) (gprof, bool) {
	h := m.horizon(w.horizon)
	bestScore := -1.0
	var best gprof
	found := false
	for _, g := range m.feasible {
		if !w.canFit(m, g) {
			continue
		}
		eta := m.etaIfAdded(g)
		if eta > h+tokenTol {
			continue
		}
		score := g.mu / float64(g.u)
		if !found || score > bestScore+tokenTol || (math.Abs(score-bestScore) <= tokenTol && g.typ < best.typ) {
			best = g
			bestScore = score
			found = true
		}
	}
	return best, found
}

func (m *mstate) etaIfAdded(g gprof) float64 {
	if m.spareReady(g.typ) > 0 {
		return 0
	}
	if eta, ok := m.nextSpareWarming(g.typ); ok {
		return eta
	}
	return g.cold
}

func (w *world) snapshot() map[string]map[string]int {
	out := make(map[string]map[string]int, len(w.models))
	for _, m := range w.models {
		out[m.id] = cloneAlloc(m.alloc)
	}
	return out
}

func (w *world) restore(snap map[string]map[string]int) {
	for _, m := range w.models {
		m.alloc = cloneAlloc(snap[m.id])
	}
}

func (w *world) solveIdeal() {
	w.ensureReserved()
	for _, tier := range w.tiers() {
		w.triage(tier)
		w.fairness(tier)
	}
}

// solveEvict starts from the observed footprint, strips replicas until every
// per-type budget holds, pulls GPU counts back up to the reserved floors when
// a lower-priority model is holding the cards, then spends whatever is left
// on triage and phi. Cooldown and the step cap do not apply.
func (w *world) solveEvict() {
	floors := w.reservedFloors()
	w.seedCurrent()
	w.fit(floors)
	w.ensureReserved()
	for _, tier := range w.tiers() {
		w.triage(tier)
		w.fairness(tier)
	}
}

func (w *world) seedCurrent() {
	for _, m := range w.models {
		m.alloc = m.currentMap()
	}
}

func (w *world) reservedFloors() map[string]int {
	s := newWorld(w.req)
	s.eps = w.eps
	s.horizon = w.horizon
	_ = s.computeDemand()
	s.ensureReserved()
	out := make(map[string]int, len(s.models))
	for _, m := range s.models {
		out[m.id] = m.gpusUsed()
	}
	return out
}

// fit removes replicas while a GPU type is over budget, then moves cards from
// models above their reserved floor to models still under it.
func (w *world) fit(floors map[string]int) {
	for n := 0; n < w.stepLimit()*4; n++ {
		if over := w.overTypes(); len(over) > 0 {
			if w.removeForBudget(over, floors) {
				continue
			}
			return
		}
		gap := w.firstGap(floors)
		if gap == nil {
			return
		}
		if !w.reclaimOnce(gap, floors) {
			return
		}
	}
}

func (w *world) firstGap(floors map[string]int) *mstate {
	var gap *mstate
	for _, m := range w.byPriority() {
		if m.gpusUsed() < floors[m.id] {
			gap = m
			break
		}
	}
	return gap
}

func (w *world) reclaimOnce(gap *mstate, floors map[string]int) bool {
	donor, typ, ok := w.pickDonor(nil, floors)
	if !ok {
		return false
	}
	w.dropReplica(donor, typ)
	g, placed := w.pickReservedType(gap)
	if !placed {
		donor.alloc[typ]++
		return false
	}
	w.addReplica(gap, g)
	return true
}

func (w *world) removeForBudget(over []string, floors map[string]int) bool {
	if m, typ, ok := w.pickDonor(over, floors); ok {
		w.dropReplica(m, typ)
		return true
	}
	if w.rehome(over, floors) {
		return true
	}
	if m, typ, ok := w.pickAny(over); ok {
		w.dropReplica(m, typ)
		m.note("reserved_deficit")
		return true
	}
	return false
}

func (w *world) dropReplica(m *mstate, typ string) {
	if m.alloc[typ] <= 0 {
		return
	}
	m.alloc[typ]--
	if m.alloc[typ] == 0 {
		delete(m.alloc, typ)
	}
}

type shrinkCand struct {
	m      *mstate
	typ    string
	tier   int
	excess int
	phi    float64
	id     string
}

func (w *world) shrinkCands(only []string, floors map[string]int, respectFloor bool) []shrinkCand {
	allow := map[string]bool{}
	for _, typ := range only {
		allow[typ] = true
	}
	var cs []shrinkCand
	for _, m := range w.models {
		for _, typ := range allocTypes(m.alloc) {
			if len(allow) > 0 && !allow[typ] {
				continue
			}
			u := m.uOf(typ)
			if u <= 0 {
				continue
			}
			if respectFloor && m.gpusUsed()-u < floors[m.id] {
				continue
			}
			excess := m.gpusUsed() - m.in.ReservedMinGPUs
			if excess < 0 {
				excess = 0
			}
			cs = append(cs, shrinkCand{
				m: m, typ: typ, tier: m.in.Tier, excess: excess,
				phi: m.shrinkScore(w.horizon), id: m.id,
			})
		}
	}
	sort.Slice(cs, func(i, j int) bool {
		a, b := cs[i], cs[j]
		if a.tier != b.tier {
			return a.tier > b.tier
		}
		if a.excess != b.excess {
			return a.excess > b.excess
		}
		if a.phi != b.phi {
			return a.phi > b.phi
		}
		if a.id != b.id {
			return a.id < b.id
		}
		return a.typ < b.typ
	})
	return cs
}

func (w *world) pickDonor(over []string, floors map[string]int) (*mstate, string, bool) {
	cs := w.shrinkCands(over, floors, true)
	if len(cs) == 0 {
		return nil, "", false
	}
	return cs[0].m, cs[0].typ, true
}

func (w *world) pickAny(over []string) (*mstate, string, bool) {
	cs := w.shrinkCands(over, map[string]int{}, false)
	if len(cs) == 0 {
		return nil, "", false
	}
	return cs[0].m, cs[0].typ, true
}

func (w *world) rehome(over []string, floors map[string]int) bool {
	overSet := map[string]bool{}
	for _, typ := range over {
		overSet[typ] = true
	}
	for _, c := range w.shrinkCands(over, floors, false) {
		w.dropReplica(c.m, c.typ)
		dest, ok := w.pickRehomeDest(c.m, c.typ, overSet)
		if !ok {
			c.m.alloc[c.typ]++
			continue
		}
		w.addReplica(c.m, dest)
		return true
	}
	return false
}

func (w *world) pickRehomeDest(m *mstate, src string, over map[string]bool) (gprof, bool) {
	var cands []gprof
	for _, g := range m.feasible {
		if g.typ == src || over[g.typ] {
			continue
		}
		if !w.canFit(m, g) {
			continue
		}
		cands = append(cands, g)
	}
	if len(cands) == 0 {
		return gprof{}, false
	}
	sort.Slice(cands, func(i, j int) bool { return betterPlace(m, cands[i], cands[j]) })
	return cands[0], true
}
