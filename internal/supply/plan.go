package supply

import "sort"

// Plan computes target replicas for one tick.
//
// Demand stays in tokens per second until a concrete GPU type is chosen.
// Allocation is reserved minimums, then per-tier triage, then weighted
// max-min on the leftover. A replica added by this call is warming until its
// cold start elapses; it is not ready capacity.
func Plan(req Request) (Response, error) {
	if err := validate(req); err != nil {
		return Response{}, err
	}
	base := newWorld(req)
	if err := base.computeDemand(); err != nil {
		return Response{}, err
	}
	if base.currentOverBudget() {
		w := newWorld(req)
		if err := w.computeDemand(); err != nil {
			return Response{}, err
		}
		w.evict = true
		w.solveEvict()
		return w.response(), nil
	}
	ideal := newWorld(req)
	if err := ideal.computeDemand(); err != nil {
		return Response{}, err
	}
	ideal.solveIdeal()
	if base.evictionRequested() {
		ideal.evict = true
		return ideal.response(), nil
	}
	blended := newWorld(req)
	if err := blended.computeDemand(); err != nil {
		return Response{}, err
	}
	blended.applyHysteresis(ideal)
	return blended.response(), nil
}

func (w *world) applyHysteresis(ideal *world) {
	bypass := map[string]bool{}
	for i, m := range w.models {
		im := ideal.models[i]
		cur := m.currentMap()
		switch {
		case m.missesIfWait():
			bypass[m.id] = true
			m.alloc = cloneAlloc(im.alloc)
			m.notes = append([]string{}, im.notes...)
			m.note("deadline_bypass")
		case m.inCooldown(w.now):
			m.alloc = cur
			m.note("cooldown_hold")
		default:
			stepped := approach(cur, im.alloc, m.in.MaxStep)
			m.alloc = stepped
			if sameAlloc(stepped, im.alloc) {
				m.notes = append([]string{}, im.notes...)
			} else if m.in.MaxStep > 0 {
				m.note("step_clamped")
			}
		}
	}
	if !w.overBudget() {
		return
	}
	floors := w.reservedFloors()
	for i, m := range w.models {
		if !bypass[m.id] {
			continue
		}
		g := ideal.models[i].gpusUsed()
		if g > floors[m.id] {
			floors[m.id] = g
		}
	}
	w.evict = true
	w.fit(floors)
	w.ensureReserved()
}

// missesIfWait reports that a satisfiable backlog bucket misses its deadline
// if the plan stays on the replicas already up or warming.
func (m *mstate) missesIfWait() bool {
	if !m.hasSatisfiable() {
		return false
	}
	prev := m.alloc
	m.alloc = m.currentMap()
	met := m.deadlinesMet()
	m.alloc = prev
	return !met
}

func approach(cur, ideal map[string]int, maxStep int) map[string]int {
	if maxStep <= 0 {
		return cloneAlloc(ideal)
	}
	out := cloneAlloc(cur)
	seen := map[string]struct{}{}
	var types []string
	for _, src := range []map[string]int{cur, ideal} {
		for typ := range src {
			if _, ok := seen[typ]; ok {
				continue
			}
			seen[typ] = struct{}{}
			types = append(types, typ)
		}
	}
	sort.Strings(types)
	for step := 0; step < maxStep; step++ {
		bestTyp := ""
		bestGap := 0
		for _, typ := range types {
			gap := ideal[typ] - out[typ]
			if gap < 0 {
				gap = -gap
			}
			if gap > bestGap || (gap == bestGap && gap > 0 && (bestTyp == "" || typ < bestTyp)) {
				bestGap = gap
				bestTyp = typ
			}
		}
		if bestGap == 0 {
			break
		}
		if ideal[bestTyp] > out[bestTyp] {
			out[bestTyp]++
			continue
		}
		out[bestTyp]--
		if out[bestTyp] == 0 {
			delete(out, bestTyp)
		}
	}
	return out
}

func (w *world) response() Response {
	resp := Response{EvictionReplan: w.evict}
	for _, m := range w.models {
		resp.Models = append(resp.Models, m.plan(w.horizon))
	}
	used := w.usedByType()
	seen := map[string]bool{}
	var types []string
	for typ := range w.budget {
		seen[typ] = true
		types = append(types, typ)
	}
	for typ, n := range used {
		if n > 0 && !seen[typ] {
			types = append(types, typ)
		}
	}
	sort.Strings(types)
	for _, typ := range types {
		resp.BudgetUsed = append(resp.BudgetUsed, GPUBudget{Type: typ, GPUs: used[typ]})
	}
	return resp
}

func (m *mstate) plan(globalH float64) ModelPlan {
	p := ModelPlan{
		ModelID:                     m.id,
		TargetGPUs:                  m.gpusUsed(),
		DemandTokensPerSec:          m.demand,
		SmoothedArrivalTokensPerSec: m.arrival,
		DrainTokensPerSec:           m.drain,
		ReadyTokensPerSec:           m.readyTPS(),
		WarmingTokensPerSec:         m.warmingTPS(),
		UnsatisfiableTokens:         m.unsat,
		Phi:                         m.phi(globalH),
		Notes:                       append([]string{}, m.notes...),
	}
	if m.in.ReservedMinGPUs > m.gpusUsed() {
		p.ReservedDeficitGPUs = m.in.ReservedMinGPUs - m.gpusUsed()
		mNote := false
		for _, n := range p.Notes {
			if n == "reserved_deficit" {
				mNote = true
				break
			}
		}
		if !mNote {
			p.Notes = append(p.Notes, "reserved_deficit")
		}
	}
	for _, typ := range allocTypes(m.alloc) {
		p.TargetReplicas = append(p.TargetReplicas, ReplicaCount{Type: typ, Replicas: m.alloc[typ]})
	}
	if p.Notes == nil {
		p.Notes = []string{}
	}
	return p
}
