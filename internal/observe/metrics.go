package observe

import (
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Names match RFC 0001 §5.6. Later phases add labels; they do not rename these.
const (
	MetricQueueDepth   = "llm_async_gateway_queue_depth"
	MetricClaimed      = "llm_async_gateway_claimed"
	MetricInflight     = "llm_async_gateway_inflight"
	MetricBudget       = "llm_async_gateway_dispatch_budget"
	MetricGateDecision = "llm_async_gateway_gate_decisions_total"
	MetricAttempts     = "llm_async_gateway_attempts_total"
	MetricSlack        = "llm_async_gateway_deadline_slack_seconds"
	MetricQueueWait    = "llm_async_gateway_queue_wait_seconds"
	MetricUpstream     = "llm_async_gateway_upstream_seconds"
	MetricBatchJobs    = "llm_async_gateway_batch_jobs"
	MetricTokens       = "llm_async_gateway_tokens_total"
)

// QueueStats is a point-in-time read of Redis gauges that do not need MySQL.
type QueueStats struct {
	Pool      string
	Depth     map[string]float64
	Claimed   float64
	Inflight  map[string]float64
	BatchJobs map[string]float64
	Budget    float64
}

// StatsFunc reads gauges at scrape time.
type StatsFunc func() QueueStats

// Metrics is the process registry.
type Metrics struct {
	reg           *prometheus.Registry
	attempts      *prometheus.CounterVec
	gateDecisions *prometheus.CounterVec
	slack         *prometheus.HistogramVec
	queueWait     *prometheus.HistogramVec
	upstream      *prometheus.HistogramVec
	tokens        *prometheus.CounterVec
	budget        *prometheus.GaugeVec
}

// NewMetrics registers the §5.6 series that this process can produce.
// stats may be nil for a process that only increments counters.
func NewMetrics(stats StatsFunc) *Metrics {
	reg := prometheus.NewRegistry()
	m := &Metrics{reg: reg}
	m.attempts = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: MetricAttempts,
		Help: "Upstream attempts by terminal or retry outcome.",
	}, []string{"pool", "tier", "result"})
	m.gateDecisions = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: MetricGateDecision,
		Help: "Gate admission decisions.",
	}, []string{"pool", "tier", "gate", "reason"})
	m.slack = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    MetricSlack,
		Help:    "Seconds between claim time and the request deadline. Negative means the deadline already passed.",
		Buckets: []float64{-60, -10, -1, 0, 1, 5, 15, 30, 60, 120, 300, 600, 1800, 3600, 86400},
	}, []string{"pool", "tier"})
	m.queueWait = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    MetricQueueWait,
		Help:    "Seconds a request waited on the ready queue before it was claimed.",
		Buckets: prometheus.DefBuckets,
	}, []string{"pool", "tier"})
	m.upstream = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    MetricUpstream,
		Help:    "Upstream HTTP call latency in seconds.",
		Buckets: []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60},
	}, []string{"pool", "tier", "code"})
	m.tokens = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: MetricTokens,
		Help: "Tokens observed on successful upstream responses.",
	}, []string{"pool", "tier", "direction"})
	m.budget = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: MetricBudget,
		Help: "Remaining admission budget in [0, 1] reported by a gate.",
	}, []string{"pool", "tier", "gate"})
	reg.MustRegister(m.attempts, m.gateDecisions, m.slack, m.queueWait, m.upstream, m.tokens, m.budget)
	if stats != nil {
		reg.MustRegister(newStatsCollector(stats))
	}
	return m
}

// Handler serves the Prometheus text exposition.
func (m *Metrics) Handler() http.Handler {
	if m == nil {
		return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "metrics unavailable", http.StatusServiceUnavailable)
		})
	}
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{})
}

// Attempt adds one dispatch outcome. result is ok, retry, expired, cancelled, or failed.
func (m *Metrics) Attempt(pool, tier, result string) {
	if m == nil {
		return
	}
	m.attempts.WithLabelValues(pool, tier, result).Inc()
}

// GateDecision adds one admission decision.
func (m *Metrics) GateDecision(pool, tier, gate, reason string) {
	if m == nil {
		return
	}
	m.gateDecisions.WithLabelValues(pool, tier, gate, reason).Inc()
}

// ObserveClaim records deadline slack and queue wait at claim time.
func (m *Metrics) ObserveClaim(pool, tier string, createdUnix, deadlineUnix int64, now time.Time) {
	if m == nil {
		return
	}
	slack := time.Unix(deadlineUnix, 0).Sub(now).Seconds()
	wait := now.Sub(time.Unix(createdUnix, 0)).Seconds()
	if wait < 0 {
		wait = 0
	}
	m.slack.WithLabelValues(pool, tier).Observe(slack)
	m.queueWait.WithLabelValues(pool, tier).Observe(wait)
}

// ObserveUpstream records one upstream call.
func (m *Metrics) ObserveUpstream(pool, tier string, code int, d time.Duration) {
	if m == nil {
		return
	}
	label := "error"
	if code > 0 {
		label = strconv.Itoa(code)
	}
	m.upstream.WithLabelValues(pool, tier, label).Observe(d.Seconds())
}

// Tokens adds observed usage.
func (m *Metrics) Tokens(pool, tier, direction string, n int) {
	if m == nil || n <= 0 {
		return
	}
	m.tokens.WithLabelValues(pool, tier, direction).Add(float64(n))
}

// SetBudget publishes the latest Budget() sample for a gate.
func (m *Metrics) SetBudget(pool, tier, gate string, value float64) {
	if m == nil {
		return
	}
	if value < 0 {
		value = 0
	}
	if value > 1 {
		value = 1
	}
	m.budget.WithLabelValues(pool, tier, gate).Set(value)
}

type statsCollector struct {
	stats StatsFunc
	depth *prometheus.Desc
	claim *prometheus.Desc
	infl  *prometheus.Desc
	jobs  *prometheus.Desc
}

func newStatsCollector(stats StatsFunc) *statsCollector {
	return &statsCollector{
		stats: stats,
		depth: prometheus.NewDesc(MetricQueueDepth, "Ready queue depth.", []string{"pool", "tier"}, nil),
		claim: prometheus.NewDesc(MetricClaimed, "Requests currently leased.", []string{"pool"}, nil),
		infl:  prometheus.NewDesc(MetricInflight, "Shared in-flight admissions.", []string{"pool", "tier"}, nil),
		jobs:  prometheus.NewDesc(MetricBatchJobs, "Batch jobs by status.", []string{"status"}, nil),
	}
}

func (c *statsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.depth
	ch <- c.claim
	ch <- c.infl
	ch <- c.jobs
}

// Prime creates the fixed label sets at zero so a scrape before the first
// attempt still exposes queue, admission, and outcome series.
func (m *Metrics) Prime(pool string) {
	if m == nil {
		return
	}
	if pool == "" {
		pool = "default"
	}
	tiers := []string{"interactive", "async", "batch"}
	results := []string{"ok", "retry", "expired", "cancelled", "failed"}
	reasons := []string{"continue", "refuse_local", "refuse_shared"}
	for _, tier := range tiers {
		for _, result := range results {
			m.attempts.WithLabelValues(pool, tier, result)
		}
		m.slack.WithLabelValues(pool, tier)
		m.queueWait.WithLabelValues(pool, tier)
		m.budget.WithLabelValues(pool, tier, "local")
		for _, reason := range reasons {
			m.gateDecisions.WithLabelValues(pool, tier, "local", reason)
		}
	}
}

func (c *statsCollector) Collect(ch chan<- prometheus.Metric) {
	snap := c.stats()
	pool := snap.Pool
	if pool == "" {
		pool = "default"
	}
	depth := map[string]float64{"interactive": 0, "async": 0, "batch": 0}
	for tier, n := range snap.Depth {
		depth[tier] = n
	}
	for tier, n := range depth {
		ch <- prometheus.MustNewConstMetric(c.depth, prometheus.GaugeValue, n, pool, tier)
	}
	ch <- prometheus.MustNewConstMetric(c.claim, prometheus.GaugeValue, snap.Claimed, pool)
	inflight := map[string]float64{"interactive": 0, "async": 0, "batch": 0}
	for tier, n := range snap.Inflight {
		inflight[tier] = n
	}
	for tier, n := range inflight {
		ch <- prometheus.MustNewConstMetric(c.infl, prometheus.GaugeValue, n, pool, tier)
	}
	for status, n := range snap.BatchJobs {
		ch <- prometheus.MustNewConstMetric(c.jobs, prometheus.GaugeValue, n, status)
	}
}
