// Package metrics keeps lock-free counters on the hot path and serves
// JSON snapshots plus Prometheus text exposition on the admin listener.
package metrics

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// BackendStats accumulates per-backend traffic. Updated under Registry.mu,
// which is only held for map lookup plus a few integer ops.
type BackendStats struct {
	Requests     uint64
	Errors       uint64
	LatencySumNs uint64
}

// Registry is safe for concurrent use by all proxy goroutines.
type Registry struct {
	start time.Time

	mu  sync.Mutex
	per map[string]*BackendStats

	total       atomic.Uint64
	ok          atomic.Uint64
	failed      atomic.Uint64
	unavailable atomic.Uint64
	inflight    atomic.Int64
}

// New returns an empty registry stamped with the process start time.
func New() *Registry {
	return &Registry{start: time.Now(), per: make(map[string]*BackendStats)}
}

// IncInflight tracks a request entering the proxy fan-out.
func (m *Registry) IncInflight() { m.inflight.Add(1) }

// DecInflight tracks a request leaving the proxy fan-out.
func (m *Registry) DecInflight() { m.inflight.Add(-1) }

// Observe records one completed attempt. status 0 means the attempt never
// reached upstream (transport error); 2xx-4xx counts as served.
func (m *Registry) Observe(id string, status int, d time.Duration) {
	m.total.Add(1)
	served := status >= 200 && status < 500
	if served {
		m.ok.Add(1)
	} else {
		m.failed.Add(1)
	}
	m.mu.Lock()
	s := m.per[id]
	if s == nil {
		s = &BackendStats{}
		m.per[id] = s
	}
	s.Requests++
	if !served {
		s.Errors++
	}
	s.LatencySumNs += uint64(d.Nanoseconds())
	m.mu.Unlock()
}

// NoBackend records a request rejected for lack of healthy backends.
func (m *Registry) NoBackend() {
	m.total.Add(1)
	m.failed.Add(1)
	m.unavailable.Add(1)
}

// BackendView is the JSON/Prometheus view of one backend's stats.
type BackendView struct {
	Requests uint64  `json:"requests"`
	Errors   uint64  `json:"errors"`
	AvgMs    float64 `json:"avg_ms"`
}

// Snapshot is the full point-in-time JSON view served at /metrics.
type Snapshot struct {
	UptimeSeconds int64                  `json:"uptime_seconds"`
	Total         uint64                 `json:"total"`
	OK            uint64                 `json:"ok"`
	Failed        uint64                 `json:"failed"`
	Unavailable   uint64                 `json:"unavailable"`
	Inflight      int64                  `json:"inflight"`
	Backends      map[string]BackendView `json:"backends"`
}

// Snapshot copies current counters (cheap: atomics + one short lock).
func (m *Registry) Snapshot() Snapshot {
	s := Snapshot{
		UptimeSeconds: int64(time.Since(m.start).Seconds()),
		Total:         m.total.Load(),
		OK:            m.ok.Load(),
		Failed:        m.failed.Load(),
		Unavailable:   m.unavailable.Load(),
		Inflight:      m.inflight.Load(),
		Backends:      make(map[string]BackendView),
	}
	m.mu.Lock()
	for id, st := range m.per {
		v := BackendView{Requests: st.Requests, Errors: st.Errors}
		if st.Requests > 0 {
			v.AvgMs = float64(st.LatencySumNs) / float64(st.Requests) / 1e6
		}
		s.Backends[id] = v
	}
	m.mu.Unlock()
	return s
}

// Prometheus renders the same counters in Prometheus text exposition format.
func (m *Registry) Prometheus() string {
	s := m.Snapshot()
	var b strings.Builder
	fmt.Fprintf(&b, "# HELP overpass_requests_total Total proxied attempts.\n")
	fmt.Fprintf(&b, "# TYPE overpass_requests_total counter\n")
	fmt.Fprintf(&b, "overpass_requests_total %d\n", s.Total)
	fmt.Fprintf(&b, "# HELP overpass_requests_failed Failed attempts (5xx, transport errors, no backend).\n")
	fmt.Fprintf(&b, "# TYPE overpass_requests_failed counter\n")
	fmt.Fprintf(&b, "overpass_requests_failed %d\n", s.Failed)
	fmt.Fprintf(&b, "# HELP overpass_inflight_requests Requests inside the proxy fan-out right now.\n")
	fmt.Fprintf(&b, "# TYPE overpass_inflight_requests gauge\n")
	fmt.Fprintf(&b, "overpass_inflight_requests %d\n", s.Inflight)
	fmt.Fprintf(&b, "# HELP overpass_uptime_seconds Seconds since process start.\n")
	fmt.Fprintf(&b, "# TYPE overpass_uptime_seconds counter\n")
	fmt.Fprintf(&b, "overpass_uptime_seconds %d\n", s.UptimeSeconds)
	for id, v := range s.Backends {
		fmt.Fprintf(&b, "overpass_backend_requests_total{backend=%q} %d\n", id, v.Requests)
		fmt.Fprintf(&b, "overpass_backend_errors_total{backend=%q} %d\n", id, v.Errors)
	}
	return b.String()
}
