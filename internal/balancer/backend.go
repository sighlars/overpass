// Package balancer implements backend selection, proxying with bounded
// retries, per-backend circuit breaking, and active health checking.
package balancer

import (
	"net"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"time"
)

// Backend is one upstream server plus its live concurrency state.
// Hot-path counters use atomics; the failure bookkeeping uses a small
// mutex because it is only touched on failures and health checks.
type Backend struct {
	ID         string
	target     *url.URL
	healthPath string
	director   func(*http.Request)

	alive   atomic.Bool
	conns   atomic.Int64
	drained atomic.Bool

	mu       sync.Mutex
	fails    int
	maxFails int
	openedAt time.Time
	cooldown time.Duration
}

func newBackend(id string, target *url.URL, healthPath string, maxFails int, cooldown time.Duration) *Backend {
	b := &Backend{
		ID:         id,
		target:     target,
		healthPath: healthPath,
		maxFails:   maxFails,
		cooldown:   cooldown,
	}
	b.alive.Store(true)
	b.director = func(r *http.Request) {
		r.URL.Scheme = target.Scheme
		r.URL.Host = target.Host
		r.URL.Path = singleJoiningSlash(target.Path, r.URL.Path)
		r.Host = target.Host
		if clientIP, _, err := net.SplitHostPort(r.RemoteAddr); err == nil {
			if prior := r.Header.Get("X-Forwarded-For"); prior != "" {
				r.Header.Set("X-Forwarded-For", prior+", "+clientIP)
			} else {
				r.Header.Set("X-Forwarded-For", clientIP)
			}
		}
	}
	return b
}

func singleJoiningSlash(a, b string) string {
	aslash := len(a) > 0 && a[len(a)-1] == '/'
	bslash := len(b) > 0 && b[0] == '/'
	switch {
	case aslash && bslash:
		return a + b[1:]
	case !aslash && !bslash:
		return a + "/" + b
	}
	return a + b
}

// Available reports whether the backend may receive new requests.
func (b *Backend) Available() bool {
	return b.alive.Load() && !b.drained.Load()
}

// ConnAdd registers an in-flight request, returning the new count.
func (b *Backend) ConnAdd() int64 {
	return b.conns.Add(1)
}

// ConnDone releases an in-flight request.
func (b *Backend) ConnDone() {
	b.conns.Add(-1)
}

// Load returns current in-flight requests (used by least_conn).
func (b *Backend) Load() int64 {
	return b.conns.Load()
}

func (b *Backend) recordSuccess() {
	b.mu.Lock()
	b.fails = 0
	b.mu.Unlock()
	b.alive.Store(true)
}

// recordFailure counts a failure and opens the circuit once MaxFails
// consecutive failures accumulate. The breaker half-opens through the
// active health checker (probeDue), never through live traffic.
func (b *Backend) recordFailure() {
	b.mu.Lock()
	b.fails++
	open := b.fails >= b.maxFails
	if open {
		b.openedAt = time.Now()
	}
	b.mu.Unlock()
	if open {
		b.alive.Store(false)
	}
}

// probeDue reports whether enough cooldown elapsed to allow one probe.
func (b *Backend) probeDue(now time.Time) bool {
	if b.alive.Load() {
		return false
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return now.Sub(b.openedAt) >= b.cooldown
}

// SetDrained parks the backend: in-flight requests finish, new ones skip it.
func (b *Backend) SetDrained(d bool) {
	b.drained.Store(d)
}

// BackendInfo is the admin-facing snapshot of one backend.
type BackendInfo struct {
	ID      string `json:"id"`
	URL     string `json:"url"`
	Alive   bool   `json:"alive"`
	Drained bool   `json:"drained"`
	Conns   int64  `json:"conns"`
	Fails   int    `json:"fails"`
}

// Info snapshots backend state for the admin API.
func (b *Backend) Info() BackendInfo {
	b.mu.Lock()
	fails := b.fails
	b.mu.Unlock()
	return BackendInfo{
		ID:      b.ID,
		URL:     b.target.String(),
		Alive:   b.alive.Load(),
		Drained: b.drained.Load(),
		Conns:   b.conns.Load(),
		Fails:   fails,
	}
}
