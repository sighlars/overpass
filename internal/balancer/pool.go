package balancer

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"overpass/internal/config"
	"overpass/internal/metrics"
)

// Strategy selects the backend for each request.
type Strategy string

const (
	RoundRobin Strategy = "round_robin"
	LeastConn  Strategy = "least_conn"
	Random     Strategy = "random"
)

// Pool owns the backend set, the selection strategy, proxying with bounded
// retries, and the background health-check loop.
type Pool struct {
	mu       sync.RWMutex
	backends []*Backend

	strategy    Strategy
	rr          atomic.Uint64
	reqIDs      atomic.Uint64
	maxAttempts int

	hcEvery   time.Duration
	hcTimeout time.Duration

	metrics   *metrics.Registry
	log       *slog.Logger
	transport *http.Transport

	stopCh  chan struct{}
	stopped atomic.Bool
	wg      sync.WaitGroup
}

// NewPool builds a pool from validated config. Transport is shared across
// all backends so idle connections are reused process-wide.
func NewPool(cfg config.Config, m *metrics.Registry, log *slog.Logger) (*Pool, error) {
	if len(cfg.Backends) == 0 {
		return nil, fmt.Errorf("pool needs at least one backend")
	}
	transport := &http.Transport{
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 10,
		IdleConnTimeout:     90 * time.Second,
		DialContext: (&net.Dialer{
			Timeout:   5 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
	}
	p := &Pool{
		strategy:    Strategy(cfg.Strategy),
		maxAttempts: cfg.MaxAttempts,
		hcEvery:     time.Duration(cfg.HealthEveryMs) * time.Millisecond,
		hcTimeout:   time.Duration(cfg.HealthTimeoutMs) * time.Millisecond,
		metrics:     m,
		log:         log,
		transport:   transport,
		stopCh:      make(chan struct{}),
	}
	for i, bc := range cfg.Backends {
		u, _ := url.Parse(bc.URL) // validated by config
		id := fmt.Sprintf("backend-%d", i+1)
		p.backends = append(p.backends, newBackend(
			id, u, bc.HealthPath, bc.MaxFails,
			time.Duration(bc.FailTimeoutMs)*time.Millisecond,
		))
	}
	return p, nil
}

// Next returns the next available backend per the pool strategy,
// or nil when nothing may take traffic right now.
func (p *Pool) Next() *Backend {
	p.mu.RLock()
	defer p.mu.RUnlock()
	avail := make([]*Backend, 0, len(p.backends))
	for _, b := range p.backends {
		if b.Available() {
			avail = append(avail, b)
		}
	}
	if len(avail) == 0 {
		return nil
	}
	switch p.strategy {
	case LeastConn:
		best := avail[0]
		for _, b := range avail[1:] {
			if b.Load() < best.Load() {
				best = b
			}
		}
		return best
	case Random:
		return avail[rand.Intn(len(avail))]
	default: // RoundRobin
		return avail[int(p.rr.Add(1)-1)%len(avail)]
	}
}

func (p *Pool) count() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.backends)
}

// statusRecorder captures the status code without changing streaming behavior.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(s int) {
	r.status = s
	r.ResponseWriter.WriteHeader(s)
}

func (r *statusRecorder) Flush() {
	if f, ok := r.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// ServeHTTP implements http.Handler: pick, proxy, retry next backend on
// transport errors up to maxAttempts. A 5xx from upstream is served as-is
// (documented behavior) but counts against the backend's breaker.
func (p *Pool) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	if r.Header.Get("X-Request-ID") == "" {
		r.Header.Set("X-Request-ID", strconv.FormatUint(p.reqIDs.Add(1), 10))
	}

	attempts := p.maxAttempts
	if n := p.count(); attempts > n {
		attempts = n
	}
	for i := 0; i < attempts; i++ {
		if r.Context().Err() != nil {
			return // client gone or server shutting down
		}
		b := p.Next()
		if b == nil {
			break
		}
		w.Header().Set("X-Proxy-Backend", b.ID)
		b.ConnAdd()
		p.metrics.IncInflight()
		rw := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		err := p.doProxy(b, rw, r)
		b.ConnDone()
		p.metrics.DecInflight()
		if err != nil {
			b.recordFailure()
			p.metrics.Observe(b.ID, 0, time.Since(start))
			p.log.Warn("proxy attempt failed, trying next backend",
				"backend", b.ID, "attempt", i+1, "err", err)
			continue
		}
		if rw.status >= 500 {
			b.recordFailure()
		} else {
			b.recordSuccess()
		}
		p.metrics.Observe(b.ID, rw.status, time.Since(start))
		return
	}
	p.metrics.NoBackend()
	http.Error(w, "Bad Gateway: no healthy backends", http.StatusBadGateway)
}

// doProxy runs one attempt through a per-request ReverseProxy so the error
// handler can report back without writing a partial response.
func (p *Pool) doProxy(b *Backend, w http.ResponseWriter, r *http.Request) error {
	var perr error
	rp := &httputil.ReverseProxy{
		Director:  b.director,
		Transport: p.transport,
		// Capture the error without writing: the caller retries
		// or writes the final 502 itself.
		ErrorHandler: func(_ http.ResponseWriter, _ *http.Request, err error) {
			perr = err
		},
	}
	rp.ServeHTTP(w, r)
	return perr
}

// Start launches the background health-check loop.
func (p *Pool) Start() {
	p.wg.Add(1)
	go p.healthLoop()
}

// Stop halts health checking after in-flight probes finish.
func (p *Pool) Stop() {
	if p.stopped.CompareAndSwap(false, true) {
		close(p.stopCh)
		p.wg.Wait()
	}
}

func (p *Pool) healthLoop() {
	defer p.wg.Done()
	t := time.NewTicker(p.hcEvery)
	defer t.Stop()
	for {
		select {
		case <-p.stopCh:
			return
		case <-t.C:
			p.checkAll()
		}
	}
}

func (p *Pool) checkAll() {
	p.mu.RLock()
	list := make([]*Backend, len(p.backends))
	copy(list, p.backends)
	p.mu.RUnlock()
	now := time.Now()
	for _, b := range list {
		if b.alive.Load() || b.probeDue(now) {
			p.probe(b)
		}
	}
}

func (p *Pool) probe(b *Backend) {
	ctx, cancel := context.WithTimeout(context.Background(), p.hcTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, b.target.String()+b.healthPath, nil)
	if err != nil {
		b.recordFailure()
		return
	}
	resp, err := (&http.Client{Transport: p.transport}).Do(req)
	if err == nil {
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
	if err == nil && resp.StatusCode < 500 {
		b.recordSuccess()
	} else {
		b.recordFailure()
	}
}

// Snapshot lists backend state for the admin API.
func (p *Pool) Snapshot() []BackendInfo {
	p.mu.RLock()
	defer p.mu.RUnlock()
	out := make([]BackendInfo, 0, len(p.backends))
	for _, b := range p.backends {
		out = append(out, b.Info())
	}
	return out
}

// SetDrained parks (true) or restores (false) a backend by ID.
func (p *Pool) SetDrained(id string, d bool) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	for _, b := range p.backends {
		if b.ID == id {
			b.SetDrained(d)
			return true
		}
	}
	return false
}
