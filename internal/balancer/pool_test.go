package balancer

import (
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"
	"time"

	"overpass/internal/config"
	"overpass/internal/metrics"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func testConfig(urls ...string) config.Config {
	c := config.Config{
		Listen:          ":0",
		AdminListen:     ":0",
		Strategy:        "round_robin",
		MaxAttempts:     3,
		HealthEveryMs:   50,
		HealthTimeoutMs: 500,
	}
	for _, u := range urls {
		c.Backends = append(c.Backends, config.BackendConfig{
			URL: u, HealthPath: "/", MaxFails: 2, FailTimeoutMs: 100,
		})
	}
	if err := c.Validate(); err != nil {
		panic(err)
	}
	return c
}

func liveBackend(t *testing.T, name string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		fmt.Fprint(w, name)
	}))
}

func TestRoundRobinDistributesEvenly(t *testing.T) {
	s1 := liveBackend(t, "a")
	defer s1.Close()
	s2 := liveBackend(t, "b")
	defer s2.Close()
	s3 := liveBackend(t, "c")
	defer s3.Close()

	p, err := NewPool(testConfig(s1.URL, s2.URL, s3.URL), metrics.New(), testLogger())
	if err != nil {
		t.Fatal(err)
	}
	counts := map[string]int{}
	for i := 0; i < 90; i++ {
		b := p.Next()
		if b == nil {
			t.Fatal("Next returned nil with all backends alive")
		}
		counts[b.ID]++
	}
	for id, n := range counts {
		if n != 30 {
			t.Fatalf("backend %s got %d requests, want 30", id, n)
		}
	}
}

func TestLeastConnAvoidsLoaded(t *testing.T) {
	s1 := liveBackend(t, "a")
	defer s1.Close()
	s2 := liveBackend(t, "b")
	defer s2.Close()

	cfg := testConfig(s1.URL, s2.URL)
	cfg.Strategy = "least_conn"
	p, err := NewPool(cfg, metrics.New(), testLogger())
	if err != nil {
		t.Fatal(err)
	}
	p.backends[0].ConnAdd()
	p.backends[0].ConnAdd()
	for i := 0; i < 10; i++ {
		if got := p.Next(); got.ID == p.backends[0].ID {
			t.Fatalf("least_conn picked loaded backend %s", got.ID)
		}
	}
}

func TestSkipsUnhealthyBackend(t *testing.T) {
	s1 := liveBackend(t, "a")
	defer s1.Close()
	s2 := liveBackend(t, "b")
	defer s2.Close()

	p, err := NewPool(testConfig(s1.URL, s2.URL), metrics.New(), testLogger())
	if err != nil {
		t.Fatal(err)
	}
	dead := p.backends[0]
	dead.recordFailure()
	dead.recordFailure() // opens the breaker (maxFails=2)
	if dead.Available() {
		t.Fatal("backend should be unavailable after maxFails")
	}
	for i := 0; i < 20; i++ {
		if got := p.Next(); got.ID == dead.ID {
			t.Fatal("Next returned an open-breaker backend")
		}
	}
}

func TestBreakerHalfOpensAfterCooldown(t *testing.T) {
	u, _ := url.Parse("http://localhost:9")
	b := newBackend("backend-1", u, "/", 2, 50*time.Millisecond)
	b.recordFailure()
	b.recordFailure()
	if b.probeDue(time.Now()) {
		t.Fatal("probe should not be due inside cooldown")
	}
	time.Sleep(60 * time.Millisecond)
	if !b.probeDue(time.Now()) {
		t.Fatal("probe should be due after cooldown")
	}
	b.recordSuccess()
	if !b.Available() {
		t.Fatal("backend should be available after successful probe")
	}
}

func TestServeHTTPRetriesDeadThenSucceeds(t *testing.T) {
	dead := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {}))
	deadURL := dead.URL
	dead.Close() // port now refuses connections: deterministic transport error

	live := liveBackend(t, "ok")
	defer live.Close()

	// Round-robin starts at backends[0], so the dead one is tried first.
	p, err := NewPool(testConfig(deadURL, live.URL), metrics.New(), testLogger())
	if err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodGet, "http://proxy/", nil)
	rec := httptest.NewRecorder()
	p.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (retry should have recovered)", rec.Code)
	}
	if got := rec.Header().Get("X-Proxy-Backend"); got != "backend-2" {
		t.Fatalf("X-Proxy-Backend = %q, want backend-2", got)
	}
	if body := rec.Body.String(); body != "ok" {
		t.Fatalf("body = %q, want ok", body)
	}
}

func TestServeHTTP502WhenAllDead(t *testing.T) {
	dead := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {}))
	deadURL := dead.URL
	dead.Close()

	p, err := NewPool(testConfig(deadURL), metrics.New(), testLogger())
	if err != nil {
		t.Fatal(err)
	}
	// Open the breaker so Next() yields nothing.
	p.backends[0].recordFailure()
	p.backends[0].recordFailure()

	rec := httptest.NewRecorder()
	p.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "http://proxy/", nil))
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", rec.Code)
	}
}

func TestDrainExcludesBackend(t *testing.T) {
	s1 := liveBackend(t, "a")
	defer s1.Close()
	s2 := liveBackend(t, "b")
	defer s2.Close()

	p, err := NewPool(testConfig(s1.URL, s2.URL), metrics.New(), testLogger())
	if err != nil {
		t.Fatal(err)
	}
	if !p.SetDrained("backend-1", true) {
		t.Fatal("SetDrained returned false for known backend")
	}
	for i := 0; i < 10; i++ {
		if got := p.Next(); got.ID != "backend-2" {
			t.Fatalf("drained backend still selected: %s", got.ID)
		}
	}
	if p.SetDrained("backend-9", true) {
		t.Fatal("SetDrained returned true for unknown backend")
	}
}

func TestConcurrentNextAndSnapshotRace(t *testing.T) {
	s1 := liveBackend(t, "a")
	defer s1.Close()
	s2 := liveBackend(t, "b")
	defer s2.Close()

	p, err := NewPool(testConfig(s1.URL, s2.URL), metrics.New(), testLogger())
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				if b := p.Next(); b != nil {
					b.ConnAdd()
					b.ConnDone()
				}
				_ = p.Snapshot()
			}
		}()
	}
	wg.Wait()
}
