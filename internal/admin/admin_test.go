package admin

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"overpass/internal/balancer"
	"overpass/internal/config"
	"overpass/internal/metrics"
)

func testStack(t *testing.T) (*balancer.Pool, *metrics.Registry) {
	t.Helper()
	s1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, "a")
	}))
	t.Cleanup(s1.Close)
	s2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, "b")
	}))
	t.Cleanup(s2.Close)

	cfg := config.Config{
		Strategy: "round_robin", MaxAttempts: 2,
		HealthEveryMs: 50, HealthTimeoutMs: 500,
		Backends: []config.BackendConfig{
			{URL: s1.URL, HealthPath: "/", MaxFails: 2, FailTimeoutMs: 100},
			{URL: s2.URL, HealthPath: "/", MaxFails: 2, FailTimeoutMs: 100},
		},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	m := metrics.New()
	pool, err := balancer.NewPool(cfg, m, log)
	if err != nil {
		t.Fatal(err)
	}
	return pool, m
}

func TestHealthzAndMetrics(t *testing.T) {
	pool, m := testStack(t)
	srv := httptest.NewServer(New(pool, m, slog.New(slog.NewTextHandler(io.Discard, nil))))
	defer srv.Close()

	if res, err := http.Get(srv.URL + "/healthz"); err != nil || res.StatusCode != 200 {
		t.Fatalf("healthz: %v %v", res, err)
	} else {
		res.Body.Close()
	}

	res, err := http.Get(srv.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	var snap map[string]any
	if err := json.NewDecoder(res.Body).Decode(&snap); err != nil {
		t.Fatal(err)
	}
	if _, ok := snap["total"]; !ok {
		t.Fatalf("metrics missing total: %v", snap)
	}
}

func TestDrainRoundTrip(t *testing.T) {
	pool, m := testStack(t)
	srv := httptest.NewServer(New(pool, m, slog.New(slog.NewTextHandler(io.Discard, nil))))
	defer srv.Close()

	drain := func(id string, want int) {
		t.Helper()
		res, err := http.Post(srv.URL+"/admin/backends/"+id+"/drain", "", nil)
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		if res.StatusCode != want {
			t.Fatalf("drain %s: status %d, want %d", id, res.StatusCode, want)
		}
	}
	drain("backend-1", 200)
	drain("backend-9", 404)

	for _, b := range pool.Snapshot() {
		if b.ID == "backend-1" && !b.Drained {
			t.Fatal("backend-1 should be drained")
		}
	}

	res, err := http.Post(srv.URL+"/admin/backends/backend-1/undrain", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		t.Fatalf("undrain: status %d", res.StatusCode)
	}
}
