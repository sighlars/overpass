package metrics

import (
	"strings"
	"testing"
	"time"
)

func TestObserveAndSnapshot(t *testing.T) {
	m := New()
	m.Observe("backend-1", 200, 5*time.Millisecond)
	m.Observe("backend-1", 500, 5*time.Millisecond)
	m.Observe("backend-2", 0, time.Millisecond)

	s := m.Snapshot()
	if s.Total != 3 || s.OK != 1 || s.Failed != 2 {
		t.Fatalf("counters wrong: %+v", s)
	}
	if s.Backends["backend-1"].Requests != 2 || s.Backends["backend-1"].Errors != 1 {
		t.Fatalf("backend-1 view wrong: %+v", s.Backends["backend-1"])
	}
	if s.Backends["backend-1"].AvgMs <= 0 {
		t.Fatal("expected positive avg latency")
	}
}

func TestNoBackend(t *testing.T) {
	m := New()
	m.NoBackend()
	s := m.Snapshot()
	if s.Unavailable != 1 || s.Failed != 1 || s.Total != 1 {
		t.Fatalf("counters wrong: %+v", s)
	}
}

func TestPrometheusFormat(t *testing.T) {
	m := New()
	m.Observe("backend-1", 200, time.Millisecond)
	out := m.Prometheus()
	for _, want := range []string{
		"overpass_requests_total 1",
		`overpass_backend_requests_total{backend="backend-1"} 1`,
		"# TYPE overpass_inflight_requests gauge",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("prometheus output missing %q:\n%s", want, out)
		}
	}
}
