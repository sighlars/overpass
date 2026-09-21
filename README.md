# overpass

A concurrent L7 reverse proxy and load balancer in Go. One binary, stdlib only:
active health checks, per-backend circuit breakers, three balancing strategies,
bounded retries, runtime drain, and Prometheus metrics.

```
clients ──► :8080 (proxy) ──┬──► backend-1 ──┐
                             ├──► backend-2 ──┤ health-checked,
                             └──► backend-3 ──┘ circuit-broken, drained live

              :9090 (admin) ── /healthz /metrics /admin/backends
```

## Why this exists

A load balancer is the densest honest demo of Go concurrency: every request
fans out across goroutines, shared backend state is hot, failures arrive
concurrently, and shutdown must not drop in-flight work. No frameworks, no
codegen — just goroutines, channels-free atomics, mutexes, and contexts.

| Primitive | Where | Why |
|---|---|---|
| `atomic.Uint64/Bool/Int64` | Round-robin counter, alive flags, conn counts, request IDs, metric counters | Hot path: no lock contention per request |
| `sync.RWMutex` | Backend list, failure bookkeeping, metric label map | Read-heavy or cold paths where mutex clarity wins |
| `context` | Health probes, shutdown, client-cancelled retries | Deadlines and cancellation propagate, never leak |
| `sync.WaitGroup` | Health loop lifecycle | Clean stop after in-flight probes finish |
| `http/httputil.ReverseProxy` | Per-attempt proxying over one shared `http.Transport` | Connection reuse without third-party deps |
| `slog` | Structured JSON logs with per-attempt fields | Observable without a logging framework |

## Quickstart

```bash
# 1. Start three demo backends (separate terminals)
go run ./demo/echo -port 9001 -name alpha
go run ./demo/echo -port 9002 -name bravo
go run ./demo/echo -port 9003 -name charlie

# 2. Start the balancer (reads overpass.json)
go run ./cmd/overpass

# 3. Watch traffic rotate
curl -s localhost:8080
curl -s -D - localhost:8080 -o /dev/null | grep -i x-proxy-backend

# 4. Drain one backend live, keep hammering it
curl -X POST localhost:9090/admin/backends/backend-1/drain
curl -s localhost:9090/admin/backends | jq .

# 5. Metrics (JSON + Prometheus)
curl -s localhost:9090/metrics | jq .
curl -s localhost:9090/metrics/prometheus
```

Flags override the config file: `-listen :8080 -admin-listen :9090
-strategy least_conn -config overpass.json`.

## Endpoints

| Listener | Endpoint | Purpose |
|---|---|---|
| `:8080` | `/*` | Proxied traffic (`X-Proxy-Backend`, `X-Request-ID` headers) |
| `:9090` | `GET /healthz` | Liveness |
| `:9090` | `GET /metrics` | JSON counters, per-backend avg latency |
| `:9090` | `GET /metrics/prometheus` | Prometheus text exposition |
| `:9090` | `GET /admin/backends` | Alive/drained/conns/fails per backend |
| `:9090` | `POST /admin/backends/{id}/drain` | Park a backend (finishes in-flight) |
| `:9090` | `POST /admin/backends/{id}/undrain` | Return it to rotation |

## Design notes

- **Retry semantics:** transport errors retry on the next backend up to
  `max_attempts` (capped by backend count). An upstream 5xx is served as-is
  but counts against that backend's breaker — retrying served responses would
  double side effects.
- **Breaker half-open:** only the active health loop probes dead backends
  after `fail_timeout_ms`. Live traffic never touches an open breaker, so a
  recovering backend can't be thunder-herded.
- **Least-conn is approximate:** in-flight counts are atomic but sampled at
  pick time. Exact fairness would need a lock on the hot path; this trades a
  little precision for zero contention.
- **Shutdown:** SIGINT/SIGTERM stops listeners, gives 10s for drain, then
  halts the health loop. Client-cancelled requests abort retries immediately.

## Test

```bash
go vet ./...
go test -race ./...
```

`TestConcurrentNextAndSnapshotRace` hammers selection and snapshots from 8
goroutines; everything is expected clean under `-race`.

## Layout

```
cmd/overpass      flags, wiring, graceful shutdown
internal/config   JSON config, defaults, validation
internal/balancer pool, strategies, proxy+retry, breaker, health loop
internal/metrics  atomic counters, JSON snapshot, Prometheus text
internal/admin    sidecar HTTP API
demo/echo         toy backend (name reporting, /healthz, delay flag)
```

MIT.
