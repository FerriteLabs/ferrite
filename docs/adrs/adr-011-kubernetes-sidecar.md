# ADR-011: Kubernetes Sidecar Mode

## Status

**Proposed** — February 2026

## Context

In Kubernetes environments, applications frequently need low-latency caching but face network overhead when connecting to a centralized cache cluster (even within the same cluster). A sidecar pattern — where Ferrite runs alongside the application pod — provides sub-millisecond cache access with transparent fallback to a central Ferrite cluster.

This is similar to how Envoy works as a sidecar proxy, but for caching.

## Decision

Implement a lightweight sidecar mode for Ferrite that runs as a co-located cache with transparent cluster fallback.

### Architecture

```
┌─── Application Pod ──────────────────────────┐
│                                               │
│  ┌─────────────────┐  ┌───────────────────┐  │
│  │  Application     │  │  Ferrite Sidecar   │  │
│  │  Container       │──│  Container          │  │
│  │  (connects to    │  │  - 128MB RAM limit  │  │
│  │   localhost:6379)│  │  - LRU eviction     │  │
│  └─────────────────┘  │  - Read-through      │  │
│                        │  - Write-through     │  │
│                        └──────┬──────────────┘  │
│                               │                  │
└───────────────────────────────┼──────────────────┘
                                │ (cache miss)
                                ▼
                    ┌──────────────────────┐
                    │  Ferrite Cluster      │
                    │  (central, persistent)│
                    └──────────────────────┘
```

### Sidecar Behavior

**Read path (cache hit):**
1. App → `GET key` → Sidecar (localhost, <0.1ms)
2. Sidecar has key in local memory → return immediately

**Read path (cache miss):**
1. App → `GET key` → Sidecar (localhost)
2. Sidecar misses → forwards to Cluster
3. Cluster returns value → Sidecar caches locally + returns to App

**Write path:**
1. App → `SET key value` → Sidecar
2. Sidecar stores locally + forwards to Cluster (async write-through)
3. Cluster confirms → Sidecar acknowledges

### Configuration

```toml
# ferrite-sidecar.toml
[sidecar]
enabled = true
mode = "read-through"        # read-through | write-through | write-behind
upstream = "ferrite-cluster.default.svc:6379"
local_memory = "128mb"       # Local cache size
eviction = "lru"             # LRU | LFU | random
ttl_default = 300            # Default TTL for cached entries (seconds)
connect_timeout = "100ms"    # Upstream connect timeout
read_timeout = "50ms"        # Upstream read timeout

[sidecar.prefetch]
enabled = true
patterns = ["session:*", "config:*"]  # Pre-warm these patterns

[sidecar.metrics]
enabled = true
port = 9091                  # Separate from main metrics
```

### Kubernetes Deployment

**Sidecar injection (manual):**
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: my-app
spec:
  template:
    spec:
      containers:
        - name: app
          image: my-app:latest
          env:
            - name: REDIS_URL
              value: "redis://localhost:6379"
        - name: ferrite-sidecar
          image: ferritelabs/ferrite:latest
          args: ["--config", "/etc/ferrite/sidecar.toml"]
          resources:
            limits:
              memory: "128Mi"
              cpu: "100m"
          ports:
            - containerPort: 6379
            - containerPort: 9091  # Metrics
```

**Sidecar injection (automatic via MutatingWebhook):**
```yaml
# Add annotation to enable auto-injection
metadata:
  annotations:
    ferrite.io/sidecar-inject: "true"
    ferrite.io/sidecar-memory: "128Mi"
    ferrite.io/sidecar-upstream: "ferrite-cluster.default.svc:6379"
```

### Implementation Phases

1. **Phase 1** (2 weeks): `--sidecar` mode flag with upstream forwarding on cache miss
2. **Phase 2** (2 weeks): Read-through + write-through + write-behind strategies
3. **Phase 3** (1 week): Pattern-based prefetching from upstream
4. **Phase 4** (2 weeks): Kubernetes MutatingWebhook for auto-injection
5. **Phase 5** (1 week): Grafana dashboard for sidecar hit rates + latency

### Key Design Decisions

- **Protocol transparency**: Sidecar speaks RESP, same as any Redis. App doesn't know it's talking to a sidecar.
- **Consistency model**: Eventual consistency. Sidecar cache may be stale for up to `ttl_default` seconds.
- **Failure mode**: If upstream is unreachable, serve stale data from local cache (configurable).
- **Memory management**: Strict LRU eviction within `local_memory` budget. No swapping.

## Consequences

### Positive
- Sub-millisecond cache access for co-located applications
- Zero application code changes (same Redis protocol)
- Reduces load on central Ferrite cluster
- Natural Kubernetes deployment story

### Negative
- Memory overhead per pod (128MB default)
- Eventual consistency — stale reads possible
- Increased operational complexity (sidecar per pod)
- Cache invalidation across sidecars is eventually consistent

### Alternatives Considered
- **Client-side caching (Redis 6 tracking)**: Requires client support, more complex
- **Envoy cache filter**: Limited to HTTP, doesn't support Redis protocol
- **Istio/service mesh caching**: Too heavy for simple key-value caching
