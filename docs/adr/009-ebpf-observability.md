# ADR-009: eBPF-Based Observability

## Status
Proposed

## Context

Ferrite currently uses tracing + Prometheus metrics + optional OpenTelemetry for observability.
While comprehensive, these approaches require code instrumentation which:
- Adds overhead to hot paths (even when disabled, branch prediction costs exist)
- Requires recompilation to add new tracing points
- Cannot observe kernel-level I/O behavior (io_uring completions, syscall latencies)
- Cannot provide zero-overhead production profiling

eBPF (extended Berkeley Packet Filter) allows attaching programs to kernel and userspace
tracepoints without modifying the application binary. This is increasingly standard in
production database deployments (see: bpftrace for PostgreSQL, eBPF-based Redis monitoring).

## Decision

We propose adding optional eBPF-based observability as a **complement** to existing
instrumentation (not a replacement). The approach:

### Phase 1: USDT Probes (Low Effort)
Add User Statically-Defined Tracing (USDT) probe points to key paths:
- `ferrite:command:start` / `ferrite:command:end` — command latency
- `ferrite:storage:read` / `ferrite:storage:write` — storage tier access
- `ferrite:replication:sync` — replication events
- `ferrite:connection:accept` / `ferrite:connection:close` — connection lifecycle

Implementation: Use the `probe` crate or raw `asm!` to emit USDT markers.
These have **zero overhead** when no tracer is attached.

### Phase 2: BPF Programs (Moderate Effort)
Ship a `ferrite-bpf/` directory with:
- Pre-built bpftrace scripts for common monitoring scenarios
- A `ferrite-top` tool (like `redis-cli --stat` but using eBPF)
- Latency histograms without sampling

### Phase 3: io_uring Observability (Advanced)
Attach to io_uring kernel tracepoints to monitor:
- Submission queue depth and completion latencies
- Disk I/O patterns and queue saturation
- Memory-mapped region access patterns

### Requirements
- Linux 5.8+ kernel (for CO-RE / BTF support)
- libbpf or aya (Rust eBPF framework)
- Feature-gated behind `--features ebpf`
- macOS/Windows: graceful no-op (USDT probes simply don't fire)

## Consequences

### Positive
- Zero-overhead production tracing (probes are NOPs when unattached)
- Kernel-level visibility into io_uring and filesystem behavior
- No recompilation needed to add new monitoring
- Standard tooling compatibility (bpftrace, perf, SystemTap)

### Negative
- Linux-only (eBPF programs don't work on macOS/Windows)
- Requires elevated privileges (CAP_BPF or root) to attach probes
- Additional build complexity for BPF program compilation
- USDT probes add a small binary size overhead (~1 byte per probe point)

### Risks
- eBPF API stability across kernel versions
- User confusion about which observability approach to use
- Maintenance burden for BPF programs across kernel versions

## References
- [aya (Rust eBPF)](https://github.com/aya-rs/aya)
- [bpftrace](https://github.com/bpftrace/bpftrace)
- [USDT probes in Rust](https://docs.rs/probe/latest/probe/)
- [eBPF for Redis monitoring](https://www.brendangregg.com/blog/2016-10-12/linux-bcc-nodejs-usdt.html)
