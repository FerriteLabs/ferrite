# ADR-002: Epoch-Based Memory Reclamation

- **Date:** 2026-02-20
- **Status:** Accepted

## Context

Ferrite requires lock-free concurrent reads for high-throughput key-value operations. Since
Rust has no garbage collector, we need an explicit memory reclamation strategy that allows
readers to safely access data without acquiring locks, while writers can deallocate stale
records without causing use-after-free bugs. The solution must integrate well with Rust's
ownership model and unsafe code guidelines.

## Decision

We use `crossbeam-epoch` for epoch-based memory reclamation. Under this scheme:

- Each thread "pins" the current epoch before accessing shared data, signaling that it
  may hold references to current records.
- Writers defer destruction of stale records until all threads have advanced past the
  epoch in which the record was removed.
- The epoch advances globally, and garbage is collected once no thread is pinned to an
  older epoch.

All `unsafe` blocks interacting with epoch-protected data carry `// SAFETY:` comments
explaining why the access is sound.

## Consequences

### Positive

- Lock-free reads with no contention on shared mutexes or read-write locks
- Proven concurrency pattern well-suited to Rust's safety model
- `crossbeam-epoch` is a mature, widely-audited crate in the Rust ecosystem
- Integrates naturally with the HybridLog mutable region for concurrent access

### Negative

- Deferred cleanup means memory is not freed immediately after a record becomes unreachable
- Under sustained write-heavy workloads, deferred garbage can accumulate temporarily
- Requires disciplined `// SAFETY:` annotations on all `unsafe` blocks; missing annotations
  are a code-review blocker
- Developers must understand epoch semantics to avoid subtle lifetime bugs
