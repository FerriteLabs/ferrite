# ADR-003: Thread-Per-Core Architecture

- **Date:** 2026-02-20
- **Status:** Accepted

## Context

Ferrite needs to maximize throughput and minimize latency for database operations. Traditional
thread-pool architectures incur overhead from context switching, lock contention, and cache
invalidation when multiple threads access shared data structures. For a high-performance
key-value store, cross-thread synchronization is a primary bottleneck.

## Decision

We adopt a thread-per-core architecture where:

- One shard and one io_uring instance are pinned to each CPU core.
- Keys are routed to cores via `hash(key) % num_cores`, ensuring that all operations
  for a given key execute on the same core.
- Each core owns its shard exclusively — no shared mutable state between cores for
  normal key-value operations.
- Network I/O is distributed across cores, with each core handling its own set of
  client connections.

## Consequences

### Positive

- Near-zero cross-thread contention for key-value operations
- Optimal CPU cache utilization since each core works on its own data
- Predictable latency without lock convoys or priority inversions
- Each io_uring instance operates independently, maximizing disk I/O parallelism

### Negative

- Single-key operations are limited to one core's throughput
- Uneven key distribution can cause hot-core imbalance
- Multi-key transactions spanning cores require cross-shard coordination
- Core count determines maximum shard count, limiting vertical scaling flexibility
