# ADR-001: HybridLog Tiered Storage Engine

- **Date:** 2026-02-20
- **Status:** Accepted

## Context

Ferrite needs a storage engine that balances memory speed with disk capacity. Traditional
in-memory stores (like Redis) are fast but limited by available RAM. Pure disk-based stores
offer capacity but sacrifice latency. We need an architecture that provides the speed of
memory with the capacity of disk, while maintaining predictable performance characteristics
for a key-value store workload.

## Decision

We adopt a Microsoft FASTER-inspired HybridLog storage engine with three tiers:

1. **Mutable Region (memory)** — Hot data lives in-memory for sub-microsecond access.
   Writes always go here first.
2. **Read-Only Region (mmap)** — Warm data is memory-mapped. Reads are fast but updates
   require a copy-up to the mutable region.
3. **Disk Region (io_uring)** — Cold data is stored on disk, accessed via io_uring for
   async I/O on Linux (with tokio::fs fallback on other platforms).

Data flows from mutable → read-only → disk as memory pressure increases. A head-offset
and tail-offset track the boundaries between tiers.

## Consequences

### Positive

- Enables Ferrite's core "speed of memory, capacity of disk" value proposition
- Hot data stays in memory for the fastest possible access
- Dataset can exceed available RAM without crashing or evicting hot keys
- io_uring integration provides near-optimal async disk I/O on Linux

### Negative

- Adds complexity in compaction and tier management logic
- Read-modify-write operations on cold data require tier promotion (copy-up)
- Tuning tier boundaries and compaction thresholds requires workload-specific configuration
- The mmap read-only region may cause unpredictable page-fault latencies under memory pressure
