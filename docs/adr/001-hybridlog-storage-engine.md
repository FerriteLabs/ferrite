# ADR-001: HybridLog Storage Engine

## Status
Accepted

## Context
Ferrite needs a storage engine that can handle datasets larger than available RAM while maintaining sub-millisecond latency for hot data. Traditional approaches force a choice between in-memory speed (Redis) or disk capacity (RocksDB/LevelDB), but not both.

Microsoft's FASTER research paper demonstrated a three-tier log-structured approach that provides memory-speed access for hot data with automatic tiering to disk for cold data.

## Decision
We adopt a HybridLog-based storage engine inspired by Microsoft FASTER with three tiers:

1. **Mutable Region (Memory)**: Hot data with in-place updates using DashMap and epoch-based reclamation
2. **Read-Only Region (mmap)**: Warm data with zero-copy reads via memory-mapped files
3. **Disk Region (io_uring/tokio)**: Cold data with async sequential I/O

Key design choices:
- Thread-per-core architecture with per-thread io_uring instances
- Epoch-based reclamation instead of garbage collection
- Automatic data movement between tiers based on access patterns

## Consequences
**Positive:**
- Hot data access at memory speed (<100ns)
- Can handle datasets much larger than RAM
- Predictable latency (no GC pauses)

**Negative:**
- More complex than pure in-memory stores
- Requires careful unsafe code in the mmap layer
- io_uring is Linux-only (graceful fallback to tokio::fs on other platforms)
