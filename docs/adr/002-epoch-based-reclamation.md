# ADR-002: Epoch-Based Memory Reclamation

## Status
Accepted

## Context
The HybridLog storage engine requires concurrent access from multiple threads without traditional locking. We need a memory reclamation strategy that:
- Allows lock-free reads on the hot path
- Safely reclaims memory when no readers reference it
- Has minimal overhead per operation

Options considered:
1. **Reference counting (Arc)**: Simple but atomic increment/decrement on every access
2. **Hazard pointers**: Per-thread protection, good for few pointers
3. **Epoch-based reclamation (EBR)**: Amortized cost, good for many concurrent readers

## Decision
We use epoch-based reclamation via the `crossbeam-epoch` crate. Threads "pin" the current epoch when accessing shared data, and memory is only reclaimed when all threads have moved past the epoch in which the data was retired.

## Consequences
**Positive:**
- Near-zero overhead for read operations (just an epoch check)
- No per-object reference counting
- Well-tested implementation in crossbeam-epoch
- Natural fit for the HybridLog's read-mostly workload

**Negative:**
- Memory reclamation is deferred (slightly higher memory usage)
- Requires careful reasoning about epoch boundaries in unsafe code
- All unsafe blocks must document epoch-related safety invariants
