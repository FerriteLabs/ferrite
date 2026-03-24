# ferrite-pangea

CXL Tier-0 memory abstraction for Ferrite — see
[ADR-023](../../docs/adrs/adr-023-pangea-cxl-tier0.md).

A `CxlAllocator` trait + an in-memory reference implementation, plus
a `Tier0Cache` LRU and a NUMA-style multi-node topology.

## Status

🧪 **Spike (P1).** Self-contained workspace crate. Not yet wired into
the main `ferrite` binary; the integration handlers (`PNG.*` command
family) ship in a later phase. Real CXL hardware integration ships in P2.

## Components

| Module | Purpose |
|---|---|
| `allocator` | `CxlAllocator` trait + `InMemoryCxlAllocator` (Vec-backed arena, fixed-size pages, first-fit reuse) |
| `cache` | `Tier0Cache<A>` per-keyspace LRU with hit/miss/eviction counters and pluggable `EvictionPolicy` |
| `topology` | `NumaTopology<A>` routes allocations across N nodes via `HashMod` / `LeastUsed` / `RoundRobin`, owns the key→`Locator` index |
| `working_set` | Sliding-window hit tracker with `top(n)` and `promotion_candidates(min_hits)` |

## Quick start

```rust
use std::sync::Arc;
use ferrite_pangea::{CxlAllocator, EvictionPolicy, InMemoryCxlAllocator, Tier0Cache};

let alloc = InMemoryCxlAllocator::shared(64 * 1024, 4096);
let cache: Tier0Cache<InMemoryCxlAllocator> =
    Tier0Cache::new(alloc, 4, EvictionPolicy::Lru);

cache.insert("k".into(), b"hello".to_vec()).unwrap();
assert_eq!(cache.get("k"), Some(b"hello".to_vec()));
```

NUMA topology:

```rust
use ferrite_pangea::{InMemoryCxlAllocator, NumaTopology, RoutingPolicy};

let nodes = (0..4)
    .map(|_| InMemoryCxlAllocator::shared(64 * 1024, 256))
    .collect();
let topo = NumaTopology::new(nodes, RoutingPolicy::HashMod);
topo.allocate("user:42", b"alice").unwrap();
assert_eq!(topo.read("user:42"), Some(b"alice".to_vec()));
```

## Promotion / demotion (sketch)

```rust
use ferrite_pangea::WorkingSet;

let ws = WorkingSet::new(1_000);
for _ in 0..50 { ws.record("hot:profile:42"); }
let hot = ws.promotion_candidates(20); // keys hit ≥ 20 times in last 1k
```

## Testing

```sh
cargo test -p ferrite-pangea
cargo clippy -p ferrite-pangea --all-targets -- -D warnings
```

23 unit tests + 1 doc test, strict-clippy clean.

## License

Same as the parent workspace.
