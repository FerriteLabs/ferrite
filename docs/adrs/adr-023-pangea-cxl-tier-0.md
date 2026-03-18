# ADR-023: Pangea — CXL Tier-0 Storage

**Status:** Proposed (P2 complete)
**Date:** 2026-04-18
**Author:** FerriteLabs
**Supersedes:** ADR-023 (Spike)

## Context

Ferrite's HybridLog is a three-tier engine (mutable RAM → read-only mmap
→ disk with io_uring). All three tiers assume a single-host memory
hierarchy. CXL 3.0 (Compute Express Link) hardware shipping in 2025
introduces a fourth tier: pooled, byte-addressable, cache-coherent
memory shared across multiple hosts at ~200ns latency — between local
DRAM (~80ns) and NVMe (~10µs).

Two real workloads benefit immediately:

1. **Cross-host RECALL in Mnemo (ADR-018)**: an agent's hot working set
   on host A is needed on host B during failover or relocation; today
   this means re-loading from disk.
2. **Forge module cache (ADR-019)**: large compiled wasm modules
   (~50 MiB each for big LLM-distilled functions) duplicated across all
   workers waste DRAM. Pooled CXL holds one shared copy.

No KV store today exposes a CXL tier. Vendors (Samsung, SK Hynix,
Micron) ship the hardware with vendor-specific shims; database support
is a green field.

## Decision

Build **Pangea**: a Tier-0 storage layer that uses CXL.mem-attached
pools for cross-host shared memory, sitting above mmap and below
mutable-RAM in the read path, and used as a write-shadow tier for
Mnemo and Forge specifically.

Architecture rule: Pangea is **opt-in per keyspace**. The default
HybridLog behavior is unchanged so deployments without CXL hardware see
zero overhead.

### Non-goals

- Not a distributed memory disaggregation system (e.g. RAMCloud). Pangea
  is a tier within the existing HybridLog, not a replacement engine.
- Not a substitute for replication. Pangea pools fail; pages held only
  there are not durable.
- Not a NUMA scheduler. The OS kernel keeps doing NUMA placement;
  Pangea explicitly *opts pages out* of NUMA-local placement when they
  go to the pool.

## Data model

Pages in the pool carry a per-page header:

```
struct CxlPage {
    owner_host_id: u64,
    epoch: u64,            // for fence/invalidate
    flags: u32,            // tier, dirty, pinned
    payload: [u8; PAGE - 16],
}
```

A per-keyspace bitmap in regular DRAM tracks which pages currently live
in the CXL pool vs in the local mmap tier.

## APIs (Phase-0 contract)

There is no client-facing command surface — Pangea is engine-internal.
The configuration surface is:

```toml
[storage.tier0]
enabled = true
device  = "cxl0"                # /dev/dax0.0 or vendor-specific
size    = "32G"
keyspaces = ["__ferrite:mnemo:*", "__ferrite:forge:*"]
policy  = "lru"                 # lru | hot-set | manual
```

A new admin command `STG.TIER0 STATS` reports per-keyspace usage,
hit-rate, and eviction counters.

## Tenancy & isolation

Pages are tagged with their owning tenant. Eviction policy is
tenant-aware: a tenant's working set cannot displace another tenant's
pinned working set beyond a configurable share.

## Composition diagram

```
read path (with Pangea enabled for the keyspace):
  Client ──► dispatch ──► mutable RAM (epoch latch)
                              │ miss
                              ▼
                          Tier-0 (CXL pool, ~200ns)
                              │ miss
                              ▼
                          Tier-1 (mmap, ~1µs)
                              │ miss
                              ▼
                          Tier-2 (io_uring/NVMe, ~10µs)

write path:
  Client ──► dispatch ──► mutable RAM
                              │
                              └─► (async) demote to Tier-0 on cool
```

Promotion to mutable RAM happens on access; demotion to CXL pool happens
on cooling under the keyspace policy.

## Phase-0 deliverables

- `crates/ferrite-pangea` spike crate with:
  - A `CxlAllocator` trait (page alloc/free/map/unmap on a pool).
  - An `InMemoryCxlAllocator` reference impl backed by a regular
    `Vec<u8>` so unit tests run on developer laptops without hardware.
  - A `Tier0Cache` LRU with the per-keyspace eviction policy.
- ADR-023 (this doc) promoted from spike to Proposed.
- Phase 0 → Phase 1 exit criterion: `Tier0Cache` benchmarks at 10 M
  ops/s on the in-memory allocator (validating the cache mechanics
  before hardware is in the loop).

## Phase 1 deliverables

- A real `LinuxCxlAllocator` against `/dev/dax*` using `mmap` with
  the right flags.
- Engine integration: Mnemo + Forge keyspaces opt in via config;
  HybridLog promotes/demotes pages.
- Multi-host coherence test: two ferrite processes pointed at the same
  pool see consistent reads under the cache-coherence model the
  hardware exposes.

## Eval plan (Phase 2)

- Mnemo recall p99 with hot working set 4× DRAM size: ≥ 5× faster than
  the same workload using only Tier-1/Tier-2.
- Forge module load time when 16 workers share one 50 MiB compiled
  module: ≤ 1.1× single-load time (vs ~16× on pure DRAM today).
- Failover RTO with `policy = hot-set`: ≤ 5 s for 1 GiB working set
  (replica picks up pages from the pool without re-reading from disk).
- Zero throughput regression on workloads where Tier-0 is disabled.

## Consequences

- New optional dependency on the CXL kernel ABI (`cxl_dax`, ndctl).
  Builds without that ABI fall back to the in-memory allocator and
  refuse to enable `[storage.tier0]` at runtime.
- Replication semantics: pages held only in Tier-0 are not durable.
  Replicators must demote dirty Tier-0 pages to Tier-2 (io_uring) on
  promotion, or when explicit `BGSAVE` runs.
- Operational surface: monitoring needs new metrics (pool fill,
  eviction rate, cross-host read share).

## Open questions

1. Hardware vendor — multi-vendor support from day one (Samsung CMM-D,
   SK Hynix Niagara, Micron CZ120) or pick one for P1 and add others
   in P3?
2. Coherence model — do we rely on hardware cache coherence (CXL.cache)
   or use software-managed consistency (CXL.mem only)?
3. Failure model — is a CXL pool partition a host fault or a
   storage-layer fault from the engine's perspective?

## Exit criteria for Phase 0

- ferrite-pangea builds with `Tier0Cache` + `InMemoryCxlAllocator`.
- LRU eviction proven correct under property tests.
- 10 M ops/s on the in-memory allocator on a developer laptop.
- Reference design doc shows how a real CXL allocator slots in without
  changes to the cache layer.

## Development note

In development mode (no real CXL hardware), Pangea uses an
`InMemoryCxlAllocator` backed by a `Vec<u8>` arena. This simulated CXL
tier lets all unit tests, benchmarks, and the `PNG.*` command surface run
on standard developer laptops. The `PNG.DETECT` command reports
`detection_method: Simulated` in this mode. Real hardware integration
(via `/dev/dax*`) is planned for Phase 3.
