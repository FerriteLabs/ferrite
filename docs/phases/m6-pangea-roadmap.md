# Pangea (M6) — Implementation Roadmap

> Goal: CXL Tier-0 memory tier between Mutable and Read-Only mmap, transparent to users.

## Phase index

| ID | Phase | Effort | Acceptance |
|---|---|---|---|
| m6-p0-spike | Spike | 4 wk | bench numbers vs DRAM on real CXL hardware |
| m6-p1-tier | Tier integration | 6 wk | transparent CXL tier with telemetry |
| m6-p2-policy | Promotion/demotion policy | 5 wk | ≤ 5% perf regression at 2× DRAM capacity |
| m6-p3-hyperscaler | Hyperscaler validation | 8 wk | AWS/Azure/GCP CXL preview case study |
| m6-p4-ga | GA | 5 wk | pricing/sizing guidance, ops docs |

## Files & crates

```
crates/ferrite-pangea/
├── src/
│   ├── lib.rs
│   ├── tier.rs           # CXL tier abstraction (between Mutable and ReadOnly)
│   ├── alloc.rs          # NUMA-aware allocator using libnuma
│   ├── policy.rs         # promotion/demotion based on access freq + recency
│   ├── telemetry.rs      # per-tier hit rates, latency, migration counters
│   └── feature.rs        # runtime detection of CXL availability
crates/ferrite-core/src/storage/hybrid_log.rs   # add CXL tier hook (no-op without ferrite-pangea)
```

## Per-phase deliverables

### P0 spike

- Hardware procurement: Marvell Structera S or equivalent + CXL-capable server.
- Latency characterization: CXL load p50/p99 vs DRAM, per-NUMA-node.
- Decision: CXL Type-3 (memory expander) target; Type-2 (accelerator) deferred.
- ADR-023 documents the tier integration design.

### P1 tier integration

- New tier slot in `ferrite-core/src/storage/hybrid_log.rs` between Mutable and Read-Only.
- Behind `pangea` feature flag; no-op when disabled or no CXL detected.
- Allocator uses `numa_alloc_onnode` (libnuma) for explicit CXL placement.
- Telemetry: per-tier hit rate, p99 access latency, migration events.

### P2 promotion/demotion policy

- Working-set-aware migration: hot CXL pages promoted to DRAM; cold DRAM demoted.
- Pressure-driven: triggers on DRAM pressure threshold.
- Acceptance bench: ≤ 5% perf regression vs all-DRAM at 2× DRAM working-set size.

### P3 hyperscaler validation

- AWS / Azure / GCP CXL preview enrolment (status of each tracked in
  `docs/phases/cxl-availability-tracker.md`).
- Validate on each cloud's preview hardware; publish case study.

### P4 GA

- Pricing/sizing calculator: $/GiB savings vs DRAM at common working-set ratios.
- Ops docs: detection, monitoring, troubleshooting.
- ADR flipped to `Accepted (GA)`.

## Risks

| Risk | Mitigation |
|---|---|
| CXL hardware availability slips | watch list trigger in main plan; reschedule per CXL-tracker doc |
| Cross-NUMA latency wrecks p99 | NUMA-aware allocator + scheduling; bench on real hardware not emulator |
| Limited initial customer demand (cost not yet in their favour) | target only customers with > 256 GiB working sets |
| Wasm/io-uring subsystems regress on Pangea-enabled builds | strict feature-flag isolation; CI matrix with/without `pangea` |
