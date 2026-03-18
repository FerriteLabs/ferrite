# Concord (M5) — Implementation Roadmap

> Goal: native CRDT types + active-active multi-master geo-replication via δ-CRDTs +
> Merkle anti-entropy (reusing Lucidity).

## Phase index

| ID | Phase | Effort | Acceptance |
|---|---|---|---|
| m5-p0-spike | Spike | 4 wk | δ-CRDT framework picked; TLA+ merge proofs |
| m5-p1-single | Single-region | 8 wk | COUNTER + OR-SET working with API + tests |
| m5-p2-multi | Multi-region | 10 wk | 2-region active-active demo |
| m5-p3-nregion | N-region + chaos | 10 wk | partition tolerance; 3 design partners |
| m5-p4-ga | GA | 6 wk | data-sovereignty routing; Studio conflict UI |

## Files & crates

```
crates/ferrite-concord/
├── src/
│   ├── lib.rs
│   ├── types/
│   │   ├── counter.rs      # PN-Counter
│   │   ├── orset.rs        # Observed-Remove Set
│   │   ├── lwwmap.rs       # LWW-Map
│   │   ├── reg.rs          # MV-Register
│   │   └── seq.rs          # RGA / Treedoc-style sequence
│   ├── dvv.rs              # dotted-version-vector + compaction
│   ├── delta.rs            # δ-CRDT propagation envelope
│   ├── gossip.rs           # gossip transport
│   ├── antientropy.rs      # Merkle-tree-based anti-entropy (uses ferrite-lucidity)
│   └── routing.rs          # data-sovereignty rules
src/commands/handlers/concord.rs   # CRDT.* commands
```

## Per-phase deliverables

### P0 spike

- ADR-022: choice of δ-CRDT framework (Almeida et al. 2018 baseline).
- TLA+ models for COUNTER + OR-SET; TLC model-checked under partition + duplication.
- Throwaway prototype: single-process counter convergence test.

### P1 single-region prototype

- Commands:
  ```
  CRDT.COUNTER.INCR  <key> [BY n]              -> new value
  CRDT.COUNTER.GET   <key>                     -> value
  CRDT.SET.ADD       <key> <member>            -> OK
  CRDT.SET.REMOVE    <key> <member>            -> OK
  CRDT.SET.MEMBERS   <key>                     -> array
  ```
- Tests: convergence under reordering, duplicate delivery; type-mismatch rejection.

### P2 multi-region

- Gossip layer with anti-entropy via Merkle trees (reusing `ferrite-lucidity`).
- 2-region demo: AWS us-east-1 + eu-west-1 with simulated WAN latency.
- Telemetry: convergence-time metric (`concord.convergence_seconds` histogram).

### P3 N-region + chaos

- Partition-tolerance tests via toxiproxy.
- DVV compaction strategy + benchmark of metadata growth.
- 3 external design partners running production-shaped workloads.

### P4 GA

- Data-sovereignty routing: per-key region pinning rules (e.g. `concord.routing.eu = "*:user:eu:*"`).
- Studio conflict UI: time-series of conflicts per region pair.
- Admin tools: force-resync of a region, manual conflict resolution.
- ADR flipped to `Accepted (GA)`.

## Reuse of Lucidity

- Merkle accumulator from `ferrite-lucidity` is used directly for anti-entropy
  comparisons. **No duplicate Merkle implementation.**
- If Lucidity slipped, Concord P2 cannot start.

## Risks

| Risk | Mitigation |
|---|---|
| Vector-clock metadata blowup | DVV compaction; bounded clock cardinality; reject writes that would exceed bound |
| CRDT semantics confuse users | conservative defaults (counters and sets only at P1); per-type docs with merge examples |
| Gossip storms at scale | configurable fan-out + back-pressure; circuit breaker |
| Conflict-resolution UX | Studio UI mandatory before Beta |
