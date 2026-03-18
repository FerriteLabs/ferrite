# Wave 2 Exit Gate — Lucidity + Chronicle

## Hard requirements

### Lucidity (M3)

- [ ] At GA before EU AI Act enforcement (Aug 2026 absolute deadline).
- [ ] At least one audit-firm partner case study published.
- [ ] Compliance mapping doc reviewed by external counsel.
- [ ] Forge-detection test green in CI for the audit log.

### Chronicle (M4)

- [ ] At Beta with branch-per-PR pattern documented.
- [ ] At least 2 design partners using branches in production-ish workloads.
- [ ] Storage GC verified on a 100 GB dataset with no live-branch leakage.

### Shared infrastructure

- [ ] Merkle hash-chain crate is **shared** between Lucidity and Concord (no duplicated
  implementation). If divergence emerged during Wave 2, it must be reconciled before
  this gate.
- [ ] ADR-018 (Mnemo) tombstones now leverage Lucidity proofs (closes M1 open question 3).

### Cross-cutting

- [ ] Hiring: distributed-systems specialist hired or contracted for Concord (`cw1-hiring` continued).
- [ ] Docs/observability/bench harness all current with both moonshots.

## Decision protocol

Same as Wave 1 gate doc. If Lucidity slips past Aug 2026, halt all other work and
prioritise: regulatory deadline outweighs portfolio sequencing.

## Dependency unlocks

| Wave 3 task | Unlocked by |
|---|---|
| `m5-p0-spike` (Concord spike) | Wave 2 gate (Lucidity Merkle infra reused) |
| `m6-p0-spike` (Pangea spike) | Wave 2 gate + hyperscaler CXL availability signal |
