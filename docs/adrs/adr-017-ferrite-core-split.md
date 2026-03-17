# ADR-017: Splitting `ferrite-core` into Focused Sub-Crates

## Status

Proposed

## Date

2026-04-17

## Context

`ferrite-core` is the foundational crate of the Ferrite workspace. It contains the storage engine, RESP protocol, persistence, and many cross-cutting concerns. Over the v0.1 → v0.3 evolution it has grown to:

- **~170,000 lines of Rust source** (≈47% of the entire workspace)
- **247 source files** across 30+ top-level modules
- The largest single files are `persistence/backup/manager.rs` (2,679 LOC), `query/parser.rs` (2,286), `config.rs` (2,155), `auth/acl.rs` (2,138), `compatibility/suite.rs` (1,991), and `cluster/cluster_bus.rs` (1,629).
- Compile times for incremental rebuilds dominate developer iteration; CI's `cargo clippy --workspace` and `cargo test --workspace` jobs are gated by `ferrite-core` recompilation.

ADR-005 ("Extension Crate Architecture") established the rule that **extension crates have zero dependencies on `ferrite-core` or each other** — only the top-level binary integrates them. This rule is followed for the 11 extension crates, but `ferrite-core` itself was treated as the sole "blessed" foundation crate and accumulated everything that was not feature-specific.

The result is that `ferrite-core` violates the spirit of ADR-005 from the inside: it has become the very monolith ADR-005 sought to avoid. Its modules are not all truly foundational; many (cluster, query, observability, embedded, triggers, tiering) are themselves features that other features could depend on selectively.

### Concrete pain points

- **Compile latency**: A trivial change in `cluster/cluster_bus.rs` requires recompiling the entire 170k-LOC crate before any dependent crate (top-level binary, integration tests) can be checked. Local incremental rebuild is 30–90 seconds; cold rebuild is 4–8 minutes.
- **CI parallelism**: The `clippy` and `test` jobs cannot fan out per-area because they all depend on `ferrite-core`. The "changed extensions only" CI path falls back to `cargo test --workspace` whenever `crates/ferrite-core/**` is touched — which is the majority of PRs.
- **Lint adoption**: `ferrite-core` is the only crate that has not yet opted in to workspace lints (`unwrap_used = "deny"`, etc.). Splitting it will let strict lints land in newer sub-crates immediately while older code is hardened progressively.
- **API surface**: `ferrite-core` exports types that conflate foundation (e.g. `Store`, `Frame`, `Error`) with features (e.g. `ChaosEngine`, `SchemaRegistry`, `BackupManager`). Embedded-mode users are forced to depend on the kitchen sink.
- **Dependency hygiene**: 50+ direct dependencies in `ferrite-core/Cargo.toml`. Many (e.g. `chacha20poly1305`, `mlua`, `object_store`, `reqwest`, OpenTelemetry) are only required by sub-features but force every consumer to compile-check them.

## Decision

We will split `ferrite-core` into **one foundation crate** (still called `ferrite-core`, narrower scope) and **a small number of focused sub-crates**. The split is incremental: each phase produces a green workspace and is independently revert-able. Extension crates remain as-is per ADR-005.

### Target topology

```
ferrite (top-level binary)
   ├─ ferrite-core              ── storage, protocol, error, config, runtime, metrics primitives
   ├─ ferrite-cluster           ── hash slots, gossip, cluster_bus, slot migration
   ├─ ferrite-query             ── FerriteQL parser/executor, schema registry, sdk pipeline, streaming views
   ├─ ferrite-observability     ── chaos engine, slowlog, latency tracker, telemetry exporters
   ├─ ferrite-persistence       ── backup manager, AOF/RDB, snapshot/restore, lifecycle
   ├─ ferrite-embedded          ── embedded-mode façade + mobile SDK code generators
   ├─ … (existing extension crates unchanged)
```

Each new crate may depend on `ferrite-core` (downward only). No two sub-crates may depend on each other; the integration layer is the top-level binary, exactly as ADR-005 specifies for extensions.

### Phased rollout

The split is **executed one crate per PR** in the following order. Each PR is reviewable in isolation and CI must remain green throughout.

| Phase | New crate | Approx. LOC moved | Public API surface | Risk |
|---|---|---|---|---|
| 1 | `ferrite-observability` | ~6k | `ChaosEngine`, `SlowLog`, `LatencyTracker` | Low — self-contained, no callers in storage hot path |
| 2 | `ferrite-persistence` | ~12k | `BackupManager`, `Snapshot`, `Restore` | Medium — currently leaks types into `Store` |
| 3 | `ferrite-cluster` | ~14k | `ClusterBus`, `SlotMap`, `GossipState` | Medium — top-level handlers depend on it |
| 4 | `ferrite-query` | ~18k | `Parser`, `SchemaRegistry`, `StreamingViews` | High — query types used in many handlers |
| 5 | `ferrite-embedded` | ~4k | `Database` façade, mobile-SDK codegen | Low |

After phase 5, `ferrite-core` should be reduced to ~115k LOC of true foundation (storage engine, protocol, runtime, error/config). Further splits (e.g. `ferrite-auth`, `ferrite-protocol`) may follow but are not required by this ADR.

### Mechanical migration steps per phase

1. Create `crates/ferrite-<name>/` with its own `Cargo.toml`, `src/lib.rs`, opting into workspace lints.
2. Move modules verbatim (`git mv` + `cargo fmt`). Module paths inside the new crate stay identical to minimize diff noise.
3. In `ferrite-core`, replace the moved modules with `pub use ferrite_<name>::*;` re-exports for one release cycle (deprecation window).
4. Update `Cargo.toml` of any crate that directly used the moved types to depend on the new crate instead.
5. Mark the `pub use` re-exports `#[deprecated]` in the next minor release; remove them in the release after.
6. Verify `cargo check --workspace --all-features`, `cargo clippy --workspace --all-features -- -D warnings`, and `cargo test --workspace` all pass.
7. Update `CLAUDE.md`, `docs/DEPENDENCY_GRAPH.md`, and the README crate diagram.

### What stays in `ferrite-core`

After all phases, `ferrite-core` contains only:

- `storage/` — HybridLog, memory tier, mmap tier, disk tier, epoch reclamation
- `protocol/` — RESP2/RESP3 parser, framer, types
- `error.rs` + `error/codes.rs` — workspace-wide error type
- `config.rs` — top-level configuration types (sub-crates own their own config sections)
- `runtime/` — tokio runtime helpers, thread-per-core executor primitives
- `metrics/` — Prometheus exporter primitives (NOT the recorder; that stays in observability)
- `auth/` — ACL primitives (split off later if the crate grows)

## Consequences

### Positive

- **Compile time**: A change in cluster code no longer recompiles the storage engine. Estimated 40–60% reduction in incremental rebuild time for the most common edit paths.
- **CI parallelism**: The `changes` job can fan out tests/clippy per sub-crate; PRs touching only `ferrite-cluster` skip query/persistence test runs.
- **Lint hygiene**: Each new sub-crate opts into workspace lints from day one. The deny list (`unwrap_used`, `print_stdout`, etc.) is enforced incrementally instead of requiring a single mega-PR against `ferrite-core`.
- **API discoverability**: Embedded-mode and SDK users can depend on focused crates. `ferrite-embedded` users no longer pull in cluster gossip code.
- **Dependency surface**: Heavy optional dependencies (`mlua`, `chacha20poly1305`, `opentelemetry-*`) move to the crates that actually use them, simplifying cargo feature wiring.
- **Documentation**: `cargo doc` per crate becomes navigable; today's `ferrite-core` rustdoc is unwieldy.
- **Reinforces ADR-005**: The "no peer dependencies between non-foundation crates" rule applies uniformly.

### Negative

- **Churn**: ~50k LOC moves between crates over five PRs. Code review burden during the rollout is real.
- **Deprecation window**: Re-exports double the maintenance surface for one release cycle.
- **External users**: Anyone depending on `ferrite-core` directly (currently the embedded-mode users) sees deprecation warnings until they migrate to the new crates. Mitigation: keep the re-exports for a full minor release.
- **Cross-crate refactors**: Some types currently passed by reference between modules will need to become `pub` (or wrapped) to cross crate boundaries. Each phase will identify these explicitly.
- **Potential cyclic dependencies surfaced late**: Today's modules often have implicit cycles hidden by being in the same crate. Splitting may force minor refactors (e.g. moving a shared trait into `ferrite-core`).

### Neutral

- **Build artefacts size**: Largely unchanged; the same code compiles, just in different crates.
- **Runtime performance**: No change. Crate boundaries do not affect monomorphisation or inlining when LTO is enabled (release builds).

## Alternatives Considered

### A. Leave `ferrite-core` as-is

Keeps things simple, but the compile-time and lint-adoption pain points worsen as features are added. Rejected because the trend is unsustainable.

### B. One mega-PR splitting everything at once

Conceptually clean but unreviewable. Rejected.

### C. Split by feature flag instead of crate

E.g. gate cluster code behind `--features cluster` inside `ferrite-core`. Reduces compile time only when the feature is off; does not improve API surface or lint adoption. Rejected.

### D. Move everything to extension crates

Make even foundation modules into "extension" crates with no peer dependencies. Forces ugly indirection (every extension would need to re-implement basic primitives). Rejected.

## Implementation Notes

- Each phase PR title must be `refactor(core-split): extract ferrite-<name>` for traceability.
- Each phase must include a `CHANGELOG.md` entry under `### Changed` listing the moved modules.
- `docs/DEPENDENCY_GRAPH.md` must be regenerated after each phase (or kept manually accurate).
- The `version-bump.yml` workflow in `ferrite-ops` does not need changes; the binary version is unaffected.
- This ADR is a **plan**, not a commitment. Phases may be paused or reordered if feedback during phase 1 surfaces blockers.

## References

- ADR-005 — Extension Crate Architecture (the rule this ADR extends inward)
- ADR-016 — Handler State Migration from OnceLock to Store-Integrated Persistence (precedent for incremental migration)
- `docs/DEPENDENCY_GRAPH.md` — current dependency map
- `CLAUDE.md` — workspace overview
