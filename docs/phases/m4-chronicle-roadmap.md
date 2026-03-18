# Chronicle (M4) — Implementation Roadmap

> Goal: branch, diff, merge, and time-travel the entire keyspace with copy-on-write.
> Depends on Forge GA (custom merge functions).

## Phase index

| ID | Phase | Effort | Acceptance |
|---|---|---|---|
| m4-p0-spike | Spike | 3 wk | 1 M-key branch in < 100 ms |
| m4-p1-branch | Branch primitive | 5 wk | BRANCH.* commands + replication compat |
| m4-p2-merge | Diff & merge | 5 wk | 3-way merge with Forge custom-fn strategies |
| m4-p3-timetravel | AS OF reads | 3 wk | PITR demo on 100 GB |
| m4-p4-alpha | Alpha | 4 wk | Studio branch graph; 2 design partners |
| m4-p5-ga | Beta → GA | 5 wk | Storage guardrails, branch GC |

## Files & crates

```
crates/ferrite-chronicle/
├── src/
│   ├── lib.rs
│   ├── hamt.rs           # persistent HAMT
│   ├── branch.rs         # BranchId, BranchRef, switch logic
│   ├── cow.rs            # copy-on-write boundary across HybridLog read-only + disk
│   ├── diff.rs           # diff iterator
│   ├── merge.rs          # 3-way merge driver (calls Forge fn for custom strategies)
│   ├── timetravel.rs     # AS OF resolver
│   └── gc.rs             # branch garbage collector
src/commands/handlers/branch.rs
src/server/handler.rs            # BRANCH.* + writes are branch-scoped via session var
```

## Per-phase deliverables

### P0 spike

- ADR-021 documenting representation choice (HAMT vs Bw-Tree vs Adaptive Radix Tree).
- Bench: `branch_create` vs dataset size {1M, 10M, 100M}.
- Decision: persistent HAMT with structural sharing across HybridLog tiers.

### P1 branch primitive

- Commands:
  ```
  BRANCH.CREATE  <name> [FROM <base>]    -> OK
  BRANCH.SWITCH  <name>                  -> OK   (per-connection branch)
  BRANCH.LIST                            -> array of branch metadata
  BRANCH.DELETE  <name>                  -> OK
  BRANCH.SHOW    <name>                  -> { base, created_at, parent_branch, refcount }
  ```
- All read/write handlers receive an active branch via session-local context.
- Replication contract: replicate `(branch_id, replid, offset)` tuples instead of bare offset.
- Tests: branch isolation (writes on `feature` invisible on `main`); branch lifecycle survives restart.

### P2 diff & merge

- Commands:
  ```
  BRANCH.DIFF    <a> <b>                              -> stream of (key, op, val_a, val_b)
  BRANCH.MERGE   <src> INTO <dst> [STRATEGY <fn>]     -> { merged, conflicts }
  ```
- Strategies: `lww` (last-write-wins by epoch), `prefer-src`, `prefer-dst`, `forge:<fn-name>`.
- Conflict reporting: returns conflict set as a streamable iterator.
- Tests: merge with no conflicts; merge with conflicts and each strategy; deterministic merge across replicas.

### P3 time-travel

- `AS OF <epoch>` clause supported on read commands via `BRANCH.READAS <epoch> ...`.
- Retention controlled by `chronicle.retention_epochs = N`.
- Tests: PITR restore on 100 GB dataset within budget; reads outside retention error cleanly.

### P4 alpha

- Studio UI: branch graph visualization (`crates/ferrite-studio/src/branches.rs`).
- CI integration example: branch-per-PR pattern documented and used by 2 design partners.
- Hardening: branch GC reclaims storage of unreachable versions.

### P5 GA

- Storage-cost guardrails: `chronicle.max_total_branches`, `chronicle.max_branch_age`.
- Docs page covering branching model + interop with replication.
- ADR flipped to `Accepted (GA)`.

## Dependency on Forge

`BRANCH.MERGE STRATEGY forge:<fn-name>` requires Forge GA. P2 cannot complete without
m2-p5-ga signed off. If Forge slips, Chronicle's P2 ships with the built-in strategies
only and adds Forge integration in a P2.5 patch.

## Risks

| Risk | Mitigation |
|---|---|
| Storage cost explosion from branches | aggressive GC + per-branch quota + alerting |
| Merge semantics confuse users | strategy selector defaults to `prefer-dst` (least surprising); docs include decision tree |
| Branching breaks existing clients that don't know about branches | session-level default branch is `main`; full back-compat |
| HAMT operation overhead on hot path | bench-driven; fall back to flat structure for branch=`main` if needed |
