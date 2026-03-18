# ADR-021: Chronicle — Branchable State

**Status:** Accepted (Beta)
**Date:** 2026-04-18
**Author:** FerriteLabs
**Supersedes:** ADR-021 (Spike)

## Context

Agent workflows, AI evaluation pipelines, and CI integration tests
repeatedly need a recurring primitive: *fork the database, run a
hypothetical, throw it away or merge it back*. Today this is approximated
with disk snapshots (Redis BGSAVE), with separate clusters per branch, or
with Dolt-style copy-on-write that has no native KV semantics. None of
these are O(1).

Mnemo (ADR-018) makes this pain worse: agents want to "fork the world,
try a tool call, and roll back" thousands of times per session — once per
speculative tool invocation in chain-of-thought planners.

## Decision

Build **Chronicle**: native, O(1), copy-on-write *branches* over the
HybridLog. Branches share immutable pages with their parent and store a
delta overlay in the mutable region. The same primitive serves three
audiences with one mechanism:

1. **Agents** (`MEM.BRANCH/MERGE/DROP`): branch agent state, run a
   tentative tool call, merge or discard.
2. **CI / eval harnesses**: spin up a per-test branch in O(1), rather
   than per-test container with a fresh DB.
3. **Operations** (`CHR.SHADOW`): mirror live writes to a branch for
   schema-migration testing on production data without touching prod.

### Non-goals

- Not git. No three-way merges, no conflict resolution UI. Conflicts at
  merge time fail the merge — caller decides.
- Not a database fork in the SQL sense. Logical schema is shared; only
  KV state diverges.

## Data model

A branch is identified by a 128-bit ULID and stored in two places:

```
__ferrite:chronicle:b:<branch_id>           → branch metadata {parent, created_at, owner_tenant, ttl}
__ferrite:chronicle:p:<branch_id>:<key>     → overlay value (write-through)
__ferrite:chronicle:t:<branch_id>:<key>     → tombstone marker (delete-through)
```

Reads on a branch follow: tombstone? → overlay? → parent (recursive).
Writes on a branch only ever touch the overlay/tombstone keyspace.

The parent's HybridLog pages are reference-counted. A page is freed
only when no live branch references it.

## APIs (Phase-0 contract)

| Command | Purpose | Returns |
|---|---|---|
| `CHR.BRANCH [from <branch>]` | Create branch (default: from main) | `<branch_id>` |
| `CHR.MERGE <branch> [--allow-conflicts]` | Merge into parent | `{merged, conflicts}` |
| `CHR.DROP <branch>` | Discard branch | `OK` |
| `CHR.LIST` | List branches owned by current tenant | `[{id, parent, ttl, size}]` |
| `CHR.DIFF <a> <b>` | Set of keys differing between branches | `[{key, op, side}]` |
| `CHR.USE <branch>` | Pin connection to a branch | `OK` |
| `CHR.SHADOW <branch> <pattern>` | Mirror live writes matching pattern into branch | `OK` |

`CHR.USE` is sticky for the connection — clients reading on a branch
just call regular GET/SET; the dispatch layer reroutes via the overlay.
Mnemo + Forge inherit the branched view automatically.

## Tenancy & isolation

Branches are tenant-scoped. A tenant cannot create a branch from another
tenant's main, nor see another tenant's branches. Cross-tenant writes
into a branch are rejected at the same layer that rejects cross-tenant
GETs today.

## Composition diagram

```
read on branch B:
  Client ──► dispatch ──► overlay(B,key)? ─yes─► return
                              │
                              no
                              ▼
                          tombstone(B,key)? ─yes─► return NIL
                              │
                              no
                              ▼
                          parent(B) read

write on branch B:
  Client ──► dispatch ──► overlay(B,key) ← value
```

Overlay storage uses the same HybridLog tier the rest of Ferrite uses;
branches inherit the same backup, replication, and crash-recovery story.

## Phase-0 deliverables

- `crates/ferrite-chronicle` spike crate with: branch registry, overlay
  reader, in-memory CoW page tracker.
- ADR-021 (this doc) promoted from spike to Proposed.
- A reference `BranchedKV<S: Store>` adapter that wraps any `Store` with
  branch-aware reads/writes — usable by Mnemo + Forge unit tests.
- **HAMT implementation complete** (`hamt.rs`): 32-way persistent Hash
  Array Mapped Trie with structural sharing. Branching (clone) is O(1)
  via `Arc`; writes are O(log₃₂ N) via path-copying. Handles hash
  collisions via dedicated `Collision` nodes. Supports `get`, `insert`,
  `remove`, `iter`, and `diff` operations.
- **Branch GC** (`gc.rs`): configurable garbage collector that reclaims
  expired (TTL-based), aged-out, and excess branches. Controlled via
  `CHR.GC` and `CHR.CONFIG` handler commands.
- **HamtBranchedStore** type alias: preparatory adapter for production
  HAMT-backed branched KV.

## Phase 1 deliverables

- Server commands wired (`CHR.*`).
- Replication: branch ops are replicated; followers apply overlays into
  their own per-branch keyspace.
- TTL on branches (auto-drop after `chronicle.branch_ttl` of inactivity).
- Mnemo `MEM.BRANCH/MERGE/DROP` shortcuts that bind a branch to a single
  agent's keyspace.

## Eval plan (Phase 2)

- Branch creation p99 ≤ 1 ms regardless of parent size.
- 1 K simultaneous branches consume ≤ 100 MB overhead beyond their
  overlay size.
- `CHR.MERGE` of a branch with N writes completes in O(N), not O(parent).
- Mnemo speculative-branch use case: 10 K branch-merge cycles/s on a
  single core.

## Consequences

- Page reference-counting must integrate with the existing HybridLog
  reclamation epoch. Real risk: GC pause regressions if implemented
  naively (mitigated by per-branch lazy refcount drop on `CHR.DROP`).
- Backup tooling needs to learn about branches — `BACKUP --include-branches`
  and `--exclude-branches` flags land in P2.
- `CHR.SHADOW` in production needs careful rate limiting; runaway shadow
  rules could double the write workload silently.

## Open questions

1. Branch-of-branch nesting depth limit — none, or hard cap at 16 to
   bound recursive read cost?
2. Should `CHR.MERGE` be transactional w.r.t. concurrent writes to the
   parent, or eventually consistent with last-writer-wins on conflict?
3. UI for branches: do we expose them to `ferrite-cli` and TUI as a
   first-class concept, or keep them server-side only in P1?

## Exit criteria for Phase 0

- ferrite-chronicle builds with `BranchedKV` adapter.
- Mnemo's `recall` runs unmodified against a branched store and returns
  branch-scoped results in tests.
- 1 M branch creates in < 10 s on a single thread (no real overlay
  writes), proving the metadata path is O(1).
