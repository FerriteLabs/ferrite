# ADR-022: Concord — Multi-Master CRDTs

**Status:** Accepted (Beta)
**Date:** 2026-04-18
**Author:** FerriteLabs
**Supersedes:** ADR-022 (Spike)

## Context

Ferrite's existing replication is single-leader. Agents and edge
deployments increasingly need write availability in multiple regions
simultaneously without splitting the namespace per region. The historical
answer (DynamoDB Global Tables, Cosmos DB multi-region writes) has two
problems for our audience:

1. **Conflict resolution is opaque.** "Last writer wins" surprises
   developers when clocks drift; LWW is not a reasonable default for
   counters, sets, or maps.
2. **No convergence proof.** Operators can't tell from the API whether
   two regions have actually converged after a partition heals.

The CRDT literature (Shapiro et al. 2011) has been mature for 14 years
but is rarely available behind a Redis-compatible API.

## Decision

Build **Concord**: a multi-master replication mode with CRDT-typed keys,
per-tenant configurable conflict resolution, and an explicit
*convergence query* API.

Initial CRDT type set (P1):

- **G-Counter** / **PN-Counter** — monotonic and incrementable counters
- **OR-Set** — observed-remove set (add wins on tie)
- **LWW-Register** with per-write logical clock (HLC)
- **2P-Map** — keys can be added/removed once

Type is declared at key creation via `CON.TYPE.SET <key> <type>` (or the
type-specific commands `CON.CTR.INCR`, `CON.SET.ADD`, etc., which create
the key with the right type implicitly).

### Non-goals

- Not a Riak / Cassandra-style "eventually consistent everything" mode.
  Untyped keys (regular GET/SET) remain single-leader.
- Not a transaction system. Multi-key transactions are out of scope and
  remain bound to the leader.

## Data model

```
__ferrite:concord:t:<tenant>:k:<key>      → CRDT state envelope {type, payload, hlc_max}
__ferrite:concord:t:<tenant>:m:<key>      → per-key delta-shipping cursor
__ferrite:concord:t:<tenant>:c:<region>   → per-region clock vector
```

The CRDT payload is a type-specific encoding. State-based replication
ships full state on first sync; delta-state CRDTs (Almeida et al. 2018)
ship only deltas after that, dramatically reducing bandwidth.

## APIs (Phase-0 contract)

| Command | Purpose | Returns |
|---|---|---|
| `CON.CTR.INCR <key> <delta>` | Increment a PN-counter | `<new_value>` |
| `CON.CTR.GET <key>` | Read counter | `<value>` |
| `CON.SET.ADD <key> <member>` | Add to OR-set | `OK` |
| `CON.SET.REM <key> <member>` | Remove from OR-set | `OK` |
| `CON.SET.MEMBERS <key>` | Read set | `[member, ...]` |
| `CON.REG.SET <key> <value>` | Set LWW register (HLC stamped) | `OK` |
| `CON.REG.GET <key>` | Read register | `<value>` |
| `CON.MAP.PUT/DEL/GET` | 2P-Map ops | type-specific |
| `CON.CONVERGED <key>` | Returns 1 iff all known regions agree on the state | `0|1` |
| `CON.SYNC <region>` | Force-pull deltas from a region | `{pulled, applied}` |

`CON.CONVERGED` is the killer feature — it's a first-class API, not a
log-trawling exercise. It works by comparing the per-key vector clock
to the per-region cursor.

## Tenancy & isolation

Concord operates within a tenant; cross-tenant CRDTs are explicitly
disallowed. Each tenant has its own region map.

## Composition diagram

```
write in region us-east:
  Client ──► CON.CTR.INCR k 5
                │
                ▼
   Apply locally (state=v6) ──► append delta to outbox
                                  │
                                  ▼
                          replicator ships δ to {eu-west, ap-south}

read in region eu-west:
  Client ──► CON.CTR.GET k
                │
                ▼
   Read local state (state=v6 if synced, v5 if behind)
```

## Phase-0 deliverables

- `crates/ferrite-concord` spike crate with G-Counter, PN-Counter, and
  OR-Set state-based CRDTs as pure Rust with property tests.
- ADR-022 (this doc) promoted from spike to Proposed.
- Convergence-test harness: random ops on N replicas with random
  partitions, asserts state equality after `CON.CONVERGED → 1` for all
  keys.
- **Chaos testing** (`chaos.rs`): partition/heal, duplicate, and
  reorder simulation utilities for verifying CRDT convergence under
  adverse network conditions.
- **Data sovereignty routing** (`routing.rs`): per-key region pinning
  with glob-pattern matching, priority-ordered rules, and handler
  commands `CON.ROUTE`, `CON.ADDRULE`, `CON.RULES`.

## Phase 1 deliverables

- Server commands wired with replication (delta-state shipping).
- Hybrid Logical Clocks (Kulkarni et al. 2014) shared with Lucidity for
  global ordering of audited writes.
- Per-tenant region topology config and the cross-region delta shipper.
- LWW-Register and 2P-Map types.

## Eval plan (Phase 2)

- 5-region chaos test (random 30%-loss + 50ms-jitter network) for 1 hour:
  zero CRDT divergence after the partition heals.
- Counter throughput per region ≥ 50K incrs/s on a single core.
- Delta-state bandwidth on a 1000-key OR-Set is < 10× full-state baseline.
- `CON.CONVERGED` returns within 1 ms even for 1 M-element keys.

## Consequences

- New per-tenant clock-vector storage; size proportional to
  `tenants × regions`. Bounded and small.
- HLC integration with Lucidity (ADR-020) is a hard dependency — both
  features need a shared, monotonic per-tenant clock to be useful.
- Cross-region replication uses the existing TLS transport but adds a
  per-region authentication identity.

## Open questions

1. Pluggable CRDT types — fixed set in P1, or trait-driven so tenants
   can register their own (e.g. RGA for collaborative text)?
2. Convergence-rate SLO — is "eventually" enough, or do we publish a
   bounded staleness guarantee per topology shape?
3. Cross-region command routing — does the client always talk to its
   nearest region, or do we add a `CON.PIN <region>` for stickiness?

## Exit criteria for Phase 0

- ferrite-concord crate builds with G-Counter, PN-Counter, OR-Set as
  pure Rust modules + property tests.
- Convergence test harness in `crates/ferrite-concord/tests/` runs 1000
  random workloads to convergence with zero divergence.
- Reference rust client demonstrates a 3-replica counter convergence
  scenario in a single integration test.
