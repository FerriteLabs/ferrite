# ferrite-concord

State-based and δ-state CRDTs for Ferrite — see [ADR-022](../../docs/adrs/adr-022-concord-crdt.md).

## Status

🧪 **Spike (P1).** Self-contained workspace crate. Not yet wired into the main `ferrite` binary; the integration handlers (`CON.*` command family) ship in a later phase.

## Types

| Type | Semantics | Use case |
|---|---|---|
| `GCounter` | Grow-only counter; per-replica slots, merge = max | Page views, request totals |
| `PnCounter` | Pair of `GCounter`s for inc/dec | Inventory, balances |
| `OrSet<T>` | Observed-Remove set with per-replica seqs | Tag sets, follower lists |
| `LwwRegister<T>` | Last-Writer-Wins (replica-id breaks ties) | User profile fields |
| `MvRegister<T>` | Vector-clock multi-value (concurrent writers preserved) | Shopping cart, settings |

All types implement the [`Crdt`](src/lib.rs) trait (`merge`); the
mutable types also implement [`Delta`](src/delta.rs) for δ-state
replication (ship only the changed slot).

## Quick start

```rust
use ferrite_concord::{Crdt, GCounter};

let mut a = GCounter::new();
let mut b = GCounter::new();
a.increment("node-A", 5);
b.increment("node-B", 3);
a.merge(&b);
assert_eq!(a.value(), 8);
```

## Design notes

- **OR-Set "add wins"**: `add` allocates a fresh `(replica, seq)` tag.
  `remove` only tombstones tags currently observed, so a concurrent
  re-add (with a new tag) survives a merge.
- **MvRegister dominance**: a `VectorClock a` dominates `b` iff every
  component of `b` is ≤ the corresponding component of `a` AND at
  least one component is strictly less (treating missing components as
  zero). Dominated entries are pruned on merge.
- **δ-state**: `mutate` returns the delta to ship; `merge_delta` is
  equivalent to `merge` of the corresponding full state (delta-state
  convergence theorem, Almeida et al. 2018).

## Testing

```sh
cargo test -p ferrite-concord
cargo clippy -p ferrite-concord --all-targets -- -D warnings
```

23 unit tests + 1 doc test, strict-clippy clean.

## License

Same as the parent workspace.
