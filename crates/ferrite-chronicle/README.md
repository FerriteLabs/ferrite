# ferrite-chronicle

Branchable state for Ferrite — see [ADR-021](../../docs/adrs/adr-021-chronicle-branchable-state.md).

Think `git branch` for a key-value store: zero-copy fork, write into
the branch, merge or discard.

## Status

🧪 **Spike (P1).** Self-contained workspace crate. Not yet wired into
the main `ferrite` binary; the integration handlers (`CHR.*` command
family) ship in a later phase.

## Components

| Type | Purpose |
|---|---|
| `BranchRegistry` | Tracks branch metadata (parent chain, tenant, TTL); reaps expired branches |
| `BranchedKv<S>` | Generic adapter wrapping any `BaseKv` with per-branch overlay reads/writes |
| `BaseKv` | Minimal trait the underlying store must implement (`get` / `set` / `del`) |
| `InMemoryKv` | Reference impl for tests |

## Read semantics

For a key `k` while branch `B` is active:

1. Walk `B`'s ancestry (descendant first); for each branch in the
   chain, return the first overlay hit (value or tombstone-as-`None`).
2. If no overlay matches, fall through to the base store.

## Quick start

```rust
use ferrite_chronicle::{BaseKv, BranchedKv, BranchRegistry, InMemoryKv};

let base = InMemoryKv::default();
base.set("user:1", b"alice".to_vec());

let bk = BranchedKv::new(base, BranchRegistry::new());
let b = bk.create_branch(None, "tenant").unwrap();
bk.use_branch(Some(b.clone()));
bk.set("user:1", b"bob".to_vec());          // overlay write
assert_eq!(bk.get("user:1"), Some(b"bob".to_vec()));

bk.use_branch(None);                         // back to main
assert_eq!(bk.get("user:1"), Some(b"alice".to_vec()));

bk.use_branch(Some(b.clone()));
let snap = bk.snapshot().unwrap();
bk.set("user:1", b"carol".to_vec());
bk.rollback(snap);                           // back to "bob"
```

## Features at a glance

- ✅ Copy-on-write per branch — branch creation is O(1)
- ✅ Nested branches inherit parent overlays
- ✅ Tombstones for branch-local deletes
- ✅ TTL with `reap_expired(now_ms)` reaper
- ✅ Per-branch snapshot / rollback
- ✅ Per-branch stats (writes / deletes / overlay keys / snapshots)
- ✅ Tenant-scoped branch creation (cross-tenant fork rejected)

## Testing

```sh
cargo test -p ferrite-chronicle
cargo clippy -p ferrite-chronicle --all-targets -- -D warnings
```

18 unit tests + 1 doc test, strict-clippy clean.

## License

Same as the parent workspace.
