# ferrite-lucidity

Verifiable audit plane for Ferrite — RFC 9162-style binary Merkle
tree, signed tree heads, witness-detected forks. See
[ADR-020](../../docs/adrs/adr-020-lucidity-verifiable-audit.md).

## Status

🧪 **Spike (P1).** Self-contained workspace crate. Not yet wired into
the main `ferrite` binary; the integration handlers (`LUC.*` command
family) ship in a later phase.

## Components

| Module | Purpose |
|---|---|
| `leaf` | `Leaf { op, key_hash, value_hash, ts_ms }` with SHA-256 + 0x00 domain separator (RFC 9162 §2.1) |
| `merkle` | Pure-function Merkle root, inclusion proofs, consistency proofs |
| `log` | `AuditLog` — append leaves, snapshot signed tree heads, produce proofs |
| `signer` | `Signer` trait + deterministic `MockSigner` for tests |
| `ed25519` | Real `Ed25519Signer` backed by `ed25519-dalek` v2 |
| `witness` | `InMemoryWitness` — records STH chain, detects forks (same size, different root) and regressions |

## Quick start

```rust
use ferrite_lucidity::{AuditLog, Ed25519Signer, InMemoryWitness, Leaf, verify_inclusion};

let signer = Ed25519Signer::generate("audit-1");
let log = AuditLog::new(Box::new(signer));

log.append(Leaf::for_set(b"user:42", b"alice", 1));
log.append(Leaf::for_set(b"user:43", b"bob",   2));
log.append(Leaf::for_del(b"user:42",            3));

let sth   = log.signed_tree_head();
let proof = log.inclusion_proof(0).unwrap();
assert!(verify_inclusion(&proof, &sth.root));

let witness = InMemoryWitness::new();
witness.record(&sth).unwrap();
```

## Forget leaves

`Leaf::for_forget(key, ts)` records that a value was *forgotten* —
the leaf hash is preserved (so the tree stays consistent) but no
`value_hash` is committed, making the original value unrecoverable.
This is the GDPR / right-to-be-forgotten primitive that lets Ferrite
honour deletion requests without breaking audit continuity.

## Crypto

- **Mock signer**: SHA-256 of `(id || size || root || ts_ms)` —
  deterministic, **not cryptographically secure**, intended only for
  tests and the eval harness.
- **Real signer**: Ed25519 over the canonical `[u8; 48]` payload
  `(size_be || root || ts_ms_be)`.

P2 will add ML-DSA-65 (post-quantum) behind the same `Signer` trait.

## Testing

```sh
cargo test -p ferrite-lucidity
cargo clippy -p ferrite-lucidity --all-targets -- -D warnings
```

29 unit tests + 1 doc test, strict-clippy clean.

## License

Same as the parent workspace.
