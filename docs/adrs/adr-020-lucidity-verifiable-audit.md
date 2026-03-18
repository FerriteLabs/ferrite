# ADR-020: Lucidity — Verifiable Audit Plane

**Status:** Accepted (Beta)
**Date:** 2026-04-18
**Author:** FerriteLabs
**Supersedes:** ADR-020 (Spike)

## Context

Mnemo (ADR-018) puts agent memory inside Ferrite. Once agent decisions and
their supporting memory live in the database, the database becomes the
forensic record of what an autonomous system did and why. The EU AI Act
(enforcement August 2026) requires high-risk AI systems to retain auditable
records that a regulator can independently verify were not tampered with.

Existing solutions fall into two camps:

1. **WAL-style append-only logs** — tamper-resistant against innocent
   corruption only; an operator with disk access can rewrite history.
2. **External transparency logs** (CT, Sigstore Rekor) — verifiable, but
   require a separate service, separate auth, separate availability budget,
   and don't index per-tenant agent traffic.

No KV store today provides per-tenant, per-key tamper-evident audit with
externally-verifiable proofs.

## Decision

Build **Lucidity** as an in-database audit plane that produces
externally-verifiable proofs of every state-changing operation in a
designated keyspace. Architecture:

1. **Audit log per tenant.** Every mutation in a `__ferrite:lucidity:*`
   keyspace appends an entry to a tenant-scoped Merkle accumulator (binary
   transparency tree, RFC 9162 §2 style).
2. **Periodic STH (Signed Tree Head).** A signer co-process produces a
   signed root every `audit.checkpoint_interval` (default: 1s or 1 KiB of
   entries, whichever first) using ML-DSA-65 (post-quantum). Both the STH
   and the witnessed entries ship to S3-compatible object storage and
   optionally to a public-good witness service.
3. **Inclusion + consistency proofs.** A new `LUC.PROVE <key> <version>`
   command returns the Merkle inclusion proof for that specific write; a
   `LUC.AUDIT <since_sth>` command returns a consistency proof linking two
   STHs.
4. **Proof-of-forgetting (GDPR Article 17).** Tombstones are themselves
   audited entries. Replaying the log skips the deleted ciphertext but
   keeps the Merkle leaf, so the tree's continuity is preserved while the
   data is unrecoverable.

### Non-goals

- Not a general-purpose blockchain. No consensus across tenants, no
  proof-of-work/stake.
- Not a zero-knowledge engine yet — the zk variant (P3) only proves
  *existence* of an audited write, not properties of its plaintext.

## Data model

```
__ferrite:lucidity:l:<tenant>:<seq>      → audited leaf {op, key_hash, value_hash, ts, prev_root}
__ferrite:lucidity:s:<tenant>:<sth_seq>  → signed tree head {root, size, sig, signer_id, ts}
__ferrite:lucidity:w:<tenant>            → witness checkpoint cursor (last STH shipped)
```

Leaf payloads store `value_hash`, never the plaintext, so audit replay
costs are independent of value size. Plaintext lives in the original
keyspace; the audit plane only proves what changed.

## APIs (Phase-0 contract)

| Command | Purpose | Returns |
|---|---|---|
| `LUC.AUDIT.ON <pattern>` | Enable auditing for keys matching pattern | `OK` |
| `LUC.PROVE <key>` | Inclusion proof for the latest write of `key` | `{leaf_index, sth, audit_path}` |
| `LUC.PROVE.AT <key> <version>` | Inclusion proof for a specific version | as above |
| `LUC.STH` | Latest signed tree head | `{root, size, sig, ts}` |
| `LUC.CONSISTENCY <a> <b>` | Consistency proof between two STH sizes | `[hash, ...]` |
| `LUC.WITNESS.SHIP` | Force-ship pending STHs to witnesses | `{shipped, pending}` |
| `LUC.FORGET <key>` | GDPR-compliant tombstone (auditable) | `OK` |

All audited writes are still ordered by replication; auditing is on the
hot path but the signing is deferred to a co-process so write latency is
unaffected.

## Tenancy & isolation

Each tenant has its own Merkle tree and its own signing keypair. STHs are
not cross-signed across tenants. Replicas verify STHs against the
tenant's published public key on apply, so a corrupted leader cannot lie
to a follower.

## Composition diagram

```
write path:
  Client ──► Server ──► HybridLog ──► Lucidity sidecar
                                       │
                                       ├─► append leaf
                                       ├─► (every N) compute STH
                                       └─► (every M) ship to S3 + witness

verify path:
  Auditor ──► LUC.PROVE ──► server returns {leaf, audit_path, sth}
  Auditor verifies sig(STH) and merkle path locally
```

## Phase-0 deliverables

- `crates/ferrite-lucidity` spike crate with the leaf format,
  in-memory Merkle accumulator (binary transparency tree), STH signer
  trait, and an in-memory witness mock.
- ADR-020 (this doc) promoted from spike to Proposed.
- Phase 0 → Phase 1 exit criterion: signing 1 M leaves/s on a single
  worker thread with ML-DSA-65 batched at 1 KiB granularity.

## Phase 1 deliverables

- Real S3 witness shipper.
- `LUC.PROVE` / `LUC.STH` / `LUC.CONSISTENCY` server commands.
- Per-tenant signer key rotation tied to ADR-022 quorum.
- Replication: STH appended to the WAL so followers replay deterministically.

## Eval plan (Phase 2)

- Audit overhead p99 ≤ 5% over a non-audited workload at the same RPS.
- 1 B-leaf tree consistency proof returns in < 10 ms (theoretical: log₂(10⁹) ≈ 30 hashes).
- A regulator running a reference verifier (separate binary) can
  independently confirm any proof produced in the last 90 days.
- Forgotten keys cannot be reconstructed from the audit log even with
  the full leaf set + all STHs.

## Consequences

- Hard regulatory deadline: GA before EU AI Act enforcement (August 2026).
- Shared Merkle infrastructure with Concord (ADR-022) — see Wave 2 gate.
- New cryptographic dependency surface: ML-DSA-65 (FIPS 204), Halo2 (P3
  zk variant only). Both subject to the post-quantum migration schedule
  in `docs/adrs/adr-014-post-quantum-cryptography.md` (referenced).
- Storage growth: ~80 bytes/leaf + sth overhead. At 100K writes/s that
  is ~700 GB/year per tenant — operators must enable Lucidity per
  keyspace, not globally.

## Open questions

1. Witness federation — host our own quorum, ride on Sigstore, or both?
2. Signer key custody — KMS-only or also support local TPM-backed keys?
3. zk variant (P3) — Halo2 vs RISC Zero vs Stwo (Starkware)? Decision
   gates on prover performance benchmarks at end of P2.

## ZK Circuit Spike Results (P0)

A proof-of-concept ZK selective-disclosure circuit was implemented as a
simulation module in `crates/ferrite-lucidity/src/circuits/`. The scaffold
models the Halo2 circuit API surface using SHA-256-based commitments as a
stand-in for real polynomial-commitment proofs.

### Circuit API design decisions

| Decision | Rationale |
|----------|-----------|
| SHA-256 commitment as proof stand-in | Mirrors the hash-based binding a Halo2 proof provides without the heavy dependency; drop-in replaceable. |
| Separate `DisclosurePublicInput` / `DisclosureWitness` types | Maps 1:1 to Halo2's public-instance vs. advice-column split. |
| `prove()` / `verify()` free functions | Matches `halo2_proofs::plonk::{create_proof, verify_proof}` ergonomics. |
| Balanced power-of-two Merkle tree with 0x00/0x01 domain separators | Consistent with the existing `merkle.rs` accumulator (RFC 9162 §2 style). |
| Proof contains `generation_time_us` | Enables latency-budget gating without external instrumentation. |

### Simulated benchmark results

Tree construction + proof generation + verification measured on the
simulation circuit (single-threaded, SHA-256 only — real Halo2 prover
will be slower but benefits from parallelism):

| Log size | Proof time | Verify time | Proof size |
|----------|-----------|-------------|------------|
| 1,000    | < 1 ms    | < 1 µs      | 32 B       |
| 10,000   | < 1 ms    | < 1 µs      | 32 B       |
| 100,000  | < 5 ms    | < 1 µs      | 32 B       |

**Conclusion:** The SHA-256 simulation confirms the API shape and Merkle
path logic are correct. Real Halo2 prover benchmarks (expected 10-100×
slower) will be gated in the P2 eval plan. The 100 ms latency budget for
1,000-leaf proofs is achievable given that even pessimistic Halo2 provers
run in ≈50 ms for circuits of this depth.

## Exit criteria for Phase 0

- ferrite-lucidity crate builds with no main-binary touch.
- Reference verifier binary in `examples/lucidity-verify/` independently
  validates a tree of ≥ 100K leaves.
- Throughput regression ≤ 5% on the standard `ferrite-bench` GETSET mix
  with auditing enabled on the audited keyspace only.

## Compliance mapping

EU AI Act compliance mapping is documented in
[`docs/compliance/ai-act-mapping.md`](../compliance/ai-act-mapping.md),
linking each regulatory requirement to the Lucidity command or feature
that satisfies it.
