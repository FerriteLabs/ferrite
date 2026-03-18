# Lucidity (M3) — Implementation Roadmap

> Goal: tamper-evident audit plane over HybridLog with selective ZK proofs and
> post-quantum signatures. Regulatory deadline: EU AI Act enforcement (Aug 2026).

## Phase index

| ID | Phase | Effort | Acceptance |
|---|---|---|---|
| m3-p0-spike | Spike | 3 wk | Halo2 prover meets latency budget; CT-log variant chosen |
| m3-p1-chain | Hash chain | 5 wk | AUDIT.PROVE/VERIFY for inclusion proofs |
| m3-p2-pq | PQ signing | 3 wk | ML-DSA checkpoint signatures + verifier CLI |
| m3-p3-forget | Proof of forgetting | 4 wk | GDPR demo with auditor-verifiable receipts |
| m3-p4-zk | ZK selective disclosure | 5 wk | < 2 s proof for 1k-record window |
| m3-p5-ga | Beta → GA | 5 wk | Compliance docs + audit-firm case study |

## Files & crates

```
crates/ferrite-lucidity/
├── Cargo.toml          # halo2_proofs, pqcrypto-mldsa, blake3, rs-merkle (or custom)
├── src/
│   ├── lib.rs
│   ├── chain.rs        # append-only Merkle log; per-epoch root
│   ├── proof.rs        # inclusion / non-membership proofs (RFC 9162-style)
│   ├── pq.rs           # ML-DSA (FIPS 204) sign / verify wrappers
│   ├── tombstone.rs    # proof-of-forgetting circuit input prep
│   ├── circuits/
│   │   └── disclose.rs # Halo2 selective-disclosure circuit
│   └── checkpoint.rs   # epoch-checkpoint scheduler
├── tests/
└── benches/
src/commands/handlers/audit.rs    # AUDIT.PROVE / AUDIT.VERIFY / AUDIT.CHECKPOINT
ferrite-cli/src/audit_verify.rs   # standalone verifier
```

## Per-phase deliverables

### P0 spike

- ADR-020 documenting Merkle variant + Halo2 prover-throughput numbers.
- Throwaway crate `crates/ferrite-lucidity-spike/` with bench: proof time vs log size.
- Decision: RFC 9162 binary CT-log (Certificate Transparency) — proven in adversarial
  settings, audit-firm familiarity.

### P1 hash chain

- HybridLog mutable region append-only log gets a Merkle accumulator.
- Per-epoch root checkpoint published as `__ferrite:audit:cp:<epoch>`.
- Commands:
  ```
  AUDIT.PROVE   <key> [AT epoch]      -> { root, path[], leaf_hash, epoch }
  AUDIT.VERIFY  <root> <leaf> <path>  -> 0|1
  AUDIT.ROOT    [epoch]               -> current root
  AUDIT.CHECKPOINT                    -> force a checkpoint (admin)
  ```
- Tests: forge-detection (mutate one byte, verify fails); replication (root identical on replicas).

### P2 PQ signing

- Each checkpoint signed with ML-DSA-65 (FIPS 204).
- Key rotation: rolling window of N active verifier keys.
- Standalone verifier (`ferrite-cli audit verify --root ... --sig ... --pubkey ...`).
- Tests: rotation; expired-key rejection; tampered-signature rejection.

### P3 proof of forgetting

- Tombstone records published with proof that key is unreachable from current root.
- GDPR receipt format: signed JSON `{ tenant, key_hash, tombstone_epoch, root, sig }`.
- Demo flow in `ferrite-docs/examples/gdpr-forget/`.
- Tests: re-add of forgotten key produces fresh record (no zombie); receipt verifies offline.

### P4 ZK selective disclosure

- Halo2 circuit proving "this query result is consistent with a key in the audited set"
  without revealing the key.
- Circuit input: Merkle path, record hash, public predicate (e.g. balance ≥ X).
- Acceptance: < 2 s proof for 1 k-record window on EPYC 9554P.
- Tests: forged proof rejected; right-key wrong-predicate rejected.

### P5 GA

- Compliance documentation mapping each AI-Act requirement to a Lucidity guarantee.
- ≥ 1 audit-firm partner case study.
- ADR status flipped to `Accepted (GA)` before Aug 2026.

## Risks

| Risk | Mitigation |
|---|---|
| Halo2 prover too slow for ad-hoc queries | tier proofs (eager checkpoint, lazy ZK); cache circuit compile artefacts |
| ML-DSA reference impl perf | pqcrypto-mldsa benchmarked in P2 spike; fallback to liboqs if needed |
| AI Act interpretation drift | engage compliance counsel early; track delegated acts updates |
| Storage overhead of audit log | configurable retention; cold-tier archival; per-key opt-out for non-regulated data |
