# EU AI Act Compliance Mapping — Lucidity

This document maps EU AI Act requirements to Lucidity guarantees provided
by Ferrite's verifiable audit plane (ADR-020).

## Mapping

| AI Act Requirement | Article | Lucidity Guarantee | Command/Feature |
|---|---|---|---|
| Record keeping | Art. 12 | Tamper-evident audit log | `LUC.APPEND` |
| Traceability | Art. 12(2) | Inclusion proofs | `LUC.PROOF` |
| Data governance | Art. 10 | Proof of forgetting (GDPR) | `LUC.FORGET` |
| Transparency | Art. 13 | Selective disclosure proofs | `circuits/disclose` |
| Post-market monitoring | Art. 72 | Signed checkpoints | `LUC.HEAD`, `LUC.CHECKPOINT` |
| Technical documentation | Art. 11 | Full audit trail | `LUC.LEAVES` |

## Notes

- **Art. 12 — Record keeping:** Every state-changing operation in a Lucidity-audited
  keyspace appends a leaf to a per-tenant Merkle accumulator (RFC 9162 §2 style).
  Leaves store `value_hash`, never plaintext, so audit replay costs are independent
  of value size.

- **Art. 12(2) — Traceability:** `LUC.PROOF <index>` returns a Merkle inclusion proof
  that a specific write exists in the audited log. An external verifier can confirm
  the proof against a published Signed Tree Head (STH) without access to the database.

- **Art. 10 — Data governance / GDPR Art. 17:** `LUC.FORGET <tenant> <key>` appends a
  `FORGET` tombstone leaf, preserving the Merkle tree's continuity while making the
  original data unrecoverable. A `ForgetReceipt` is returned as cryptographic proof
  that the forgetting occurred at a specific epoch.

- **Art. 13 — Transparency:** The ZK selective-disclosure circuit
  (`crates/ferrite-lucidity/src/circuits/disclose.rs`) proves "this query result is
  consistent with a key in the audited set" without revealing the key itself.

- **Art. 72 — Post-market monitoring:** `LUC.HEAD` returns the latest Signed Tree Head
  (STH) with a cryptographic signature. `LUC.CHECKPOINT` forces a snapshot for
  point-in-time compliance audits.

- **Art. 11 — Technical documentation:** `LUC.LEAVES` provides paginated access to the
  full audit trail for regulatory review.

## References

- [ADR-020: Lucidity — Verifiable Audit Plane](../adrs/adr-020-lucidity-verifiable-audit.md)
- [EU AI Act (Regulation 2024/1689)](https://eur-lex.europa.eu/eli/reg/2024/1689/oj)
- [RFC 9162: Certificate Transparency Version 2.0](https://www.rfc-editor.org/rfc/rfc9162)
