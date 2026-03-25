//! Cross-crate integration showcase for the Ferrite spike crates.
//!
//! Adapter-only — no new domain logic, just demonstrates that the
//! six spike crates compose into the patterns the production
//! handlers will use.  Exercised by the integration tests in
//! `tests/`.

#![forbid(unsafe_code)]
#![allow(missing_docs)]
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

pub mod chronicle_audit;
pub mod mnemo_forge;
pub mod pangea_crdt;

pub use chronicle_audit::{audit_chronicle_op, verify_chronicle_audit, ChronicleAuditedKv};
pub use mnemo_forge::{parse_forge_result, score_record_for_forge, MnemoForgeBridge};
pub use pangea_crdt::{allocate_crdt, deserialize_gcounter, serialize_gcounter, PangeaCrdtStore};
