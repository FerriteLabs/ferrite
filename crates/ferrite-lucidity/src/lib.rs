//! Lucidity — Verifiable audit plane.
//!
//! See ADR-020 (`docs/adrs/adr-020-lucidity-verifiable-audit.md`).
//!
//! P0 spike: leaf format, binary Merkle accumulator (RFC 9162 §2-style),
//! signed tree heads (with a swappable `Signer` trait), and an inclusion
//! / consistency proof verifier.  All in pure Rust, no real signatures
//! yet — the [`MockSigner`] returns a deterministic byte string so
//! tests can verify the round trip.
//!
//! # Quick start
//!
//! ```
//! use ferrite_lucidity::{AuditLog, Leaf, MockSigner, verify_inclusion};
//!
//! let signer = MockSigner::new("test-signer");
//! let mut log = AuditLog::new(Box::new(signer));
//! log.append(Leaf::for_set(b"user:42", b"alice", 1));
//! log.append(Leaf::for_set(b"user:43", b"bob", 2));
//! let sth = log.signed_tree_head();
//! let proof = log.inclusion_proof(0).unwrap();
//! assert!(verify_inclusion(&proof, &sth.root));
//! ```

#![forbid(unsafe_code)]
#![allow(missing_docs)] // P0 spike — docs land in P1 with LUC.* handlers.
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

pub mod circuits;
pub mod ed25519;
pub mod key_rotation;
pub mod leaf;
pub mod log;
pub mod merkle;
pub mod pq;
pub mod signer;
pub mod tombstone;
pub mod witness;

pub use ed25519::Ed25519Signer;
pub use key_rotation::KeyRotation;
pub use leaf::{Leaf, Op};
pub use log::{AuditError, AuditLog};
pub use merkle::{verify_consistency, verify_inclusion, ConsistencyProof, InclusionProof};
pub use pq::{MlDsaLevel, PqSigner};
pub use signer::{MockSigner, SignedTreeHead, Signer};
pub use tombstone::ForgetReceipt;
pub use witness::{InMemoryWitness, WitnessError};
