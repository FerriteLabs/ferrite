//! ZK circuit scaffolding for Lucidity.
//!
//! Production builds will use `halo2_proofs`; this module provides the circuit
//! API surface with SHA-256-based simulation so we can benchmark prover
//! latency without the heavy dependency.

pub mod benchmark;
pub mod disclose;
