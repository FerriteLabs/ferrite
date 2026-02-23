// Core crate under active development — using targeted dead_code annotations
// #![allow(dead_code)] -- removed, using targeted annotations

//! # Ferrite Core
//!
//! Core engine for Ferrite — storage, protocol, commands, clustering, and persistence.
//!
//! This crate contains all Tier 1 (Stable) and Tier 2 (Beta) modules that form
//! the production database engine.

// ── Tier 1 — Stable ──────────────────────────────────────────────────────────

pub mod auth;
pub mod config;
pub mod error;
pub mod io;
pub mod metrics;
pub mod persistence;
pub mod protocol;
pub mod query;
pub mod runtime;
pub mod startup_errors;
pub mod storage;

#[cfg(feature = "crypto")]
pub mod crypto;

pub mod telemetry;

// ── Tier 2 — Beta ────────────────────────────────────────────────────────────

pub mod cluster;
pub mod compatibility;
pub mod embedded;
pub mod network;
pub mod observability;
pub mod tiering;
pub mod transaction;

// ── Modules with core deps (moved here to avoid circular deps) ───────────────

pub mod audit;
pub mod grpc;
pub mod optimizer;
pub mod temporal;
pub mod triggers;
pub mod views;

// ── Public re-exports ────────────────────────────────────────────────────────

pub use config::Config;
pub use error::{FerriteError, Result};
