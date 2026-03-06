// Extension crate under active development — using targeted dead_code annotations
#![forbid(unsafe_code)]
#![allow(missing_docs)] // Extension crate under active development; docs added progressively

//! # ferrite-plugins
//!
//! Plugin system, WASM runtime, and CRDTs for Ferrite.
//!
//! Provides a sandboxed plugin system with hot-reload, WASM-based
//! user-defined functions, CRDTs for conflict-free replication,
//! FaaS integration, and a plugin marketplace.
//!
//! # Modules
//!
//! - [`plugin`] — Plugin lifecycle, loading, and sandboxing
//! - [`wasm`] — WASM runtime and host functions
//! - [`crdt`] — Conflict-Free Replicated Data Types
//! - [`marketplace`] — Plugin discovery and distribution
//! - [`faas`] — Function-as-a-Service integration
//! - [`sdk`] — Plugin SDK for building type-safe extensions
//! - [`adaptive`] — Adaptive plugin scheduling
//! - [`redis_module`] — Redis module compatibility layer

pub mod adaptive;
pub mod crdt;
pub mod faas;
pub mod marketplace;
pub mod plugin;
pub mod redis_module;
/// Plugin SDK for building type-safe Ferrite extensions.
pub mod sdk;
pub mod wasm;
