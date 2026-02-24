// Extension crate under active development — using targeted dead_code annotations
// #![allow(dead_code)] -- removed, using targeted annotations

//! # ferrite-plugins
//!
//! Plugin system, WASM runtime, and CRDTs for Ferrite

pub mod adaptive;
pub mod crdt;
pub mod faas;
pub mod marketplace;
pub mod plugin;
pub mod redis_module;
/// Plugin SDK for building type-safe Ferrite extensions.
pub mod sdk;
pub mod wasm;
