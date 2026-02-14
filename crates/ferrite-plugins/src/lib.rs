// Extension crate under active development — dead_code suppressed at crate level
// TODO: Remove when API surface stabilizes for v1.0
#![allow(dead_code)]

//! # ferrite-plugins
//!
//! Plugin system, WASM runtime, and CRDTs for Ferrite

pub mod plugin;
pub mod redis_module;
pub mod wasm;
pub mod marketplace;
pub mod crdt;
pub mod adaptive;
