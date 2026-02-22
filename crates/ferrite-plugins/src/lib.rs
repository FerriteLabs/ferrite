// Extension crate under active development — using targeted dead_code annotations
// #![allow(dead_code)] -- removed, using targeted annotations

//! # ferrite-plugins
//!
//! Plugin system, WASM runtime, and CRDTs for Ferrite

pub mod plugin;
pub mod redis_module;
pub mod wasm;
pub mod marketplace;
pub mod crdt;
pub mod adaptive;
