// Extension crate under active development — dead_code suppressed at crate level
// TODO: Remove when API surface stabilizes for v1.0
#![allow(dead_code)]
#![forbid(unsafe_code)]
//! # ferrite-streaming
//!
//! Event streaming, CDC, and data pipelines for Ferrite

pub mod streaming;
pub mod cdc;
pub mod pipeline;
