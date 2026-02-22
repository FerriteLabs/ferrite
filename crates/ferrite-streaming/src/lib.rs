// Extension crate under active development — using targeted dead_code annotations
// #![allow(dead_code)] -- removed, using targeted annotations
#![forbid(unsafe_code)]
//! # ferrite-streaming
//!
//! Event streaming, CDC, and data pipelines for Ferrite

pub mod streaming;
pub mod cdc;
pub mod pipeline;
