// Extension crate under active development — using targeted dead_code annotations
// #![allow(dead_code)] -- removed, using targeted annotations
#![forbid(unsafe_code)]
#![warn(missing_docs)]
//! # ferrite-streaming
//!
//! Event streaming, CDC, and data pipelines for Ferrite

pub mod cdc;
pub mod kafka;
pub mod pipeline;
pub mod streaming;
