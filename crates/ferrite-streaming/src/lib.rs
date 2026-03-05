// Extension crate under active development — using targeted dead_code annotations
// #![allow(dead_code)] -- removed, using targeted annotations
#![forbid(unsafe_code)]
#![warn(missing_docs)]
//! # ferrite-streaming
//!
//! Event streaming, CDC, and data pipelines for Ferrite.
//!
//! Provides change data capture (CDC) for tracking mutations, Kafka-compatible
//! event streaming, and composable data pipelines for real-time processing.
//!
//! # Modules
//!
//! - [`cdc`] — Change Data Capture engine
//! - [`streaming`] — Core event streaming infrastructure
//! - [`kafka`] — Kafka-compatible protocol adapter
//! - [`pipeline`] — Composable data transformation pipelines

pub mod cdc;
pub mod kafka;
pub mod pipeline;
pub mod streaming;
