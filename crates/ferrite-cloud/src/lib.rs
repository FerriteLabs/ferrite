// Extension crate under active development — using targeted dead_code annotations
// #![allow(dead_code)] -- removed, using targeted annotations
#![forbid(unsafe_code)]
#![warn(missing_docs)]
//! # ferrite-cloud
//!
//! Cloud storage integration (S3/GCS/Azure) for Ferrite.
//!
//! Provides cloud-tier storage backends, cost optimization, edge deployment,
//! and multi-cloud management for cold data tiering.
//!
//! # Modules
//!
//! - [`cloud`] — Core cloud storage abstraction
//! - [`s3`] — S3-compatible storage backend
//! - [`multicloud`] — Multi-cloud failover and routing
//! - [`edge`] — Edge deployment and sync
//! - [`serverless`] — Serverless function integration
//! - [`costoptimizer`] — Cost-aware storage tiering
//! - [`managed`] — Managed service integration

pub mod cloud;
pub mod costoptimizer;
pub mod edge;
pub mod managed;
pub mod multicloud;
pub mod s3;
pub mod serverless;
