// Extension crate under active development — dead_code suppressed at crate level
// TODO: Remove when API surface stabilizes for v1.0
#![allow(dead_code)]
#![forbid(unsafe_code)]
//! # ferrite-cloud
//!
//! Cloud storage integration (S3/GCS/Azure) for Ferrite

pub mod cloud;
pub mod s3;
pub mod multicloud;
pub mod serverless;
pub mod edge;
pub mod costoptimizer;
