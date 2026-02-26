// Extension crate under active development — using targeted dead_code annotations
// #![allow(dead_code)] -- removed, using targeted annotations
#![forbid(unsafe_code)]
#![warn(missing_docs)]
//! # ferrite-cloud
//!
//! Cloud storage integration (S3/GCS/Azure) for Ferrite

pub mod cloud;
pub mod costoptimizer;
pub mod edge;
pub mod multicloud;
pub mod s3;
pub mod serverless;
