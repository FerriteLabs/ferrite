// Extension crate under active development — using targeted dead_code annotations
// #![allow(dead_code)] -- removed, using targeted annotations
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
