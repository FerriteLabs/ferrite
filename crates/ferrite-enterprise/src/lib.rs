// Extension crate under active development — using targeted dead_code annotations
// #![allow(dead_code)] -- removed, using targeted annotations
#![forbid(unsafe_code)]
#![warn(missing_docs)]
//! # ferrite-enterprise
//!
//! Enterprise features — multi-tenancy, governance, audit, federation

pub mod active_active;
pub mod federation;
pub mod governance;
pub mod mesh;
pub mod policy;
pub mod proxy;
pub mod tenancy;
