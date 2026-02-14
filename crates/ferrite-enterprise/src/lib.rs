// Extension crate under active development — dead_code suppressed at crate level
// TODO: Remove when API surface stabilizes for v1.0
#![allow(dead_code)]
#![forbid(unsafe_code)]
//! # ferrite-enterprise
//!
//! Enterprise features — multi-tenancy, governance, audit, federation

pub mod tenancy;
pub mod governance;
pub mod policy;
pub mod federation;
pub mod proxy;
