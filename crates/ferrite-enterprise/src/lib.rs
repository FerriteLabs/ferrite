// Extension crate under active development — using targeted dead_code annotations
// #![allow(dead_code)] -- removed, using targeted annotations
#![forbid(unsafe_code)]
//! # ferrite-enterprise
//!
//! Enterprise features — multi-tenancy, governance, audit, federation

pub mod tenancy;
pub mod governance;
pub mod policy;
pub mod federation;
pub mod proxy;
