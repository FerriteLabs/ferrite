// Extension crate under active development — using targeted dead_code annotations
// #![allow(dead_code)] -- removed, using targeted annotations
#![forbid(unsafe_code)]
//! # ferrite-studio
//!
//! Web-based management UI and interactive playground for Ferrite

pub mod devtools;
pub mod insights;
pub mod playground;
pub mod studio;
