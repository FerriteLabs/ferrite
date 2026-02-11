// Extension crate under active development — dead_code suppressed at crate level
// TODO: Remove when API surface stabilizes for v1.0
#![allow(dead_code)]
#![forbid(unsafe_code)]
//! # ferrite-search
//!
//! Full-text search, query engine, and auto-indexing for Ferrite

pub mod autoindex;
pub mod routing;
pub mod schema;
pub mod search;
