// Extension crate under active development — using targeted dead_code annotations
// #![allow(dead_code)] -- removed, using targeted annotations
#![forbid(unsafe_code)]
//! # ferrite-search
//!
//! Full-text search, query engine, and auto-indexing for Ferrite

pub mod autoindex;
pub mod bm25;
pub mod routing;
pub mod schema;
pub mod search;
