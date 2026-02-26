// Extension crate under active development — using targeted dead_code annotations
// #![allow(dead_code)] -- removed, using targeted annotations
#![forbid(unsafe_code)]
#![warn(missing_docs)]
//! # ferrite-graph
//!
//! Graph data model and traversal operations for Ferrite

pub mod cypher;
pub mod graph;
