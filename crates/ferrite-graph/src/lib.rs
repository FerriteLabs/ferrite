// Extension crate under active development — using targeted dead_code annotations
// #![allow(dead_code)] -- removed, using targeted annotations
#![forbid(unsafe_code)]
//! # ferrite-graph
//!
//! Graph data model and traversal operations for Ferrite

pub mod cypher;
pub mod graph;
