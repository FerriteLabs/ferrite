// Extension crate under active development — using targeted dead_code annotations
// #![allow(dead_code)] -- removed, using targeted annotations
#![forbid(unsafe_code)]
#![warn(missing_docs)]
//! # ferrite-graph
//!
//! Graph data model and traversal operations for Ferrite.
//!
//! Provides a property graph model with Cypher query language support,
//! enabling graph traversal, pattern matching, and relationship queries.
//!
//! # Modules
//!
//! - [`graph`] — Graph storage, nodes, edges, and traversal
//! - [`cypher`] — Cypher query language parser and executor

pub mod cypher;
pub mod graph;
