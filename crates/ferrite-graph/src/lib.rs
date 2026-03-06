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

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod cypher;
pub mod graph;
