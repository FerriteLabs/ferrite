// Extension crate under active development — using targeted dead_code annotations
// #![allow(dead_code)] -- removed, using targeted annotations
#![forbid(unsafe_code)]
#![warn(missing_docs)]
//! # ferrite-document
//!
//! JSON document store with path queries for Ferrite.
//!
//! Provides MongoDB-compatible document storage with JSONPath queries,
//! indexing, and aggregation over nested JSON structures.
//!
//! # Modules
//!
//! - [`document`] — Document storage, queries, and indexing

pub mod document;
