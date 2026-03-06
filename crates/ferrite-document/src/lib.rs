// Extension crate under active development — using targeted dead_code annotations
#![forbid(unsafe_code)]
#![allow(missing_docs)] // Extension crate under active development; docs added progressively
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
