// Extension crate under active development — using targeted dead_code annotations
#![forbid(unsafe_code)]
#![allow(missing_docs)] // Extension crate under active development; docs added progressively
//! # ferrite-search
//!
//! Full-text search, query engine, and auto-indexing for Ferrite.
//!
//! This crate provides BM25-based full-text search, schema management,
//! automatic indexing, and query routing for the Ferrite database.
//!
//! # Modules
//!
//! - [`search`] — Core search engine with query execution
//! - [`bm25`] — BM25 relevance scoring
//! - [`schema`] — Index schema definition and management
//! - [`autoindex`] — Automatic field indexing
//! - [`routing`] — Query routing for distributed search
//! - [`global_index`] — Global index coordination

pub mod autoindex;
pub mod bm25;
pub mod global_index;
pub mod routing;
pub mod schema;
pub mod search;
