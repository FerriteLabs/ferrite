// Extension crate under active development — using targeted dead_code annotations
// #![allow(dead_code)] -- removed, using targeted annotations
#![forbid(unsafe_code)]
#![warn(missing_docs)]
//! # ferrite-document
//!
//! JSON document store with path queries for Ferrite

pub mod document;
