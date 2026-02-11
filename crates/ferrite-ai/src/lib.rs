// Extension crate under active development — dead_code suppressed at crate level
// TODO: Remove when API surface stabilizes for v1.0
#![allow(dead_code)]

//! # ferrite-ai
//!
//! AI/ML features for Ferrite — vector search, semantic caching, RAG

pub mod vector;
pub mod semantic;
pub mod rag;
pub mod graphrag;
pub mod embedding;
pub mod inference;
pub mod agent_memory;
pub mod conversation;
