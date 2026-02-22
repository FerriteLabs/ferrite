// Extension crate under active development — using targeted dead_code annotations
// #![allow(dead_code)] -- removed, using targeted annotations

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
