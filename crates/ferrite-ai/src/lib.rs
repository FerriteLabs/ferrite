// Extension crate under active development — using targeted dead_code annotations
// #![allow(dead_code)] -- removed, using targeted annotations

//! # ferrite-ai
//!
//! AI/ML features for Ferrite — vector search, semantic caching, RAG

pub mod agent_memory;
pub mod conversation;
pub mod embedding;
pub mod graphrag;
pub mod hybrid;
pub mod inference;
pub mod rag;
pub mod semantic;
pub mod vector;
