// Extension crate under active development — using targeted dead_code annotations
#![deny(unsafe_code)]
#![allow(missing_docs)] // Extension crate under active development; docs added progressively

//! # ferrite-ai
//!
//! AI/ML features for Ferrite — vector search, semantic caching, and RAG pipelines.
//!
//! This crate provides vector similarity search (HNSW, IVF, Flat), semantic
//! caching, retrieval-augmented generation (RAG), embedding management,
//! and conversational AI memory.
//!
//! # Modules
//!
//! - [`vector`] — Vector index creation and similarity search
//! - [`semantic`] — Semantic caching (cache by meaning)
//! - [`embedding`] — Embedding generation and management
//! - [`rag`] — Retrieval-Augmented Generation pipelines
//! - [`graphrag`] — Graph-enhanced RAG
//! - [`conversation`] — Conversational memory and context
//! - [`agent_memory`] — Persistent agent memory
//! - [`inference`] — Local ML model inference
//! - [`hybrid`] — Hybrid search (vector + keyword)

pub mod agent_memory;
pub mod conversation;
pub mod embedding;
pub mod graphrag;
pub mod hybrid;
pub mod inference;
pub mod rag;
pub mod semantic;
pub mod vector;
