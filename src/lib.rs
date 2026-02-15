// Main crate under active development — dead_code suppressed at crate level
// TODO: Remove when API surface stabilizes for v1.0
#![allow(dead_code)]

//! # Ferrite
//!
//! A high-performance, tiered-storage key-value store designed as a drop-in Redis
//! replacement. Built in Rust with epoch-based concurrency and io_uring-first persistence.
//!
//! **The speed of memory, the capacity of disk, the economics of cloud.**
//!
//! This is the top-level crate that re-exports all workspace crates for a unified API.
//! Individual crates can also be used independently for a smaller dependency footprint.
//!
//! ## Crate Organization
//!
//! | Crate | Description |
//! |-------|-------------|
//! | [`ferrite-core`] | Core engine: storage, protocol, commands, clustering |
//! | [`ferrite-search`] | Full-text search, query engine, auto-indexing |
//! | [`ferrite-ai`] | Vector search, semantic caching, RAG pipelines |
//! | [`ferrite-graph`] | Graph data model and traversal |
//! | [`ferrite-timeseries`] | Time-series data ingestion and downsampling |
//! | [`ferrite-document`] | JSON document store with path queries |
//! | [`ferrite-streaming`] | Event streaming, CDC, and data pipelines |
//! | [`ferrite-cloud`] | Cloud storage integration (S3/GCS/Azure) |
//! | [`ferrite-enterprise`] | Multi-tenancy, governance, and federation |
//! | [`ferrite-plugins`] | Plugin system, WASM, and CRDTs |
//! | [`ferrite-studio`] | Web management UI and playground |

// ── Re-exports from ferrite-core ─────────────────────────────────────────────
// Core modules are re-exported at the top level for backwards compatibility.

pub use ferrite_core::config;
pub use ferrite_core::error;
pub use ferrite_core::startup_errors;
pub use ferrite_core::storage;
pub use ferrite_core::persistence;
pub use ferrite_core::protocol;
pub use ferrite_core::io;
pub use ferrite_core::metrics;
pub use ferrite_core::runtime;
pub use ferrite_core::auth;
pub use ferrite_core::cluster;
pub use ferrite_core::network;
pub use ferrite_core::embedded as core_embedded;
pub use ferrite_core::tiering;
pub use ferrite_core::observability;
pub use ferrite_core::compatibility;
pub use ferrite_core::transaction;
pub use ferrite_core::temporal;
pub use ferrite_core::triggers;
pub use ferrite_core::audit;
pub use ferrite_core::grpc;
pub use ferrite_core::query;
pub use ferrite_core::telemetry;

#[cfg(feature = "crypto")]
pub use ferrite_core::crypto;

// ── Re-exports from extension crates ─────────────────────────────────────────

pub use ferrite_search as search;
pub use ferrite_ai as ai;
pub use ferrite_graph as graph;
pub use ferrite_timeseries as timeseries;
pub use ferrite_document as document;
pub use ferrite_streaming as streaming;
pub use ferrite_plugins as plugins;

#[cfg(feature = "cloud")]
pub use ferrite_cloud as cloud;

#[cfg(feature = "experimental")]
pub use ferrite_enterprise as enterprise;

#[cfg(feature = "experimental")]
pub use ferrite_studio as studio;

#[cfg(feature = "experimental")]
pub use ferrite_k8s as k8s;

// ── Top-level re-exports for convenience ─────────────────────────────────────

pub use ferrite_core::Config;
pub use ferrite_core::{FerriteError, Result};

// ── Top-level modules (remain in this crate) ─────────────────────────────────

pub mod commands;
pub mod embedded;
pub mod server;
pub mod replication;
pub mod sdk;
pub mod migration;
pub mod wasm;

#[cfg(feature = "tui")]
pub mod tui;
