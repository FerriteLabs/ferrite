//! # ferrite-rs — Official Rust Client SDK for Ferrite
//!
//! An ergonomic, high-performance Rust client for [Ferrite](https://github.com/ferritelabs/ferrite),
//! a Redis-compatible key-value store. Also works with standard Redis servers.
//!
//! ## Features
//!
//! - **Async-first** — built on Tokio with a full async API
//! - **Blocking wrapper** — `Client` for non-async contexts
//! - **Connection pooling** — configurable pool with automatic reconnection
//! - **RESP2 protocol** — faithful implementation of the Redis wire protocol
//! - **Builder pattern** — `client.set("k", "v").ex(3600).nx().execute().await?`
//! - **TLS support** — optional, via the `tls` feature flag
//! - **Comprehensive commands** — strings, lists, hashes, sets, sorted sets, server
//!
//! ## Quick Start (async)
//!
//! ```ignore
//! use ferrite_rs::AsyncClient;
//!
//! #[tokio::main]
//! async fn main() -> ferrite_rs::Result<()> {
//!     let client = AsyncClient::connect("127.0.0.1", 6379).await?;
//!
//!     // SET with builder pattern
//!     client.set("greeting", "hello").ex(60).execute().await?;
//!
//!     // GET
//!     let val = client.get("greeting").await?;
//!     println!("greeting = {}", val);
//!
//!     Ok(())
//! }
//! ```
//!
//! ## Quick Start (blocking)
//!
//! ```ignore
//! use ferrite_rs::Client;
//!
//! fn main() -> ferrite_rs::Result<()> {
//!     let client = Client::connect("127.0.0.1", 6379)?;
//!     client.set("counter", "0")?;
//!     let n = client.incr("counter")?;
//!     println!("counter = {}", n);
//!     Ok(())
//! }
//! ```

pub mod async_client;
pub mod client;
pub mod commands;
pub mod connection;
pub mod error;
pub mod pool;
pub mod resp;
pub mod types;

// ── Re-exports for ergonomic top-level usage ────────────────────────────────

pub use async_client::AsyncClient;
pub use client::Client;
pub use connection::ConnectionConfig;
pub use error::{Error, Result};
pub use pool::PoolConfig;
pub use types::{ToArg, Value};

// ── Ferrite-specific command builders ───────────────────────────────────────

pub use commands::ferriteql::{CreateViewCommand, DropViewCommand, QueryCommand, QueryViewCommand};
pub use commands::semantic::{
    SemanticDelCommand, SemanticGetCommand, SemanticSetCommand, SemanticStatsCommand,
};
pub use commands::vector::{
    VectorAddCommand, VectorCreateCommand, VectorDeleteCommand, VectorInfoCommand,
    VectorSearchCommand,
};
