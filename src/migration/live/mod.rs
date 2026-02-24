//! Zero-downtime live migration from Redis to Ferrite.
//!
//! This module provides a complete migration pipeline:
//! 1. Connect to a source Redis instance via RESP
//! 2. Bulk-sync all keys (SCAN + GET/DUMP)
//! 3. Continuously replicate changes via keyspace notifications
//! 4. Verify consistency by sampling keys
//! 5. Cut over traffic to Ferrite
//!
//! # Usage
//!
//! ```ignore
//! use ferrite::migration::live::{MigrationEngine, MigrationConfig};
//!
//! let engine = MigrationEngine::new("redis://localhost:6379".into(), MigrationConfig::default());
//! let state = engine.start_bulk_sync().await?;
//! engine.verify().await?;
//! engine.cutover().await?;
//! ```

#![forbid(unsafe_code)]

pub mod connector;
pub mod state;
pub mod sync_engine;
pub mod verifier;

pub use connector::{KeyDump, RedisConnector, RedisInfo};
pub use state::{MigrationPhase, MigrationState, MigrationStatus};
pub use sync_engine::{MigrationConfig, MigrationEngine};
pub use verifier::{MigrationVerifier, VerificationReport};
