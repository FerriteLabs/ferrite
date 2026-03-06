//! Command handlers organized by category
//!
//! This module contains handler functions extracted from the monolithic executor.
//! Each submodule groups related commands together for better maintainability.
//!
//! # Architecture & Migration Plan
//!
//! This module serves as the **integration layer** between the RESP protocol dispatcher
//! and the underlying extension crates. Over time, handler logic will progressively
//! migrate into their respective extension crates (under `crates/`), with this module
//! delegating to trait-based `CommandHandler` implementations rather than containing
//! the logic directly.
//!
//! # Handler Maturity & Persistence Model
//!
//! Handlers fall into two categories based on how they manage state:
//!
//! ## Store-integrated (production-grade)
//!
//! These handlers operate on the main [`Store`] and participate fully in persistence,
//! replication, and cluster routing. Their data survives restarts and is included in
//! RDB/AOF snapshots, replication streams, and backup/restore operations.
//!
//! **Handlers**: `admin`, `cluster`, `keys`, `server`, `transaction`, `query`,
//! `replication_cmd`, `ebpf`, `version`, `temporal`, `trigger`, `views`
//!
//! ## OnceLock-singleton (experimental)
//!
//! These handlers manage state via `static OnceLock<T>` singletons initialized on
//! first use. This means:
//!
//! - **No persistence**: State is lost on server restart
//! - **No replication**: State is not propagated to replicas
//! - **No backup/restore**: State is not included in RDB/AOF snapshots
//! - **No cluster routing**: Operations execute on the local node only
//!
//! This is an intentional design choice for rapid feature prototyping. Each handler
//! will be migrated to Store-integrated persistence as it matures.
//!
//! **Handlers**: `agent`, `analytics`, `audit`, `chaos`, `classify`, `cloud`,
//! `consensus`, `contract`, `costoptimizer`, `document`, `edge`, `feature_store`,
//! `federation`, `functions`, `gateway`, `global_index`, `graph`, `inference`,
//! `lineage`, `locks`, `marketplace`, `mesh`, `multicloud`, `observe`, `optimizer`,
//! `pipeline`, `policy`, `policy_engine`, `protocol`, `rag`, `s3`, `scaling`,
//! `schema`, `semantic`, `slots`, `smart_proxy_cmd`, `timeseries`, `vector`,
//! `vector_ingest`, `wasm`
//!
//! ## Dead code (declared but not routed)
//!
//! The following handlers are fully implemented but not yet wired into the command
//! dispatcher. They are preserved for future activation:
//!
//! `autoindex`, `conversation`, `costoptimizer`, `multicloud`, `policy`, `s3`,
//! `slots`, `vector_ingest`
//!
//! ## Migration Targets
//!
//! | Handler file      | Target crate          | Status    |
//! |-------------------|-----------------------|-----------|
//! | `vector.rs`       | `ferrite-ai`          | Pending   |
//! | `semantic.rs`     | `ferrite-ai`          | Pending   |
//! | `rag.rs`          | `ferrite-ai`          | Pending   |
//! | `graph.rs`        | `ferrite-graph`       | Pending   |
//! | `timeseries.rs`   | `ferrite-timeseries`  | Pending   |
//! | `document.rs`     | `ferrite-document`    | Pending   |
//! | `crdt.rs`         | `ferrite-plugins`     | Pending   |
//! | `wasm.rs`         | `ferrite-plugins`     | Pending   |
//! | `schema.rs`       | `ferrite-search`      | Pending   |
//! | `autoindex.rs`    | `ferrite-search`      | Pending   |
//!
//! ## Non-migrating handlers
//!
//! The following handlers are part of the core integration layer and will remain here:
//! `admin`, `cluster`, `keys`, `server`, `transaction`, `temporal`, `trigger`.
//!
//! ## How migration works
//!
//! 1. Each extension crate defines a `CommandHandler` trait with its dispatch interface.
//! 2. The extension crate exposes a `pub mod handlers` re-exporting handler functions.
//! 3. This module delegates to the crate's handler, keeping the RESP dispatch stable.
//! 4. The handler file here becomes a thin shim that calls into the extension crate.

use std::sync::Arc;

use bytes::Bytes;

use crate::auth::SharedAcl;
use crate::config::SharedConfig;
use crate::protocol::Frame;
use crate::runtime::{SharedClientRegistry, SharedSlowLog, SharedSubscriptionManager};
use crate::storage::Store;

use super::blocking::{
    SharedBlockingListManager, SharedBlockingSortedSetManager, SharedBlockingStreamManager,
};
use super::ScriptExecutor;

// Handler modules — core
pub mod admin;
pub mod agent;
pub mod audit;
pub mod cluster;
pub mod crdt;
pub mod ebpf;
pub mod federation;
pub mod keys;
pub mod query;
pub mod server;
#[cfg(feature = "experimental")]
pub mod slots;
pub mod temporal;
pub mod timeseries;
pub mod transaction;
pub mod trigger;
pub mod vector;
pub mod version;
pub mod wasm;

// Handler modules — serverless functions
pub mod functions;

// Handler modules — experimental (require "experimental" feature)
#[cfg(feature = "experimental")]
pub mod autoindex;
#[cfg(feature = "experimental")]
pub mod conversation;
#[cfg(feature = "experimental")]
pub mod costoptimizer;
#[cfg(feature = "experimental")]
pub mod document;
#[cfg(feature = "experimental")]
pub mod graph;
#[cfg(feature = "experimental")]
pub mod inference;
#[cfg(feature = "experimental")]
pub mod multicloud;
#[cfg(feature = "experimental")]
pub mod policy;
#[cfg(feature = "experimental")]
pub mod replication_cmd;
#[cfg(feature = "experimental")]
pub mod s3;
#[cfg(feature = "experimental")]
pub mod schema;

// Handler modules — cloud feature
#[cfg(feature = "cloud")]
pub mod cloud;
#[cfg(feature = "cloud")]
pub mod edge;
#[cfg(feature = "cloud")]
pub mod rag;
#[cfg(feature = "cloud")]
pub mod semantic;

// Handler modules — protocol
pub mod protocol;

// Handler modules — observability
pub mod observe;
pub mod views;

// Handler modules — data lineage & pipelines
pub mod lineage;
pub mod optimizer;
pub mod pipeline;

// Handler modules — smart proxy
pub mod smart_proxy_cmd;

// Handler modules — distributed locking
pub mod locks;

// Handler modules — consensus-as-a-service
pub mod consensus;

// Handler modules — real-time feature store
pub mod feature_store;

// Handler modules — programmable access control
pub mod policy_engine;

// Handler modules — data contract registry
pub mod contract;

// Handler modules — analytics engine
pub mod analytics;

// Handler modules — global secondary indexes
pub mod global_index;

// Handler modules — chaos engineering
pub mod chaos;

// Handler modules — WASM extension marketplace
pub mod marketplace;

// Handler modules — predictive scaling
#[cfg(feature = "cloud")]
pub mod scaling;

// Handler modules — data classification
pub mod classify;

// Handler modules — edge mesh networking
#[cfg(feature = "cloud")]
pub mod mesh;

/// Context passed to all handler functions
///
/// This struct provides access to all shared state needed by command handlers.
/// It borrows from CommandExecutor to avoid unnecessary cloning.
pub struct HandlerContext<'a> {
    /// Shared storage
    pub store: &'a Arc<Store>,
    /// Shared pub/sub subscription manager
    pub pubsub_manager: &'a SharedSubscriptionManager,
    /// Shared ACL manager
    pub acl: &'a SharedAcl,
    /// Lua script executor
    pub script_executor: &'a ScriptExecutor,
    /// Blocking list manager for BLPOP/BRPOP
    pub blocking_manager: &'a SharedBlockingListManager,
    /// Blocking stream manager for XREAD BLOCK/XREADGROUP BLOCK
    pub blocking_stream_manager: &'a SharedBlockingStreamManager,
    /// Blocking sorted set manager for BZPOPMIN/BZPOPMAX/BZMPOP
    pub blocking_zset_manager: &'a SharedBlockingSortedSetManager,
    /// Shared configuration for hot reload
    pub config: &'a Option<SharedConfig>,
    /// Slow query log
    pub slowlog: &'a SharedSlowLog,
    /// Client registry for tracking connected clients
    pub client_registry: &'a SharedClientRegistry,
    /// Current database index
    pub db: u8,
}

impl<'a> HandlerContext<'a> {
    /// Create a new handler context
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        store: &'a Arc<Store>,
        pubsub_manager: &'a SharedSubscriptionManager,
        acl: &'a SharedAcl,
        script_executor: &'a ScriptExecutor,
        blocking_manager: &'a SharedBlockingListManager,
        blocking_stream_manager: &'a SharedBlockingStreamManager,
        blocking_zset_manager: &'a SharedBlockingSortedSetManager,
        config: &'a Option<SharedConfig>,
        slowlog: &'a SharedSlowLog,
        client_registry: &'a SharedClientRegistry,
        db: u8,
    ) -> Self {
        Self {
            store,
            pubsub_manager,
            acl,
            script_executor,
            blocking_manager,
            blocking_stream_manager,
            blocking_zset_manager,
            config,
            slowlog,
            client_registry,
            db,
        }
    }
}

/// Helper to create error frames
#[inline]
pub fn err_frame(msg: &str) -> Frame {
    Frame::Error(Bytes::from(format!("ERR {}", msg)))
}

/// Helper to create simple OK frame
#[inline]
pub fn ok_frame() -> Frame {
    Frame::Simple(Bytes::from("OK"))
}

/// Helper to create PONG frame
#[inline]
pub fn pong_frame() -> Frame {
    Frame::Simple(Bytes::from("PONG"))
}

/// Log a one-time warning that a command uses ephemeral in-memory state.
///
/// Experimental handlers store data in process-local `OnceLock` singletons
/// rather than the main [`Store`]. This means their state is **not persisted**,
/// **not replicated**, and **not included in backups**. This helper emits a
/// single warning per command family so operators are aware.
#[cold]
pub fn warn_experimental(command_family: &str) {
    // tracing::warn_once is not available; use a static set to deduplicate.
    use std::sync::Mutex;
    use std::collections::HashSet;

    static WARNED: std::sync::LazyLock<Mutex<HashSet<String>>> =
        std::sync::LazyLock::new(|| Mutex::new(HashSet::new()));

    if let Ok(mut set) = WARNED.lock() {
        if set.insert(command_family.to_owned()) {
            tracing::warn!(
                command_family,
                "Experimental command family '{}' uses ephemeral in-memory state. \
                 Data will NOT be persisted, replicated, or included in backups.",
                command_family,
            );
        }
    }
}
