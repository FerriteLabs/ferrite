//! # CRDT (Conflict-free Replicated Data Types)
//!
//! Built-in CRDT support for geo-distributed deployments without coordination
//! overhead. CRDTs enable true multi-master replication with automatic,
//! deterministic conflict resolution.
//!
//! ## Why CRDTs?
//!
//! In distributed systems, concurrent updates can conflict. Traditional approaches
//! require coordination (consensus, locks) which adds latency. CRDTs guarantee:
//!
//! - **Strong Eventual Consistency**: All replicas converge to the same state
//! - **No Coordination**: Updates accepted locally without waiting
//! - **Deterministic Merging**: Same result regardless of message order
//! - **Partition Tolerance**: Continue operating during network splits
//!
//! ## CRDT Types
//!
//! ### Counters
//!
//! | Type | Operations | Use Case |
//! |------|------------|----------|
//! | GCounter | increment | Page views, likes |
//! | PNCounter | increment, decrement | Inventory, balances |
//!
//! ### Registers
//!
//! | Type | Resolution | Use Case |
//! |------|------------|----------|
//! | LwwRegister | Last-writer-wins (timestamp) | User profiles, settings |
//! | MvRegister | Multi-value (preserve all) | Shopping carts, comments |
//!
//! ### Sets
//!
//! | Type | Operations | Use Case |
//! |------|------------|----------|
//! | OrSet | add, remove | Tags, memberships |
//! | LwwElementSet | add, remove (with scores) | Leaderboards |
//!
//! ### Maps
//!
//! | Type | Operations | Use Case |
//! |------|------------|----------|
//! | OrMap | set field, remove field | User objects, documents |
//!
//! ## Quick Start
//!
//! ### PNCounter (Positive-Negative Counter)
//!
//! ```no_run
//! use ferrite::crdt::{PNCounter, SiteId};
//!
//! // Each datacenter has a unique site ID
//! let us_east = SiteId::new(1);
//! let eu_west = SiteId::new(2);
//!
//! // US East increments inventory
//! let mut counter_us = PNCounter::new();
//! counter_us.increment(us_east, 100);  // +100 items received
//!
//! // EU West processes orders (concurrent)
//! let mut counter_eu = PNCounter::new();
//! counter_eu.decrement(eu_west, 25);   // -25 items sold
//!
//! // Replicas sync - merge is commutative and idempotent
//! counter_us.merge(&counter_eu);
//! assert_eq!(counter_us.value(), 75);  // 100 - 25 = 75
//!
//! // Merge again (idempotent - no change)
//! counter_us.merge(&counter_eu);
//! assert_eq!(counter_us.value(), 75);
//! ```
//!
//! ### LwwRegister (Last-Writer-Wins Register)
//!
//! ```no_run
//! use ferrite::crdt::{LwwRegister, HybridTimestamp};
//!
//! // User updates profile from two devices
//! let mut reg1 = LwwRegister::new("Alice".to_string());
//! let mut reg2 = LwwRegister::new("Alice".to_string());
//!
//! // Phone update (earlier timestamp)
//! reg1.set("Alice Smith".to_string(), HybridTimestamp::new(100, 0, 1));
//!
//! // Laptop update (later timestamp)
//! reg2.set("Alice J. Smith".to_string(), HybridTimestamp::new(200, 0, 2));
//!
//! // Merge - later timestamp wins
//! reg1.merge(&reg2);
//! assert_eq!(reg1.value(), "Alice J. Smith");
//! ```
//!
//! ### OrSet (Observed-Remove Set)
//!
//! ```no_run
//! use ferrite::crdt::{OrSet, SiteId};
//!
//! let site1 = SiteId::new(1);
//! let site2 = SiteId::new(2);
//!
//! // Site 1 adds tags
//! let mut tags1 = OrSet::new();
//! tags1.add("rust", site1);
//! tags1.add("database", site1);
//!
//! // Site 2 adds different tags concurrently
//! let mut tags2 = OrSet::new();
//! tags2.add("redis", site2);
//! tags2.add("database", site2);  // Also adds "database"
//!
//! // Merge preserves all unique adds
//! tags1.merge(&tags2);
//! assert!(tags1.contains("rust"));
//! assert!(tags1.contains("redis"));
//! assert!(tags1.contains("database"));
//!
//! // Remove only affects local observations
//! tags1.remove("database", site1);
//! tags1.merge(&tags2);  // site2's "database" still exists
//! assert!(tags1.contains("database"));  // Re-added by site2
//! ```
//!
//! ### OrMap (Observed-Remove Map)
//!
//! ```no_run
//! use ferrite::crdt::{OrMap, SiteId, CrdtValue};
//!
//! let site1 = SiteId::new(1);
//! let site2 = SiteId::new(2);
//!
//! // Build a user profile across datacenters
//! let mut profile1 = OrMap::new();
//! profile1.set("name", CrdtValue::String("Alice".to_string()), site1);
//! profile1.set("email", CrdtValue::String("alice@example.com".to_string()), site1);
//!
//! let mut profile2 = OrMap::new();
//! profile2.set("name", CrdtValue::String("Alice Smith".to_string()), site2);
//! profile2.set("phone", CrdtValue::String("+1234567890".to_string()), site2);
//!
//! // Merge combines all fields, resolves conflicts
//! profile1.merge(&profile2);
//! // "name" conflict resolved by timestamp
//! // "email" and "phone" both preserved
//! ```
//!
//! ## Redis CLI Commands
//!
//! ```text
//! # PNCounter operations
//! CRDT.INCR inventory:item123 100
//! CRDT.DECR inventory:item123 25
//! CRDT.GET inventory:item123
//!
//! # LwwRegister operations
//! CRDT.SET user:123:name "Alice Smith"
//! CRDT.GET user:123:name
//!
//! # OrSet operations
//! CRDT.SADD tags:post:456 "rust" "database"
//! CRDT.SREM tags:post:456 "database"
//! CRDT.SMEMBERS tags:post:456
//!
//! # Sync state between sites
//! CRDT.SYNC inventory:item123 <encoded_state>
//! ```
//!
//! ## Hybrid Logical Clocks
//!
//! Ferrite uses Hybrid Logical Clocks (HLC) for causality tracking:
//!
//! ```no_run
//! use ferrite::crdt::{HybridClock, HybridTimestamp};
//!
//! // Each node maintains a hybrid clock
//! let mut clock = HybridClock::new(1);  // node_id = 1
//!
//! // Generate timestamps for local events
//! let ts1 = clock.now();
//! let ts2 = clock.now();
//! assert!(ts2 > ts1);  // Monotonically increasing
//!
//! // Receive remote timestamp and update clock
//! let remote_ts = HybridTimestamp::new(1000, 5, 2);
//! clock.update(remote_ts);
//!
//! // Next local timestamp will be >= remote
//! let ts3 = clock.now();
//! assert!(ts3 > remote_ts);
//! ```
//!
//! ## Geo-Replication Example
//!
//! ```text
//! US-East ──────────────────────────────────────▶
//!    │ SET user:123:balance 1000 (ts=100)
//!    │                    ▲
//!    │                    │ async sync
//!    ▼                    │
//! EU-West ◀──────────────────────────────────────
//!    │ DECR user:123:balance 100 (ts=150)
//!    │
//!    ▼
//! Both converge to: balance = 900
//! ```
//!
//! ## Performance
//!
//! | Operation | Latency | Notes |
//! |-----------|---------|-------|
//! | Local write | <1μs | No coordination |
//! | Merge | O(state size) | Efficient delta sync |
//! | Clock update | <100ns | HLC is fast |
//!
//! ## Best Practices
//!
//! 1. **Choose the right CRDT**: PNCounter for quantities, LwwRegister for profiles
//! 2. **Use unique site IDs**: Each datacenter needs a distinct identifier
//! 3. **Monitor clock drift**: Keep NTP synchronized across sites
//! 4. **Design for conflicts**: Multi-value registers when conflicts are important
//! 5. **Compact periodically**: Remove tombstones from OrSets

#![allow(dead_code, unused_imports, unused_variables)]
mod clock;
mod counter;
mod map;
mod register;
mod set;
pub mod sync;
mod types;

pub use clock::{HybridClock, HybridTimestamp, VectorClock};
pub use counter::{GCounter, PNCounter};
pub use map::OrMap;
pub use register::{LwwRegister, MvRegister};
pub use set::{LwwElementSet, OrSet};
pub use types::{CrdtOperation, CrdtState, CrdtType, CrdtValue, SiteId, UniqueTag};

use serde::{Deserialize, Serialize};

/// Configuration for CRDT operations
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CrdtConfig {
    /// Enable CRDT support
    pub enabled: bool,
    /// Local site identifier
    pub site_id: String,
    /// Replication mode
    pub mode: ReplicationMode,
    /// Sync interval in milliseconds
    pub sync_interval_ms: u64,
    /// Batch size for sync
    pub batch_size: usize,
    /// Log conflicts for debugging
    pub log_conflicts: bool,
}

impl Default for CrdtConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            site_id: "default".to_string(),
            mode: ReplicationMode::Async,
            sync_interval_ms: 100,
            batch_size: 1000,
            log_conflicts: true,
        }
    }
}

/// Replication mode
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReplicationMode {
    /// Asynchronous replication (default)
    Async,
    /// Synchronous replication (waits for peer acknowledgment)
    Sync,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_crdt_config_default() {
        let config = CrdtConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.site_id, "default");
        assert_eq!(config.mode, ReplicationMode::Async);
    }
}
