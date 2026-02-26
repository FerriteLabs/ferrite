#![forbid(unsafe_code)]
//! Multi-region active-active replication with conflict resolution.
//!
//! Provides vector clocks for causal ordering, conflict detection/resolution,
//! region management, and a cross-region replication engine.

pub mod conflict;
pub mod region;
pub mod replicator;
pub mod vector_clock;

pub use conflict::{ConflictResolution, ConflictResolver, ConflictStrategy, ConflictWinner};
pub use region::{Region, RegionStatus};
pub use replicator::{
    ActiveActiveReplicator, ConsistencyLevel, ReplicationConfig, ReplicationStats,
};
pub use vector_clock::{ClockOrdering, ConflictRecord, ConflictResolutionKind, VectorClock};
