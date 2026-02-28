//! Storage module for Ferrite
//!
//! This module implements the storage engine with support for multiple databases.
//! The storage system uses epoch-based memory reclamation for lock-free reads.
//!
//! ## Architecture
//!
//! The storage engine provides multiple storage backends:
//!
//! 1. **Memory Store**: Simple DashMap-based storage for Phase 1
//! 2. **HybridLog**: Three-tier storage (Hot/Warm/Cold) for Phase 2+
//! 3. **Cloud**: Cloud storage tiering (S3/GCS/Azure) for Phase 4
//!
//! The HybridLog uses epoch-based reclamation for safe lock-free reads.
//! Cloud tiering allows cold data to be automatically migrated to cloud storage.

//! Cloud tiering is gated behind the `cloud` feature flag.
pub mod backend;
#[cfg(feature = "cloud")]
pub mod cloud;
pub mod epoch;
pub(crate) mod expiration;
pub mod hybridlog;
mod memory;
/// Memory allocator management, fragmentation tracking, and active defragmentation.
pub mod memory_manager;
pub mod streams;

pub use backend::{HybridLogBackendConfig, SerializableEntry, ValueType};
pub use hybridlog::{
    // Memory pressure
    detect_pressure,
    // Checksum & recovery
    recovery_scan,
    repair_log,
    start_background_tasks,
    BackgroundTaskConfig,
    BackgroundTaskHandle,
    ChecksummedHeader,
    // Compaction
    CompactionConfig,
    CompactionMetrics,
    CompactionSnapshot,
    CompactionState,
    EvictionPolicy,
    HybridLog,
    HybridLogConfig,
    HybridLogStats,
    // Telemetry
    HybridLogTelemetry,
    LatencyTimer,
    MemoryPressureConfig,
    MemoryPressureMetrics,
    MemoryPressureSnapshot,
    PressureLevel,
    RecoverySummary,
    TierMetrics,
    TierSnapshot,
};
pub use memory::{Database, Entry, StorageBackend, Store, Value};
pub use streams::{
    Stream, StreamConsumer, StreamConsumerGroup, StreamEntry, StreamEntryId, StreamPendingEntry,
};
