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

pub mod backend;
#[cfg(feature = "cloud")]
pub mod cloud;
pub mod epoch;
pub(crate) mod expiration;
pub mod hybridlog;
mod memory;
pub mod streams;

pub use backend::{HybridLogBackendConfig, SerializableEntry, ValueType};
pub use hybridlog::{
    start_background_tasks, BackgroundTaskConfig, BackgroundTaskHandle, HybridLog, HybridLogConfig,
    HybridLogStats,
    // Checksum & recovery
    recovery_scan, repair_log, ChecksummedHeader, RecoverySummary,
    // Compaction
    CompactionConfig, CompactionMetrics, CompactionSnapshot, CompactionState,
    // Memory pressure
    detect_pressure, EvictionPolicy, MemoryPressureConfig, MemoryPressureMetrics,
    MemoryPressureSnapshot, PressureLevel,
    // Telemetry
    HybridLogTelemetry, LatencyTimer, TierMetrics, TierSnapshot,
};
pub use memory::{Database, Entry, StorageBackend, Store, Value};
pub use streams::{
    Stream, StreamConsumer, StreamConsumerGroup, StreamEntry, StreamEntryId, StreamPendingEntry,
};
