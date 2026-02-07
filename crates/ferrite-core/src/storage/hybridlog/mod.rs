//! HybridLog - Three-tier storage engine
//!
//! The HybridLog is inspired by Microsoft's FASTER system. It provides
//! a three-tier storage architecture:
//!
//! 1. **Mutable Region** (Hot): In-memory, append-only, lock-free reads
//! 2. **Read-Only Region** (Warm): Memory-mapped, zero-copy reads
//! 3. **Disk Region** (Cold): Async file I/O
//!
//! Data automatically migrates between tiers based on access patterns.

pub mod address;
pub mod checksum;
pub mod compaction;
pub mod disk;
pub mod log;
pub mod memory_pressure;
pub mod mutable;
pub mod readonly;
pub mod telemetry;

#[cfg(test)]
pub mod stress;

pub use address::{LogAddress, Region};
pub use checksum::{recovery_scan, repair_log, ChecksummedHeader, RecoverySummary};
pub use compaction::{CompactionConfig, CompactionMetrics, CompactionSnapshot, CompactionState};
pub use disk::DiskRegion;
pub use log::{
    start_background_tasks, BackgroundTaskConfig, BackgroundTaskHandle, HybridLog, HybridLogConfig,
    HybridLogStats,
};
pub use memory_pressure::{
    detect_pressure, EvictionPolicy, MemoryPressureConfig, MemoryPressureMetrics,
    MemoryPressureSnapshot, PressureLevel,
};
pub use mutable::{EntryHeader, MutableRegion};
pub use readonly::ReadOnlyRegion;
pub use telemetry::{HybridLogTelemetry, LatencyTimer, TierMetrics, TierSnapshot};
