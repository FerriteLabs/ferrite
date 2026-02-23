//! Memory management and defragmentation engine
//!
//! This module provides memory tracking, allocation accounting, eviction policy
//! management, and defragmentation analysis for the Ferrite storage engine.
//!
//! ## Features
//!
//! - Per-category memory tracking with atomic counters
//! - Configurable eviction policies (LRU, LFU, Random, TTL)
//! - Fragmentation detection and defrag recommendations
//! - Memory doctor diagnostics with actionable findings

use std::sync::atomic::{AtomicU64, Ordering};

use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use thiserror::Error;

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Errors returned by memory management operations
#[derive(Debug, Error)]
pub enum MemoryError {
    /// Allocation rejected — not enough memory available
    #[error("out of memory: requested {requested} bytes but only {available} available")]
    OutOfMemory {
        /// Bytes requested
        requested: u64,
        /// Bytes still available
        available: u64,
    },

    /// Eviction attempt failed
    #[error("eviction failed")]
    EvictionFailed,

    /// Defragmentation cycle failed
    #[error("defrag failed: {0}")]
    DefragFailed(String),

    /// Internal / unexpected error
    #[error("internal error: {0}")]
    Internal(String),
}

// ---------------------------------------------------------------------------
// Enums
// ---------------------------------------------------------------------------

/// Eviction policy applied when `max_memory` is reached
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EvictionPolicy {
    /// Return errors on write when memory limit is reached
    NoEviction,
    /// Evict least-recently-used keys from all keys
    AllKeysLru,
    /// Evict least-recently-used keys from keys with an expiry set
    VolatileLru,
    /// Evict least-frequently-used keys from all keys
    AllKeysLfu,
    /// Evict least-frequently-used keys from keys with an expiry set
    VolatileLfu,
    /// Evict random keys from all keys
    AllKeysRandom,
    /// Evict random keys from keys with an expiry set
    VolatileRandom,
    /// Evict keys with the nearest TTL from keys with an expiry set
    VolatileTtl,
}

/// Logical memory category for per-type accounting
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum MemoryCategory {
    /// String / bytes values
    Strings,
    /// List values
    Lists,
    /// Hash map values
    Hashes,
    /// Set values
    Sets,
    /// Sorted-set values
    SortedSets,
    /// Stream values
    Streams,
    /// Vector (embedding) values
    Vectors,
    /// Document (JSON) values
    Documents,
    /// Graph values
    Graphs,
    /// Time-series values
    TimeSeries,
    /// Secondary index structures
    Indices,
    /// Internal overhead (dict entries, object headers, etc.)
    Overhead,
    /// Replication buffers
    Replication,
    /// AOF buffers
    Aof,
    /// Client connection buffers
    Clients,
    /// Scripting engine memory
    Scripts,
    /// Pub/Sub channel buffers
    PubSub,
}

/// Reason an eviction was requested
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EvictionReason {
    /// Allocated memory reached the configured maximum
    MaxMemoryReached,
    /// Fragmentation ratio exceeds threshold
    FragmentationHigh,
    /// Proactive eviction to prevent OOM
    OomPrevention,
}

/// Urgency level for an eviction request
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EvictionUrgency {
    /// Low urgency — background eviction acceptable
    Low,
    /// Medium urgency — schedule eviction soon
    Medium,
    /// High urgency — evict immediately
    High,
    /// Critical — reject writes until memory is freed
    Critical,
}

/// Recommended defragmentation action
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum DefragAction {
    /// Fragmentation is within acceptable bounds
    NotNeeded,
    /// Defragmentation would be beneficial
    Recommended {
        /// Estimated wall-clock time in milliseconds
        estimated_time_ms: u64,
    },
    /// Fragmentation is severe and must be addressed
    Urgent,
}

/// Severity level for a doctor finding
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DoctorSeverity {
    /// Everything is fine
    Ok,
    /// Informational notice
    Info,
    /// Potential problem detected
    Warning,
    /// Immediate attention required
    Critical,
}

// ---------------------------------------------------------------------------
// Data structs
// ---------------------------------------------------------------------------

/// Snapshot of current memory usage
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryUsage {
    /// Total bytes currently allocated
    pub total_allocated: u64,
    /// Configured maximum memory (0 = unlimited)
    pub max_memory: u64,
    /// Estimated resident-set size
    pub rss_estimate: u64,
    /// Fragmentation ratio (`rss / allocated`; 1.0 = no fragmentation)
    pub fragmentation_ratio: f64,
    /// Historical peak allocation
    pub peak_allocated: u64,
    /// Bytes used by the user dataset
    pub dataset_bytes: u64,
    /// Bytes used by internal overhead
    pub overhead_bytes: u64,
    /// Bytes eligible for eviction
    pub eviction_eligible_bytes: u64,
    /// Percentage of max memory used (0.0–100.0)
    pub usage_percent: f64,
}

/// Per-category memory breakdown
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CategoryUsage {
    /// Category this entry describes
    pub category: MemoryCategory,
    /// Bytes allocated in this category
    pub bytes: u64,
    /// Number of keys in this category
    pub keys: u64,
    /// Percentage of total allocated memory
    pub percent: f64,
}

/// A request to begin eviction
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvictionRequest {
    /// Why eviction was triggered
    pub reason: EvictionReason,
    /// Target number of bytes to free
    pub target_bytes: u64,
    /// Policy to apply
    pub policy: EvictionPolicy,
    /// How urgently memory must be freed
    pub urgency: EvictionUrgency,
}

/// Result of a defragmentation scan
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DefragReport {
    /// Current fragmentation ratio
    pub fragmentation_ratio: f64,
    /// Estimated wasted bytes due to fragmentation
    pub fragmented_bytes: u64,
    /// Number of keys that could be relocated
    pub relocatable_keys: u64,
    /// Bytes recoverable by defragmentation
    pub estimated_savings: u64,
    /// Recommended next step
    pub recommended_action: DefragAction,
}

/// A single diagnostic finding from the memory doctor
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DoctorFinding {
    /// Severity of the finding
    pub severity: DoctorSeverity,
    /// Affected subsystem or category
    pub category: String,
    /// Human-readable description
    pub message: String,
    /// Suggested remediation
    pub recommendation: String,
}

/// Cumulative memory statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryStats {
    /// Lifetime count of successful allocations
    pub total_allocations: u64,
    /// Lifetime count of deallocations
    pub total_deallocations: u64,
    /// Peak memory usage ever observed
    pub peak_usage: u64,
    /// Number of times eviction was triggered
    pub evictions_triggered: u64,
    /// Number of defrag cycles executed
    pub defrag_cycles: u64,
    /// Number of allocations rejected due to OOM
    pub oom_rejections: u64,
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Memory manager configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryConfig {
    /// Hard memory limit in bytes (0 = unlimited)
    pub max_memory_bytes: u64,
    /// Policy used when memory limit is reached
    pub eviction_policy: EvictionPolicy,
    /// Usage ratio (0.0–1.0) at which OOM warnings are emitted
    pub oom_warning_threshold: f64,
    /// Whether automatic defragmentation scanning is enabled
    pub defrag_enabled: bool,
    /// Fragmentation ratio above which defrag is recommended (e.g. 1.5 = 50%)
    pub defrag_threshold: f64,
    /// Target duration of each defrag cycle in milliseconds
    pub defrag_cycle_ms: u64,
    /// Per-key fixed overhead in bytes (hash entry + dict entry)
    pub overhead_per_key_bytes: u64,
}

impl Default for MemoryConfig {
    fn default() -> Self {
        Self {
            max_memory_bytes: 0,
            eviction_policy: EvictionPolicy::NoEviction,
            oom_warning_threshold: 0.9,
            defrag_enabled: true,
            defrag_threshold: 1.5,
            defrag_cycle_ms: 100,
            overhead_per_key_bytes: 72,
        }
    }
}

// ---------------------------------------------------------------------------
// Per-category atomic counters
// ---------------------------------------------------------------------------

/// Number of distinct `MemoryCategory` variants
const CATEGORY_COUNT: usize = 17;

/// Map a `MemoryCategory` to its index in the counters array.
fn category_index(cat: MemoryCategory) -> usize {
    match cat {
        MemoryCategory::Strings => 0,
        MemoryCategory::Lists => 1,
        MemoryCategory::Hashes => 2,
        MemoryCategory::Sets => 3,
        MemoryCategory::SortedSets => 4,
        MemoryCategory::Streams => 5,
        MemoryCategory::Vectors => 6,
        MemoryCategory::Documents => 7,
        MemoryCategory::Graphs => 8,
        MemoryCategory::TimeSeries => 9,
        MemoryCategory::Indices => 10,
        MemoryCategory::Overhead => 11,
        MemoryCategory::Replication => 12,
        MemoryCategory::Aof => 13,
        MemoryCategory::Clients => 14,
        MemoryCategory::Scripts => 15,
        MemoryCategory::PubSub => 16,
    }
}

/// Reconstruct a `MemoryCategory` from its index.
#[allow(dead_code)]
fn category_from_index(idx: usize) -> MemoryCategory {
    match idx {
        0 => MemoryCategory::Strings,
        1 => MemoryCategory::Lists,
        2 => MemoryCategory::Hashes,
        3 => MemoryCategory::Sets,
        4 => MemoryCategory::SortedSets,
        5 => MemoryCategory::Streams,
        6 => MemoryCategory::Vectors,
        7 => MemoryCategory::Documents,
        8 => MemoryCategory::Graphs,
        9 => MemoryCategory::TimeSeries,
        10 => MemoryCategory::Indices,
        11 => MemoryCategory::Overhead,
        12 => MemoryCategory::Replication,
        13 => MemoryCategory::Aof,
        14 => MemoryCategory::Clients,
        15 => MemoryCategory::Scripts,
        16 => MemoryCategory::PubSub,
        _ => unreachable!("invalid category index"),
    }
}

/// All categories in index order (for iteration).
const ALL_CATEGORIES: [MemoryCategory; CATEGORY_COUNT] = [
    MemoryCategory::Strings,
    MemoryCategory::Lists,
    MemoryCategory::Hashes,
    MemoryCategory::Sets,
    MemoryCategory::SortedSets,
    MemoryCategory::Streams,
    MemoryCategory::Vectors,
    MemoryCategory::Documents,
    MemoryCategory::Graphs,
    MemoryCategory::TimeSeries,
    MemoryCategory::Indices,
    MemoryCategory::Overhead,
    MemoryCategory::Replication,
    MemoryCategory::Aof,
    MemoryCategory::Clients,
    MemoryCategory::Scripts,
    MemoryCategory::PubSub,
];

/// Dataset categories (user data, not infrastructure overhead).
const DATASET_CATEGORIES: [MemoryCategory; 10] = [
    MemoryCategory::Strings,
    MemoryCategory::Lists,
    MemoryCategory::Hashes,
    MemoryCategory::Sets,
    MemoryCategory::SortedSets,
    MemoryCategory::Streams,
    MemoryCategory::Vectors,
    MemoryCategory::Documents,
    MemoryCategory::Graphs,
    MemoryCategory::TimeSeries,
];

// ---------------------------------------------------------------------------
// MemoryManager
// ---------------------------------------------------------------------------

/// Tracks memory usage, enforces limits, and provides defragmentation analysis.
pub struct MemoryManager {
    config: RwLock<MemoryConfig>,

    // Per-category byte counters
    category_bytes: [AtomicU64; CATEGORY_COUNT],
    // Per-category key counters
    category_keys: [AtomicU64; CATEGORY_COUNT],

    // Aggregate counters
    total_allocated: AtomicU64,
    peak_allocated: AtomicU64,
    rss_overhead: AtomicU64,

    // Lifetime stats
    total_allocations: AtomicU64,
    total_deallocations: AtomicU64,
    evictions_triggered: AtomicU64,
    defrag_cycles: AtomicU64,
    oom_rejections: AtomicU64,

    /// Timestamp of manager creation
    _created_at: DateTime<Utc>,
}

/// Helper: create a default array of `AtomicU64` zeroes.
fn new_atomic_array() -> [AtomicU64; CATEGORY_COUNT] {
    // AtomicU64 is not Copy so we use array::from_fn
    std::array::from_fn(|_| AtomicU64::new(0))
}

impl MemoryManager {
    /// Create a new `MemoryManager` with the given configuration.
    pub fn new(config: MemoryConfig) -> Self {
        Self {
            config: RwLock::new(config),
            category_bytes: new_atomic_array(),
            category_keys: new_atomic_array(),
            total_allocated: AtomicU64::new(0),
            peak_allocated: AtomicU64::new(0),
            rss_overhead: AtomicU64::new(0),
            total_allocations: AtomicU64::new(0),
            total_deallocations: AtomicU64::new(0),
            evictions_triggered: AtomicU64::new(0),
            defrag_cycles: AtomicU64::new(0),
            oom_rejections: AtomicU64::new(0),
            _created_at: Utc::now(),
        }
    }

    // -- allocation / deallocation -----------------------------------------

    /// Record a memory allocation of `size` bytes in the given `category`.
    ///
    /// Returns `Err(MemoryError::OutOfMemory)` if the allocation would exceed
    /// the configured `max_memory_bytes` (and the policy is `NoEviction`).
    pub fn allocate(&self, size: u64, category: MemoryCategory) -> Result<(), MemoryError> {
        let config = self.config.read();
        let max = config.max_memory_bytes;
        let policy = config.eviction_policy;
        drop(config);

        if max > 0 {
            let current = self.total_allocated.load(Ordering::Relaxed);
            if current.saturating_add(size) > max {
                self.oom_rejections.fetch_add(1, Ordering::Relaxed);
                if policy == EvictionPolicy::NoEviction {
                    return Err(MemoryError::OutOfMemory {
                        requested: size,
                        available: max.saturating_sub(current),
                    });
                }
                // Under a real eviction policy the caller would trigger
                // eviction; here we still record the allocation for
                // accounting purposes.
            }
        }

        let idx = category_index(category);
        self.category_bytes[idx].fetch_add(size, Ordering::Relaxed);
        self.category_keys[idx].fetch_add(1, Ordering::Relaxed);
        let new_total = self.total_allocated.fetch_add(size, Ordering::Relaxed) + size;
        self.total_allocations.fetch_add(1, Ordering::Relaxed);

        // Update peak
        let mut peak = self.peak_allocated.load(Ordering::Relaxed);
        while new_total > peak {
            match self.peak_allocated.compare_exchange_weak(
                peak,
                new_total,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => peak = actual,
            }
        }

        Ok(())
    }

    /// Record a deallocation of `size` bytes from the given `category`.
    pub fn deallocate(&self, size: u64, category: MemoryCategory) {
        let idx = category_index(category);
        self.category_bytes[idx].fetch_sub(size, Ordering::Relaxed);
        self.category_keys[idx].fetch_sub(1, Ordering::Relaxed);
        self.total_allocated.fetch_sub(size, Ordering::Relaxed);
        self.total_deallocations.fetch_add(1, Ordering::Relaxed);
    }

    // -- queries -----------------------------------------------------------

    /// Return a snapshot of current memory usage.
    pub fn get_usage(&self) -> MemoryUsage {
        let total_allocated = self.total_allocated.load(Ordering::Relaxed);
        let config = self.config.read();
        let max_memory = config.max_memory_bytes;
        drop(config);

        let rss_overhead = self.rss_overhead.load(Ordering::Relaxed);
        let rss_estimate = total_allocated.saturating_add(rss_overhead);

        let fragmentation_ratio = if total_allocated > 0 {
            rss_estimate as f64 / total_allocated as f64
        } else {
            1.0
        };

        let peak_allocated = self.peak_allocated.load(Ordering::Relaxed);

        let dataset_bytes: u64 = DATASET_CATEGORIES
            .iter()
            .map(|c| self.category_bytes[category_index(*c)].load(Ordering::Relaxed))
            .sum();

        let overhead_bytes = total_allocated.saturating_sub(dataset_bytes);

        let eviction_eligible_bytes = dataset_bytes;

        let usage_percent = if max_memory > 0 {
            (total_allocated as f64 / max_memory as f64) * 100.0
        } else {
            0.0
        };

        MemoryUsage {
            total_allocated,
            max_memory,
            rss_estimate,
            fragmentation_ratio,
            peak_allocated,
            dataset_bytes,
            overhead_bytes,
            eviction_eligible_bytes,
            usage_percent,
        }
    }

    /// Return per-category memory breakdown.
    pub fn get_category_breakdown(&self) -> Vec<CategoryUsage> {
        let total = self.total_allocated.load(Ordering::Relaxed).max(1);

        ALL_CATEGORIES
            .iter()
            .map(|&cat| {
                let idx = category_index(cat);
                let bytes = self.category_bytes[idx].load(Ordering::Relaxed);
                let keys = self.category_keys[idx].load(Ordering::Relaxed);
                let percent = (bytes as f64 / total as f64) * 100.0;
                CategoryUsage {
                    category: cat,
                    bytes,
                    keys,
                    percent,
                }
            })
            .collect()
    }

    /// Check whether eviction should be triggered. Returns `Some(request)` if
    /// the current memory state warrants eviction, or `None` if no action is
    /// needed.
    pub fn should_evict(&self) -> Option<EvictionRequest> {
        let config = self.config.read();
        let max = config.max_memory_bytes;
        let policy = config.eviction_policy;
        let warning = config.oom_warning_threshold;
        let defrag_threshold = config.defrag_threshold;
        drop(config);

        if max == 0 {
            return None;
        }

        let total = self.total_allocated.load(Ordering::Relaxed);
        let rss = total.saturating_add(self.rss_overhead.load(Ordering::Relaxed));
        let frag = if total > 0 {
            rss as f64 / total as f64
        } else {
            1.0
        };
        let usage_ratio = total as f64 / max as f64;

        // Critical: already over the limit
        if total >= max {
            self.evictions_triggered.fetch_add(1, Ordering::Relaxed);
            return Some(EvictionRequest {
                reason: EvictionReason::MaxMemoryReached,
                target_bytes: total - max + (max / 20), // free 5% headroom
                policy,
                urgency: EvictionUrgency::Critical,
            });
        }

        // High: approaching the limit
        if usage_ratio >= warning {
            let target = total.saturating_sub((max as f64 * warning) as u64);
            self.evictions_triggered.fetch_add(1, Ordering::Relaxed);
            return Some(EvictionRequest {
                reason: EvictionReason::OomPrevention,
                target_bytes: target.max(max / 100),
                policy,
                urgency: if usage_ratio >= 0.98 {
                    EvictionUrgency::High
                } else {
                    EvictionUrgency::Medium
                },
            });
        }

        // Fragmentation driven
        if frag >= defrag_threshold {
            let fragmented = rss.saturating_sub(total);
            self.evictions_triggered.fetch_add(1, Ordering::Relaxed);
            return Some(EvictionRequest {
                reason: EvictionReason::FragmentationHigh,
                target_bytes: fragmented / 2,
                policy,
                urgency: EvictionUrgency::Low,
            });
        }

        None
    }

    /// Perform a defragmentation scan and return a report.
    pub fn defrag_scan(&self) -> DefragReport {
        self.defrag_cycles.fetch_add(1, Ordering::Relaxed);

        let total = self.total_allocated.load(Ordering::Relaxed);
        let rss = total.saturating_add(self.rss_overhead.load(Ordering::Relaxed));

        let fragmentation_ratio = if total > 0 {
            rss as f64 / total as f64
        } else {
            1.0
        };

        let fragmented_bytes = rss.saturating_sub(total);

        let config = self.config.read();
        let threshold = config.defrag_threshold;
        let cycle_ms = config.defrag_cycle_ms;
        drop(config);

        // Estimate relocatable keys as those in dataset categories
        let relocatable_keys: u64 = DATASET_CATEGORIES
            .iter()
            .map(|c| self.category_keys[category_index(*c)].load(Ordering::Relaxed))
            .sum();

        // Conservative estimate: defrag recovers ~60% of fragmented bytes
        let estimated_savings = fragmented_bytes * 60 / 100;

        let recommended_action = if fragmentation_ratio < 1.1 {
            DefragAction::NotNeeded
        } else if fragmentation_ratio < threshold {
            let estimated_time_ms = (relocatable_keys / 1000).max(1) * cycle_ms;
            DefragAction::Recommended { estimated_time_ms }
        } else {
            DefragAction::Urgent
        };

        DefragReport {
            fragmentation_ratio,
            fragmented_bytes,
            relocatable_keys,
            estimated_savings,
            recommended_action,
        }
    }

    /// Run diagnostics and return a list of findings.
    pub fn memory_doctor(&self) -> Vec<DoctorFinding> {
        let mut findings = Vec::new();
        let usage = self.get_usage();

        // 1. Fragmentation check
        if usage.fragmentation_ratio > 1.5 {
            findings.push(DoctorFinding {
                severity: DoctorSeverity::Warning,
                category: "fragmentation".into(),
                message: "High fragmentation detected".into(),
                recommendation: "Enable active defragmentation or schedule a restart \
                                 during a maintenance window"
                    .into(),
            });
        }

        // 2. Memory pressure
        if usage.max_memory > 0 && usage.usage_percent > 90.0 {
            findings.push(DoctorFinding {
                severity: DoctorSeverity::Critical,
                category: "memory_pressure".into(),
                message: "Memory pressure warning".into(),
                recommendation: "Increase maxmemory, enable eviction, or remove unused keys".into(),
            });
        }

        // 3. Peak vs current
        if usage.peak_allocated > 0 && usage.total_allocated > 0 {
            let ratio = usage.peak_allocated as f64 / usage.total_allocated as f64;
            if ratio > 1.5 {
                findings.push(DoctorFinding {
                    severity: DoctorSeverity::Info,
                    category: "peak_usage".into(),
                    message: "Consider reducing maxmemory".into(),
                    recommendation: format!(
                        "Peak was {:.1}x current usage; maxmemory could be lowered to \
                         free system resources",
                        ratio
                    ),
                });
            }
        }

        // 4. Overhead ratio
        if usage.total_allocated > 0 {
            let overhead_ratio = usage.overhead_bytes as f64 / usage.total_allocated as f64;
            if overhead_ratio > 0.5 {
                findings.push(DoctorFinding {
                    severity: DoctorSeverity::Warning,
                    category: "overhead".into(),
                    message: "Too many small keys; consider aggregation".into(),
                    recommendation: "Use hashes or other aggregate structures to reduce \
                                     per-key overhead"
                        .into(),
                });
            }
        }

        // 5. RSS vs allocated (external fragmentation)
        if usage.total_allocated > 0 {
            let rss_ratio = usage.rss_estimate as f64 / usage.total_allocated as f64;
            if rss_ratio > 2.0 {
                findings.push(DoctorFinding {
                    severity: DoctorSeverity::Critical,
                    category: "external_fragmentation".into(),
                    message: "External fragmentation; restart recommended".into(),
                    recommendation: "Schedule a rolling restart to reclaim fragmented memory"
                        .into(),
                });
            }
        }

        // If nothing was flagged, emit an all-clear
        if findings.is_empty() {
            findings.push(DoctorFinding {
                severity: DoctorSeverity::Ok,
                category: "overall".into(),
                message: "Memory health is good".into(),
                recommendation: "No action required".into(),
            });
        }

        findings
    }

    /// Estimate total memory consumed by a single key-value pair, including
    /// all internal overhead.
    ///
    /// The estimate accounts for:
    /// - SDS header (9 bytes) + key content
    /// - Value encoding overhead (varies by `data_type`)
    /// - Dict entry (24 bytes)
    /// - Redis-compatible object header (16 bytes)
    pub fn get_key_memory(&self, key_size: u64, value_size: u64, data_type: &str) -> u64 {
        // SDS header (type byte + len + alloc + nul) ≈ 9 bytes
        const SDS_HEADER: u64 = 9;
        // Dict entry: 3 pointers (key, value, next) = 24 bytes
        const DICT_ENTRY: u64 = 24;
        // RedisObject header: type + encoding + lru + refcount + ptr = 16 bytes
        const OBJ_HEADER: u64 = 16;

        let key_overhead = SDS_HEADER + key_size;

        let value_overhead = match data_type {
            "string" => SDS_HEADER + value_size,
            "list" => {
                // Quicklist: header (40) + one ziplist node per entry
                40 + value_size + 11 // ziplist entry overhead
            }
            "hash" => {
                // Listpack for small hashes, hashtable for large
                if value_size < 128 {
                    value_size + 7 // listpack entry overhead
                } else {
                    value_size + DICT_ENTRY * 2
                }
            }
            "set" => {
                // Listpack for small sets, hashtable for large
                if value_size < 128 {
                    value_size + 7
                } else {
                    value_size + DICT_ENTRY
                }
            }
            "zset" => {
                // Skiplist node (32) + dict entry
                value_size + 32 + DICT_ENTRY
            }
            "stream" => {
                // Rax tree node + listpack
                value_size + 48
            }
            _ => value_size + 16,
        };

        key_overhead + value_overhead + DICT_ENTRY + OBJ_HEADER
    }

    /// Return cumulative memory statistics.
    pub fn get_stats(&self) -> MemoryStats {
        MemoryStats {
            total_allocations: self.total_allocations.load(Ordering::Relaxed),
            total_deallocations: self.total_deallocations.load(Ordering::Relaxed),
            peak_usage: self.peak_allocated.load(Ordering::Relaxed),
            evictions_triggered: self.evictions_triggered.load(Ordering::Relaxed),
            defrag_cycles: self.defrag_cycles.load(Ordering::Relaxed),
            oom_rejections: self.oom_rejections.load(Ordering::Relaxed),
        }
    }

    // -- internal helpers --------------------------------------------------

    /// Manually set the RSS overhead for testing or external RSS sampling.
    pub fn set_rss_overhead(&self, overhead: u64) {
        self.rss_overhead.store(overhead, Ordering::Relaxed);
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn default_manager() -> MemoryManager {
        MemoryManager::new(MemoryConfig::default())
    }

    fn limited_manager(max: u64) -> MemoryManager {
        MemoryManager::new(MemoryConfig {
            max_memory_bytes: max,
            ..Default::default()
        })
    }

    // -- allocation / deallocation accounting ------------------------------

    #[test]
    fn test_allocate_and_deallocate_tracking() {
        let mgr = default_manager();

        mgr.allocate(1000, MemoryCategory::Strings).unwrap();
        mgr.allocate(500, MemoryCategory::Hashes).unwrap();

        let usage = mgr.get_usage();
        assert_eq!(usage.total_allocated, 1500);

        mgr.deallocate(400, MemoryCategory::Strings);
        let usage = mgr.get_usage();
        assert_eq!(usage.total_allocated, 1100);
    }

    #[test]
    fn test_peak_tracking() {
        let mgr = default_manager();

        mgr.allocate(2000, MemoryCategory::Strings).unwrap();
        mgr.deallocate(1500, MemoryCategory::Strings);
        mgr.allocate(100, MemoryCategory::Lists).unwrap();

        let usage = mgr.get_usage();
        assert_eq!(usage.peak_allocated, 2000);
        assert_eq!(usage.total_allocated, 600);
    }

    // -- OOM handling ------------------------------------------------------

    #[test]
    fn test_oom_rejection_no_eviction() {
        let mgr = limited_manager(1000);

        mgr.allocate(800, MemoryCategory::Strings).unwrap();
        let result = mgr.allocate(300, MemoryCategory::Strings);

        assert!(result.is_err());
        match result.unwrap_err() {
            MemoryError::OutOfMemory {
                requested,
                available,
            } => {
                assert_eq!(requested, 300);
                assert_eq!(available, 200);
            }
            other => panic!("unexpected error: {other}"),
        }

        let stats = mgr.get_stats();
        assert_eq!(stats.oom_rejections, 1);
    }

    #[test]
    fn test_oom_allowed_with_eviction_policy() {
        let mgr = MemoryManager::new(MemoryConfig {
            max_memory_bytes: 1000,
            eviction_policy: EvictionPolicy::AllKeysLru,
            ..Default::default()
        });

        mgr.allocate(800, MemoryCategory::Strings).unwrap();
        // Should succeed even over limit because eviction policy is not NoEviction
        let result = mgr.allocate(300, MemoryCategory::Strings);
        assert!(result.is_ok());
    }

    // -- eviction threshold detection --------------------------------------

    #[test]
    fn test_should_evict_none_when_unlimited() {
        let mgr = default_manager();
        mgr.allocate(1_000_000, MemoryCategory::Strings).unwrap();
        assert!(mgr.should_evict().is_none());
    }

    #[test]
    fn test_should_evict_critical_at_max() {
        let mgr = limited_manager(1000);
        mgr.allocate(1000, MemoryCategory::Strings).unwrap();

        let req = mgr.should_evict().expect("should trigger eviction");
        assert_eq!(req.reason, EvictionReason::MaxMemoryReached);
        assert_eq!(req.urgency, EvictionUrgency::Critical);
    }

    #[test]
    fn test_should_evict_oom_prevention() {
        let mgr = limited_manager(1000);
        mgr.allocate(950, MemoryCategory::Strings).unwrap();

        let req = mgr.should_evict().expect("should trigger eviction");
        assert_eq!(req.reason, EvictionReason::OomPrevention);
    }

    // -- eviction policy selection -----------------------------------------

    #[test]
    fn test_eviction_policy_in_request() {
        let mgr = MemoryManager::new(MemoryConfig {
            max_memory_bytes: 1000,
            eviction_policy: EvictionPolicy::VolatileLfu,
            ..Default::default()
        });
        mgr.allocate(1000, MemoryCategory::Strings).unwrap();

        let req = mgr.should_evict().unwrap();
        assert_eq!(req.policy, EvictionPolicy::VolatileLfu);
    }

    // -- fragmentation calculation -----------------------------------------

    #[test]
    fn test_fragmentation_ratio_no_overhead() {
        let mgr = default_manager();
        mgr.allocate(1000, MemoryCategory::Strings).unwrap();

        let usage = mgr.get_usage();
        assert!((usage.fragmentation_ratio - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_fragmentation_ratio_with_rss_overhead() {
        let mgr = default_manager();
        mgr.allocate(1000, MemoryCategory::Strings).unwrap();
        mgr.set_rss_overhead(500);

        let usage = mgr.get_usage();
        assert!((usage.fragmentation_ratio - 1.5).abs() < 0.01);
    }

    #[test]
    fn test_defrag_scan_not_needed() {
        let mgr = default_manager();
        mgr.allocate(1000, MemoryCategory::Strings).unwrap();

        let report = mgr.defrag_scan();
        assert_eq!(report.recommended_action, DefragAction::NotNeeded);
    }

    #[test]
    fn test_defrag_scan_urgent() {
        let mgr = default_manager();
        mgr.allocate(1000, MemoryCategory::Strings).unwrap();
        mgr.set_rss_overhead(1000); // 2.0 ratio

        let report = mgr.defrag_scan();
        assert_eq!(report.recommended_action, DefragAction::Urgent);
        assert_eq!(report.fragmented_bytes, 1000);
    }

    // -- memory doctor findings --------------------------------------------

    #[test]
    fn test_doctor_healthy() {
        let mgr = default_manager();
        mgr.allocate(100, MemoryCategory::Strings).unwrap();

        let findings = mgr.memory_doctor();
        assert_eq!(findings.len(), 1);
        assert_eq!(findings[0].severity, DoctorSeverity::Ok);
    }

    #[test]
    fn test_doctor_high_fragmentation() {
        let mgr = default_manager();
        mgr.allocate(1000, MemoryCategory::Strings).unwrap();
        mgr.set_rss_overhead(600); // ratio = 1.6

        let findings = mgr.memory_doctor();
        assert!(findings
            .iter()
            .any(|f| f.message == "High fragmentation detected"));
    }

    #[test]
    fn test_doctor_memory_pressure() {
        let mgr = limited_manager(1000);
        mgr.allocate(950, MemoryCategory::Strings).unwrap();

        let findings = mgr.memory_doctor();
        assert!(findings
            .iter()
            .any(|f| f.message == "Memory pressure warning"));
    }

    #[test]
    fn test_doctor_peak_higher_than_current() {
        let mgr = default_manager();
        mgr.allocate(3000, MemoryCategory::Strings).unwrap();
        mgr.deallocate(2000, MemoryCategory::Strings);

        let findings = mgr.memory_doctor();
        assert!(findings
            .iter()
            .any(|f| f.message == "Consider reducing maxmemory"));
    }

    #[test]
    fn test_doctor_high_overhead() {
        let mgr = default_manager();
        // Allocate mostly to overhead categories
        mgr.allocate(800, MemoryCategory::Overhead).unwrap();
        mgr.allocate(100, MemoryCategory::Strings).unwrap();

        let findings = mgr.memory_doctor();
        assert!(findings.iter().any(|f| f.message.contains("small keys")));
    }

    #[test]
    fn test_doctor_external_fragmentation() {
        let mgr = default_manager();
        mgr.allocate(1000, MemoryCategory::Strings).unwrap();
        mgr.set_rss_overhead(1200); // rss/alloc = 2.2

        let findings = mgr.memory_doctor();
        assert!(findings
            .iter()
            .any(|f| f.message.contains("External fragmentation")));
    }

    // -- per-category breakdown --------------------------------------------

    #[test]
    fn test_category_breakdown() {
        let mgr = default_manager();
        mgr.allocate(600, MemoryCategory::Strings).unwrap();
        mgr.allocate(400, MemoryCategory::Hashes).unwrap();

        let breakdown = mgr.get_category_breakdown();
        let strings = breakdown
            .iter()
            .find(|c| c.category == MemoryCategory::Strings)
            .unwrap();
        assert_eq!(strings.bytes, 600);
        assert_eq!(strings.keys, 1);
        assert!((strings.percent - 60.0).abs() < 0.1);

        let hashes = breakdown
            .iter()
            .find(|c| c.category == MemoryCategory::Hashes)
            .unwrap();
        assert_eq!(hashes.bytes, 400);
    }

    // -- key memory estimation ---------------------------------------------

    #[test]
    fn test_key_memory_string() {
        let mgr = default_manager();
        let mem = mgr.get_key_memory(10, 100, "string");
        // key: 9+10=19, value: 9+100=109, dict: 24, obj: 16 → 168
        assert_eq!(mem, 168);
    }

    #[test]
    fn test_key_memory_list() {
        let mgr = default_manager();
        let mem = mgr.get_key_memory(5, 200, "list");
        // key: 9+5=14, value: 40+200+11=251, dict: 24, obj: 16 → 305
        assert_eq!(mem, 305);
    }

    #[test]
    fn test_key_memory_hash_small() {
        let mgr = default_manager();
        let mem = mgr.get_key_memory(8, 64, "hash");
        // key: 9+8=17, value: 64+7=71, dict: 24, obj: 16 → 128
        assert_eq!(mem, 128);
    }

    #[test]
    fn test_key_memory_hash_large() {
        let mgr = default_manager();
        let mem = mgr.get_key_memory(8, 256, "hash");
        // key: 9+8=17, value: 256+48=304, dict: 24, obj: 16 → 361
        assert_eq!(mem, 361);
    }

    #[test]
    fn test_key_memory_zset() {
        let mgr = default_manager();
        let mem = mgr.get_key_memory(6, 50, "zset");
        // key: 9+6=15, value: 50+32+24=106, dict: 24, obj: 16 → 161
        assert_eq!(mem, 161);
    }

    #[test]
    fn test_key_memory_unknown_type() {
        let mgr = default_manager();
        let mem = mgr.get_key_memory(4, 100, "custom");
        // key: 9+4=13, value: 100+16=116, dict: 24, obj: 16 → 169
        assert_eq!(mem, 169);
    }

    // -- stats -------------------------------------------------------------

    #[test]
    fn test_stats_counters() {
        let mgr = limited_manager(5000);

        mgr.allocate(100, MemoryCategory::Strings).unwrap();
        mgr.allocate(200, MemoryCategory::Lists).unwrap();
        mgr.deallocate(50, MemoryCategory::Strings);

        let stats = mgr.get_stats();
        assert_eq!(stats.total_allocations, 2);
        assert_eq!(stats.total_deallocations, 1);
        assert_eq!(stats.peak_usage, 300);
    }

    #[test]
    fn test_defrag_cycle_count() {
        let mgr = default_manager();
        mgr.allocate(100, MemoryCategory::Strings).unwrap();

        mgr.defrag_scan();
        mgr.defrag_scan();

        let stats = mgr.get_stats();
        assert_eq!(stats.defrag_cycles, 2);
    }

    #[test]
    fn test_usage_percent() {
        let mgr = limited_manager(2000);
        mgr.allocate(1000, MemoryCategory::Strings).unwrap();

        let usage = mgr.get_usage();
        assert!((usage.usage_percent - 50.0).abs() < 0.1);
    }

    #[test]
    fn test_usage_percent_unlimited() {
        let mgr = default_manager();
        mgr.allocate(1000, MemoryCategory::Strings).unwrap();

        let usage = mgr.get_usage();
        assert!((usage.usage_percent - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_config_defaults() {
        let config = MemoryConfig::default();
        assert_eq!(config.max_memory_bytes, 0);
        assert_eq!(config.eviction_policy, EvictionPolicy::NoEviction);
        assert!((config.oom_warning_threshold - 0.9).abs() < f64::EPSILON);
        assert!(config.defrag_enabled);
        assert!((config.defrag_threshold - 1.5).abs() < f64::EPSILON);
        assert_eq!(config.defrag_cycle_ms, 100);
        assert_eq!(config.overhead_per_key_bytes, 72);
    }

    #[test]
    fn test_eviction_request_fragmentation() {
        let mgr = limited_manager(10_000);
        mgr.allocate(5000, MemoryCategory::Strings).unwrap();
        mgr.set_rss_overhead(5000); // frag ratio = 2.0

        let req = mgr.should_evict().unwrap();
        assert_eq!(req.reason, EvictionReason::FragmentationHigh);
        assert_eq!(req.urgency, EvictionUrgency::Low);
    }

    #[test]
    fn test_multiple_categories_dataset_vs_overhead() {
        let mgr = default_manager();
        mgr.allocate(500, MemoryCategory::Strings).unwrap();
        mgr.allocate(300, MemoryCategory::Lists).unwrap();
        mgr.allocate(200, MemoryCategory::Overhead).unwrap();

        let usage = mgr.get_usage();
        assert_eq!(usage.dataset_bytes, 800);
        assert_eq!(usage.overhead_bytes, 200);
        assert_eq!(usage.total_allocated, 1000);
    }
}
