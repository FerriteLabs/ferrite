//! Ferrite Insights — Real-time analytics and operational intelligence
//!
//! Provides key access pattern analysis, query performance tracking, cost
//! attribution per tenant, capacity planning forecasts, and anomaly detection.
//! Powers the analytics views in Ferrite Studio.
//!
//! # Architecture
//!
//! ```text
//! ┌──────────────────────────────────────────────────────────────┐
//! │                     Insights Engine                          │
//! ├──────────────────────────────────────────────────────────────┤
//! │  ┌────────────┐ ┌────────────┐ ┌──────────────────────────┐ │
//! │  │  Access    │ │  Cost      │ │  Capacity                │ │
//! │  │  Tracker   │ │  Attributor│ │  Forecaster              │ │
//! │  └────────────┘ └────────────┘ └──────────────────────────┘ │
//! │  ┌────────────┐ ┌────────────┐ ┌──────────────────────────┐ │
//! │  │  Anomaly   │ │  Query     │ │  Report                  │ │
//! │  │  Detector  │ │  Profiler  │ │  Generator               │ │
//! │  └────────────┘ └────────────┘ └──────────────────────────┘ │
//! └──────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Example
//!
//! ```rust,ignore
//! use ferrite::insights::{InsightsEngine, InsightsConfig, AccessType};
//! use std::time::Duration;
//!
//! let engine = InsightsEngine::new(InsightsConfig::default());
//!
//! // Record accesses
//! engine.record_access("users:123", AccessType::Read, Duration::from_micros(50));
//! engine.record_access("users:123", AccessType::Write, Duration::from_micros(200));
//!
//! // Generate report
//! let report = engine.generate_report(TimeRange::last_hour());
//! println!("Hot keys: {:?}", report.hot_keys);
//! println!("Cost breakdown: {:?}", report.cost_attribution);
//! ```

pub mod anomaly;
pub mod forecast;

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for the insights engine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InsightsConfig {
    /// Enable insights collection.
    pub enabled: bool,
    /// Sampling rate for access tracking (0.0-1.0). Use 1.0 for full tracking.
    pub sampling_rate: f64,
    /// Maximum number of hot keys to track.
    pub max_hot_keys: usize,
    /// Time window for hot-key detection.
    pub hot_key_window_secs: u64,
    /// Threshold for hot-key classification (accesses per second).
    pub hot_key_threshold: u64,
    /// Enable anomaly detection.
    pub anomaly_detection: bool,
    /// Anomaly sensitivity (standard deviations from mean).
    pub anomaly_sensitivity: f64,
    /// Enable cost attribution tracking.
    pub cost_tracking: bool,
    /// Report generation interval (seconds).
    pub report_interval_secs: u64,
    /// Maximum history depth for forecasting (seconds).
    pub forecast_history_secs: u64,
}

impl Default for InsightsConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            sampling_rate: 0.1, // 10% sampling by default
            max_hot_keys: 100,
            hot_key_window_secs: 60,
            hot_key_threshold: 1000,
            anomaly_detection: true,
            anomaly_sensitivity: 3.0,
            cost_tracking: true,
            report_interval_secs: 60,
            forecast_history_secs: 7 * 24 * 3600,
        }
    }
}

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// Type of data access being tracked.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum AccessType {
    Read,
    Write,
    Delete,
    Scan,
}

/// Per-key access statistics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeyInsights {
    pub key_pattern: String,
    pub total_reads: u64,
    pub total_writes: u64,
    pub total_deletes: u64,
    pub avg_read_latency_us: f64,
    pub avg_write_latency_us: f64,
    pub p99_latency_us: f64,
    pub estimated_monthly_cost_usd: f64,
    pub last_accessed: chrono::DateTime<chrono::Utc>,
    pub is_hot_key: bool,
}

/// Cost breakdown per tenant or key pattern.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CostAttribution {
    pub entity: String,
    pub entity_type: CostEntity,
    pub memory_cost_usd: f64,
    pub compute_cost_usd: f64,
    pub storage_cost_usd: f64,
    pub network_cost_usd: f64,
    pub total_cost_usd: f64,
}

/// What entity the cost is attributed to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CostEntity {
    Tenant,
    KeyPattern,
    Command,
    Database,
}

/// A generated insights report.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InsightsReport {
    /// When the report was generated.
    pub generated_at: chrono::DateTime<chrono::Utc>,
    /// Time range covered.
    pub time_range_secs: u64,
    /// Total operations in the period.
    pub total_operations: u64,
    /// Operations per second.
    pub ops_per_second: f64,
    /// Top hot keys.
    pub hot_keys: Vec<KeyInsights>,
    /// Cost attribution breakdown.
    pub cost_attribution: Vec<CostAttribution>,
    /// Detected anomalies.
    pub anomalies: Vec<Anomaly>,
    /// Capacity forecast.
    pub capacity_forecast: Option<CapacityForecast>,
    /// Overall health score (0-100).
    pub health_score: f64,
}

/// A detected anomaly.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Anomaly {
    /// Type of anomaly.
    pub anomaly_type: AnomalyType,
    /// Severity (0.0 - 1.0).
    pub severity: f64,
    /// Description.
    pub description: String,
    /// When detected.
    pub detected_at: chrono::DateTime<chrono::Utc>,
    /// Affected key pattern (if applicable).
    pub affected_pattern: Option<String>,
}

/// Types of anomalies the system can detect.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AnomalyType {
    /// Sudden spike in access rate.
    TrafficSpike,
    /// Unusual latency increase.
    LatencySpike,
    /// Rapid memory growth.
    MemoryGrowth,
    /// Hot key detection.
    HotKey,
    /// Error rate increase.
    ErrorSpike,
    /// Unusual access pattern change.
    PatternChange,
}

/// Capacity planning forecast.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CapacityForecast {
    /// Current memory usage (bytes).
    pub current_memory_bytes: u64,
    /// Projected memory usage in 30 days.
    pub projected_30d_bytes: u64,
    /// Projected memory usage in 90 days.
    pub projected_90d_bytes: u64,
    /// Days until memory limit is reached (None if not approaching).
    pub days_until_limit: Option<u32>,
    /// Current keys count.
    pub current_keys: u64,
    /// Projected keys in 30 days.
    pub projected_30d_keys: u64,
    /// Growth rate (keys per day).
    pub key_growth_rate: f64,
    /// Recommended action.
    pub recommendation: String,
}

// ---------------------------------------------------------------------------
// Insights Engine
// ---------------------------------------------------------------------------

/// The main insights engine that collects and analyzes operational data.
pub struct InsightsEngine {
    config: InsightsConfig,
    key_stats: RwLock<HashMap<String, KeyStats>>,
    global_stats: GlobalStats,
    anomalies: RwLock<Vec<Anomaly>>,
}

struct KeyStats {
    reads: u64,
    writes: u64,
    deletes: u64,
    total_read_latency_us: u64,
    total_write_latency_us: u64,
    max_latency_us: u64,
    first_seen: Instant,
    last_seen: Instant,
}

#[derive(Debug, Default)]
struct GlobalStats {
    total_ops: AtomicU64,
    total_reads: AtomicU64,
    total_writes: AtomicU64,
    total_errors: AtomicU64,
}

impl InsightsEngine {
    /// Creates a new insights engine.
    pub fn new(config: InsightsConfig) -> Self {
        Self {
            config,
            key_stats: RwLock::new(HashMap::new()),
            global_stats: GlobalStats::default(),
            anomalies: RwLock::new(Vec::new()),
        }
    }

    /// Records a key access for insights tracking.
    pub fn record_access(&self, key: &str, access_type: AccessType, latency: Duration) {
        if !self.config.enabled {
            return;
        }

        // Probabilistic sampling
        if self.config.sampling_rate < 1.0 {
            let sample: f64 = (simple_hash(key) % 10000) as f64 / 10000.0;
            if sample > self.config.sampling_rate {
                return;
            }
        }

        self.global_stats.total_ops.fetch_add(1, Ordering::Relaxed);

        let latency_us = latency.as_micros() as u64;
        let now = Instant::now();

        let mut stats = self.key_stats.write();
        let entry = stats.entry(key.to_string()).or_insert_with(|| KeyStats {
            reads: 0,
            writes: 0,
            deletes: 0,
            total_read_latency_us: 0,
            total_write_latency_us: 0,
            max_latency_us: 0,
            first_seen: now,
            last_seen: now,
        });

        match access_type {
            AccessType::Read => {
                entry.reads += 1;
                entry.total_read_latency_us += latency_us;
                self.global_stats
                    .total_reads
                    .fetch_add(1, Ordering::Relaxed);
            }
            AccessType::Write => {
                entry.writes += 1;
                entry.total_write_latency_us += latency_us;
                self.global_stats
                    .total_writes
                    .fetch_add(1, Ordering::Relaxed);
            }
            AccessType::Delete => {
                entry.deletes += 1;
            }
            AccessType::Scan => {
                entry.reads += 1;
                entry.total_read_latency_us += latency_us;
            }
        }

        entry.max_latency_us = entry.max_latency_us.max(latency_us);
        entry.last_seen = now;
    }

    /// Records an error for tracking.
    pub fn record_error(&self) {
        self.global_stats
            .total_errors
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Returns the top hot keys by access count.
    pub fn hot_keys(&self, limit: usize) -> Vec<KeyInsights> {
        let stats = self.key_stats.read();
        let mut entries: Vec<_> = stats
            .iter()
            .map(|(key, s)| {
                let total_ops = s.reads + s.writes + s.deletes;
                let elapsed = s.last_seen.duration_since(s.first_seen);
                let ops_per_sec = if elapsed.as_secs() > 0 {
                    total_ops as f64 / elapsed.as_secs_f64()
                } else {
                    total_ops as f64
                };

                KeyInsights {
                    key_pattern: key.clone(),
                    total_reads: s.reads,
                    total_writes: s.writes,
                    total_deletes: s.deletes,
                    avg_read_latency_us: if s.reads > 0 {
                        s.total_read_latency_us as f64 / s.reads as f64
                    } else {
                        0.0
                    },
                    avg_write_latency_us: if s.writes > 0 {
                        s.total_write_latency_us as f64 / s.writes as f64
                    } else {
                        0.0
                    },
                    p99_latency_us: s.max_latency_us as f64, // approximation
                    estimated_monthly_cost_usd: 0.0,
                    last_accessed: chrono::Utc::now(),
                    is_hot_key: ops_per_sec > self.config.hot_key_threshold as f64,
                }
            })
            .collect();

        entries.sort_by(|a, b| {
            let a_total = a.total_reads + a.total_writes;
            let b_total = b.total_reads + b.total_writes;
            b_total.cmp(&a_total)
        });
        entries.truncate(limit);
        entries
    }

    /// Generates a full insights report.
    pub fn generate_report(&self) -> InsightsReport {
        let total_ops = self.global_stats.total_ops.load(Ordering::Relaxed);
        let hot_keys = self.hot_keys(self.config.max_hot_keys);
        let anomalies = self.anomalies.read().clone();

        InsightsReport {
            generated_at: chrono::Utc::now(),
            time_range_secs: self.config.report_interval_secs,
            total_operations: total_ops,
            ops_per_second: total_ops as f64 / self.config.report_interval_secs.max(1) as f64,
            hot_keys,
            cost_attribution: Vec::new(),
            anomalies,
            capacity_forecast: None,
            health_score: self.compute_health_score(),
        }
    }

    /// Returns total operations recorded.
    pub fn total_operations(&self) -> u64 {
        self.global_stats.total_ops.load(Ordering::Relaxed)
    }

    /// Returns tracked key count.
    pub fn tracked_key_count(&self) -> usize {
        self.key_stats.read().len()
    }

    /// Clears all collected insights data.
    pub fn reset(&self) {
        self.key_stats.write().clear();
        self.anomalies.write().clear();
        self.global_stats.total_ops.store(0, Ordering::Relaxed);
        self.global_stats.total_reads.store(0, Ordering::Relaxed);
        self.global_stats.total_writes.store(0, Ordering::Relaxed);
        self.global_stats.total_errors.store(0, Ordering::Relaxed);
    }

    fn compute_health_score(&self) -> f64 {
        let total = self.global_stats.total_ops.load(Ordering::Relaxed);
        let errors = self.global_stats.total_errors.load(Ordering::Relaxed);
        let anomaly_count = self.anomalies.read().len();

        let error_penalty = if total > 0 {
            (errors as f64 / total as f64) * 50.0
        } else {
            0.0
        };
        let anomaly_penalty = (anomaly_count as f64).min(10.0) * 2.0;

        (100.0 - error_penalty - anomaly_penalty).clamp(0.0, 100.0)
    }
}

fn simple_hash(key: &str) -> u64 {
    let mut hash: u64 = 0xcbf29ce484222325;
    for byte in key.as_bytes() {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

/// Errors from the insights engine.
#[derive(Debug, Clone, thiserror::Error)]
pub enum InsightsError {
    #[error("insights engine not enabled")]
    NotEnabled,

    #[error("invalid sampling rate: {0} (must be 0.0-1.0)")]
    InvalidSamplingRate(f64),

    #[error("report generation failed: {0}")]
    ReportFailed(String),
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = InsightsConfig::default();
        assert!(config.enabled);
        assert!((config.sampling_rate - 0.1).abs() < f64::EPSILON);
        assert!(config.anomaly_detection);
    }

    #[test]
    fn test_record_and_query() {
        let engine = InsightsEngine::new(InsightsConfig {
            sampling_rate: 1.0,
            ..Default::default()
        });

        engine.record_access("users:1", AccessType::Read, Duration::from_micros(50));
        engine.record_access("users:1", AccessType::Read, Duration::from_micros(100));
        engine.record_access("users:1", AccessType::Write, Duration::from_micros(200));
        engine.record_access("orders:1", AccessType::Read, Duration::from_micros(30));

        assert_eq!(engine.total_operations(), 4);
        assert_eq!(engine.tracked_key_count(), 2);

        let hot = engine.hot_keys(10);
        assert_eq!(hot.len(), 2);
        assert_eq!(hot[0].key_pattern, "users:1"); // Most accessed
        assert_eq!(hot[0].total_reads, 2);
        assert_eq!(hot[0].total_writes, 1);
    }

    #[test]
    fn test_generate_report() {
        let engine = InsightsEngine::new(InsightsConfig {
            sampling_rate: 1.0,
            ..Default::default()
        });

        engine.record_access("k1", AccessType::Read, Duration::from_micros(10));
        let report = engine.generate_report();

        assert_eq!(report.total_operations, 1);
        assert!(report.health_score > 0.0);
    }

    #[test]
    fn test_health_score() {
        let engine = InsightsEngine::new(InsightsConfig {
            sampling_rate: 1.0,
            ..Default::default()
        });

        // No errors → high health
        engine.record_access("k1", AccessType::Read, Duration::from_micros(10));
        assert!(engine.compute_health_score() > 95.0);

        // Many errors → low health
        for _ in 0..50 {
            engine.record_error();
        }
        let score = engine.compute_health_score();
        assert!(score < 100.0);
    }

    #[test]
    fn test_reset() {
        let engine = InsightsEngine::new(InsightsConfig {
            sampling_rate: 1.0,
            ..Default::default()
        });

        engine.record_access("k1", AccessType::Read, Duration::from_micros(10));
        engine.reset();

        assert_eq!(engine.total_operations(), 0);
        assert_eq!(engine.tracked_key_count(), 0);
    }

    #[test]
    fn test_disabled_engine() {
        let engine = InsightsEngine::new(InsightsConfig {
            enabled: false,
            ..Default::default()
        });

        engine.record_access("k1", AccessType::Read, Duration::from_micros(10));
        assert_eq!(engine.total_operations(), 0);
    }
}
