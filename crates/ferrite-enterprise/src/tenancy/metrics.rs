//! Tenant Metrics
//!
//! Per-tenant metrics collection for observability and billing.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;

/// Tenant usage statistics
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct TenantUsage {
    /// Memory used in bytes
    pub memory_bytes: u64,
    /// Number of keys
    pub key_count: u64,
    /// Number of active connections
    pub connection_count: u64,
    /// Memory limit
    pub memory_limit: u64,
    /// Key limit
    pub key_limit: u64,
    /// Connection limit
    pub connection_limit: u64,
}

impl TenantUsage {
    /// Get memory usage percentage
    pub fn memory_percent(&self) -> f64 {
        if self.memory_limit == 0 {
            0.0
        } else {
            (self.memory_bytes as f64 / self.memory_limit as f64) * 100.0
        }
    }

    /// Get key usage percentage
    pub fn key_percent(&self) -> f64 {
        if self.key_limit == 0 {
            0.0
        } else {
            (self.key_count as f64 / self.key_limit as f64) * 100.0
        }
    }
}

/// Per-tenant metrics
#[derive(Debug, Default)]
pub struct TenantMetrics {
    /// Total commands executed
    pub commands_total: AtomicU64,
    /// Total read operations
    pub reads_total: AtomicU64,
    /// Total write operations
    pub writes_total: AtomicU64,
    /// Total bytes read
    pub bytes_read: AtomicU64,
    /// Total bytes written
    pub bytes_written: AtomicU64,
    /// Total errors
    pub errors_total: AtomicU64,
    /// Rate limit hits
    pub rate_limit_hits: AtomicU64,
    /// Quota violations
    pub quota_violations: AtomicU64,
    /// Total latency (microseconds, for averaging)
    pub total_latency_us: AtomicU64,
    /// Peak memory usage
    pub peak_memory_bytes: AtomicU64,
    /// Peak connections
    pub peak_connections: AtomicU64,
}

impl TenantMetrics {
    /// Create new metrics
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a command execution
    pub fn record_command(&self, latency_us: u64) {
        self.commands_total.fetch_add(1, Ordering::Relaxed);
        self.total_latency_us
            .fetch_add(latency_us, Ordering::Relaxed);
    }

    /// Record a read operation
    pub fn record_read(&self, bytes: u64) {
        self.reads_total.fetch_add(1, Ordering::Relaxed);
        self.bytes_read.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Record a write operation
    pub fn record_write(&self, bytes: u64) {
        self.writes_total.fetch_add(1, Ordering::Relaxed);
        self.bytes_written.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Record an error
    pub fn record_error(&self) {
        self.errors_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a rate limit hit
    pub fn record_rate_limit(&self) {
        self.rate_limit_hits.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a quota violation
    pub fn record_quota_violation(&self) {
        self.quota_violations.fetch_add(1, Ordering::Relaxed);
    }

    /// Update peak memory
    pub fn update_peak_memory(&self, current: u64) {
        let mut peak = self.peak_memory_bytes.load(Ordering::Relaxed);
        while current > peak {
            match self.peak_memory_bytes.compare_exchange_weak(
                peak,
                current,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => peak = actual,
            }
        }
    }

    /// Update peak connections
    pub fn update_peak_connections(&self, current: u64) {
        let mut peak = self.peak_connections.load(Ordering::Relaxed);
        while current > peak {
            match self.peak_connections.compare_exchange_weak(
                peak,
                current,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => peak = actual,
            }
        }
    }

    /// Get average latency in microseconds
    pub fn avg_latency_us(&self) -> u64 {
        let total = self.total_latency_us.load(Ordering::Relaxed);
        let count = self.commands_total.load(Ordering::Relaxed);
        if count == 0 {
            0
        } else {
            total / count
        }
    }

    /// Get a snapshot of all metrics
    pub fn snapshot(&self) -> TenantMetricsSnapshot {
        TenantMetricsSnapshot {
            commands_total: self.commands_total.load(Ordering::Relaxed),
            reads_total: self.reads_total.load(Ordering::Relaxed),
            writes_total: self.writes_total.load(Ordering::Relaxed),
            bytes_read: self.bytes_read.load(Ordering::Relaxed),
            bytes_written: self.bytes_written.load(Ordering::Relaxed),
            errors_total: self.errors_total.load(Ordering::Relaxed),
            rate_limit_hits: self.rate_limit_hits.load(Ordering::Relaxed),
            quota_violations: self.quota_violations.load(Ordering::Relaxed),
            avg_latency_us: self.avg_latency_us(),
            peak_memory_bytes: self.peak_memory_bytes.load(Ordering::Relaxed),
            peak_connections: self.peak_connections.load(Ordering::Relaxed),
        }
    }

    /// Reset all counters
    pub fn reset(&self) {
        self.commands_total.store(0, Ordering::Relaxed);
        self.reads_total.store(0, Ordering::Relaxed);
        self.writes_total.store(0, Ordering::Relaxed);
        self.bytes_read.store(0, Ordering::Relaxed);
        self.bytes_written.store(0, Ordering::Relaxed);
        self.errors_total.store(0, Ordering::Relaxed);
        self.rate_limit_hits.store(0, Ordering::Relaxed);
        self.quota_violations.store(0, Ordering::Relaxed);
        self.total_latency_us.store(0, Ordering::Relaxed);
        // Don't reset peak values
    }
}

/// Snapshot of tenant metrics
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TenantMetricsSnapshot {
    /// Total commands executed
    pub commands_total: u64,
    /// Total read operations
    pub reads_total: u64,
    /// Total write operations
    pub writes_total: u64,
    /// Total bytes read
    pub bytes_read: u64,
    /// Total bytes written
    pub bytes_written: u64,
    /// Total errors
    pub errors_total: u64,
    /// Rate limit hits
    pub rate_limit_hits: u64,
    /// Quota violations
    pub quota_violations: u64,
    /// Average latency in microseconds
    pub avg_latency_us: u64,
    /// Peak memory usage
    pub peak_memory_bytes: u64,
    /// Peak connections
    pub peak_connections: u64,
}

/// Collector for all tenant metrics
pub struct TenantMetricsCollector {
    /// Per-tenant metrics
    metrics: RwLock<HashMap<String, Arc<TenantMetrics>>>,
    /// Collection interval
    collection_interval: Duration,
    /// Historical data (for billing)
    history: RwLock<Vec<MetricsHistoryEntry>>,
    /// Maximum history entries
    max_history: usize,
}

impl TenantMetricsCollector {
    /// Create a new collector
    pub fn new(collection_interval: Duration, max_history: usize) -> Self {
        Self {
            metrics: RwLock::new(HashMap::new()),
            collection_interval,
            history: RwLock::new(Vec::new()),
            max_history,
        }
    }

    /// Get or create metrics for a tenant
    pub async fn get_metrics(&self, tenant_id: &str) -> Arc<TenantMetrics> {
        let mut metrics = self.metrics.write().await;
        metrics
            .entry(tenant_id.to_string())
            .or_insert_with(|| Arc::new(TenantMetrics::new()))
            .clone()
    }

    /// Get all tenant metrics
    pub async fn get_all_metrics(&self) -> HashMap<String, TenantMetricsSnapshot> {
        let metrics = self.metrics.read().await;
        metrics
            .iter()
            .map(|(id, m)| (id.clone(), m.snapshot()))
            .collect()
    }

    /// Remove metrics for a tenant
    pub async fn remove_tenant(&self, tenant_id: &str) {
        let mut metrics = self.metrics.write().await;
        metrics.remove(tenant_id);
    }

    /// Emit per-tenant metrics to the Prometheus `metrics` crate.
    /// Should be called periodically (e.g. every `collection_interval`).
    pub async fn emit_prometheus(&self) {
        let metrics_map = self.metrics.read().await;
        for (tenant_id, m) in metrics_map.iter() {
            let labels = [("tenant", tenant_id.clone())];
            let snap = m.snapshot();
            metrics::counter!("ferrite_tenant_commands_total", &labels)
                .absolute(snap.commands_total);
            metrics::counter!("ferrite_tenant_reads_total", &labels)
                .absolute(snap.reads_total);
            metrics::counter!("ferrite_tenant_writes_total", &labels)
                .absolute(snap.writes_total);
            metrics::counter!("ferrite_tenant_bytes_read", &labels)
                .absolute(snap.bytes_read);
            metrics::counter!("ferrite_tenant_bytes_written", &labels)
                .absolute(snap.bytes_written);
            metrics::counter!("ferrite_tenant_errors_total", &labels)
                .absolute(snap.errors_total);
            metrics::counter!("ferrite_tenant_rate_limit_hits", &labels)
                .absolute(snap.rate_limit_hits);
            metrics::counter!("ferrite_tenant_quota_violations", &labels)
                .absolute(snap.quota_violations);
            metrics::gauge!("ferrite_tenant_peak_memory_bytes", &labels)
                .set(snap.peak_memory_bytes as f64);
            metrics::gauge!("ferrite_tenant_peak_connections", &labels)
                .set(snap.peak_connections as f64);
        }
    }

    /// Collect and store historical data
    pub async fn collect_history(&self) {
        let metrics = self.metrics.read().await;
        let timestamp = current_timestamp_ms();

        let entries: Vec<_> = metrics
            .iter()
            .map(|(id, m)| MetricsHistoryEntry {
                timestamp,
                tenant_id: id.clone(),
                metrics: m.snapshot(),
            })
            .collect();

        drop(metrics);

        let mut history = self.history.write().await;
        history.extend(entries);

        // Trim history if too large
        while history.len() > self.max_history {
            history.remove(0);
        }
    }

    /// Get historical data for a tenant
    pub async fn get_history(
        &self,
        tenant_id: &str,
        since: Option<u64>,
    ) -> Vec<MetricsHistoryEntry> {
        let history = self.history.read().await;
        history
            .iter()
            .filter(|e| e.tenant_id == tenant_id && since.map_or(true, |s| e.timestamp >= s))
            .cloned()
            .collect()
    }

    /// Calculate billing data for a tenant
    pub async fn calculate_billing(
        &self,
        tenant_id: &str,
        period_start: u64,
        period_end: u64,
    ) -> BillingData {
        let history = self.history.read().await;

        let entries: Vec<_> = history
            .iter()
            .filter(|e| {
                e.tenant_id == tenant_id && e.timestamp >= period_start && e.timestamp <= period_end
            })
            .collect();

        if entries.is_empty() {
            return BillingData::default();
        }

        let total_commands: u64 = entries.iter().map(|e| e.metrics.commands_total).sum();
        let total_bytes_read: u64 = entries.iter().map(|e| e.metrics.bytes_read).sum();
        let total_bytes_written: u64 = entries.iter().map(|e| e.metrics.bytes_written).sum();
        let peak_memory = entries
            .iter()
            .map(|e| e.metrics.peak_memory_bytes)
            .max()
            .unwrap_or(0);

        // Calculate average memory usage
        let avg_memory = entries
            .iter()
            .map(|e| e.metrics.peak_memory_bytes)
            .sum::<u64>()
            / entries.len() as u64;

        BillingData {
            period_start,
            period_end,
            total_commands,
            total_bytes_read,
            total_bytes_written,
            peak_memory_bytes: peak_memory,
            avg_memory_bytes: avg_memory,
            data_points: entries.len() as u64,
        }
    }
}

impl Default for TenantMetricsCollector {
    fn default() -> Self {
        Self::new(Duration::from_secs(60), 10_000)
    }
}

/// Historical metrics entry
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MetricsHistoryEntry {
    /// Timestamp
    pub timestamp: u64,
    /// Tenant ID
    pub tenant_id: String,
    /// Metrics snapshot
    pub metrics: TenantMetricsSnapshot,
}

/// Billing data for a period
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct BillingData {
    /// Period start timestamp
    pub period_start: u64,
    /// Period end timestamp
    pub period_end: u64,
    /// Total commands executed
    pub total_commands: u64,
    /// Total bytes read
    pub total_bytes_read: u64,
    /// Total bytes written
    pub total_bytes_written: u64,
    /// Peak memory usage
    pub peak_memory_bytes: u64,
    /// Average memory usage
    pub avg_memory_bytes: u64,
    /// Number of data points
    pub data_points: u64,
}

impl BillingData {
    /// Calculate estimated cost (placeholder)
    pub fn estimated_cost(&self, pricing: &PricingConfig) -> f64 {
        let command_cost = self.total_commands as f64 * pricing.cost_per_command;
        let bandwidth_cost =
            (self.total_bytes_read + self.total_bytes_written) as f64 * pricing.cost_per_byte;
        let memory_cost =
            (self.avg_memory_bytes as f64 / (1024.0 * 1024.0 * 1024.0)) * pricing.cost_per_gb_month;

        command_cost + bandwidth_cost + memory_cost
    }
}

/// Pricing configuration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PricingConfig {
    /// Cost per command (dollars)
    pub cost_per_command: f64,
    /// Cost per byte transferred (dollars)
    pub cost_per_byte: f64,
    /// Cost per GB memory per month (dollars)
    pub cost_per_gb_month: f64,
}

impl Default for PricingConfig {
    fn default() -> Self {
        Self {
            cost_per_command: 0.0000001, // $0.10 per million
            cost_per_byte: 0.00000001,   // $0.01 per GB
            cost_per_gb_month: 0.10,     // $0.10 per GB-month
        }
    }
}

/// Get current timestamp in milliseconds
fn current_timestamp_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tenant_metrics_recording() {
        let metrics = TenantMetrics::new();

        metrics.record_command(100);
        metrics.record_read(1024);
        metrics.record_write(512);

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.commands_total, 1);
        assert_eq!(snapshot.reads_total, 1);
        assert_eq!(snapshot.writes_total, 1);
        assert_eq!(snapshot.bytes_read, 1024);
        assert_eq!(snapshot.bytes_written, 512);
    }

    #[test]
    fn test_average_latency() {
        let metrics = TenantMetrics::new();

        metrics.record_command(100);
        metrics.record_command(200);
        metrics.record_command(300);

        assert_eq!(metrics.avg_latency_us(), 200);
    }

    #[test]
    fn test_peak_tracking() {
        let metrics = TenantMetrics::new();

        metrics.update_peak_memory(100);
        metrics.update_peak_memory(500);
        metrics.update_peak_memory(300); // Should not update

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.peak_memory_bytes, 500);
    }

    #[test]
    fn test_tenant_usage() {
        let usage = TenantUsage {
            memory_bytes: 500,
            memory_limit: 1000,
            key_count: 50,
            key_limit: 100,
            connection_count: 5,
            connection_limit: 10,
        };

        assert_eq!(usage.memory_percent(), 50.0);
        assert_eq!(usage.key_percent(), 50.0);
    }

    #[test]
    fn test_billing_calculation() {
        let billing = BillingData {
            period_start: 0,
            period_end: 1000,
            total_commands: 1_000_000,
            total_bytes_read: 1024 * 1024 * 1024,      // 1GB
            total_bytes_written: 512 * 1024 * 1024,    // 512MB
            peak_memory_bytes: 2 * 1024 * 1024 * 1024, // 2GB
            avg_memory_bytes: 1024 * 1024 * 1024,      // 1GB
            data_points: 100,
        };

        let pricing = PricingConfig::default();
        let cost = billing.estimated_cost(&pricing);

        assert!(cost > 0.0);
    }
}
