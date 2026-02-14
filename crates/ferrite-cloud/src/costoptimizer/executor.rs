//! Cost-aware execution hints and metrics

use serde::{Deserialize, Serialize};

use super::model::{CostModel, StorageTier};
use super::planner::OptimizedPlan;

/// Execution hints for the query executor
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionHints {
    /// Preferred storage tier
    pub preferred_tier: StorageTier,
    /// Use index if available
    pub use_index: bool,
    /// Enable caching
    pub enable_caching: bool,
    /// Batch size for operations
    pub batch_size: usize,
    /// Prefetch count
    pub prefetch_count: usize,
    /// Parallelism level
    pub parallelism: usize,
    /// Memory budget in bytes
    pub memory_budget: u64,
    /// Timeout in milliseconds
    pub timeout_ms: u64,
    /// Additional hints
    pub custom: std::collections::HashMap<String, String>,
}

impl Default for ExecutionHints {
    fn default() -> Self {
        Self {
            preferred_tier: StorageTier::Memory,
            use_index: true,
            enable_caching: true,
            batch_size: 1000,
            prefetch_count: 10,
            parallelism: 1,
            memory_budget: 100 * 1024 * 1024, // 100MB
            timeout_ms: 30000,                // 30 seconds
            custom: std::collections::HashMap::new(),
        }
    }
}

impl ExecutionHints {
    /// Create hints for minimal cost
    pub fn min_cost() -> Self {
        Self {
            preferred_tier: StorageTier::ObjectStorage,
            use_index: true,
            enable_caching: true,
            batch_size: 10000, // Large batches for efficiency
            prefetch_count: 100,
            parallelism: 1, // Less parallelism = less compute
            memory_budget: 50 * 1024 * 1024,
            timeout_ms: 300000, // 5 minutes
            custom: std::collections::HashMap::new(),
        }
    }

    /// Create hints for minimal latency
    pub fn min_latency() -> Self {
        Self {
            preferred_tier: StorageTier::Memory,
            use_index: true,
            enable_caching: true,
            batch_size: 100, // Small batches for quick response
            prefetch_count: 50,
            parallelism: 8,                   // High parallelism
            memory_budget: 500 * 1024 * 1024, // 500MB
            timeout_ms: 1000,                 // 1 second
            custom: std::collections::HashMap::new(),
        }
    }

    /// Add custom hint
    pub fn with_custom(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.custom.insert(key.into(), value.into());
        self
    }
}

/// Metrics from query execution
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CostMetrics {
    /// CPU time in milliseconds
    pub cpu_time_ms: u64,
    /// Memory used in bytes
    pub memory_bytes: u64,
    /// Storage read operations
    pub storage_reads: u64,
    /// Storage write operations
    pub storage_writes: u64,
    /// Network bytes transferred
    pub network_bytes: u64,
    /// Total execution time in milliseconds
    pub execution_time_ms: u64,
    /// Cache hits
    pub cache_hits: u64,
    /// Cache misses
    pub cache_misses: u64,
    /// Rows processed
    pub rows_processed: u64,
}

impl CostMetrics {
    /// Create empty metrics
    pub fn new() -> Self {
        Self {
            cpu_time_ms: 0,
            memory_bytes: 0,
            storage_reads: 0,
            storage_writes: 0,
            network_bytes: 0,
            execution_time_ms: 0,
            cache_hits: 0,
            cache_misses: 0,
            rows_processed: 0,
        }
    }

    /// Calculate cache hit ratio
    pub fn cache_hit_ratio(&self) -> f64 {
        let total = self.cache_hits + self.cache_misses;
        if total == 0 {
            0.0
        } else {
            self.cache_hits as f64 / total as f64
        }
    }

    /// Merge with another metrics
    pub fn merge(&self, other: &CostMetrics) -> CostMetrics {
        CostMetrics {
            cpu_time_ms: self.cpu_time_ms + other.cpu_time_ms,
            memory_bytes: self.memory_bytes.max(other.memory_bytes),
            storage_reads: self.storage_reads + other.storage_reads,
            storage_writes: self.storage_writes + other.storage_writes,
            network_bytes: self.network_bytes + other.network_bytes,
            execution_time_ms: self.execution_time_ms + other.execution_time_ms,
            cache_hits: self.cache_hits + other.cache_hits,
            cache_misses: self.cache_misses + other.cache_misses,
            rows_processed: self.rows_processed + other.rows_processed,
        }
    }
}

impl Default for CostMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Cost-aware query executor
pub struct CostAwareExecutor {
    /// Current metrics
    current_metrics: parking_lot::RwLock<CostMetrics>,
    /// Historical metrics for analysis
    history: parking_lot::RwLock<Vec<ExecutionRecord>>,
}

/// Record of a query execution
#[derive(Debug, Clone)]
struct ExecutionRecord {
    /// Query string
    query: String,
    /// Execution metrics
    metrics: CostMetrics,
    /// Timestamp
    timestamp: u64,
}

impl CostAwareExecutor {
    /// Create a new executor
    pub fn new() -> Self {
        Self {
            current_metrics: parking_lot::RwLock::new(CostMetrics::new()),
            history: parking_lot::RwLock::new(Vec::new()),
        }
    }

    /// Generate execution hints for a plan
    pub fn generate_hints(&self, plan: &OptimizedPlan, cost_model: &CostModel) -> ExecutionHints {
        let estimate = cost_model.estimate(plan);

        let mut hints = ExecutionHints {
            preferred_tier: plan.storage_tier,
            ..ExecutionHints::default()
        };

        // Adjust based on plan type
        match plan.plan_type.as_str() {
            "index_lookup" => {
                hints.use_index = true;
                hints.batch_size = 1;
                hints.prefetch_count = 1;
            }
            "scan" => {
                hints.batch_size = 10000;
                hints.prefetch_count = 100;
                // Enable parallelism for large scans
                if plan.estimated_rows > 10000 {
                    hints.parallelism = 4;
                }
            }
            "join" => {
                hints.memory_budget = 500 * 1024 * 1024; // More memory for joins
                hints.parallelism = 2;
            }
            "aggregate" => {
                hints.batch_size = 5000;
                hints.memory_budget = 200 * 1024 * 1024;
            }
            _ => {}
        }

        // Adjust memory budget based on row count and size
        let estimated_memory = plan.estimated_rows * plan.avg_row_bytes;
        if estimated_memory > hints.memory_budget {
            hints.memory_budget = estimated_memory.min(1024 * 1024 * 1024); // Max 1GB
        }

        // Set timeout based on estimated cost
        if estimate.total() > 0.01 {
            // Expensive query
            hints.timeout_ms = 60000; // 1 minute
        }

        hints
    }

    /// Record execution metrics
    pub fn record(&self, query: &str, metrics: CostMetrics) {
        use std::time::{SystemTime, UNIX_EPOCH};

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        // Update current metrics
        {
            let mut current = self.current_metrics.write();
            *current = current.merge(&metrics);
        }

        // Add to history
        {
            let mut history = self.history.write();
            history.push(ExecutionRecord {
                query: query.to_string(),
                metrics,
                timestamp,
            });

            // Keep only last 1000 records
            if history.len() > 1000 {
                history.drain(0..100);
            }
        }
    }

    /// Get current metrics
    pub fn current_metrics(&self) -> CostMetrics {
        self.current_metrics.read().clone()
    }

    /// Get average metrics from history
    pub fn average_metrics(&self) -> CostMetrics {
        let history = self.history.read();
        if history.is_empty() {
            return CostMetrics::new();
        }

        let count = history.len() as u64;
        let mut total = CostMetrics::new();

        for record in history.iter() {
            total = total.merge(&record.metrics);
        }

        CostMetrics {
            cpu_time_ms: total.cpu_time_ms / count,
            memory_bytes: total.memory_bytes, // Keep max
            storage_reads: total.storage_reads / count,
            storage_writes: total.storage_writes / count,
            network_bytes: total.network_bytes / count,
            execution_time_ms: total.execution_time_ms / count,
            cache_hits: total.cache_hits / count,
            cache_misses: total.cache_misses / count,
            rows_processed: total.rows_processed / count,
        }
    }

    /// Reset metrics
    pub fn reset(&self) {
        *self.current_metrics.write() = CostMetrics::new();
        self.history.write().clear();
    }
}

impl Default for CostAwareExecutor {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_execution_hints() {
        let hints = ExecutionHints::default();
        assert!(hints.enable_caching);
        assert_eq!(hints.preferred_tier, StorageTier::Memory);
    }

    #[test]
    fn test_cost_metrics() {
        let metrics = CostMetrics {
            cache_hits: 80,
            cache_misses: 20,
            ..Default::default()
        };

        assert!((metrics.cache_hit_ratio() - 0.8).abs() < 0.001);
    }

    #[test]
    fn test_executor_record() {
        let executor = CostAwareExecutor::new();

        let metrics = CostMetrics {
            cpu_time_ms: 100,
            rows_processed: 1000,
            ..Default::default()
        };

        executor.record("SELECT * FROM test", metrics);

        let current = executor.current_metrics();
        assert_eq!(current.cpu_time_ms, 100);
    }
}
