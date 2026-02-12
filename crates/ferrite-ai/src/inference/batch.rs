//! Batch Manager for Inference
//!
//! Handles automatic batching of inference requests for improved throughput.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

use super::{InferenceInput, InferenceOutput};

/// Batch configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchConfig {
    /// Maximum batch size
    pub max_batch_size: usize,
    /// Maximum wait time before processing incomplete batch
    pub max_wait_ms: u64,
    /// Minimum batch size to process early
    pub min_batch_size: usize,
    /// Enable dynamic batching based on load
    pub dynamic_batching: bool,
    /// Target latency for dynamic adjustment
    pub target_latency_ms: u64,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            max_batch_size: 32,
            max_wait_ms: 10,
            min_batch_size: 1,
            dynamic_batching: true,
            target_latency_ms: 50,
        }
    }
}

impl BatchConfig {
    /// Create config for high throughput
    pub fn high_throughput() -> Self {
        Self {
            max_batch_size: 64,
            max_wait_ms: 50,
            min_batch_size: 8,
            dynamic_batching: true,
            target_latency_ms: 100,
        }
    }

    /// Create config for low latency
    pub fn low_latency() -> Self {
        Self {
            max_batch_size: 8,
            max_wait_ms: 5,
            min_batch_size: 1,
            dynamic_batching: false,
            target_latency_ms: 10,
        }
    }

    /// Set max batch size
    pub fn with_max_batch_size(mut self, size: usize) -> Self {
        self.max_batch_size = size;
        self
    }

    /// Set max wait time
    pub fn with_max_wait(mut self, ms: u64) -> Self {
        self.max_wait_ms = ms;
        self
    }
}

/// A pending batch request
struct PendingRequest {
    input: InferenceInput,
    received_at: Instant,
}

/// Result of batch processing
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchResult {
    /// Outputs for each input
    pub outputs: Vec<InferenceOutput>,
    /// Batch size
    pub batch_size: usize,
    /// Total processing time
    pub processing_time_ms: u64,
    /// Time spent waiting for batch to fill
    pub wait_time_ms: u64,
    /// Whether batch was full
    pub is_full_batch: bool,
}

/// Batch statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BatchStats {
    /// Total batches processed
    pub total_batches: u64,
    /// Total requests processed
    pub total_requests: u64,
    /// Average batch size
    pub avg_batch_size: f64,
    /// Average processing time
    pub avg_processing_time_ms: f64,
    /// Average wait time
    pub avg_wait_time_ms: f64,
    /// Full batch rate
    pub full_batch_rate: f64,
}

/// Batch manager for accumulating and processing inference requests
pub struct BatchManager {
    config: BatchConfig,
    pending: Mutex<VecDeque<PendingRequest>>,
    batch_started: Mutex<Option<Instant>>,
    // Stats
    total_batches: AtomicU64,
    total_requests: AtomicU64,
    total_processing_time: AtomicU64,
    total_wait_time: AtomicU64,
    full_batches: AtomicU64,
    current_batch_size: AtomicU64,
}

impl BatchManager {
    /// Create a new batch manager
    pub fn new(config: BatchConfig) -> Self {
        Self {
            config,
            pending: Mutex::new(VecDeque::new()),
            batch_started: Mutex::new(None),
            total_batches: AtomicU64::new(0),
            total_requests: AtomicU64::new(0),
            total_processing_time: AtomicU64::new(0),
            total_wait_time: AtomicU64::new(0),
            full_batches: AtomicU64::new(0),
            current_batch_size: AtomicU64::new(0),
        }
    }

    /// Create with default config
    pub fn with_defaults() -> Self {
        Self::new(BatchConfig::default())
    }

    /// Add a request to the batch
    pub fn add(&self, input: InferenceInput) {
        let mut pending = self.pending.lock();
        let mut started = self.batch_started.lock();

        if started.is_none() {
            *started = Some(Instant::now());
        }

        pending.push_back(PendingRequest {
            input,
            received_at: Instant::now(),
        });

        self.current_batch_size
            .store(pending.len() as u64, Ordering::Relaxed);
    }

    /// Check if batch is ready to process
    pub fn is_ready(&self) -> bool {
        let pending = self.pending.lock();
        let started = self.batch_started.lock();

        if pending.len() >= self.config.max_batch_size {
            return true;
        }

        if let Some(start_time) = *started {
            if pending.len() >= self.config.min_batch_size
                && start_time.elapsed().as_millis() as u64 >= self.config.max_wait_ms
            {
                return true;
            }
        }

        false
    }

    /// Get current batch size
    pub fn current_size(&self) -> usize {
        self.pending.lock().len()
    }

    /// Take the current batch for processing
    pub fn take_batch(&self) -> Option<Vec<InferenceInput>> {
        let mut pending = self.pending.lock();
        let mut started = self.batch_started.lock();

        if pending.is_empty() {
            return None;
        }

        let batch_size = pending.len().min(self.config.max_batch_size);
        let inputs: Vec<InferenceInput> = pending.drain(..batch_size).map(|r| r.input).collect();

        *started = if pending.is_empty() {
            None
        } else {
            Some(Instant::now())
        };

        self.current_batch_size
            .store(pending.len() as u64, Ordering::Relaxed);

        Some(inputs)
    }

    /// Process batch and update stats
    pub fn record_batch(&self, batch_size: usize, processing_time_ms: u64, wait_time_ms: u64) {
        self.total_batches.fetch_add(1, Ordering::Relaxed);
        self.total_requests
            .fetch_add(batch_size as u64, Ordering::Relaxed);
        self.total_processing_time
            .fetch_add(processing_time_ms, Ordering::Relaxed);
        self.total_wait_time
            .fetch_add(wait_time_ms, Ordering::Relaxed);

        if batch_size >= self.config.max_batch_size {
            self.full_batches.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Get batch statistics
    pub fn stats(&self) -> BatchStats {
        let total_batches = self.total_batches.load(Ordering::Relaxed);
        let total_requests = self.total_requests.load(Ordering::Relaxed);
        let total_processing = self.total_processing_time.load(Ordering::Relaxed);
        let total_wait = self.total_wait_time.load(Ordering::Relaxed);
        let full_batches = self.full_batches.load(Ordering::Relaxed);

        BatchStats {
            total_batches,
            total_requests,
            avg_batch_size: if total_batches > 0 {
                total_requests as f64 / total_batches as f64
            } else {
                0.0
            },
            avg_processing_time_ms: if total_batches > 0 {
                total_processing as f64 / total_batches as f64
            } else {
                0.0
            },
            avg_wait_time_ms: if total_batches > 0 {
                total_wait as f64 / total_batches as f64
            } else {
                0.0
            },
            full_batch_rate: if total_batches > 0 {
                full_batches as f64 / total_batches as f64
            } else {
                0.0
            },
        }
    }

    /// Get configuration
    pub fn config(&self) -> &BatchConfig {
        &self.config
    }

    /// Clear pending requests
    pub fn clear(&self) {
        self.pending.lock().clear();
        *self.batch_started.lock() = None;
        self.current_batch_size.store(0, Ordering::Relaxed);
    }
}

impl Default for BatchManager {
    fn default() -> Self {
        Self::with_defaults()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_batch_config() {
        let default = BatchConfig::default();
        assert_eq!(default.max_batch_size, 32);

        let high_throughput = BatchConfig::high_throughput();
        assert_eq!(high_throughput.max_batch_size, 64);

        let low_latency = BatchConfig::low_latency();
        assert_eq!(low_latency.max_batch_size, 8);
    }

    #[test]
    fn test_batch_manager() {
        let manager = BatchManager::new(BatchConfig::default().with_max_batch_size(3));

        manager.add(InferenceInput::Scalar(1.0));
        assert_eq!(manager.current_size(), 1);
        assert!(!manager.is_ready());

        manager.add(InferenceInput::Scalar(2.0));
        manager.add(InferenceInput::Scalar(3.0));
        assert_eq!(manager.current_size(), 3);
        assert!(manager.is_ready());

        let batch = manager.take_batch().unwrap();
        assert_eq!(batch.len(), 3);
        assert_eq!(manager.current_size(), 0);
    }

    #[test]
    fn test_batch_stats() {
        let manager = BatchManager::new(BatchConfig::default().with_max_batch_size(2));

        manager.record_batch(2, 10, 5);
        manager.record_batch(2, 8, 3);

        let stats = manager.stats();
        assert_eq!(stats.total_batches, 2);
        assert_eq!(stats.total_requests, 4);
        assert!((stats.avg_batch_size - 2.0).abs() < 0.001);
    }

    #[test]
    fn test_partial_batch() {
        let manager = BatchManager::new(BatchConfig::default().with_max_batch_size(10));

        manager.add(InferenceInput::Scalar(1.0));
        manager.add(InferenceInput::Scalar(2.0));

        // Not ready yet (not enough items and timeout not expired)
        assert!(!manager.is_ready());

        // But we can still take what we have
        let batch = manager.take_batch().unwrap();
        assert_eq!(batch.len(), 2);
    }
}
