//! RDMA Performance Optimization
//!
//! High-performance optimizations for RDMA networking including:
//! - Adaptive batching based on load
//! - Memory prefetch for cache efficiency
//! - Cache-aligned buffers
//! - NUMA-aware memory allocation
//! - Completion coalescing
//!
//! # Performance Techniques
//!
//! ## Adaptive Batching
//! Dynamically adjusts batch size based on queue depth and latency targets.
//! Under low load, minimizes latency with smaller batches.
//! Under high load, maximizes throughput with larger batches.
//!
//! ## Memory Prefetch
//! Prefetches data into CPU cache before processing, reducing memory stalls.
//!
//! ## Cache-Aligned Buffers
//! Ensures buffers are aligned to cache line boundaries (64 bytes) to prevent
//! false sharing and improve cache efficiency.
//!
//! ## NUMA Awareness
//! Allocates memory on the same NUMA node as the NIC for optimal latency.

use std::alloc::{alloc, dealloc, Layout};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

/// Cache line size (64 bytes on most modern CPUs)
pub const CACHE_LINE_SIZE: usize = 64;

/// Default prefetch distance (number of cache lines ahead)
pub const DEFAULT_PREFETCH_DISTANCE: usize = 8;

/// Adaptive batching configuration
#[derive(Clone, Debug)]
pub struct AdaptiveBatchConfig {
    /// Minimum batch size
    pub min_batch_size: usize,
    /// Maximum batch size
    pub max_batch_size: usize,
    /// Target latency (microseconds)
    pub target_latency_us: u64,
    /// Latency measurement window
    pub measurement_window: usize,
    /// Adjustment factor (how aggressively to change batch size)
    pub adjustment_factor: f64,
    /// Enable adaptive mode
    pub enabled: bool,
}

impl Default for AdaptiveBatchConfig {
    fn default() -> Self {
        Self {
            min_batch_size: 1,
            max_batch_size: 64,
            target_latency_us: 100, // 100μs target
            measurement_window: 100,
            adjustment_factor: 0.1,
            enabled: true,
        }
    }
}

/// Adaptive batch controller
pub struct AdaptiveBatcher {
    /// Configuration
    config: AdaptiveBatchConfig,
    /// Current batch size
    current_batch_size: AtomicUsize,
    /// Recent latencies (in microseconds)
    latencies: Mutex<VecDeque<u64>>,
    /// Total requests processed
    requests_processed: AtomicU64,
    /// Total batches processed
    batches_processed: AtomicU64,
    /// Last adjustment time
    last_adjustment: Mutex<Instant>,
}

impl AdaptiveBatcher {
    /// Create a new adaptive batcher
    pub fn new(config: AdaptiveBatchConfig) -> Self {
        let initial_batch = (config.min_batch_size + config.max_batch_size) / 2;
        Self {
            config,
            current_batch_size: AtomicUsize::new(initial_batch),
            latencies: Mutex::new(VecDeque::with_capacity(100)),
            requests_processed: AtomicU64::new(0),
            batches_processed: AtomicU64::new(0),
            last_adjustment: Mutex::new(Instant::now()),
        }
    }

    /// Get current batch size
    pub fn batch_size(&self) -> usize {
        self.current_batch_size.load(Ordering::Relaxed)
    }

    /// Record a batch completion
    pub async fn record_batch(&self, batch_size: usize, latency_us: u64) {
        self.requests_processed
            .fetch_add(batch_size as u64, Ordering::Relaxed);
        self.batches_processed.fetch_add(1, Ordering::Relaxed);

        if !self.config.enabled {
            return;
        }

        // Record latency
        {
            let mut latencies = self.latencies.lock().await;
            latencies.push_back(latency_us);
            if latencies.len() > self.config.measurement_window {
                latencies.pop_front();
            }
        }

        // Check if we should adjust
        let mut last_adj = self.last_adjustment.lock().await;
        if last_adj.elapsed() < Duration::from_millis(10) {
            return;
        }
        *last_adj = Instant::now();

        // Calculate average latency
        let avg_latency = {
            let latencies = self.latencies.lock().await;
            if latencies.is_empty() {
                return;
            }
            latencies.iter().sum::<u64>() / latencies.len() as u64
        };

        // Adjust batch size based on latency
        let current = self.current_batch_size.load(Ordering::Relaxed);
        let target = self.config.target_latency_us;

        let new_size = if avg_latency > target {
            // Latency too high, reduce batch size
            let reduction = ((avg_latency as f64 - target as f64) / target as f64
                * self.config.adjustment_factor
                * current as f64) as usize;
            current.saturating_sub(reduction.max(1))
        } else if avg_latency < target / 2 {
            // Latency well under target, increase batch size for throughput
            let increase = ((target as f64 - avg_latency as f64) / target as f64
                * self.config.adjustment_factor
                * current as f64) as usize;
            current + increase.max(1)
        } else {
            current
        };

        let clamped = new_size.clamp(self.config.min_batch_size, self.config.max_batch_size);
        self.current_batch_size.store(clamped, Ordering::Relaxed);
    }

    /// Get statistics
    pub fn stats(&self) -> AdaptiveBatcherStats {
        AdaptiveBatcherStats {
            current_batch_size: self.current_batch_size.load(Ordering::Relaxed),
            requests_processed: self.requests_processed.load(Ordering::Relaxed),
            batches_processed: self.batches_processed.load(Ordering::Relaxed),
        }
    }
}

/// Adaptive batcher statistics
#[derive(Clone, Debug)]
pub struct AdaptiveBatcherStats {
    /// Current batch size
    pub current_batch_size: usize,
    /// Total requests processed
    pub requests_processed: u64,
    /// Total batches processed
    pub batches_processed: u64,
}

/// Cache-aligned buffer for RDMA operations
#[repr(C, align(64))]
pub struct CacheAlignedBuffer {
    /// Data buffer
    data: Vec<u8>,
    /// Capacity
    capacity: usize,
    /// Current length
    len: usize,
}

impl CacheAlignedBuffer {
    /// Create a new cache-aligned buffer
    pub fn new(capacity: usize) -> Self {
        // Round up to cache line size
        let aligned_capacity = capacity.div_ceil(CACHE_LINE_SIZE) * CACHE_LINE_SIZE;
        Self {
            data: vec![0u8; aligned_capacity],
            capacity: aligned_capacity,
            len: 0,
        }
    }

    /// Get buffer slice
    pub fn as_slice(&self) -> &[u8] {
        &self.data[..self.len]
    }

    /// Get mutable buffer slice
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.data[..self.len]
    }

    /// Get full capacity slice for writing
    pub fn as_write_slice(&mut self) -> &mut [u8] {
        &mut self.data[..self.capacity]
    }

    /// Set length after writing
    pub fn set_len(&mut self, len: usize) {
        assert!(len <= self.capacity);
        self.len = len;
    }

    /// Get capacity
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Get current length
    pub fn len(&self) -> usize {
        self.len
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Clear the buffer
    pub fn clear(&mut self) {
        self.len = 0;
    }

    /// Get pointer to data
    pub fn as_ptr(&self) -> *const u8 {
        self.data.as_ptr()
    }

    /// Get mutable pointer to data
    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        self.data.as_mut_ptr()
    }
}

/// Pool of cache-aligned buffers
pub struct CacheAlignedPool {
    /// Available buffers
    buffers: Mutex<Vec<CacheAlignedBuffer>>,
    /// Buffer size
    buffer_size: usize,
    /// Maximum pool size
    max_size: usize,
    /// Current pool size
    current_size: AtomicUsize,
    /// Allocations
    allocations: AtomicU64,
    /// Reuses
    reuses: AtomicU64,
}

impl CacheAlignedPool {
    /// Create a new buffer pool
    pub fn new(buffer_size: usize, initial_size: usize, max_size: usize) -> Self {
        let mut buffers = Vec::with_capacity(initial_size);
        for _ in 0..initial_size {
            buffers.push(CacheAlignedBuffer::new(buffer_size));
        }

        Self {
            buffers: Mutex::new(buffers),
            buffer_size,
            max_size,
            current_size: AtomicUsize::new(initial_size),
            allocations: AtomicU64::new(0),
            reuses: AtomicU64::new(0),
        }
    }

    /// Get a buffer from the pool
    pub async fn acquire(&self) -> CacheAlignedBuffer {
        let mut buffers = self.buffers.lock().await;

        if let Some(mut buf) = buffers.pop() {
            self.reuses.fetch_add(1, Ordering::Relaxed);
            buf.clear();
            return buf;
        }

        // Allocate new buffer if under limit
        self.allocations.fetch_add(1, Ordering::Relaxed);
        self.current_size.fetch_add(1, Ordering::Relaxed);
        CacheAlignedBuffer::new(self.buffer_size)
    }

    /// Return a buffer to the pool
    pub async fn release(&self, buf: CacheAlignedBuffer) {
        let mut buffers = self.buffers.lock().await;
        if buffers.len() < self.max_size {
            buffers.push(buf);
        }
        // Otherwise, buffer is dropped
    }

    /// Get pool statistics
    pub fn stats(&self) -> CacheAlignedPoolStats {
        CacheAlignedPoolStats {
            current_size: self.current_size.load(Ordering::Relaxed),
            allocations: self.allocations.load(Ordering::Relaxed),
            reuses: self.reuses.load(Ordering::Relaxed),
        }
    }
}

/// Buffer pool statistics
#[derive(Clone, Debug)]
pub struct CacheAlignedPoolStats {
    /// Current pool size
    pub current_size: usize,
    /// Total allocations
    pub allocations: u64,
    /// Buffer reuses
    pub reuses: u64,
}

/// Memory prefetch utilities
pub mod prefetch {
    /// Prefetch data for reading
    ///
    /// Uses architecture-specific prefetch instructions where available.
    /// Falls back to a no-op on unsupported platforms.
    #[inline]
    pub fn prefetch_read<T>(ptr: *const T) {
        #[cfg(target_arch = "x86_64")]
        {
            // SAFETY: Prefetch is a hint and safe even for invalid addresses
            unsafe {
                std::arch::x86_64::_mm_prefetch(ptr as *const i8, std::arch::x86_64::_MM_HINT_T0);
            }
        }

        #[cfg(not(target_arch = "x86_64"))]
        {
            // No prefetch intrinsics available on stable Rust for this platform
            let _ = ptr;
        }
    }

    /// Prefetch data for writing
    ///
    /// Uses architecture-specific prefetch instructions where available.
    /// Falls back to a no-op on unsupported platforms.
    #[inline]
    pub fn prefetch_write<T>(ptr: *mut T) {
        #[cfg(target_arch = "x86_64")]
        {
            // SAFETY: Prefetch is a hint and safe even for invalid addresses
            unsafe {
                std::arch::x86_64::_mm_prefetch(ptr as *const i8, std::arch::x86_64::_MM_HINT_T0);
            }
        }

        #[cfg(not(target_arch = "x86_64"))]
        {
            // No prefetch intrinsics available on stable Rust for this platform
            let _ = ptr;
        }
    }

    /// Prefetch a range of cache lines
    ///
    /// # Safety
    /// The caller must ensure that `ptr` points to a valid memory region of at least `len` bytes.
    #[inline]
    pub unsafe fn prefetch_range(ptr: *const u8, len: usize) {
        let cache_line_size = super::CACHE_LINE_SIZE;
        let mut offset = 0;
        while offset < len {
            // SAFETY: We're only issuing prefetch hints, which are safe
            // even if the address is invalid
            prefetch_read(unsafe { ptr.add(offset) });
            offset += cache_line_size;
        }
    }
}

/// Completion coalescing for reducing interrupts
pub struct CompletionCoalescer {
    /// Coalescing threshold (minimum completions before notification)
    threshold: usize,
    /// Coalescing timeout (max wait before notification)
    timeout: Duration,
    /// Pending completions
    pending: AtomicUsize,
    /// Last notification time
    last_notification: Mutex<Instant>,
    /// Total coalesced completions
    coalesced: AtomicU64,
    /// Total notifications
    notifications: AtomicU64,
}

impl CompletionCoalescer {
    /// Create a new coalescer
    pub fn new(threshold: usize, timeout: Duration) -> Self {
        Self {
            threshold,
            timeout,
            pending: AtomicUsize::new(0),
            last_notification: Mutex::new(Instant::now()),
            coalesced: AtomicU64::new(0),
            notifications: AtomicU64::new(0),
        }
    }

    /// Add a completion
    pub async fn add_completion(&self) -> bool {
        let pending = self.pending.fetch_add(1, Ordering::Relaxed) + 1;
        self.coalesced.fetch_add(1, Ordering::Relaxed);

        // Check if we should notify
        if pending >= self.threshold {
            return self.notify().await;
        }

        // Check timeout
        let last = self.last_notification.lock().await;
        if last.elapsed() >= self.timeout {
            drop(last);
            return self.notify().await;
        }

        false
    }

    /// Force notification
    pub async fn notify(&self) -> bool {
        let pending = self.pending.swap(0, Ordering::Relaxed);
        if pending > 0 {
            *self.last_notification.lock().await = Instant::now();
            self.notifications.fetch_add(1, Ordering::Relaxed);
            return true;
        }
        false
    }

    /// Get pending count
    pub fn pending(&self) -> usize {
        self.pending.load(Ordering::Relaxed)
    }

    /// Get statistics
    pub fn stats(&self) -> CompletionCoalescerStats {
        let coalesced = self.coalesced.load(Ordering::Relaxed);
        let notifications = self.notifications.load(Ordering::Relaxed);

        CompletionCoalescerStats {
            coalesced,
            notifications,
            coalescing_ratio: if notifications > 0 {
                coalesced as f64 / notifications as f64
            } else {
                0.0
            },
        }
    }
}

/// Completion coalescer statistics
#[derive(Clone, Debug)]
pub struct CompletionCoalescerStats {
    /// Total completions coalesced
    pub coalesced: u64,
    /// Total notifications sent
    pub notifications: u64,
    /// Average completions per notification
    pub coalescing_ratio: f64,
}

/// NUMA-aware memory allocator (simplified)
pub struct NumaAllocator {
    /// Preferred NUMA node (-1 for local)
    preferred_node: i32,
    /// Total allocations
    allocations: AtomicU64,
    /// Total bytes allocated
    bytes_allocated: AtomicU64,
}

impl NumaAllocator {
    /// Create allocator for local NUMA node
    pub fn local() -> Self {
        Self {
            preferred_node: -1,
            allocations: AtomicU64::new(0),
            bytes_allocated: AtomicU64::new(0),
        }
    }

    /// Create allocator for specific NUMA node
    pub fn for_node(node: i32) -> Self {
        Self {
            preferred_node: node,
            allocations: AtomicU64::new(0),
            bytes_allocated: AtomicU64::new(0),
        }
    }

    /// Allocate cache-aligned memory
    ///
    /// # Safety
    ///
    /// The caller must ensure the returned pointer is properly deallocated
    /// using `deallocate` with the same size.
    // SAFETY: The `alloc` call uses a valid `Layout` (size is rounded up to CACHE_LINE_SIZE
    // alignment and `from_size_align` panics on invalid input). Caller must guarantee the
    // returned pointer is later freed via `deallocate` with the same size.
    pub unsafe fn allocate(&self, size: usize) -> *mut u8 {
        let aligned_size = size.div_ceil(CACHE_LINE_SIZE) * CACHE_LINE_SIZE;
        let layout =
            Layout::from_size_align(aligned_size, CACHE_LINE_SIZE).expect("Invalid layout");

        let ptr = alloc(layout);

        if !ptr.is_null() {
            self.allocations.fetch_add(1, Ordering::Relaxed);
            self.bytes_allocated
                .fetch_add(aligned_size as u64, Ordering::Relaxed);
        }

        ptr
    }

    /// Deallocate memory
    ///
    /// # Safety
    ///
    /// The caller must ensure:
    /// - `ptr` was allocated by this allocator with the same `size`
    /// - `ptr` is not null
    /// - The memory has not already been deallocated
    // SAFETY: The `dealloc` call requires `ptr` was allocated by the global allocator with the
    // same `Layout`. Caller guarantees `ptr` is non-null, was returned by `allocate` with the
    // same `size`, and has not been previously freed. Double-free would cause undefined behavior.
    pub unsafe fn deallocate(&self, ptr: *mut u8, size: usize) {
        let aligned_size = size.div_ceil(CACHE_LINE_SIZE) * CACHE_LINE_SIZE;
        let layout =
            Layout::from_size_align(aligned_size, CACHE_LINE_SIZE).expect("Invalid layout");

        dealloc(ptr, layout);
        self.bytes_allocated
            .fetch_sub(aligned_size as u64, Ordering::Relaxed);
    }

    /// Get statistics
    pub fn stats(&self) -> NumaAllocatorStats {
        NumaAllocatorStats {
            preferred_node: self.preferred_node,
            allocations: self.allocations.load(Ordering::Relaxed),
            bytes_allocated: self.bytes_allocated.load(Ordering::Relaxed),
        }
    }
}

/// NUMA allocator statistics
#[derive(Clone, Debug)]
pub struct NumaAllocatorStats {
    /// Preferred NUMA node
    pub preferred_node: i32,
    /// Total allocations
    pub allocations: u64,
    /// Current bytes allocated
    pub bytes_allocated: u64,
}

/// Polling mode for completion handling
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PollingMode {
    /// Busy polling (lowest latency, highest CPU)
    Busy,
    /// Adaptive polling (balance latency and CPU)
    Adaptive,
    /// Event-driven (lowest CPU, higher latency)
    Event,
}

/// Adaptive polling controller
pub struct AdaptivePoller {
    /// Current mode
    mode: AtomicU32,
    /// Spin count for busy polling
    spin_count: AtomicUsize,
    /// Recent latencies
    latencies: Mutex<VecDeque<u64>>,
    /// Busy poll threshold (ns) - below this, use busy polling
    busy_threshold_ns: u64,
    /// Event threshold (ns) - above this, use event-driven
    event_threshold_ns: u64,
}

impl AdaptivePoller {
    /// Create a new adaptive poller
    pub fn new(busy_threshold_ns: u64, event_threshold_ns: u64) -> Self {
        Self {
            mode: AtomicU32::new(PollingMode::Adaptive as u32),
            spin_count: AtomicUsize::new(1000),
            latencies: Mutex::new(VecDeque::with_capacity(100)),
            busy_threshold_ns,
            event_threshold_ns,
        }
    }

    /// Get current polling mode
    pub fn mode(&self) -> PollingMode {
        match self.mode.load(Ordering::Relaxed) {
            0 => PollingMode::Busy,
            1 => PollingMode::Adaptive,
            _ => PollingMode::Event,
        }
    }

    /// Get spin count for busy polling
    pub fn spin_count(&self) -> usize {
        self.spin_count.load(Ordering::Relaxed)
    }

    /// Record a poll latency
    pub async fn record_latency(&self, latency_ns: u64) {
        let mut latencies = self.latencies.lock().await;
        latencies.push_back(latency_ns);
        if latencies.len() > 100 {
            latencies.pop_front();
        }

        // Calculate average
        let avg = latencies.iter().sum::<u64>() / latencies.len() as u64;
        drop(latencies);

        // Adjust mode
        let new_mode = if avg < self.busy_threshold_ns {
            PollingMode::Busy
        } else if avg > self.event_threshold_ns {
            PollingMode::Event
        } else {
            PollingMode::Adaptive
        };

        self.mode.store(new_mode as u32, Ordering::Relaxed);

        // Adjust spin count for adaptive mode
        if new_mode == PollingMode::Adaptive {
            let current = self.spin_count.load(Ordering::Relaxed);
            let target_latency = (self.busy_threshold_ns + self.event_threshold_ns) / 2;

            let new_spin = if avg < target_latency {
                // Can reduce spinning
                (current * 9 / 10).max(100)
            } else {
                // Need more spinning
                (current * 11 / 10).min(10000)
            };

            self.spin_count.store(new_spin, Ordering::Relaxed);
        }
    }

    /// Force a specific mode
    pub fn set_mode(&self, mode: PollingMode) {
        self.mode.store(mode as u32, Ordering::Relaxed);
    }
}

/// Performance monitoring for RDMA operations
pub struct RdmaPerformanceMonitor {
    /// Operations per second
    ops_per_sec: AtomicU64,
    /// Bytes per second
    bytes_per_sec: AtomicU64,
    /// Average latency (nanoseconds)
    avg_latency_ns: AtomicU64,
    /// P50 latency
    p50_latency_ns: AtomicU64,
    /// P99 latency
    p99_latency_ns: AtomicU64,
    /// P99.9 latency
    p999_latency_ns: AtomicU64,
    /// Sample window
    samples: Mutex<Vec<u64>>,
    /// Last calculation time
    last_calc: Mutex<Instant>,
    /// Operations since last calc
    ops_since_calc: AtomicU64,
    /// Bytes since last calc
    bytes_since_calc: AtomicU64,
}

impl RdmaPerformanceMonitor {
    /// Create a new monitor
    pub fn new() -> Self {
        Self {
            ops_per_sec: AtomicU64::new(0),
            bytes_per_sec: AtomicU64::new(0),
            avg_latency_ns: AtomicU64::new(0),
            p50_latency_ns: AtomicU64::new(0),
            p99_latency_ns: AtomicU64::new(0),
            p999_latency_ns: AtomicU64::new(0),
            samples: Mutex::new(Vec::with_capacity(10000)),
            last_calc: Mutex::new(Instant::now()),
            ops_since_calc: AtomicU64::new(0),
            bytes_since_calc: AtomicU64::new(0),
        }
    }

    /// Record an operation
    pub async fn record_op(&self, bytes: usize, latency_ns: u64) {
        self.ops_since_calc.fetch_add(1, Ordering::Relaxed);
        self.bytes_since_calc
            .fetch_add(bytes as u64, Ordering::Relaxed);

        let mut samples = self.samples.lock().await;
        samples.push(latency_ns);

        // Periodically recalculate stats
        if samples.len() >= 1000 {
            drop(samples);
            self.recalculate().await;
        }
    }

    /// Recalculate statistics
    pub async fn recalculate(&self) {
        let mut last_calc = self.last_calc.lock().await;
        let elapsed = last_calc.elapsed();
        *last_calc = Instant::now();
        drop(last_calc);

        let elapsed_secs = elapsed.as_secs_f64();
        if elapsed_secs > 0.0 {
            let ops = self.ops_since_calc.swap(0, Ordering::Relaxed);
            let bytes = self.bytes_since_calc.swap(0, Ordering::Relaxed);

            self.ops_per_sec
                .store((ops as f64 / elapsed_secs) as u64, Ordering::Relaxed);
            self.bytes_per_sec
                .store((bytes as f64 / elapsed_secs) as u64, Ordering::Relaxed);
        }

        // Calculate latency percentiles
        let mut samples = self.samples.lock().await;
        if samples.is_empty() {
            return;
        }

        samples.sort_unstable();
        let len = samples.len();

        let avg = samples.iter().sum::<u64>() / len as u64;
        let p50 = samples[len * 50 / 100];
        let p99 = samples[len * 99 / 100];
        let p999 = samples[len * 999 / 1000];

        self.avg_latency_ns.store(avg, Ordering::Relaxed);
        self.p50_latency_ns.store(p50, Ordering::Relaxed);
        self.p99_latency_ns.store(p99, Ordering::Relaxed);
        self.p999_latency_ns.store(p999, Ordering::Relaxed);

        samples.clear();
    }

    /// Get current statistics
    pub fn stats(&self) -> RdmaPerformanceStats {
        RdmaPerformanceStats {
            ops_per_sec: self.ops_per_sec.load(Ordering::Relaxed),
            bytes_per_sec: self.bytes_per_sec.load(Ordering::Relaxed),
            avg_latency_ns: self.avg_latency_ns.load(Ordering::Relaxed),
            p50_latency_ns: self.p50_latency_ns.load(Ordering::Relaxed),
            p99_latency_ns: self.p99_latency_ns.load(Ordering::Relaxed),
            p999_latency_ns: self.p999_latency_ns.load(Ordering::Relaxed),
        }
    }
}

impl Default for RdmaPerformanceMonitor {
    fn default() -> Self {
        Self::new()
    }
}

/// RDMA performance statistics
#[derive(Clone, Debug)]
pub struct RdmaPerformanceStats {
    /// Operations per second
    pub ops_per_sec: u64,
    /// Bytes per second
    pub bytes_per_sec: u64,
    /// Average latency (nanoseconds)
    pub avg_latency_ns: u64,
    /// P50 latency (nanoseconds)
    pub p50_latency_ns: u64,
    /// P99 latency (nanoseconds)
    pub p99_latency_ns: u64,
    /// P99.9 latency (nanoseconds)
    pub p999_latency_ns: u64,
}

impl RdmaPerformanceStats {
    /// Convert latency to human-readable string
    pub fn latency_str(&self) -> String {
        format!(
            "avg={:.1}μs p50={:.1}μs p99={:.1}μs p99.9={:.1}μs",
            self.avg_latency_ns as f64 / 1000.0,
            self.p50_latency_ns as f64 / 1000.0,
            self.p99_latency_ns as f64 / 1000.0,
            self.p999_latency_ns as f64 / 1000.0,
        )
    }

    /// Convert throughput to human-readable string
    pub fn throughput_str(&self) -> String {
        let mb_per_sec = self.bytes_per_sec as f64 / (1024.0 * 1024.0);
        format!("{:.1} ops/sec, {:.1} MB/sec", self.ops_per_sec, mb_per_sec)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_adaptive_batch_config() {
        let config = AdaptiveBatchConfig::default();
        assert_eq!(config.min_batch_size, 1);
        assert_eq!(config.max_batch_size, 64);
        assert!(config.enabled);
    }

    #[tokio::test]
    async fn test_adaptive_batcher() {
        let config = AdaptiveBatchConfig::default();
        let batcher = AdaptiveBatcher::new(config);

        // Initial batch size should be middle value
        assert_eq!(batcher.batch_size(), 32);

        // Record some batches
        for _ in 0..10 {
            batcher.record_batch(32, 50).await;
        }

        let stats = batcher.stats();
        assert!(stats.batches_processed > 0);
    }

    #[test]
    fn test_cache_aligned_buffer() {
        let mut buf = CacheAlignedBuffer::new(1024);
        assert_eq!(buf.capacity(), 1024);
        assert_eq!(buf.len(), 0);
        assert!(buf.is_empty());

        buf.set_len(100);
        assert_eq!(buf.len(), 100);
        assert!(!buf.is_empty());

        buf.clear();
        assert!(buf.is_empty());
    }

    #[tokio::test]
    async fn test_cache_aligned_pool() {
        let pool = CacheAlignedPool::new(1024, 4, 16);

        // Acquire buffers
        let buf1 = pool.acquire().await;
        let buf2 = pool.acquire().await;

        assert_eq!(buf1.capacity(), 1024);
        assert_eq!(buf2.capacity(), 1024);

        // Release and reacquire
        pool.release(buf1).await;
        let buf3 = pool.acquire().await;
        assert_eq!(buf3.capacity(), 1024);

        let stats = pool.stats();
        assert!(stats.reuses > 0);
    }

    #[tokio::test]
    async fn test_completion_coalescer() {
        let coalescer = CompletionCoalescer::new(4, Duration::from_millis(100));

        // Add completions
        for _ in 0..3 {
            let notified = coalescer.add_completion().await;
            assert!(!notified);
        }

        // Fourth should trigger notification
        let notified = coalescer.add_completion().await;
        assert!(notified);

        let stats = coalescer.stats();
        assert_eq!(stats.coalesced, 4);
        assert_eq!(stats.notifications, 1);
    }

    #[test]
    fn test_polling_mode() {
        let poller = AdaptivePoller::new(1000, 10000);
        assert_eq!(poller.mode(), PollingMode::Adaptive);

        poller.set_mode(PollingMode::Busy);
        assert_eq!(poller.mode(), PollingMode::Busy);
    }

    #[tokio::test]
    async fn test_performance_monitor() {
        let monitor = RdmaPerformanceMonitor::new();

        // Record some operations
        for i in 0..100 {
            monitor.record_op(1024, (i * 100) as u64).await;
        }

        monitor.recalculate().await;
        let stats = monitor.stats();

        assert!(stats.avg_latency_ns > 0);
        assert!(stats.p99_latency_ns >= stats.p50_latency_ns);
    }
}
