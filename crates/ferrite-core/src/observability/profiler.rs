//! Performance Profiler
//!
//! CPU and memory profiling with flame graph support.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use uuid::Uuid;

/// Profiler for performance analysis
pub struct Profiler {
    sessions: RwLock<HashMap<String, Arc<ProfilingSession>>>,
    sample_rate: f64,
    max_duration_secs: u64,
}

impl Profiler {
    /// Create a new profiler
    pub fn new(sample_rate: f64, max_duration_secs: u64) -> Self {
        Self {
            sessions: RwLock::new(HashMap::new()),
            sample_rate: sample_rate.clamp(0.0, 1.0),
            max_duration_secs,
        }
    }

    /// Start a profiling session
    pub fn start(&self, profile_type: ProfileType) -> String {
        let session_id = Uuid::new_v4().to_string();
        let session = Arc::new(ProfilingSession::new(
            session_id.clone(),
            profile_type,
            self.sample_rate,
            self.max_duration_secs,
        ));

        tokio::task::block_in_place(|| {
            let rt = tokio::runtime::Handle::current();
            rt.block_on(async {
                let mut sessions = self.sessions.write().await;
                sessions.insert(session_id.clone(), session.clone());
            });
        });

        // Start the profiling session
        session.start();

        session_id
    }

    /// Stop a profiling session and get results
    pub fn stop(&self, session_id: &str) -> Option<ProfileData> {
        tokio::task::block_in_place(|| {
            let rt = tokio::runtime::Handle::current();
            rt.block_on(async {
                let sessions = self.sessions.read().await;
                let session = sessions.get(session_id)?;
                session.stop();
                Some(session.get_data().await)
            })
        })
    }

    /// Get number of active profiling sessions
    pub fn active_sessions(&self) -> usize {
        tokio::task::block_in_place(|| {
            let rt = tokio::runtime::Handle::current();
            rt.block_on(async {
                let sessions = self.sessions.read().await;
                sessions.values().filter(|s| s.is_active()).count()
            })
        })
    }

    /// Record a sample (called from hot paths when profiling is active)
    pub fn record_sample(&self, session_id: &str, sample: ProfileSample) {
        tokio::task::block_in_place(|| {
            let rt = tokio::runtime::Handle::current();
            rt.block_on(async {
                let sessions = self.sessions.read().await;
                if let Some(session) = sessions.get(session_id) {
                    if session.is_active() && session.should_sample() {
                        session.record(sample).await;
                    }
                }
            });
        });
    }

    /// Check if a session should sample (probabilistic)
    pub fn should_sample(&self, session_id: &str) -> bool {
        tokio::task::block_in_place(|| {
            let rt = tokio::runtime::Handle::current();
            rt.block_on(async {
                let sessions = self.sessions.read().await;
                sessions
                    .get(session_id)
                    .map(|s| s.is_active() && s.should_sample())
                    .unwrap_or(false)
            })
        })
    }

    /// Clean up completed sessions
    pub async fn cleanup(&self, max_age_secs: u64) {
        let mut sessions = self.sessions.write().await;
        let now = current_timestamp_ms();
        let max_age_ms = max_age_secs * 1000;

        sessions.retain(|_, session| {
            let age = now.saturating_sub(session.started_at);
            age < max_age_ms || session.is_active()
        });
    }
}

/// A profiling session
pub struct ProfilingSession {
    /// Session identifier
    pub session_id: String,
    /// Profile type
    pub profile_type: ProfileType,
    /// When the session started
    pub started_at: u64,
    /// Session duration
    stopped_at: RwLock<Option<u64>>,
    /// Active flag
    active: AtomicBool,
    /// Sample rate
    sample_rate: f64,
    /// Maximum duration
    max_duration_secs: u64,
    /// Start instant (for duration tracking)
    start_instant: RwLock<Option<Instant>>,
    /// Collected samples
    samples: RwLock<Vec<ProfileSample>>,
    /// Sample counter
    sample_count: AtomicU64,
    /// Aggregated stack traces (for flame graph)
    stack_counts: RwLock<HashMap<String, u64>>,
    /// Memory snapshots
    memory_snapshots: RwLock<Vec<MemorySnapshot>>,
}

impl ProfilingSession {
    /// Create a new profiling session
    pub fn new(
        session_id: String,
        profile_type: ProfileType,
        sample_rate: f64,
        max_duration_secs: u64,
    ) -> Self {
        Self {
            session_id,
            profile_type,
            started_at: current_timestamp_ms(),
            stopped_at: RwLock::new(None),
            active: AtomicBool::new(false),
            sample_rate,
            max_duration_secs,
            start_instant: RwLock::new(None),
            samples: RwLock::new(Vec::new()),
            sample_count: AtomicU64::new(0),
            stack_counts: RwLock::new(HashMap::new()),
            memory_snapshots: RwLock::new(Vec::new()),
        }
    }

    /// Start the profiling session
    pub fn start(&self) {
        self.active.store(true, Ordering::Relaxed);
        tokio::task::block_in_place(|| {
            let rt = tokio::runtime::Handle::current();
            rt.block_on(async {
                let mut start_instant = self.start_instant.write().await;
                *start_instant = Some(Instant::now());
            });
        });
    }

    /// Stop the profiling session
    pub fn stop(&self) {
        self.active.store(false, Ordering::Relaxed);
        tokio::task::block_in_place(|| {
            let rt = tokio::runtime::Handle::current();
            rt.block_on(async {
                let mut stopped_at = self.stopped_at.write().await;
                if stopped_at.is_none() {
                    *stopped_at = Some(current_timestamp_ms());
                }
            });
        });
    }

    /// Check if session is active
    pub fn is_active(&self) -> bool {
        if !self.active.load(Ordering::Relaxed) {
            return false;
        }

        // Check if max duration exceeded
        tokio::task::block_in_place(|| {
            let rt = tokio::runtime::Handle::current();
            rt.block_on(async {
                let start_instant = self.start_instant.read().await;
                if let Some(start) = *start_instant {
                    if start.elapsed().as_secs() > self.max_duration_secs {
                        return false;
                    }
                }
                true
            })
        })
    }

    /// Check if we should take a sample (probabilistic)
    pub fn should_sample(&self) -> bool {
        rand::random::<f64>() < self.sample_rate
    }

    /// Record a sample
    pub async fn record(&self, sample: ProfileSample) {
        let mut samples = self.samples.write().await;
        samples.push(sample.clone());
        self.sample_count.fetch_add(1, Ordering::Relaxed);

        // Aggregate stack trace for flame graph
        if let Some(ref stack) = sample.stack_trace {
            let stack_key = stack.join(";");
            let mut stack_counts = self.stack_counts.write().await;
            *stack_counts.entry(stack_key).or_insert(0) += 1;
        }
    }

    /// Record a memory snapshot
    pub async fn record_memory(&self, snapshot: MemorySnapshot) {
        let mut snapshots = self.memory_snapshots.write().await;
        snapshots.push(snapshot);
    }

    /// Get aggregated profile data
    pub async fn get_data(&self) -> ProfileData {
        let samples = self.samples.read().await;
        let stack_counts = self.stack_counts.read().await;
        let memory_snapshots = self.memory_snapshots.read().await;

        let stopped_at = self.stopped_at.read().await;
        let duration_ms = stopped_at.unwrap_or_else(current_timestamp_ms) - self.started_at;

        // Calculate hot spots
        let mut hot_spots: Vec<HotSpot> = stack_counts
            .iter()
            .map(|(stack, count)| {
                let frames: Vec<&str> = stack.split(';').collect();
                let function = frames.last().unwrap_or(&"unknown").to_string();
                HotSpot {
                    function,
                    count: *count,
                    percentage: (*count as f64 / samples.len().max(1) as f64) * 100.0,
                    stack_trace: frames.iter().map(|s| s.to_string()).collect(),
                }
            })
            .collect();

        hot_spots.sort_by(|a, b| b.count.cmp(&a.count));
        hot_spots.truncate(50); // Top 50 hot spots

        ProfileData {
            session_id: self.session_id.clone(),
            profile_type: self.profile_type.clone(),
            started_at: self.started_at,
            duration_ms,
            sample_count: self.sample_count.load(Ordering::Relaxed),
            hot_spots,
            flame_graph_data: generate_flame_graph(&stack_counts),
            memory_timeline: memory_snapshots.clone(),
            summary: ProfileSummary {
                total_samples: samples.len() as u64,
                avg_duration_us: samples
                    .iter()
                    .map(|s| s.duration_us)
                    .sum::<u64>()
                    .checked_div(samples.len() as u64)
                    .unwrap_or(0),
                max_duration_us: samples.iter().map(|s| s.duration_us).max().unwrap_or(0),
                min_duration_us: samples.iter().map(|s| s.duration_us).min().unwrap_or(0),
            },
        }
    }
}

/// Type of profiling
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProfileType {
    /// CPU profiling
    Cpu,
    /// Memory profiling
    Memory,
    /// Both CPU and memory
    Full,
    /// Custom profile type
    Custom(String),
}

/// A single profile sample
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ProfileSample {
    /// Timestamp
    pub timestamp_us: u64,
    /// Operation name
    pub operation: String,
    /// Duration in microseconds
    pub duration_us: u64,
    /// Stack trace (if captured)
    pub stack_trace: Option<Vec<String>>,
    /// Memory allocated (bytes)
    pub memory_bytes: Option<u64>,
    /// Additional metadata
    pub metadata: HashMap<String, String>,
}

impl ProfileSample {
    /// Create a new CPU sample
    pub fn cpu(operation: impl Into<String>, duration_us: u64) -> Self {
        Self {
            timestamp_us: current_timestamp_us(),
            operation: operation.into(),
            duration_us,
            stack_trace: None,
            memory_bytes: None,
            metadata: HashMap::new(),
        }
    }

    /// Create a new memory sample
    pub fn memory(operation: impl Into<String>, memory_bytes: u64) -> Self {
        Self {
            timestamp_us: current_timestamp_us(),
            operation: operation.into(),
            duration_us: 0,
            stack_trace: None,
            memory_bytes: Some(memory_bytes),
            metadata: HashMap::new(),
        }
    }

    /// Add stack trace
    pub fn with_stack(mut self, stack: Vec<String>) -> Self {
        self.stack_trace = Some(stack);
        self
    }

    /// Add metadata
    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }
}

/// Memory snapshot for timeline
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MemorySnapshot {
    /// Timestamp
    pub timestamp_ms: u64,
    /// Heap used bytes
    pub heap_used: u64,
    /// Heap total bytes
    pub heap_total: u64,
    /// RSS bytes
    pub rss: u64,
    /// Number of allocations
    pub allocations: u64,
}

/// Hot spot in the profile
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HotSpot {
    /// Function name
    pub function: String,
    /// Sample count
    pub count: u64,
    /// Percentage of total samples
    pub percentage: f64,
    /// Full stack trace
    pub stack_trace: Vec<String>,
}

/// Profile data summary
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ProfileSummary {
    /// Total samples collected
    pub total_samples: u64,
    /// Average duration
    pub avg_duration_us: u64,
    /// Maximum duration
    pub max_duration_us: u64,
    /// Minimum duration
    pub min_duration_us: u64,
}

/// Complete profile data
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ProfileData {
    /// Session identifier
    pub session_id: String,
    /// Profile type
    pub profile_type: ProfileType,
    /// Start timestamp
    pub started_at: u64,
    /// Duration in milliseconds
    pub duration_ms: u64,
    /// Total samples collected
    pub sample_count: u64,
    /// Hot spots (top functions)
    pub hot_spots: Vec<HotSpot>,
    /// Flame graph data (folded format)
    pub flame_graph_data: String,
    /// Memory timeline
    pub memory_timeline: Vec<MemorySnapshot>,
    /// Summary statistics
    pub summary: ProfileSummary,
}

impl ProfileData {
    /// Generate a simple text report
    pub fn to_report(&self) -> String {
        let mut report = String::new();

        report.push_str(&format!("Profile Report: {}\n", self.session_id));
        report.push_str(&format!("Type: {:?}\n", self.profile_type));
        report.push_str(&format!("Duration: {}ms\n", self.duration_ms));
        report.push_str(&format!("Samples: {}\n", self.sample_count));
        report.push_str("\nTop Hot Spots:\n");

        for (i, hs) in self.hot_spots.iter().take(10).enumerate() {
            report.push_str(&format!(
                "  {}. {} - {} samples ({:.1}%)\n",
                i + 1,
                hs.function,
                hs.count,
                hs.percentage
            ));
        }

        report.push_str("\nSummary:\n");
        report.push_str(&format!(
            "  Avg duration: {}μs\n",
            self.summary.avg_duration_us
        ));
        report.push_str(&format!(
            "  Max duration: {}μs\n",
            self.summary.max_duration_us
        ));
        report.push_str(&format!(
            "  Min duration: {}μs\n",
            self.summary.min_duration_us
        ));

        report
    }
}

/// Generate flame graph data in folded format
fn generate_flame_graph(stack_counts: &HashMap<String, u64>) -> String {
    let mut lines: Vec<String> = stack_counts
        .iter()
        .map(|(stack, count)| format!("{} {}", stack, count))
        .collect();
    lines.sort();
    lines.join("\n")
}

/// Get current timestamp in milliseconds
fn current_timestamp_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Get current timestamp in microseconds
fn current_timestamp_us() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_profile_sample_cpu() {
        let sample = ProfileSample::cpu("GET", 100);
        assert_eq!(sample.operation, "GET");
        assert_eq!(sample.duration_us, 100);
    }

    #[test]
    fn test_profile_sample_memory() {
        let sample = ProfileSample::memory("allocate", 1024);
        assert_eq!(sample.memory_bytes, Some(1024));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_profiler_creation() {
        let profiler = Profiler::new(0.1, 300);
        assert_eq!(profiler.active_sessions(), 0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_profiling_session() {
        let session = ProfilingSession::new(
            "test".to_string(),
            ProfileType::Cpu,
            1.0, // 100% sampling for test
            300,
        );

        session.start();
        assert!(session.is_active());

        session.record(ProfileSample::cpu("test_op", 50)).await;

        let data = session.get_data().await;
        assert_eq!(data.sample_count, 1);
    }

    #[test]
    fn test_flame_graph_generation() {
        let mut stack_counts = HashMap::new();
        stack_counts.insert("main;foo;bar".to_string(), 10);
        stack_counts.insert("main;foo;baz".to_string(), 5);

        let fg = generate_flame_graph(&stack_counts);
        assert!(fg.contains("main;foo;bar 10"));
        assert!(fg.contains("main;foo;baz 5"));
    }
}
