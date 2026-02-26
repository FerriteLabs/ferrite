//! Built-in Observability Platform
//!
//! First-class observability with query tracing, slow query analysis,
//! and performance recommendations without external tooling.
//!
//! # Features
//!
//! - Session-based query tracing
//! - Slow query analysis with recommendations
//! - Live profiling for CPU and memory
//! - Built-in flame graph generation
//! - Query cost estimation
//!
//! # Example
//!
//! ```ignore
//! // Enable tracing for a session
//! TRACE ON
//!
//! // Run commands...
//! SET foo bar
//! GET foo
//!
//! // Analyze the session
//! TRACE ANALYZE
//!
//! // Live profiling
//! PROFILE START
//! // ... do work ...
//! PROFILE STOP
//! ```

#![allow(dead_code)]
pub mod advisor;
pub mod analyzer;
pub mod anomaly;
pub mod chaos;
/// Chaos engineering experiment runner with fault injection and safety validation.
pub mod chaos_engine;
pub mod correlation;
/// Structured logging, latency tracking, and crash diagnostics.
pub mod diagnostics;
pub mod distributed_trace;
/// eBPF zero-overhead kernel-level tracing and profiling.
pub mod ebpf;
/// Linux-native eBPF tracer using ftrace/perf_event_open (no aya-rs required).
#[cfg(target_os = "linux")]
pub mod ebpf_linux;
pub mod heatmap;
pub mod profiler;
pub mod recommendations;
pub mod slow_query_log;
pub mod trace;

pub use analyzer::{QueryAnalysis, QueryAnalyzer, SlowQuery};
pub use anomaly::{
    Alert, AlertSeverity, AnomalyConfig, AnomalyDetector, BaselineStats, MetricName,
};
pub use distributed_trace::{DistributedTraceContext, Span, SpanBuilder, TraceContextPropagator};
pub use heatmap::{
    HeatmapData, LatencyHeatmap, LatencySummary, TierDistributionSnapshot, TierSnapshot,
    TierStatsCollector,
};
pub use profiler::{ProfileData, ProfileType, Profiler, ProfilingSession};
pub use recommendations::{Recommendation, RecommendationEngine, RecommendationType, Severity};
pub use slow_query_log::{SlowQueryEntry, SlowQueryLog};
pub use trace::{SpanContext, TraceEvent, TraceSession, Tracer};

use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

/// Configuration for observability features
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ObservabilityConfig {
    /// Enable tracing
    pub tracing_enabled: bool,
    /// Enable profiling
    pub profiling_enabled: bool,
    /// Slow query threshold in microseconds
    pub slow_query_threshold_us: u64,
    /// Maximum trace events to keep per session
    pub max_trace_events: usize,
    /// Maximum sessions to keep
    pub max_sessions: usize,
    /// Auto-cleanup interval in seconds
    pub cleanup_interval_secs: u64,
    /// Enable AI-powered recommendations
    pub recommendations_enabled: bool,
    /// Profile sampling rate (0.0 to 1.0)
    pub profile_sample_rate: f64,
    /// Maximum profile duration in seconds
    pub max_profile_duration_secs: u64,
}

impl Default for ObservabilityConfig {
    fn default() -> Self {
        Self {
            tracing_enabled: true,
            profiling_enabled: true,
            slow_query_threshold_us: 10_000, // 10ms
            max_trace_events: 10_000,
            max_sessions: 1_000,
            cleanup_interval_secs: 300, // 5 minutes
            recommendations_enabled: true,
            profile_sample_rate: 0.01,      // 1% sampling
            max_profile_duration_secs: 300, // 5 minutes max
        }
    }
}

/// Observability manager that coordinates tracing, profiling, and analysis
pub struct ObservabilityManager {
    config: ObservabilityConfig,
    tracer: Arc<Tracer>,
    profiler: Arc<Profiler>,
    analyzer: Arc<QueryAnalyzer>,
    recommendation_engine: Arc<RecommendationEngine>,
    stats: ObservabilityStats,
    enabled: AtomicBool,
}

impl ObservabilityManager {
    /// Create a new observability manager
    pub fn new(config: ObservabilityConfig) -> Self {
        let analyzer = Arc::new(QueryAnalyzer::new(config.slow_query_threshold_us));
        let recommendation_engine = Arc::new(RecommendationEngine::new());

        Self {
            tracer: Arc::new(Tracer::new(config.max_trace_events, config.max_sessions)),
            profiler: Arc::new(Profiler::new(
                config.profile_sample_rate,
                config.max_profile_duration_secs,
            )),
            analyzer,
            recommendation_engine,
            enabled: AtomicBool::new(config.tracing_enabled),
            stats: ObservabilityStats::default(),
            config,
        }
    }

    /// Enable/disable observability
    pub fn set_enabled(&self, enabled: bool) {
        self.enabled.store(enabled, Ordering::Relaxed);
    }

    /// Check if observability is enabled
    pub fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }

    /// Start a new trace session for a connection
    pub fn start_trace(&self, connection_id: u64) -> Option<String> {
        if !self.is_enabled() {
            return None;
        }

        let session_id = self.tracer.start_session(connection_id);
        self.stats.traces_started.fetch_add(1, Ordering::Relaxed);
        Some(session_id)
    }

    /// Stop a trace session
    pub fn stop_trace(&self, session_id: &str) {
        self.tracer.stop_session(session_id);
        self.stats.traces_completed.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a trace event
    pub fn record_event(&self, session_id: &str, event: TraceEvent) {
        self.tracer.record_event(session_id, event.clone());

        // Check if this is a slow query
        if event.duration_us > self.config.slow_query_threshold_us {
            self.stats.slow_queries.fetch_add(1, Ordering::Relaxed);
            self.analyzer.record_slow_query(&event);
        }

        self.stats.events_recorded.fetch_add(1, Ordering::Relaxed);
    }

    /// Analyze a trace session
    pub async fn analyze_session(&self, session_id: &str) -> Option<SessionAnalysis> {
        let session = self.tracer.get_session(session_id)?;
        let events = session.events.read().await;

        let analysis = self.analyzer.analyze(&events);
        let recommendations = self.recommendation_engine.generate(&analysis);

        Some(SessionAnalysis {
            session_id: session_id.to_string(),
            connection_id: session.connection_id,
            started_at: session.started_at,
            duration_us: session.duration_us(),
            event_count: events.len(),
            analysis,
            recommendations,
        })
    }

    /// Start profiling
    pub fn start_profiling(&self, profile_type: ProfileType) -> Option<String> {
        if !self.config.profiling_enabled {
            return None;
        }

        let session_id = self.profiler.start(profile_type);
        self.stats.profiles_started.fetch_add(1, Ordering::Relaxed);
        Some(session_id)
    }

    /// Stop profiling and get results
    pub fn stop_profiling(&self, session_id: &str) -> Option<ProfileData> {
        let data = self.profiler.stop(session_id)?;
        self.stats
            .profiles_completed
            .fetch_add(1, Ordering::Relaxed);
        Some(data)
    }

    /// Get live statistics
    pub fn live_stats(&self) -> LiveStats {
        LiveStats {
            traces_active: self.tracer.active_sessions(),
            profiles_active: self.profiler.active_sessions(),
            slow_queries_count: self.stats.slow_queries.load(Ordering::Relaxed),
            total_events: self.stats.events_recorded.load(Ordering::Relaxed),
        }
    }

    /// Get the tracer
    pub fn tracer(&self) -> &Arc<Tracer> {
        &self.tracer
    }

    /// Get the profiler
    pub fn profiler(&self) -> &Arc<Profiler> {
        &self.profiler
    }

    /// Get the analyzer
    pub fn analyzer(&self) -> &Arc<QueryAnalyzer> {
        &self.analyzer
    }

    /// Get configuration
    pub fn config(&self) -> &ObservabilityConfig {
        &self.config
    }

    /// Get observability statistics
    pub fn stats(&self) -> ObservabilityStatsSnapshot {
        ObservabilityStatsSnapshot {
            traces_started: self.stats.traces_started.load(Ordering::Relaxed),
            traces_completed: self.stats.traces_completed.load(Ordering::Relaxed),
            profiles_started: self.stats.profiles_started.load(Ordering::Relaxed),
            profiles_completed: self.stats.profiles_completed.load(Ordering::Relaxed),
            slow_queries: self.stats.slow_queries.load(Ordering::Relaxed),
            events_recorded: self.stats.events_recorded.load(Ordering::Relaxed),
        }
    }
}

impl Default for ObservabilityManager {
    fn default() -> Self {
        Self::new(ObservabilityConfig::default())
    }
}

/// Internal statistics tracking
#[derive(Default)]
struct ObservabilityStats {
    traces_started: AtomicU64,
    traces_completed: AtomicU64,
    profiles_started: AtomicU64,
    profiles_completed: AtomicU64,
    slow_queries: AtomicU64,
    events_recorded: AtomicU64,
}

/// Snapshot of observability statistics
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ObservabilityStatsSnapshot {
    /// Total trace sessions started
    pub traces_started: u64,
    /// Total trace sessions completed
    pub traces_completed: u64,
    /// Total profiling sessions started
    pub profiles_started: u64,
    /// Total profiling sessions completed
    pub profiles_completed: u64,
    /// Total slow queries detected
    pub slow_queries: u64,
    /// Total events recorded
    pub events_recorded: u64,
}

/// Live statistics
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LiveStats {
    /// Active trace sessions
    pub traces_active: usize,
    /// Active profiling sessions
    pub profiles_active: usize,
    /// Slow queries detected
    pub slow_queries_count: u64,
    /// Total events recorded
    pub total_events: u64,
}

/// Complete analysis of a trace session
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SessionAnalysis {
    /// Session identifier
    pub session_id: String,
    /// Connection that owned the session
    pub connection_id: u64,
    /// When the session started (Unix timestamp ms)
    pub started_at: u64,
    /// Total duration in microseconds
    pub duration_us: u64,
    /// Number of events in the session
    pub event_count: usize,
    /// Query analysis
    pub analysis: QueryAnalysis,
    /// Recommendations
    pub recommendations: Vec<Recommendation>,
}

/// Observability errors
#[derive(Debug, Clone, thiserror::Error)]
pub enum ObservabilityError {
    /// Session not found
    #[error("session not found: {0}")]
    SessionNotFound(String),

    /// Profiling not available
    #[error("profiling not available: {0}")]
    ProfilingNotAvailable(String),

    /// Limit exceeded
    #[error("limit exceeded: {0}")]
    LimitExceeded(String),

    /// Internal error
    #[error("internal error: {0}")]
    Internal(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_observability_config_default() {
        let config = ObservabilityConfig::default();
        assert!(config.tracing_enabled);
        assert!(config.profiling_enabled);
        assert_eq!(config.slow_query_threshold_us, 10_000);
    }

    #[test]
    fn test_observability_manager_creation() {
        let manager = ObservabilityManager::default();
        assert!(manager.is_enabled());
    }

    #[test]
    fn test_enable_disable() {
        let manager = ObservabilityManager::default();
        assert!(manager.is_enabled());

        manager.set_enabled(false);
        assert!(!manager.is_enabled());

        manager.set_enabled(true);
        assert!(manager.is_enabled());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_start_trace() {
        let manager = ObservabilityManager::default();
        let session_id = manager.start_trace(1).unwrap();
        assert!(!session_id.is_empty());

        let stats = manager.stats();
        assert_eq!(stats.traces_started, 1);
    }
}
