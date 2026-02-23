//! Query Analyzer
//!
//! Analyzes query patterns and identifies performance issues.

use super::trace::TraceEvent;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::RwLock;

/// Query analyzer for identifying performance patterns
pub struct QueryAnalyzer {
    slow_query_threshold_us: u64,
    slow_queries: RwLock<Vec<SlowQuery>>,
    pattern_stats: RwLock<HashMap<String, PatternStats>>,
    max_slow_queries: usize,
}

impl QueryAnalyzer {
    /// Create a new query analyzer
    pub fn new(slow_query_threshold_us: u64) -> Self {
        Self {
            slow_query_threshold_us,
            slow_queries: RwLock::new(Vec::new()),
            pattern_stats: RwLock::new(HashMap::new()),
            max_slow_queries: 1000,
        }
    }

    /// Record a slow query
    pub fn record_slow_query(&self, event: &TraceEvent) {
        let slow_query = SlowQuery {
            timestamp_us: event.timestamp_us,
            command: event.command.clone().unwrap_or_default(),
            key_pattern: event.key_pattern.clone(),
            duration_us: event.duration_us,
            attributes: event.attributes.clone(),
        };

        let mut queries = self.slow_queries.write().unwrap_or_else(|e| e.into_inner());
        if queries.len() >= self.max_slow_queries {
            queries.remove(0);
        }
        queries.push(slow_query);

        // Update pattern stats
        if let Some(ref pattern) = event.key_pattern {
            let pattern_key = self.normalize_pattern(pattern);
            let mut stats = self
                .pattern_stats
                .write()
                .unwrap_or_else(|e| e.into_inner());
            let entry = stats.entry(pattern_key).or_default();
            entry.record(event.duration_us);
        }
    }

    /// Analyze a set of trace events
    pub fn analyze(&self, events: &[TraceEvent]) -> QueryAnalysis {
        let mut analysis = QueryAnalysis::default();

        if events.is_empty() {
            return analysis;
        }

        let mut command_stats: HashMap<String, CommandStats> = HashMap::new();
        let mut key_pattern_stats: HashMap<String, PatternStats> = HashMap::new();
        let mut slow_queries = Vec::new();

        for event in events {
            analysis.total_queries += 1;
            analysis.total_duration_us += event.duration_us;

            // Track command statistics
            if let Some(ref cmd) = event.command {
                let stats = command_stats.entry(cmd.clone()).or_default();
                stats.record(event.duration_us);
            }

            // Track key pattern statistics
            if let Some(ref key) = event.key_pattern {
                let pattern = self.normalize_pattern(key);
                let stats = key_pattern_stats.entry(pattern).or_default();
                stats.record(event.duration_us);
            }

            // Track slow queries
            if event.duration_us > self.slow_query_threshold_us {
                analysis.slow_query_count += 1;
                slow_queries.push(SlowQuery {
                    timestamp_us: event.timestamp_us,
                    command: event.command.clone().unwrap_or_default(),
                    key_pattern: event.key_pattern.clone(),
                    duration_us: event.duration_us,
                    attributes: event.attributes.clone(),
                });
            }
        }

        // Calculate averages
        analysis.avg_duration_us = analysis.total_duration_us / analysis.total_queries.max(1);

        // Convert command stats
        analysis.command_breakdown = command_stats
            .into_iter()
            .map(|(cmd, stats)| CommandBreakdown {
                command: cmd,
                count: stats.count,
                total_duration_us: stats.total_duration_us,
                avg_duration_us: stats.avg_duration_us(),
                max_duration_us: stats.max_duration_us,
                min_duration_us: stats.min_duration_us,
            })
            .collect();
        analysis
            .command_breakdown
            .sort_by(|a, b| b.total_duration_us.cmp(&a.total_duration_us));

        // Convert key pattern stats
        analysis.key_pattern_breakdown = key_pattern_stats
            .into_iter()
            .map(|(pattern, stats)| KeyPatternBreakdown {
                pattern,
                count: stats.count,
                total_duration_us: stats.total_duration_us,
                avg_duration_us: stats.avg_duration_us(),
            })
            .collect();
        analysis
            .key_pattern_breakdown
            .sort_by(|a, b| b.count.cmp(&a.count));

        // Add slow queries
        analysis.slow_queries = slow_queries;

        // Detect patterns
        analysis.detected_patterns = self.detect_patterns(events);

        analysis
    }

    /// Normalize a key to a pattern (replace IDs with *)
    fn normalize_pattern(&self, key: &str) -> String {
        // Simple normalization: replace numeric segments with *
        let parts: Vec<&str> = key.split(':').collect();
        let normalized: Vec<String> = parts
            .iter()
            .map(|part| {
                if part.chars().all(|c| c.is_numeric())
                    || (part.len() > 20 && part.chars().all(|c| c.is_alphanumeric()))
                {
                    "*".to_string()
                } else {
                    part.to_string()
                }
            })
            .collect();
        normalized.join(":")
    }

    /// Detect common patterns and anti-patterns
    fn detect_patterns(&self, events: &[TraceEvent]) -> Vec<DetectedPattern> {
        let mut patterns = Vec::new();

        // Detect N+1 query pattern
        let mut consecutive_similar = 0;
        let mut last_pattern: Option<String> = None;

        for event in events {
            if let Some(ref key) = event.key_pattern {
                let pattern = self.normalize_pattern(key);
                if Some(&pattern) == last_pattern.as_ref() {
                    consecutive_similar += 1;
                } else {
                    if consecutive_similar > 10 {
                        patterns.push(DetectedPattern {
                            pattern_type: PatternType::NPlusOne,
                            description: format!(
                                "N+1 query detected: {} consecutive queries to pattern '{}'",
                                consecutive_similar,
                                last_pattern.as_ref().unwrap_or(&String::new())
                            ),
                            severity: Severity::Warning,
                            affected_keys: vec![last_pattern.clone().unwrap_or_default()],
                            suggestion: "Consider using MGET for batch retrieval".to_string(),
                        });
                    }
                    consecutive_similar = 1;
                    last_pattern = Some(pattern);
                }
            }
        }

        // Detect hot key pattern
        let mut key_counts: HashMap<String, u64> = HashMap::new();
        for event in events {
            if let Some(ref key) = event.key_pattern {
                *key_counts.entry(key.clone()).or_insert(0) += 1;
            }
        }

        let total_events = events.len() as u64;
        for (key, count) in key_counts {
            let percentage = (count as f64 / total_events as f64) * 100.0;
            if percentage > 20.0 && count > 100 {
                patterns.push(DetectedPattern {
                    pattern_type: PatternType::HotKey,
                    description: format!(
                        "Hot key detected: '{}' accessed {} times ({:.1}% of queries)",
                        key, count, percentage
                    ),
                    severity: Severity::Warning,
                    affected_keys: vec![key],
                    suggestion: "Consider caching this key locally or distributing load"
                        .to_string(),
                });
            }
        }

        // Detect large scan pattern
        let mut scan_count = 0;
        for event in events {
            if let Some(ref cmd) = event.command {
                if cmd == "SCAN" || cmd == "KEYS" {
                    scan_count += 1;
                }
            }
        }

        if scan_count > 10 {
            patterns.push(DetectedPattern {
                pattern_type: PatternType::LargeScan,
                description: format!(
                    "Excessive scanning detected: {} SCAN/KEYS operations",
                    scan_count
                ),
                severity: Severity::Warning,
                affected_keys: vec![],
                suggestion: "Consider using more specific key patterns or secondary indexes"
                    .to_string(),
            });
        }

        patterns
    }

    /// Get recent slow queries
    pub fn get_slow_queries(&self, limit: usize) -> Vec<SlowQuery> {
        let queries = self.slow_queries.read().unwrap_or_else(|e| e.into_inner());
        queries.iter().rev().take(limit).cloned().collect()
    }

    /// Get pattern statistics
    pub fn get_pattern_stats(&self) -> Vec<(String, PatternStats)> {
        let stats = self.pattern_stats.read().unwrap_or_else(|e| e.into_inner());
        stats.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
    }

    /// Clear all collected data
    pub fn clear(&self) {
        self.slow_queries
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .clear();
        self.pattern_stats
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .clear();
    }
}

/// Slow query record
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SlowQuery {
    /// Timestamp
    pub timestamp_us: u64,
    /// Command name
    pub command: String,
    /// Key pattern
    pub key_pattern: Option<String>,
    /// Duration in microseconds
    pub duration_us: u64,
    /// Additional attributes
    pub attributes: HashMap<String, String>,
}

/// Query analysis result
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct QueryAnalysis {
    /// Total number of queries
    pub total_queries: u64,
    /// Total duration in microseconds
    pub total_duration_us: u64,
    /// Average duration in microseconds
    pub avg_duration_us: u64,
    /// Number of slow queries
    pub slow_query_count: u64,
    /// Breakdown by command
    pub command_breakdown: Vec<CommandBreakdown>,
    /// Breakdown by key pattern
    pub key_pattern_breakdown: Vec<KeyPatternBreakdown>,
    /// Slow queries
    pub slow_queries: Vec<SlowQuery>,
    /// Detected patterns/anti-patterns
    pub detected_patterns: Vec<DetectedPattern>,
}

/// Command-level statistics
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CommandBreakdown {
    /// Command name
    pub command: String,
    /// Number of executions
    pub count: u64,
    /// Total duration
    pub total_duration_us: u64,
    /// Average duration
    pub avg_duration_us: u64,
    /// Maximum duration
    pub max_duration_us: u64,
    /// Minimum duration
    pub min_duration_us: u64,
}

/// Key pattern statistics
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct KeyPatternBreakdown {
    /// Key pattern
    pub pattern: String,
    /// Number of accesses
    pub count: u64,
    /// Total duration
    pub total_duration_us: u64,
    /// Average duration
    pub avg_duration_us: u64,
}

/// Detected pattern or anti-pattern
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DetectedPattern {
    /// Pattern type
    pub pattern_type: PatternType,
    /// Human-readable description
    pub description: String,
    /// Severity level
    pub severity: Severity,
    /// Affected keys/patterns
    pub affected_keys: Vec<String>,
    /// Suggested fix
    pub suggestion: String,
}

/// Types of detected patterns
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PatternType {
    /// N+1 query pattern
    NPlusOne,
    /// Hot key access pattern
    HotKey,
    /// Large scan operation
    LargeScan,
    /// Missing index
    MissingIndex,
    /// Inefficient data structure
    InefficientDataStructure,
    /// Memory pressure
    MemoryPressure,
    /// Custom pattern
    Custom(String),
}

/// Severity levels
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Severity {
    /// Informational
    Info,
    /// Warning
    Warning,
    /// Critical
    Critical,
}

/// Internal command statistics
#[derive(Clone, Debug, Default)]
struct CommandStats {
    count: u64,
    total_duration_us: u64,
    max_duration_us: u64,
    min_duration_us: u64,
}

impl CommandStats {
    fn new() -> Self {
        Self {
            count: 0,
            total_duration_us: 0,
            max_duration_us: 0,
            min_duration_us: u64::MAX,
        }
    }

    fn record(&mut self, duration_us: u64) {
        self.count += 1;
        self.total_duration_us += duration_us;
        self.max_duration_us = self.max_duration_us.max(duration_us);
        self.min_duration_us = self.min_duration_us.min(duration_us);
    }

    fn avg_duration_us(&self) -> u64 {
        if self.count == 0 {
            0
        } else {
            self.total_duration_us / self.count
        }
    }
}

/// Pattern statistics
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct PatternStats {
    /// Access count
    pub count: u64,
    /// Total duration
    pub total_duration_us: u64,
    /// Maximum duration
    pub max_duration_us: u64,
    /// Minimum duration
    pub min_duration_us: u64,
}

impl PatternStats {
    fn new() -> Self {
        Self {
            count: 0,
            total_duration_us: 0,
            max_duration_us: 0,
            min_duration_us: u64::MAX,
        }
    }

    fn record(&mut self, duration_us: u64) {
        self.count += 1;
        self.total_duration_us += duration_us;
        self.max_duration_us = self.max_duration_us.max(duration_us);
        self.min_duration_us = self.min_duration_us.min(duration_us);
    }

    /// Get average duration
    pub fn avg_duration_us(&self) -> u64 {
        if self.count == 0 {
            0
        } else {
            self.total_duration_us / self.count
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::observability::trace::{EventStatus, EventType};

    fn create_test_event(command: &str, key: &str, duration_us: u64) -> TraceEvent {
        TraceEvent {
            sequence: 0,
            timestamp_us: 0,
            event_type: EventType::Command,
            command: Some(command.to_string()),
            key_pattern: Some(key.to_string()),
            duration_us,
            status: EventStatus::Ok,
            attributes: HashMap::new(),
            span_context: None,
        }
    }

    #[test]
    fn test_analyzer_creation() {
        let analyzer = QueryAnalyzer::new(10_000);
        assert!(analyzer.get_slow_queries(10).is_empty());
    }

    #[test]
    fn test_pattern_normalization() {
        let analyzer = QueryAnalyzer::new(10_000);

        assert_eq!(analyzer.normalize_pattern("user:123"), "user:*");
        assert_eq!(
            analyzer.normalize_pattern("user:123:profile"),
            "user:*:profile"
        );
        assert_eq!(analyzer.normalize_pattern("cache:key"), "cache:key");
    }

    #[test]
    fn test_basic_analysis() {
        let analyzer = QueryAnalyzer::new(10_000);

        let events = vec![
            create_test_event("GET", "user:1", 100),
            create_test_event("GET", "user:2", 150),
            create_test_event("SET", "user:3", 200),
        ];

        let analysis = analyzer.analyze(&events);

        assert_eq!(analysis.total_queries, 3);
        assert_eq!(analysis.command_breakdown.len(), 2);
    }

    #[test]
    fn test_slow_query_detection() {
        let analyzer = QueryAnalyzer::new(100); // 100μs threshold

        let events = vec![
            create_test_event("GET", "user:1", 50),
            create_test_event("GET", "user:2", 200), // Slow
            create_test_event("GET", "user:3", 50),
        ];

        let analysis = analyzer.analyze(&events);

        assert_eq!(analysis.slow_query_count, 1);
        assert_eq!(analysis.slow_queries.len(), 1);
        assert_eq!(analysis.slow_queries[0].duration_us, 200);
    }

    #[test]
    fn test_hot_key_detection() {
        let analyzer = QueryAnalyzer::new(10_000);

        // Create many accesses to the same key
        // Hot key detection requires: percentage > 20% AND count > 100
        let mut events = Vec::new();
        for _ in 0..150 {
            events.push(create_test_event("GET", "hot:key", 100));
        }
        for i in 0..10 {
            events.push(create_test_event("GET", &format!("other:{}", i), 100));
        }

        let analysis = analyzer.analyze(&events);

        let hot_key_patterns: Vec<_> = analysis
            .detected_patterns
            .iter()
            .filter(|p| p.pattern_type == PatternType::HotKey)
            .collect();

        assert!(!hot_key_patterns.is_empty());
    }
}
