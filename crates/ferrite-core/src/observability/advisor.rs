//! AI Advisor for Observability
//!
//! Analyzes slow query patterns, memory hotspots, and access patterns to
//! generate actionable recommendations. Uses rule-based heuristics with
//! confidence scoring to surface only high-value suggestions.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

/// Configuration for the AI advisor
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdvisorConfig {
    /// Enable the advisor
    pub enabled: bool,
    /// Minimum confidence to surface a recommendation (0.0-1.0)
    pub min_confidence: f64,
    /// Maximum recommendations per analysis
    pub max_recommendations: usize,
    /// How often to run background analysis
    pub analysis_interval: Duration,
    /// Slow query threshold (microseconds)
    pub slow_query_threshold_us: u64,
    /// Memory hotspot threshold (% of total memory)
    pub hotspot_threshold_pct: f64,
    /// Key scan warning threshold
    pub scan_warning_threshold: usize,
}

impl Default for AdvisorConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            min_confidence: 0.7,
            max_recommendations: 10,
            analysis_interval: Duration::from_secs(60),
            slow_query_threshold_us: 10_000,
            hotspot_threshold_pct: 10.0,
            scan_warning_threshold: 10_000,
        }
    }
}

/// A hotspot detected in the system
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Hotspot {
    /// Key pattern or prefix
    pub pattern: String,
    /// Type of hotspot
    pub hotspot_type: HotspotType,
    /// Percentage of resource consumed
    pub resource_pct: f64,
    /// Number of keys matching this pattern
    pub key_count: usize,
    /// Memory consumed (bytes)
    pub memory_bytes: u64,
    /// Operations per second
    pub ops_per_sec: f64,
}

/// Types of resource hotspots
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum HotspotType {
    /// Memory consumption hotspot
    Memory,
    /// CPU/operations hotspot
    Operations,
    /// Network bandwidth hotspot
    Network,
    /// Key count concentration
    KeyCount,
}

/// An EXPLAIN-like plan for a query or command
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryPlan {
    /// Command being analyzed
    pub command: String,
    /// Estimated cost (relative units)
    pub estimated_cost: f64,
    /// Plan steps
    pub steps: Vec<PlanStep>,
    /// Warnings about potential issues
    pub warnings: Vec<String>,
    /// Optimization suggestions
    pub suggestions: Vec<String>,
}

/// A single step in a query plan
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanStep {
    /// Step description
    pub description: String,
    /// Estimated rows/keys affected
    pub estimated_keys: usize,
    /// Estimated cost (relative)
    pub cost: f64,
    /// Index used (if any)
    pub index_used: Option<String>,
}

/// An advisor recommendation with confidence scoring
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdvisorRecommendation {
    /// Unique recommendation ID
    pub id: String,
    /// Category
    pub category: RecommendationCategory,
    /// Short title
    pub title: String,
    /// Detailed description with rationale
    pub description: String,
    /// Expected impact (e.g., "reduce P99 by 30%")
    pub expected_impact: String,
    /// Confidence score (0.0-1.0)
    pub confidence: f64,
    /// Priority (lower = more important)
    pub priority: u32,
    /// Actionable command to fix the issue (if applicable)
    pub fix_command: Option<String>,
}

/// Categories of recommendations
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RecommendationCategory {
    /// Create or modify an index
    Indexing,
    /// Change eviction policy
    EvictionPolicy,
    /// Optimize data structure usage
    DataStructure,
    /// Configuration tuning
    Configuration,
    /// Query optimization
    QueryOptimization,
    /// Memory optimization
    MemoryOptimization,
    /// Connection management
    ConnectionManagement,
    /// Tiering recommendation
    Tiering,
}

/// Input data for advisor analysis
#[derive(Debug, Clone, Default)]
pub struct AdvisorInput {
    /// Key patterns and their frequencies
    pub key_patterns: HashMap<String, PatternInfo>,
    /// Slow queries observed
    pub slow_queries: Vec<SlowQueryInfo>,
    /// Current memory usage by prefix
    pub memory_by_prefix: HashMap<String, u64>,
    /// Total memory used
    pub total_memory_bytes: u64,
    /// Total number of keys
    pub total_keys: usize,
    /// Commands per second by type
    pub commands_per_sec: HashMap<String, f64>,
    /// Current eviction policy
    pub eviction_policy: String,
    /// Active connections
    pub active_connections: u32,
    /// Max connections configured
    pub max_connections: u32,
}

/// Info about a key pattern
#[derive(Debug, Clone, Default)]
pub struct PatternInfo {
    /// Number of keys matching this pattern.
    pub count: usize,
    /// Total memory used by keys matching this pattern.
    pub total_memory: u64,
    /// Operations per second for this pattern.
    pub ops_per_sec: f64,
    /// Average size of values in bytes.
    pub avg_size: u64,
}

/// Info about a slow query
#[derive(Debug, Clone)]
pub struct SlowQueryInfo {
    /// Command that was slow.
    pub command: String,
    /// Duration of the slow query in microseconds.
    pub duration_us: u64,
    /// Number of keys scanned during the query.
    pub keys_scanned: usize,
    /// Key pattern involved in the query.
    pub key_pattern: String,
}

/// The AI advisor engine
pub struct AiAdvisor {
    config: AdvisorConfig,
    recommendations_generated: AtomicU64,
    last_analysis: RwLock<Option<Instant>>,
    last_recommendations: RwLock<Vec<AdvisorRecommendation>>,
}

impl AiAdvisor {
    /// Create a new AI advisor with the given configuration.
    pub fn new(config: AdvisorConfig) -> Self {
        Self {
            config,
            recommendations_generated: AtomicU64::new(0),
            last_analysis: RwLock::new(None),
            last_recommendations: RwLock::new(Vec::new()),
        }
    }

    /// Run a full analysis and generate recommendations
    pub fn analyze(&self, input: &AdvisorInput) -> Vec<AdvisorRecommendation> {
        let mut recommendations = Vec::new();

        self.check_memory_hotspots(input, &mut recommendations);
        self.check_slow_queries(input, &mut recommendations);
        self.check_scan_patterns(input, &mut recommendations);
        self.check_eviction_policy(input, &mut recommendations);
        self.check_connection_usage(input, &mut recommendations);
        self.check_key_distribution(input, &mut recommendations);

        // Filter by confidence and limit
        recommendations.retain(|r| r.confidence >= self.config.min_confidence);
        recommendations.sort_by(|a, b| a.priority.cmp(&b.priority));
        recommendations.truncate(self.config.max_recommendations);

        self.recommendations_generated
            .fetch_add(recommendations.len() as u64, Ordering::Relaxed);
        *self.last_analysis.write() = Some(Instant::now());
        *self.last_recommendations.write() = recommendations.clone();

        recommendations
    }

    /// Generate an EXPLAIN plan for a command
    pub fn explain(&self, command: &str, key_pattern: &str, total_keys: usize) -> QueryPlan {
        let mut steps = Vec::new();
        let mut warnings = Vec::new();
        let mut suggestions = Vec::new();
        let mut estimated_cost = 1.0;

        let cmd_upper = command.to_uppercase();

        match cmd_upper.as_str() {
            "KEYS" => {
                steps.push(PlanStep {
                    description: "Full key space scan".to_string(),
                    estimated_keys: total_keys,
                    cost: total_keys as f64,
                    index_used: None,
                });
                estimated_cost = total_keys as f64;
                warnings.push(format!(
                    "KEYS scans all {} keys - O(N) complexity",
                    total_keys
                ));
                suggestions.push("Use SCAN with cursor instead of KEYS".to_string());
            }
            "SCAN" => {
                let batch = 10;
                steps.push(PlanStep {
                    description: "Cursor-based incremental scan".to_string(),
                    estimated_keys: batch,
                    cost: batch as f64,
                    index_used: None,
                });
                estimated_cost = batch as f64;
            }
            "GET" | "SET" | "DEL" => {
                steps.push(PlanStep {
                    description: format!("Hash table lookup for '{}'", key_pattern),
                    estimated_keys: 1,
                    cost: 1.0,
                    index_used: Some("primary hash index".to_string()),
                });
                estimated_cost = 1.0;
            }
            "HGETALL" => {
                steps.push(PlanStep {
                    description: "Enumerate all hash fields".to_string(),
                    estimated_keys: 1,
                    cost: 10.0, // Varies with hash size
                    index_used: Some("primary hash index".to_string()),
                });
                estimated_cost = 10.0;
                suggestions.push("Use HMGET for specific fields if possible".to_string());
            }
            _ => {
                steps.push(PlanStep {
                    description: format!("Execute {}", cmd_upper),
                    estimated_keys: 1,
                    cost: 1.0,
                    index_used: None,
                });
            }
        }

        QueryPlan {
            command: cmd_upper,
            estimated_cost,
            steps,
            warnings,
            suggestions,
        }
    }

    /// Get last set of recommendations
    pub fn last_recommendations(&self) -> Vec<AdvisorRecommendation> {
        self.last_recommendations.read().clone()
    }

    /// Total recommendations generated
    pub fn total_generated(&self) -> u64 {
        self.recommendations_generated.load(Ordering::Relaxed)
    }

    // --- Internal rule checks ---

    fn check_memory_hotspots(&self, input: &AdvisorInput, recs: &mut Vec<AdvisorRecommendation>) {
        if input.total_memory_bytes == 0 {
            return;
        }
        for (prefix, &memory) in &input.memory_by_prefix {
            let pct = memory as f64 / input.total_memory_bytes as f64 * 100.0;
            if pct >= self.config.hotspot_threshold_pct {
                recs.push(AdvisorRecommendation {
                    id: format!("mem-hotspot-{}", prefix),
                    category: RecommendationCategory::MemoryOptimization,
                    title: format!("Memory hotspot: '{}' prefix", prefix),
                    description: format!(
                        "Keys matching '{}*' consume {:.1}% of total memory ({} bytes). \
                         Consider eviction policy tuning or tiering these keys to disk.",
                        prefix, pct, memory
                    ),
                    expected_impact: format!("Reduce memory usage by up to {:.0}%", pct * 0.5),
                    confidence: 0.9,
                    priority: 1,
                    fix_command: Some(format!("CONFIG SET tier-policy:{}* ssd", prefix)),
                });
            }
        }
    }

    fn check_slow_queries(&self, input: &AdvisorInput, recs: &mut Vec<AdvisorRecommendation>) {
        let slow_patterns: HashMap<String, Vec<&SlowQueryInfo>> = {
            let mut map: HashMap<String, Vec<&SlowQueryInfo>> = HashMap::new();
            for sq in &input.slow_queries {
                if sq.duration_us >= self.config.slow_query_threshold_us {
                    map.entry(sq.command.clone()).or_default().push(sq);
                }
            }
            map
        };

        for (cmd, queries) in &slow_patterns {
            if queries.len() >= 3 {
                let avg_us: u64 =
                    queries.iter().map(|q| q.duration_us).sum::<u64>() / queries.len() as u64;
                recs.push(AdvisorRecommendation {
                    id: format!("slow-{}", cmd.to_lowercase()),
                    category: RecommendationCategory::QueryOptimization,
                    title: format!("Recurring slow command: {}", cmd),
                    description: format!(
                        "{} has been slow {} times (avg {}us). \
                         Consider optimizing the access pattern or adding an index.",
                        cmd,
                        queries.len(),
                        avg_us
                    ),
                    expected_impact: format!("Reduce {} P99 latency by 50%+", cmd),
                    confidence: 0.85,
                    priority: 2,
                    fix_command: None,
                });
            }
        }
    }

    fn check_scan_patterns(&self, input: &AdvisorInput, recs: &mut Vec<AdvisorRecommendation>) {
        for sq in &input.slow_queries {
            if sq.keys_scanned >= self.config.scan_warning_threshold {
                recs.push(AdvisorRecommendation {
                    id: format!("scan-{}", sq.key_pattern),
                    category: RecommendationCategory::Indexing,
                    title: format!("Large key scan on '{}'", sq.key_pattern),
                    description: format!(
                        "Command {} scanned {} keys matching '{}'. \
                         Consider using SCAN with cursor pagination.",
                        sq.command, sq.keys_scanned, sq.key_pattern
                    ),
                    expected_impact: "Reduce latency spikes from full scans".to_string(),
                    confidence: 0.9,
                    priority: 1,
                    fix_command: Some("Use SCAN instead of KEYS".to_string()),
                });
            }
        }
    }

    fn check_eviction_policy(&self, input: &AdvisorInput, recs: &mut Vec<AdvisorRecommendation>) {
        let total_ops: f64 = input.commands_per_sec.values().sum();
        let read_ops: f64 = ["GET", "HGET", "MGET", "LRANGE", "SMEMBERS"]
            .iter()
            .filter_map(|cmd| input.commands_per_sec.get(*cmd))
            .sum();

        if total_ops > 0.0 && read_ops / total_ops >= 0.8 && input.eviction_policy != "allkeys-lfu"
        {
            recs.push(AdvisorRecommendation {
                id: "eviction-lfu".to_string(),
                category: RecommendationCategory::EvictionPolicy,
                title: "Consider LFU eviction for read-heavy workload".to_string(),
                description: format!(
                    "Workload is {:.0}% reads. LFU eviction keeps frequently-accessed keys \
                     in memory better than LRU for this pattern.",
                    read_ops / total_ops * 100.0
                ),
                expected_impact: "Improve cache hit rate by 5-15%".to_string(),
                confidence: 0.75,
                priority: 3,
                fix_command: Some("CONFIG SET maxmemory-policy allkeys-lfu".to_string()),
            });
        }
    }

    fn check_connection_usage(&self, input: &AdvisorInput, recs: &mut Vec<AdvisorRecommendation>) {
        if input.max_connections > 0 {
            let usage_pct = input.active_connections as f64 / input.max_connections as f64 * 100.0;
            if usage_pct > 80.0 {
                recs.push(AdvisorRecommendation {
                    id: "conn-limit".to_string(),
                    category: RecommendationCategory::ConnectionManagement,
                    title: "Connection limit approaching".to_string(),
                    description: format!(
                        "Using {}/{} connections ({:.0}%). Consider increasing max_connections \
                         or implementing connection pooling.",
                        input.active_connections, input.max_connections, usage_pct
                    ),
                    expected_impact: "Prevent connection refused errors".to_string(),
                    confidence: 0.95,
                    priority: 1,
                    fix_command: Some(format!(
                        "CONFIG SET maxclients {}",
                        input.max_connections * 2
                    )),
                });
            }
        }
    }

    fn check_key_distribution(&self, input: &AdvisorInput, recs: &mut Vec<AdvisorRecommendation>) {
        if input.key_patterns.len() >= 2 {
            let sizes: Vec<usize> = input.key_patterns.values().map(|p| p.count).collect();
            let max = *sizes.iter().max().unwrap_or(&0) as f64;
            let avg = sizes.iter().sum::<usize>() as f64 / sizes.len() as f64;
            if avg > 0.0 && max / avg > 10.0 {
                let hot_pattern = input
                    .key_patterns
                    .iter()
                    .max_by_key(|(_, v)| v.count)
                    .map(|(k, _)| k.clone())
                    .unwrap_or_default();

                recs.push(AdvisorRecommendation {
                    id: "key-skew".to_string(),
                    category: RecommendationCategory::DataStructure,
                    title: "Key distribution skew detected".to_string(),
                    description: format!(
                        "Pattern '{}' has significantly more keys than others. \
                         This may cause uneven memory distribution in cluster mode.",
                        hot_pattern
                    ),
                    expected_impact: "Improve cluster balance".to_string(),
                    confidence: 0.7,
                    priority: 4,
                    fix_command: None,
                });
            }
        }
    }
}

impl Default for AiAdvisor {
    fn default() -> Self {
        Self::new(AdvisorConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_input() -> AdvisorInput {
        let mut input = AdvisorInput::default();
        input.total_memory_bytes = 1_000_000;
        input.total_keys = 10_000;
        input.eviction_policy = "allkeys-lru".to_string();
        input.max_connections = 100;
        input.active_connections = 50;

        input
            .memory_by_prefix
            .insert("session:".to_string(), 500_000);
        input.memory_by_prefix.insert("cache:".to_string(), 300_000);

        input.commands_per_sec.insert("GET".to_string(), 8000.0);
        input.commands_per_sec.insert("SET".to_string(), 2000.0);

        input
    }

    #[test]
    fn test_default_config() {
        let config = AdvisorConfig::default();
        assert!(config.enabled);
        assert!(config.min_confidence > 0.0);
    }

    #[test]
    fn test_analyze_memory_hotspot() {
        let advisor = AiAdvisor::default();
        let input = sample_input();
        let recs = advisor.analyze(&input);

        // session: prefix uses 50% of memory — should trigger hotspot
        let hotspot = recs
            .iter()
            .find(|r| r.category == RecommendationCategory::MemoryOptimization);
        assert!(hotspot.is_some(), "should detect memory hotspot");
    }

    #[test]
    fn test_analyze_eviction_policy() {
        let advisor = AiAdvisor::default();
        let input = sample_input();
        let recs = advisor.analyze(&input);

        // 80% reads should suggest LFU
        let eviction = recs
            .iter()
            .find(|r| r.category == RecommendationCategory::EvictionPolicy);
        assert!(
            eviction.is_some(),
            "should suggest LFU for read-heavy workload"
        );
    }

    #[test]
    fn test_analyze_slow_queries() {
        let advisor = AiAdvisor::default();
        let mut input = sample_input();

        for _ in 0..5 {
            input.slow_queries.push(SlowQueryInfo {
                command: "KEYS".to_string(),
                duration_us: 50_000,
                keys_scanned: 50_000,
                key_pattern: "*".to_string(),
            });
        }

        let recs = advisor.analyze(&input);
        let slow = recs
            .iter()
            .find(|r| r.category == RecommendationCategory::QueryOptimization);
        assert!(slow.is_some(), "should detect recurring slow command");
    }

    #[test]
    fn test_analyze_connection_limit() {
        let advisor = AiAdvisor::default();
        let mut input = sample_input();
        input.active_connections = 90;
        input.max_connections = 100;

        let recs = advisor.analyze(&input);
        let conn = recs
            .iter()
            .find(|r| r.category == RecommendationCategory::ConnectionManagement);
        assert!(conn.is_some(), "should warn about connection limit");
    }

    #[test]
    fn test_explain_keys() {
        let advisor = AiAdvisor::default();
        let plan = advisor.explain("KEYS", "*", 100_000);

        assert_eq!(plan.command, "KEYS");
        assert!(plan.estimated_cost > 1.0);
        assert!(!plan.warnings.is_empty());
        assert!(!plan.suggestions.is_empty());
    }

    #[test]
    fn test_explain_get() {
        let advisor = AiAdvisor::default();
        let plan = advisor.explain("GET", "mykey", 100_000);

        assert_eq!(plan.command, "GET");
        assert_eq!(plan.estimated_cost, 1.0);
        assert!(plan.steps[0].index_used.is_some());
    }

    #[test]
    fn test_min_confidence_filtering() {
        let advisor = AiAdvisor::new(AdvisorConfig {
            min_confidence: 0.99,
            ..Default::default()
        });
        let input = sample_input();
        let recs = advisor.analyze(&input);
        // All recommendations should meet the high threshold
        for rec in &recs {
            assert!(rec.confidence >= 0.99);
        }
    }

    #[test]
    fn test_max_recommendations() {
        let advisor = AiAdvisor::new(AdvisorConfig {
            max_recommendations: 2,
            min_confidence: 0.0,
            ..Default::default()
        });
        let mut input = sample_input();
        input.active_connections = 90;
        for _ in 0..5 {
            input.slow_queries.push(SlowQueryInfo {
                command: "KEYS".to_string(),
                duration_us: 50_000,
                keys_scanned: 50_000,
                key_pattern: "*".to_string(),
            });
        }

        let recs = advisor.analyze(&input);
        assert!(recs.len() <= 2);
    }

    #[test]
    fn test_total_generated() {
        let advisor = AiAdvisor::default();
        let input = sample_input();
        advisor.analyze(&input);
        assert!(advisor.total_generated() > 0);
    }
}
