//! Routing strategies

use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Routing strategy determining how queries are distributed
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum RoutingStrategy {
    /// Automatic selection based on query analysis and metrics
    #[default]
    Auto,
    /// Round-robin distribution across replicas
    RoundRobin,
    /// Route to target with lowest latency
    LeastLatency,
    /// Route to target with least active connections
    LeastConnections,
    /// Prefer local/nearby nodes
    Locality,
    /// Consistent hash-based routing (for cache affinity)
    ConsistentHash,
    /// Always route to primary
    Primary,
    /// Random selection
    Random,
    /// Weighted distribution
    Weighted,
    /// Custom strategy by name
    Custom(String),
}

impl RoutingStrategy {
    /// Parse strategy from string
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "auto" => Some(RoutingStrategy::Auto),
            "round_robin" | "roundrobin" => Some(RoutingStrategy::RoundRobin),
            "least_latency" | "leastlatency" => Some(RoutingStrategy::LeastLatency),
            "least_connections" | "leastconnections" => Some(RoutingStrategy::LeastConnections),
            "locality" | "local" => Some(RoutingStrategy::Locality),
            "consistent_hash" | "consistenthash" | "consistent" => {
                Some(RoutingStrategy::ConsistentHash)
            }
            "primary" => Some(RoutingStrategy::Primary),
            "random" => Some(RoutingStrategy::Random),
            "weighted" => Some(RoutingStrategy::Weighted),
            _ if s.starts_with("custom:") => Some(RoutingStrategy::Custom(s[7..].to_string())),
            _ => None,
        }
    }

    /// Convert to string representation
    pub fn as_str(&self) -> &str {
        match self {
            RoutingStrategy::Auto => "auto",
            RoutingStrategy::RoundRobin => "round_robin",
            RoutingStrategy::LeastLatency => "least_latency",
            RoutingStrategy::LeastConnections => "least_connections",
            RoutingStrategy::Locality => "locality",
            RoutingStrategy::ConsistentHash => "consistent_hash",
            RoutingStrategy::Primary => "primary",
            RoutingStrategy::Random => "random",
            RoutingStrategy::Weighted => "weighted",
            RoutingStrategy::Custom(_) => "custom",
        }
    }

    /// Check if this strategy can use replicas
    pub fn allows_replicas(&self) -> bool {
        !matches!(self, RoutingStrategy::Primary)
    }

    /// Check if this strategy needs metrics
    pub fn needs_metrics(&self) -> bool {
        matches!(
            self,
            RoutingStrategy::Auto
                | RoutingStrategy::LeastLatency
                | RoutingStrategy::LeastConnections
        )
    }
}

/// Strategy configuration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StrategyConfig {
    /// Primary strategy
    pub strategy: RoutingStrategy,
    /// Fallback strategy if primary fails
    pub fallback: Option<RoutingStrategy>,
    /// For weighted strategy: weights per target
    pub weights: Option<Vec<(String, u32)>>,
    /// Locality configuration
    pub locality: Option<LocalityConfig>,
    /// Consistent hash configuration
    pub consistent_hash: Option<ConsistentHashConfig>,
    /// Health check configuration
    pub health_check: Option<HealthCheckConfig>,
}

impl Default for StrategyConfig {
    fn default() -> Self {
        Self {
            strategy: RoutingStrategy::Auto,
            fallback: Some(RoutingStrategy::Primary),
            weights: None,
            locality: None,
            consistent_hash: None,
            health_check: Some(HealthCheckConfig::default()),
        }
    }
}

/// Locality-based routing configuration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LocalityConfig {
    /// Prefer nodes in same zone
    pub prefer_same_zone: bool,
    /// Prefer nodes in same region
    pub prefer_same_region: bool,
    /// Current zone
    pub current_zone: Option<String>,
    /// Current region
    pub current_region: Option<String>,
    /// Maximum latency before considering remote nodes (ms)
    pub max_local_latency_ms: u64,
}

impl Default for LocalityConfig {
    fn default() -> Self {
        Self {
            prefer_same_zone: true,
            prefer_same_region: true,
            current_zone: None,
            current_region: None,
            max_local_latency_ms: 50,
        }
    }
}

/// Consistent hash configuration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ConsistentHashConfig {
    /// Number of virtual nodes per target
    pub virtual_nodes: u32,
    /// Hash algorithm
    pub algorithm: String,
    /// Replication factor (how many nodes to consider)
    pub replication_factor: u32,
}

impl Default for ConsistentHashConfig {
    fn default() -> Self {
        Self {
            virtual_nodes: 150,
            algorithm: "xxhash".to_string(),
            replication_factor: 1,
        }
    }
}

/// Health check configuration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HealthCheckConfig {
    /// Enable health checks
    pub enabled: bool,
    /// Check interval
    #[serde(with = "humantime_serde")]
    pub interval: Duration,
    /// Timeout for health check
    #[serde(with = "humantime_serde")]
    pub timeout: Duration,
    /// Number of failures before marking unhealthy
    pub unhealthy_threshold: u32,
    /// Number of successes before marking healthy
    pub healthy_threshold: u32,
}

impl Default for HealthCheckConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            interval: Duration::from_secs(5),
            timeout: Duration::from_secs(2),
            unhealthy_threshold: 3,
            healthy_threshold: 2,
        }
    }
}

// humantime_serde for Duration serialization
mod humantime_serde {
    use serde::{Deserialize, Deserializer, Serializer};
    use std::time::Duration;

    pub fn serialize<S>(duration: &Duration, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&format!("{}ms", duration.as_millis()))
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Duration, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        // Parse simple ms format
        if let Some(ms_str) = s.strip_suffix("ms") {
            if let Ok(ms) = ms_str.parse::<u64>() {
                return Ok(Duration::from_millis(ms));
            }
        }
        if let Some(s_str) = s.strip_suffix('s') {
            if let Ok(secs) = s_str.parse::<u64>() {
                return Ok(Duration::from_secs(secs));
            }
        }
        // Default to milliseconds
        if let Ok(ms) = s.parse::<u64>() {
            return Ok(Duration::from_millis(ms));
        }
        Err(serde::de::Error::custom("invalid duration format"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_strategy_from_str() {
        assert_eq!(
            RoutingStrategy::from_str("auto"),
            Some(RoutingStrategy::Auto)
        );
        assert_eq!(
            RoutingStrategy::from_str("round_robin"),
            Some(RoutingStrategy::RoundRobin)
        );
        assert_eq!(
            RoutingStrategy::from_str("least_latency"),
            Some(RoutingStrategy::LeastLatency)
        );
        assert_eq!(
            RoutingStrategy::from_str("custom:my_strategy"),
            Some(RoutingStrategy::Custom("my_strategy".to_string()))
        );
        assert_eq!(RoutingStrategy::from_str("unknown"), None);
    }

    #[test]
    fn test_strategy_properties() {
        assert!(RoutingStrategy::Auto.allows_replicas());
        assert!(!RoutingStrategy::Primary.allows_replicas());

        assert!(RoutingStrategy::LeastLatency.needs_metrics());
        assert!(!RoutingStrategy::RoundRobin.needs_metrics());
    }

    #[test]
    fn test_strategy_config_default() {
        let config = StrategyConfig::default();
        assert_eq!(config.strategy, RoutingStrategy::Auto);
        assert!(config.fallback.is_some());
    }
}
