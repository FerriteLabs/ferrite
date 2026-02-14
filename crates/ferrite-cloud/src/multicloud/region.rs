//! Cloud region definitions

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Region status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RegionStatus {
    /// Region is active and accepting traffic
    Active,
    /// Region is in standby mode
    Standby,
    /// Region is draining (no new connections)
    Draining,
    /// Region is offline
    Offline,
    /// Region is being provisioned
    Provisioning,
}

impl RegionStatus {
    /// Get string representation
    pub fn as_str(&self) -> &'static str {
        match self {
            RegionStatus::Active => "active",
            RegionStatus::Standby => "standby",
            RegionStatus::Draining => "draining",
            RegionStatus::Offline => "offline",
            RegionStatus::Provisioning => "provisioning",
        }
    }

    /// Check if region can accept traffic
    pub fn can_accept_traffic(&self) -> bool {
        matches!(self, RegionStatus::Active)
    }

    /// Check if region can accept writes
    pub fn can_accept_writes(&self) -> bool {
        matches!(self, RegionStatus::Active)
    }
}

/// Region configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegionConfig {
    /// Region identifier
    pub region_id: String,
    /// Provider name
    pub provider: String,
    /// Endpoint URL
    pub endpoint: String,
    /// Priority (lower = higher priority)
    pub priority: u32,
    /// Weight for load balancing
    pub weight: u32,
    /// Maximum connections
    pub max_connections: u32,
    /// Connection timeout in milliseconds
    pub connection_timeout_ms: u64,
    /// Read timeout in milliseconds
    pub read_timeout_ms: u64,
    /// Write timeout in milliseconds
    pub write_timeout_ms: u64,
}

impl Default for RegionConfig {
    fn default() -> Self {
        Self {
            region_id: String::new(),
            provider: String::new(),
            endpoint: String::new(),
            priority: 100,
            weight: 100,
            max_connections: 1000,
            connection_timeout_ms: 5000,
            read_timeout_ms: 30000,
            write_timeout_ms: 30000,
        }
    }
}

impl RegionConfig {
    /// Create a new region config
    pub fn new(region_id: impl Into<String>, provider: impl Into<String>) -> Self {
        Self {
            region_id: region_id.into(),
            provider: provider.into(),
            ..Default::default()
        }
    }

    /// Set endpoint
    pub fn with_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoint = endpoint.into();
        self
    }

    /// Set priority
    pub fn with_priority(mut self, priority: u32) -> Self {
        self.priority = priority;
        self
    }

    /// Set weight
    pub fn with_weight(mut self, weight: u32) -> Self {
        self.weight = weight;
        self
    }
}

/// A cloud region
#[derive(Debug, Clone)]
pub struct CloudRegion {
    name: String,
    provider: String,
    config: RegionConfig,
    status: RegionStatus,
    metadata: HashMap<String, String>,
    latency_ms: Option<f64>,
}

impl CloudRegion {
    /// Create a new cloud region
    pub fn new(name: impl Into<String>, config: RegionConfig) -> Self {
        let name = name.into();
        let provider = config.provider.clone();

        Self {
            name,
            provider,
            config,
            status: RegionStatus::Provisioning,
            metadata: HashMap::new(),
            latency_ms: None,
        }
    }

    /// Get region name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get provider name
    pub fn provider(&self) -> &str {
        &self.provider
    }

    /// Get configuration
    pub fn config(&self) -> &RegionConfig {
        &self.config
    }

    /// Get status
    pub fn status(&self) -> RegionStatus {
        self.status
    }

    /// Set status
    pub fn set_status(&mut self, status: RegionStatus) {
        self.status = status;
    }

    /// Get endpoint
    pub fn endpoint(&self) -> &str {
        &self.config.endpoint
    }

    /// Get priority
    pub fn priority(&self) -> u32 {
        self.config.priority
    }

    /// Get weight
    pub fn weight(&self) -> u32 {
        self.config.weight
    }

    /// Get latency
    pub fn latency_ms(&self) -> Option<f64> {
        self.latency_ms
    }

    /// Update latency
    pub fn update_latency(&mut self, latency_ms: f64) {
        self.latency_ms = Some(latency_ms);
    }

    /// Set metadata
    pub fn set_metadata(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.metadata.insert(key.into(), value.into());
    }

    /// Get metadata
    pub fn get_metadata(&self, key: &str) -> Option<&String> {
        self.metadata.get(key)
    }

    /// Check if region is healthy for traffic
    pub fn is_healthy(&self) -> bool {
        self.status.can_accept_traffic()
    }

    /// Activate region
    pub fn activate(&mut self) {
        self.status = RegionStatus::Active;
    }

    /// Deactivate region
    pub fn deactivate(&mut self) {
        self.status = RegionStatus::Offline;
    }

    /// Start draining
    pub fn drain(&mut self) {
        self.status = RegionStatus::Draining;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_region_creation() {
        let config = RegionConfig::new("us-east-1", "aws")
            .with_endpoint("https://ferrite.us-east-1.amazonaws.com")
            .with_priority(1);

        let region = CloudRegion::new("us-east-1", config);
        assert_eq!(region.name(), "us-east-1");
        assert_eq!(region.provider(), "aws");
        assert_eq!(region.priority(), 1);
    }

    #[test]
    fn test_region_status() {
        let config = RegionConfig::new("us-west-2", "aws");
        let mut region = CloudRegion::new("us-west-2", config);

        assert_eq!(region.status(), RegionStatus::Provisioning);
        assert!(!region.is_healthy());

        region.activate();
        assert_eq!(region.status(), RegionStatus::Active);
        assert!(region.is_healthy());

        region.drain();
        assert_eq!(region.status(), RegionStatus::Draining);
        assert!(!region.is_healthy());
    }
}
