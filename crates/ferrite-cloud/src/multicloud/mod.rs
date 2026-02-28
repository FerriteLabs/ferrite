//! Multi-Cloud Active-Active Replication
//!
//! Enables active-active deployment across multiple cloud providers
//! with automatic failover and conflict resolution.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                  Multi-Cloud Active-Active                   │
//! ├─────────────────────────────────────────────────────────────┤
//! │  ┌──────────┐   ┌──────────┐   ┌──────────┐   ┌──────────┐ │
//! │  │  Cloud   │   │ Conflict │   │  Health  │   │ Traffic  │ │
//! │  │ Manager  │──▶│ Resolver │──▶│ Monitor  │──▶│ Router   │ │
//! │  └──────────┘   └──────────┘   └──────────┘   └──────────┘ │
//! │       │              │              │              │        │
//! │       ▼              ▼              ▼              ▼        │
//! │  Multi-Region    CRDT-Based    Auto-Failover   Geo-Routing │
//! │   Sync Pool      Merging       Detection       LB Control  │
//! └─────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Features
//!
//! - **Multi-Cloud Support**: AWS, GCP, Azure, and custom providers
//! - **Active-Active**: All regions can accept writes
//! - **Conflict Resolution**: Last-write-wins or custom resolvers
//! - **Health Monitoring**: Automatic health checks and failover
//! - **Traffic Routing**: Geo-aware routing and load balancing

pub mod active_active;
pub mod consistency;
mod health;
pub mod hlc;
mod provider;
mod region;
pub mod replication_protocol;
mod routing;
mod sync;

pub use active_active::{
    ActiveActiveConfig, ActiveActiveManager, CloudProvider as ActiveCloudProvider,
    ConflictEntry, ConflictResolution as ActiveConflictResolution, PeerState, RegionHealth,
    RegionPeer, ReplicationError as ActiveReplicationError, ResolveResult, SyncResult,
    SyncStatus as ActiveSyncStatus, VectorClock,
};
pub use consistency::{ConsistencyChecker, ConsistencyLevel, ConsistencyResult};
pub use health::{HealthConfig, HealthMonitor, HealthStatus};
pub use hlc::{HlcTimestamp, HybridClock};
pub use provider::{CloudProvider, ProviderConfig, ProviderType};
pub use region::{CloudRegion, RegionConfig, RegionStatus};
pub use replication_protocol::{
    ApplyResult, ConflictResolution, ConflictResolver,
    ConflictStrategy as ReplicationConflictStrategy, OpType, ReplicationMessage, ReplicationOp,
    ReplicationProtocol, ReplicationProtocolConfig,
};
pub use routing::{RoutingConfig, RoutingStrategy, TrafficRouter};
pub use sync::{SyncConfig, SyncManager, SyncStatus};

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

/// Multi-cloud configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultiCloudConfig {
    /// This instance's region
    pub local_region: String,
    /// Sync interval in milliseconds
    pub sync_interval_ms: u64,
    /// Health check interval in milliseconds
    pub health_check_interval_ms: u64,
    /// Conflict resolution strategy
    pub conflict_resolution: ConflictStrategy,
    /// Enable automatic failover
    pub auto_failover: bool,
    /// Failover threshold (consecutive failures)
    pub failover_threshold: u32,
    /// Traffic routing strategy
    pub routing_strategy: RoutingStrategy,
}

impl Default for MultiCloudConfig {
    fn default() -> Self {
        Self {
            local_region: "us-east-1".to_string(),
            sync_interval_ms: 100,
            health_check_interval_ms: 5000,
            conflict_resolution: ConflictStrategy::LastWriteWins,
            auto_failover: true,
            failover_threshold: 3,
            routing_strategy: RoutingStrategy::NearestHealthy,
        }
    }
}

impl MultiCloudConfig {
    /// Create a new config
    pub fn new() -> Self {
        Self::default()
    }

    /// Set local region
    pub fn with_local_region(mut self, region: impl Into<String>) -> Self {
        self.local_region = region.into();
        self
    }

    /// Set conflict resolution
    pub fn with_conflict_resolution(mut self, strategy: ConflictStrategy) -> Self {
        self.conflict_resolution = strategy;
        self
    }

    /// Set routing strategy
    pub fn with_routing(mut self, strategy: RoutingStrategy) -> Self {
        self.routing_strategy = strategy;
        self
    }
}

/// Conflict resolution strategy
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConflictStrategy {
    /// Last write wins based on timestamp
    #[default]
    LastWriteWins,
    /// First write wins
    FirstWriteWins,
    /// Merge using CRDTs
    CrdtMerge,
    /// Custom resolver
    Custom,
}

/// Multi-cloud manager
pub struct MultiCloudManager {
    config: MultiCloudConfig,
    providers: RwLock<HashMap<String, Arc<CloudProvider>>>,
    regions: RwLock<HashMap<String, CloudRegion>>,
    sync_manager: SyncManager,
    health_monitor: HealthMonitor,
    router: TrafficRouter,
}

impl MultiCloudManager {
    /// Create a new multi-cloud manager
    pub fn new(config: MultiCloudConfig) -> Self {
        let sync_manager = SyncManager::new(SyncConfig {
            interval_ms: config.sync_interval_ms,
            batch_size: 1000,
            compression: true,
        });

        let health_monitor = HealthMonitor::new(HealthConfig {
            interval_ms: config.health_check_interval_ms,
            timeout_ms: 3000,
            threshold: config.failover_threshold,
        });

        let router = TrafficRouter::new(RoutingConfig {
            strategy: config.routing_strategy,
            failover_enabled: config.auto_failover,
        });

        Self {
            config,
            providers: RwLock::new(HashMap::new()),
            regions: RwLock::new(HashMap::new()),
            sync_manager,
            health_monitor,
            router,
        }
    }

    /// Create with default config
    pub fn with_defaults() -> Self {
        Self::new(MultiCloudConfig::default())
    }

    /// Register a cloud provider
    pub fn register_provider(&self, provider: CloudProvider) -> Result<(), MultiCloudError> {
        let name = provider.name().to_string();
        self.providers
            .write()
            .insert(name.clone(), Arc::new(provider));

        Ok(())
    }

    /// Register a region
    pub fn register_region(&self, region: CloudRegion) -> Result<(), MultiCloudError> {
        let name = region.name().to_string();

        // Verify provider exists
        let providers = self.providers.read();
        if !providers.contains_key(region.provider()) {
            return Err(MultiCloudError::ProviderNotFound(
                region.provider().to_string(),
            ));
        }
        drop(providers);

        self.regions.write().insert(name.clone(), region);
        self.health_monitor.add_target(&name);

        Ok(())
    }

    /// Get a region
    pub fn get_region(&self, name: &str) -> Option<CloudRegion> {
        self.regions.read().get(name).cloned()
    }

    /// List all regions
    pub fn list_regions(&self) -> Vec<CloudRegion> {
        self.regions.read().values().cloned().collect()
    }

    /// Get healthy regions
    pub fn healthy_regions(&self) -> Vec<CloudRegion> {
        let regions = self.regions.read();
        let health_states = self.health_monitor.get_all_states();

        regions
            .values()
            .filter(|r| {
                health_states
                    .get(r.name())
                    .map(|s| *s == HealthStatus::Healthy)
                    .unwrap_or(false)
            })
            .cloned()
            .collect()
    }

    /// Route a request to the best region
    pub fn route_request(&self, client_region: Option<&str>) -> Option<CloudRegion> {
        let regions = self.regions.read();
        let health_states = self.health_monitor.get_all_states();

        let healthy: Vec<&CloudRegion> = regions
            .values()
            .filter(|r| {
                health_states
                    .get(r.name())
                    .map(|s| *s == HealthStatus::Healthy)
                    .unwrap_or(false)
            })
            .collect();

        if healthy.is_empty() {
            return None;
        }

        self.router.select(&healthy, client_region).cloned()
    }

    /// Sync data to a region
    pub fn sync_to_region(
        &self,
        region: &str,
        key: &str,
        value: &[u8],
        timestamp: u64,
    ) -> Result<(), MultiCloudError> {
        let regions = self.regions.read();
        let target = regions
            .get(region)
            .ok_or_else(|| MultiCloudError::RegionNotFound(region.to_string()))?;

        self.sync_manager
            .queue_sync(target.name(), key, value, timestamp);
        Ok(())
    }

    /// Sync data to all regions
    pub fn sync_to_all(
        &self,
        key: &str,
        value: &[u8],
        timestamp: u64,
    ) -> Result<usize, MultiCloudError> {
        let regions = self.regions.read();
        let mut synced = 0;

        for region in regions.values() {
            if region.name() != self.config.local_region {
                self.sync_manager
                    .queue_sync(region.name(), key, value, timestamp);
                synced += 1;
            }
        }

        Ok(synced)
    }

    /// Resolve a conflict
    pub fn resolve_conflict(
        &self,
        local_value: &[u8],
        local_timestamp: u64,
        remote_value: &[u8],
        remote_timestamp: u64,
    ) -> Vec<u8> {
        match self.config.conflict_resolution {
            ConflictStrategy::LastWriteWins => {
                if remote_timestamp > local_timestamp {
                    remote_value.to_vec()
                } else {
                    local_value.to_vec()
                }
            }
            ConflictStrategy::FirstWriteWins => {
                if remote_timestamp < local_timestamp {
                    remote_value.to_vec()
                } else {
                    local_value.to_vec()
                }
            }
            ConflictStrategy::CrdtMerge => {
                // For simple values, use LWW
                // For complex types, CRDT merge would be used
                if remote_timestamp > local_timestamp {
                    remote_value.to_vec()
                } else {
                    local_value.to_vec()
                }
            }
            ConflictStrategy::Custom => {
                // Custom resolvers would be called here
                local_value.to_vec()
            }
        }
    }

    /// Get sync statistics
    pub fn sync_stats(&self) -> SyncStatus {
        self.sync_manager.status()
    }

    /// Get health status
    pub fn health_status(&self) -> HashMap<String, HealthStatus> {
        self.health_monitor.get_all_states()
    }

    /// Get overall status
    pub fn status(&self) -> MultiCloudStatus {
        let regions = self.regions.read();
        let health_states = self.health_monitor.get_all_states();

        let healthy_count = health_states
            .values()
            .filter(|s| **s == HealthStatus::Healthy)
            .count();

        MultiCloudStatus {
            local_region: self.config.local_region.clone(),
            total_regions: regions.len() as u64,
            healthy_regions: healthy_count as u64,
            total_providers: self.providers.read().len() as u64,
            sync_status: self.sync_manager.status(),
            conflict_strategy: self.config.conflict_resolution,
            routing_strategy: self.config.routing_strategy,
        }
    }

    /// Get configuration
    pub fn config(&self) -> &MultiCloudConfig {
        &self.config
    }
}

impl Default for MultiCloudManager {
    fn default() -> Self {
        Self::with_defaults()
    }
}

/// Multi-cloud status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultiCloudStatus {
    /// Local region
    pub local_region: String,
    /// Total regions
    pub total_regions: u64,
    /// Healthy regions
    pub healthy_regions: u64,
    /// Total providers
    pub total_providers: u64,
    /// Sync status
    pub sync_status: SyncStatus,
    /// Conflict strategy
    pub conflict_strategy: ConflictStrategy,
    /// Routing strategy
    pub routing_strategy: RoutingStrategy,
}

/// Multi-cloud errors
#[derive(Debug, Clone, thiserror::Error)]
pub enum MultiCloudError {
    /// Provider not found
    #[error("provider not found: {0}")]
    ProviderNotFound(String),

    /// Region not found
    #[error("region not found: {0}")]
    RegionNotFound(String),

    /// Sync error
    #[error("sync error: {0}")]
    SyncError(String),

    /// Health check error
    #[error("health check error: {0}")]
    HealthError(String),

    /// Connection error
    #[error("connection error: {0}")]
    ConnectionError(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config() {
        let config = MultiCloudConfig::new()
            .with_local_region("eu-west-1")
            .with_conflict_resolution(ConflictStrategy::CrdtMerge);

        assert_eq!(config.local_region, "eu-west-1");
        assert_eq!(config.conflict_resolution, ConflictStrategy::CrdtMerge);
    }

    #[test]
    fn test_conflict_resolution() {
        let manager = MultiCloudManager::with_defaults();

        let local = b"local_value";
        let remote = b"remote_value";

        // Remote is newer, should win with LWW
        let result = manager.resolve_conflict(local, 100, remote, 200);
        assert_eq!(result, remote);

        // Local is newer
        let result = manager.resolve_conflict(local, 300, remote, 200);
        assert_eq!(result, local);
    }
}
