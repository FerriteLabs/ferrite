//! Traffic routing for multi-cloud regions

use serde::{Deserialize, Serialize};

use super::region::CloudRegion;

/// Routing strategy
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum RoutingStrategy {
    /// Route to nearest healthy region
    #[default]
    NearestHealthy,
    /// Round-robin across healthy regions
    RoundRobin,
    /// Weighted round-robin based on region weights
    WeightedRoundRobin,
    /// Route to lowest latency region
    LowestLatency,
    /// Route to region with lowest load
    LeastConnections,
    /// Random selection among healthy regions
    Random,
    /// Primary with failover to secondary
    PrimaryFailover,
}

impl RoutingStrategy {
    /// Get string representation
    pub fn as_str(&self) -> &'static str {
        match self {
            RoutingStrategy::NearestHealthy => "nearest_healthy",
            RoutingStrategy::RoundRobin => "round_robin",
            RoutingStrategy::WeightedRoundRobin => "weighted_round_robin",
            RoutingStrategy::LowestLatency => "lowest_latency",
            RoutingStrategy::LeastConnections => "least_connections",
            RoutingStrategy::Random => "random",
            RoutingStrategy::PrimaryFailover => "primary_failover",
        }
    }

    /// Parse from string
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "nearest_healthy" | "nearest" => Some(RoutingStrategy::NearestHealthy),
            "round_robin" | "roundrobin" => Some(RoutingStrategy::RoundRobin),
            "weighted_round_robin" | "weighted" => Some(RoutingStrategy::WeightedRoundRobin),
            "lowest_latency" | "latency" => Some(RoutingStrategy::LowestLatency),
            "least_connections" | "least_conn" => Some(RoutingStrategy::LeastConnections),
            "random" => Some(RoutingStrategy::Random),
            "primary_failover" | "failover" => Some(RoutingStrategy::PrimaryFailover),
            _ => None,
        }
    }
}

/// Routing configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoutingConfig {
    /// Routing strategy
    pub strategy: RoutingStrategy,
    /// Enable automatic failover
    pub failover_enabled: bool,
}

impl Default for RoutingConfig {
    fn default() -> Self {
        Self {
            strategy: RoutingStrategy::NearestHealthy,
            failover_enabled: true,
        }
    }
}

/// Traffic router
pub struct TrafficRouter {
    config: RoutingConfig,
    round_robin_index: std::sync::atomic::AtomicUsize,
}

impl TrafficRouter {
    /// Create a new traffic router
    pub fn new(config: RoutingConfig) -> Self {
        Self {
            config,
            round_robin_index: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    /// Select a region based on the routing strategy
    pub fn select<'a>(
        &self,
        regions: &[&'a CloudRegion],
        client_region: Option<&str>,
    ) -> Option<&'a CloudRegion> {
        if regions.is_empty() {
            return None;
        }

        match self.config.strategy {
            RoutingStrategy::NearestHealthy => self.select_nearest(regions, client_region),
            RoutingStrategy::RoundRobin => self.select_round_robin(regions),
            RoutingStrategy::WeightedRoundRobin => self.select_weighted(regions),
            RoutingStrategy::LowestLatency => self.select_lowest_latency(regions),
            RoutingStrategy::LeastConnections => self.select_least_connections(regions),
            RoutingStrategy::Random => self.select_random(regions),
            RoutingStrategy::PrimaryFailover => self.select_primary(regions),
        }
    }

    /// Select nearest region (simplified - uses priority as proximity)
    fn select_nearest<'a>(
        &self,
        regions: &[&'a CloudRegion],
        client_region: Option<&str>,
    ) -> Option<&'a CloudRegion> {
        // If client region matches a region, prefer it
        if let Some(client) = client_region {
            if let Some(region) = regions.iter().find(|r| r.name() == client) {
                return Some(*region);
            }
        }

        // Otherwise, select by priority (lower = higher priority)
        regions.iter().min_by_key(|r| r.priority()).copied()
    }

    /// Round-robin selection
    fn select_round_robin<'a>(&self, regions: &[&'a CloudRegion]) -> Option<&'a CloudRegion> {
        let index = self
            .round_robin_index
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        regions.get(index % regions.len()).copied()
    }

    /// Weighted round-robin selection
    fn select_weighted<'a>(&self, regions: &[&'a CloudRegion]) -> Option<&'a CloudRegion> {
        let total_weight: u32 = regions.iter().map(|r| r.weight()).sum();
        if total_weight == 0 {
            return regions.first().copied();
        }

        let random_weight = rand::random::<u32>() % total_weight;
        let mut cumulative = 0;

        for region in regions {
            cumulative += region.weight();
            if random_weight < cumulative {
                return Some(*region);
            }
        }

        regions.first().copied()
    }

    /// Select region with lowest latency
    fn select_lowest_latency<'a>(&self, regions: &[&'a CloudRegion]) -> Option<&'a CloudRegion> {
        regions
            .iter()
            .filter(|r| r.latency_ms().is_some())
            .min_by(|a, b| {
                a.latency_ms()
                    .unwrap_or(f64::MAX)
                    .partial_cmp(&b.latency_ms().unwrap_or(f64::MAX))
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .copied()
            .or_else(|| regions.first().copied())
    }

    /// Select region with least connections (simplified - uses weight as inverse)
    fn select_least_connections<'a>(&self, regions: &[&'a CloudRegion]) -> Option<&'a CloudRegion> {
        // In a real implementation, this would track actual connections
        // For now, use priority as a proxy
        regions.iter().min_by_key(|r| r.priority()).copied()
    }

    /// Random selection
    fn select_random<'a>(&self, regions: &[&'a CloudRegion]) -> Option<&'a CloudRegion> {
        if regions.is_empty() {
            return None;
        }
        let index = rand::random::<usize>() % regions.len();
        regions.get(index).copied()
    }

    /// Primary with failover (select highest priority)
    fn select_primary<'a>(&self, regions: &[&'a CloudRegion]) -> Option<&'a CloudRegion> {
        regions.iter().min_by_key(|r| r.priority()).copied()
    }

    /// Get configuration
    pub fn config(&self) -> &RoutingConfig {
        &self.config
    }

    /// Update strategy
    pub fn set_strategy(&mut self, strategy: RoutingStrategy) {
        self.config.strategy = strategy;
    }
}

impl Default for TrafficRouter {
    fn default() -> Self {
        Self::new(RoutingConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::multicloud::region::RegionConfig;

    fn create_test_regions() -> Vec<CloudRegion> {
        vec![
            CloudRegion::new(
                "us-east-1",
                RegionConfig::new("us-east-1", "aws").with_priority(1),
            ),
            CloudRegion::new(
                "us-west-2",
                RegionConfig::new("us-west-2", "aws").with_priority(2),
            ),
            CloudRegion::new(
                "eu-west-1",
                RegionConfig::new("eu-west-1", "aws").with_priority(3),
            ),
        ]
    }

    #[test]
    fn test_nearest_healthy() {
        let router = TrafficRouter::new(RoutingConfig {
            strategy: RoutingStrategy::NearestHealthy,
            failover_enabled: true,
        });

        let regions = create_test_regions();
        let refs: Vec<&CloudRegion> = regions.iter().collect();

        // Without client region, should pick highest priority (lowest number)
        let selected = router.select(&refs, None);
        assert_eq!(selected.unwrap().name(), "us-east-1");

        // With matching client region
        let selected = router.select(&refs, Some("eu-west-1"));
        assert_eq!(selected.unwrap().name(), "eu-west-1");
    }

    #[test]
    fn test_round_robin() {
        let router = TrafficRouter::new(RoutingConfig {
            strategy: RoutingStrategy::RoundRobin,
            failover_enabled: true,
        });

        let regions = create_test_regions();
        let refs: Vec<&CloudRegion> = regions.iter().collect();

        let r1 = router.select(&refs, None).unwrap().name().to_string();
        let r2 = router.select(&refs, None).unwrap().name().to_string();
        let r3 = router.select(&refs, None).unwrap().name().to_string();
        let r4 = router.select(&refs, None).unwrap().name().to_string();

        // Should cycle through regions
        assert_ne!(r1, r2);
        assert_ne!(r2, r3);
        assert_eq!(r1, r4); // Back to first after full cycle
    }

    #[test]
    fn test_primary_failover() {
        let router = TrafficRouter::new(RoutingConfig {
            strategy: RoutingStrategy::PrimaryFailover,
            failover_enabled: true,
        });

        let regions = create_test_regions();
        let refs: Vec<&CloudRegion> = regions.iter().collect();

        // Should always pick primary (highest priority = lowest number)
        for _ in 0..5 {
            let selected = router.select(&refs, None);
            assert_eq!(selected.unwrap().name(), "us-east-1");
        }
    }
}
