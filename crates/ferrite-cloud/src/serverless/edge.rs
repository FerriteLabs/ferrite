//! Edge Network Management
//!
//! Manage edge locations, nodes, and geo-distributed execution.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

/// Edge location (region/datacenter)
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EdgeLocation {
    /// Location ID
    pub id: String,
    /// Display name
    pub name: String,
    /// Region code (e.g., "us-east-1")
    pub region: String,
    /// Geographic coordinates
    pub coordinates: GeoCoordinates,
    /// Nodes at this location
    pub nodes: Vec<String>,
    /// Status
    pub status: EdgeLocationStatus,
    /// Available capacity
    pub capacity: LocationCapacity,
}

/// Geographic coordinates
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GeoCoordinates {
    /// Latitude
    pub latitude: f64,
    /// Longitude
    pub longitude: f64,
}

impl GeoCoordinates {
    /// Calculate distance to another point (Haversine formula)
    pub fn distance_to(&self, other: &GeoCoordinates) -> f64 {
        let r = 6371.0; // Earth's radius in km

        let lat1 = self.latitude.to_radians();
        let lat2 = other.latitude.to_radians();
        let delta_lat = (other.latitude - self.latitude).to_radians();
        let delta_lon = (other.longitude - self.longitude).to_radians();

        let a = (delta_lat / 2.0).sin().powi(2)
            + lat1.cos() * lat2.cos() * (delta_lon / 2.0).sin().powi(2);
        let c = 2.0 * a.sqrt().atan2((1.0 - a).sqrt());

        r * c
    }
}

/// Edge location status
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum EdgeLocationStatus {
    /// Location is healthy
    Healthy,
    /// Location is degraded
    Degraded,
    /// Location is unhealthy
    Unhealthy,
    /// Location is under maintenance
    Maintenance,
    /// Location is offline
    Offline,
}

/// Location capacity
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct LocationCapacity {
    /// Total instances
    pub total_instances: usize,
    /// Available instances
    pub available_instances: usize,
    /// Memory capacity (bytes)
    pub memory_capacity: usize,
    /// Memory used (bytes)
    pub memory_used: usize,
    /// CPU capacity (millicores)
    pub cpu_capacity: usize,
    /// CPU used (millicores)
    pub cpu_used: usize,
}

/// Edge node (individual compute unit)
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EdgeNode {
    /// Node ID
    pub id: String,
    /// Location ID
    pub location_id: String,
    /// Node address
    pub address: String,
    /// Port
    pub port: u16,
    /// Node status
    pub status: EdgeNodeStatus,
    /// Resources
    pub resources: NodeResources,
    /// Deployed functions
    pub functions: Vec<String>,
    /// Last heartbeat
    pub last_heartbeat: u64,
    /// Node version
    pub version: String,
}

/// Edge node status
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum EdgeNodeStatus {
    /// Node is starting up
    Starting,
    /// Node is ready
    Ready,
    /// Node is busy (at capacity)
    Busy,
    /// Node is draining (not accepting new work)
    Draining,
    /// Node is unhealthy
    Unhealthy,
    /// Node is offline
    Offline,
}

/// Node resources
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct NodeResources {
    /// Total memory (bytes)
    pub memory_total: usize,
    /// Memory used (bytes)
    pub memory_used: usize,
    /// CPU cores
    pub cpu_cores: usize,
    /// CPU usage (0-100)
    pub cpu_usage: f32,
    /// Active instances
    pub active_instances: usize,
    /// Max instances
    pub max_instances: usize,
}

/// Edge network manager
pub struct EdgeNetwork {
    /// Locations by ID
    locations: RwLock<HashMap<String, EdgeLocation>>,
    /// Nodes by ID
    nodes: RwLock<HashMap<String, EdgeNode>>,
    /// Location to nodes mapping
    location_nodes: RwLock<HashMap<String, Vec<String>>>,
    /// Metrics
    metrics: EdgeNetworkMetrics,
}

/// Edge network metrics
#[derive(Debug, Default)]
pub struct EdgeNetworkMetrics {
    /// Total requests routed
    pub requests_routed: AtomicU64,
    /// Routing failures
    pub routing_failures: AtomicU64,
    /// Cross-region requests
    pub cross_region_requests: AtomicU64,
    /// Total latency (for averaging)
    pub total_routing_latency_us: AtomicU64,
}

impl EdgeNetwork {
    /// Create a new edge network
    pub fn new() -> Self {
        Self {
            locations: RwLock::new(HashMap::new()),
            nodes: RwLock::new(HashMap::new()),
            location_nodes: RwLock::new(HashMap::new()),
            metrics: EdgeNetworkMetrics::default(),
        }
    }

    /// Add a location
    pub fn add_location(&self, location: EdgeLocation) {
        let id = location.id.clone();
        self.locations.write().insert(id.clone(), location);
        self.location_nodes.write().entry(id).or_default();
    }

    /// Add a node
    pub fn add_node(&self, node: EdgeNode) {
        let node_id = node.id.clone();
        let location_id = node.location_id.clone();

        self.nodes.write().insert(node_id.clone(), node);
        self.location_nodes
            .write()
            .entry(location_id)
            .or_default()
            .push(node_id);
    }

    /// Get location by ID
    pub fn get_location(&self, id: &str) -> Option<EdgeLocation> {
        self.locations.read().get(id).cloned()
    }

    /// Get node by ID
    pub fn get_node(&self, id: &str) -> Option<EdgeNode> {
        self.nodes.read().get(id).cloned()
    }

    /// List all locations
    pub fn list_locations(&self) -> Vec<EdgeLocation> {
        self.locations.read().values().cloned().collect()
    }

    /// List nodes at a location
    pub fn list_nodes_at_location(&self, location_id: &str) -> Vec<EdgeNode> {
        let node_ids = self
            .location_nodes
            .read()
            .get(location_id)
            .cloned()
            .unwrap_or_default();

        let nodes = self.nodes.read();
        node_ids
            .iter()
            .filter_map(|id| nodes.get(id).cloned())
            .collect()
    }

    /// Find nearest location to coordinates
    pub fn find_nearest_location(&self, coords: &GeoCoordinates) -> Option<EdgeLocation> {
        let locations = self.locations.read();

        locations
            .values()
            .filter(|loc| loc.status == EdgeLocationStatus::Healthy)
            .min_by(|a, b| {
                let dist_a = a.coordinates.distance_to(coords);
                let dist_b = b.coordinates.distance_to(coords);
                dist_a
                    .partial_cmp(&dist_b)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .cloned()
    }

    /// Select best node for execution
    pub fn select_node(&self, location_id: &str, function_id: &str) -> Option<EdgeNode> {
        let node_ids = self
            .location_nodes
            .read()
            .get(location_id)
            .cloned()
            .unwrap_or_default();

        let nodes = self.nodes.read();

        // Prefer nodes that already have the function deployed
        let preferred: Vec<_> = node_ids
            .iter()
            .filter_map(|id| nodes.get(id))
            .filter(|n| n.status == EdgeNodeStatus::Ready)
            .filter(|n| n.functions.contains(&function_id.to_string()))
            .collect();

        if let Some(node) = preferred
            .iter()
            .min_by_key(|n| n.resources.active_instances)
        {
            return Some((*node).clone());
        }

        // Fall back to any ready node with capacity
        node_ids
            .iter()
            .filter_map(|id| nodes.get(id))
            .filter(|n| n.status == EdgeNodeStatus::Ready)
            .filter(|n| n.resources.active_instances < n.resources.max_instances)
            .min_by_key(|n| n.resources.active_instances)
            .cloned()
    }

    /// Update node status
    pub fn update_node_status(&self, node_id: &str, status: EdgeNodeStatus) {
        if let Some(node) = self.nodes.write().get_mut(node_id) {
            node.status = status;
        }
    }

    /// Update node resources
    pub fn update_node_resources(&self, node_id: &str, resources: NodeResources) {
        if let Some(node) = self.nodes.write().get_mut(node_id) {
            node.resources = resources;
        }
    }

    /// Record node heartbeat
    pub fn record_heartbeat(&self, node_id: &str) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        if let Some(node) = self.nodes.write().get_mut(node_id) {
            node.last_heartbeat = now;
        }
    }

    /// Get healthy locations
    pub fn get_healthy_locations(&self) -> Vec<EdgeLocation> {
        self.locations
            .read()
            .values()
            .filter(|loc| loc.status == EdgeLocationStatus::Healthy)
            .cloned()
            .collect()
    }

    /// Get total capacity
    pub fn get_total_capacity(&self) -> LocationCapacity {
        let locations = self.locations.read();

        let mut total = LocationCapacity::default();
        for loc in locations.values() {
            total.total_instances += loc.capacity.total_instances;
            total.available_instances += loc.capacity.available_instances;
            total.memory_capacity += loc.capacity.memory_capacity;
            total.memory_used += loc.capacity.memory_used;
            total.cpu_capacity += loc.capacity.cpu_capacity;
            total.cpu_used += loc.capacity.cpu_used;
        }

        total
    }

    /// Record routing metric
    pub fn record_routing(&self, success: bool, latency_us: u64, cross_region: bool) {
        self.metrics.requests_routed.fetch_add(1, Ordering::Relaxed);
        self.metrics
            .total_routing_latency_us
            .fetch_add(latency_us, Ordering::Relaxed);

        if !success {
            self.metrics
                .routing_failures
                .fetch_add(1, Ordering::Relaxed);
        }

        if cross_region {
            self.metrics
                .cross_region_requests
                .fetch_add(1, Ordering::Relaxed);
        }
    }
}

impl Default for EdgeNetwork {
    fn default() -> Self {
        Self::new()
    }
}

/// Create default edge locations
pub fn create_default_locations() -> Vec<EdgeLocation> {
    vec![
        EdgeLocation {
            id: "us-east-1".to_string(),
            name: "US East (N. Virginia)".to_string(),
            region: "us-east-1".to_string(),
            coordinates: GeoCoordinates {
                latitude: 38.9519,
                longitude: -77.4480,
            },
            nodes: vec![],
            status: EdgeLocationStatus::Healthy,
            capacity: LocationCapacity::default(),
        },
        EdgeLocation {
            id: "us-west-2".to_string(),
            name: "US West (Oregon)".to_string(),
            region: "us-west-2".to_string(),
            coordinates: GeoCoordinates {
                latitude: 45.8399,
                longitude: -119.7006,
            },
            nodes: vec![],
            status: EdgeLocationStatus::Healthy,
            capacity: LocationCapacity::default(),
        },
        EdgeLocation {
            id: "eu-west-1".to_string(),
            name: "EU (Ireland)".to_string(),
            region: "eu-west-1".to_string(),
            coordinates: GeoCoordinates {
                latitude: 53.3498,
                longitude: -6.2603,
            },
            nodes: vec![],
            status: EdgeLocationStatus::Healthy,
            capacity: LocationCapacity::default(),
        },
        EdgeLocation {
            id: "eu-central-1".to_string(),
            name: "EU (Frankfurt)".to_string(),
            region: "eu-central-1".to_string(),
            coordinates: GeoCoordinates {
                latitude: 50.1109,
                longitude: 8.6821,
            },
            nodes: vec![],
            status: EdgeLocationStatus::Healthy,
            capacity: LocationCapacity::default(),
        },
        EdgeLocation {
            id: "ap-southeast-1".to_string(),
            name: "Asia Pacific (Singapore)".to_string(),
            region: "ap-southeast-1".to_string(),
            coordinates: GeoCoordinates {
                latitude: 1.3521,
                longitude: 103.8198,
            },
            nodes: vec![],
            status: EdgeLocationStatus::Healthy,
            capacity: LocationCapacity::default(),
        },
        EdgeLocation {
            id: "ap-northeast-1".to_string(),
            name: "Asia Pacific (Tokyo)".to_string(),
            region: "ap-northeast-1".to_string(),
            coordinates: GeoCoordinates {
                latitude: 35.6762,
                longitude: 139.6503,
            },
            nodes: vec![],
            status: EdgeLocationStatus::Healthy,
            capacity: LocationCapacity::default(),
        },
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_geo_distance() {
        // New York
        let ny = GeoCoordinates {
            latitude: 40.7128,
            longitude: -74.0060,
        };
        // Los Angeles
        let la = GeoCoordinates {
            latitude: 34.0522,
            longitude: -118.2437,
        };

        let distance = ny.distance_to(&la);
        // Approximately 3944 km
        assert!(distance > 3900.0 && distance < 4000.0);
    }

    #[test]
    fn test_edge_network() {
        let network = EdgeNetwork::new();

        let location = EdgeLocation {
            id: "test-loc".to_string(),
            name: "Test Location".to_string(),
            region: "test".to_string(),
            coordinates: GeoCoordinates {
                latitude: 0.0,
                longitude: 0.0,
            },
            nodes: vec![],
            status: EdgeLocationStatus::Healthy,
            capacity: LocationCapacity::default(),
        };

        network.add_location(location.clone());
        assert!(network.get_location("test-loc").is_some());
    }

    #[test]
    fn test_find_nearest_location() {
        let network = EdgeNetwork::new();

        for loc in create_default_locations() {
            network.add_location(loc);
        }

        // Point near New York
        let coords = GeoCoordinates {
            latitude: 40.7128,
            longitude: -74.0060,
        };

        let nearest = network.find_nearest_location(&coords);
        assert!(nearest.is_some());
        assert_eq!(nearest.unwrap().id, "us-east-1");
    }

    #[test]
    fn test_node_selection() {
        let network = EdgeNetwork::new();

        network.add_location(EdgeLocation {
            id: "loc1".to_string(),
            name: "Location 1".to_string(),
            region: "test".to_string(),
            coordinates: GeoCoordinates {
                latitude: 0.0,
                longitude: 0.0,
            },
            nodes: vec![],
            status: EdgeLocationStatus::Healthy,
            capacity: LocationCapacity::default(),
        });

        network.add_node(EdgeNode {
            id: "node1".to_string(),
            location_id: "loc1".to_string(),
            address: "127.0.0.1".to_string(),
            port: 8080,
            status: EdgeNodeStatus::Ready,
            resources: NodeResources {
                active_instances: 5,
                max_instances: 10,
                ..Default::default()
            },
            functions: vec!["func1".to_string()],
            last_heartbeat: 0,
            version: "1.0".to_string(),
        });

        let selected = network.select_node("loc1", "func1");
        assert!(selected.is_some());
        assert_eq!(selected.unwrap().id, "node1");
    }
}
