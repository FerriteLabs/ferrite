//! Cluster routing and state management
//!
//! This module handles routing keys to the correct nodes in the cluster
//! and manages the overall cluster state.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use super::slots::{HashSlot, SlotInfo, SlotRange, CLUSTER_SLOTS};
use super::{ClusterNode, NodeId};

/// Cluster state
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ClusterState {
    /// Cluster state is unknown (initial state)
    Unknown,
    /// Cluster is healthy and fully operational
    Ok,
    /// Cluster is operational but degraded (some nodes failed)
    Degraded,
    /// Cluster is down (not enough nodes or slots not covered)
    Fail,
}

impl Default for ClusterState {
    fn default() -> Self {
        Self::Unknown
    }
}

impl std::fmt::Display for ClusterState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unknown => write!(f, "unknown"),
            Self::Ok => write!(f, "ok"),
            Self::Degraded => write!(f, "degraded"),
            Self::Fail => write!(f, "fail"),
        }
    }
}

/// Result of routing a key
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RouteResult {
    /// Key should be handled locally
    Local,
    /// Key is on another node (permanent redirect)
    Moved {
        /// The slot number
        slot: u16,
        /// The address of the node holding this slot
        addr: SocketAddr,
    },
    /// Key is being migrated (temporary redirect)
    Ask {
        /// The slot number
        slot: u16,
        /// The address of the node to ask
        addr: SocketAddr,
    },
    /// Slot is being imported (client needs ASKING flag)
    Importing {
        /// The slot number being imported
        slot: u16,
    },
    /// Cluster is down
    ClusterDown,
    /// Routing error
    Error(String),
}

impl RouteResult {
    /// Check if the result is local
    pub fn is_local(&self) -> bool {
        matches!(self, Self::Local)
    }

    /// Check if a redirect is needed
    pub fn is_redirect(&self) -> bool {
        matches!(self, Self::Moved { .. } | Self::Ask { .. })
    }

    /// Format as Redis error response
    #[cold]
    pub fn to_error_string(&self) -> Option<String> {
        match self {
            Self::Moved { slot, addr } => Some(format!("MOVED {} {}", slot, addr)),
            Self::Ask { slot, addr } => Some(format!("ASK {} {}", slot, addr)),
            Self::Importing { slot } => Some(format!(
                "ERR slot {} is being imported, send ASKING first",
                slot
            )),
            Self::ClusterDown => Some("CLUSTERDOWN The cluster is down".to_string()),
            Self::Error(msg) => Some(format!("ERR {}", msg)),
            Self::Local => None,
        }
    }
}

/// Cluster router for directing keys to nodes
pub struct ClusterRouter {
    /// This node's ID
    node_id: NodeId,
    /// Slot to node mapping
    slot_owners: RwLock<[Option<NodeId>; CLUSTER_SLOTS as usize]>,
    /// Slot migration state
    slot_states: RwLock<HashMap<u16, SlotInfo>>,
    /// Node addresses
    node_addrs: RwLock<HashMap<NodeId, SocketAddr>>,
    /// Current cluster state
    state: RwLock<ClusterState>,
}

impl ClusterRouter {
    /// Create a new cluster router
    pub fn new(node_id: NodeId) -> Self {
        Self {
            node_id,
            slot_owners: RwLock::new([const { None }; CLUSTER_SLOTS as usize]),
            slot_states: RwLock::new(HashMap::new()),
            node_addrs: RwLock::new(HashMap::new()),
            state: RwLock::new(ClusterState::Unknown),
        }
    }

    /// Get this node's ID
    pub fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    /// Get current cluster state
    pub fn state(&self) -> ClusterState {
        *self.state.read()
    }

    /// Set cluster state
    pub fn set_state(&self, state: ClusterState) {
        *self.state.write() = state;
    }

    /// Route a key to the appropriate node
    pub fn route(&self, key: &[u8]) -> RouteResult {
        let slot = HashSlot::for_key(key);
        self.route_slot(slot)
    }

    /// Route a slot to the appropriate node
    pub fn route_slot(&self, slot: u16) -> RouteResult {
        // Check cluster state
        if *self.state.read() == ClusterState::Fail {
            return RouteResult::ClusterDown;
        }

        let slot_owners = self.slot_owners.read();
        let owner = &slot_owners[slot as usize];

        match owner {
            Some(owner_id) => {
                // Check if it's us
                if owner_id == &self.node_id {
                    // Check for migration state
                    if let Some(info) = self.slot_states.read().get(&slot) {
                        if info.is_migrating() {
                            // Slot is being migrated - we still handle reads
                            // but writes may need ASK redirect
                            return RouteResult::Local;
                        }
                    }
                    RouteResult::Local
                } else {
                    // Get the node's address
                    let node_addrs = self.node_addrs.read();
                    match node_addrs.get(owner_id) {
                        Some(addr) => {
                            // Check if we're importing this slot
                            if let Some(info) = self.slot_states.read().get(&slot) {
                                if info.is_importing() {
                                    // We're importing - use ASK
                                    return RouteResult::Ask { slot, addr: *addr };
                                }
                            }
                            RouteResult::Moved { slot, addr: *addr }
                        }
                        None => RouteResult::Error(format!("No address for node {}", owner_id)),
                    }
                }
            }
            None => {
                // Slot not assigned
                RouteResult::Error(format!("Slot {} not assigned", slot))
            }
        }
    }

    /// Route multiple keys and ensure they're all on the same slot
    pub fn route_multi(&self, keys: &[&[u8]]) -> Result<RouteResult, String> {
        if keys.is_empty() {
            return Ok(RouteResult::Local);
        }

        let first_slot = HashSlot::for_key(keys[0]);

        // Check all keys are on the same slot
        for key in keys.iter().skip(1) {
            let slot = HashSlot::for_key(key);
            if slot != first_slot {
                return Err(
                    "CROSSSLOT Keys in request don't hash to the same slot".to_string()
                );
            }
        }

        Ok(self.route_slot(first_slot))
    }

    /// Update slot ownership
    pub fn set_slot_owner(&self, slot: u16, node_id: Option<NodeId>) {
        if slot < CLUSTER_SLOTS {
            self.slot_owners.write()[slot as usize] = node_id;
        }
    }

    /// Assign a range of slots to a node
    pub fn assign_slots(&self, node_id: &NodeId, range: &SlotRange) {
        let mut slot_owners = self.slot_owners.write();
        for slot in range.iter() {
            if slot < CLUSTER_SLOTS {
                slot_owners[slot as usize] = Some(node_id.clone());
            }
        }
    }

    /// Update node address
    pub fn set_node_address(&self, node_id: NodeId, addr: SocketAddr) {
        self.node_addrs.write().insert(node_id, addr);
    }

    /// Remove a node
    pub fn remove_node(&self, node_id: &NodeId) {
        self.node_addrs.write().remove(node_id);

        // Clear slot assignments for this node
        let mut slot_owners = self.slot_owners.write();
        for slot_owner in slot_owners.iter_mut() {
            if slot_owner.as_ref() == Some(node_id) {
                *slot_owner = None;
            }
        }
    }

    /// Start migrating a slot to another node
    pub fn start_migration(&self, slot: u16, target_node: NodeId) {
        let mut states = self.slot_states.write();
        let info = states.entry(slot).or_insert_with(|| SlotInfo::new(slot));
        info.start_migration(target_node);
    }

    /// Start importing a slot from another node
    pub fn start_import(&self, slot: u16, source_node: NodeId) {
        let mut states = self.slot_states.write();
        let info = states.entry(slot).or_insert_with(|| SlotInfo::new(slot));
        info.start_import(source_node);
    }

    /// Complete slot migration
    pub fn complete_migration(&self, slot: u16, new_owner: NodeId) {
        // Update owner
        self.set_slot_owner(slot, Some(new_owner));

        // Clear migration state
        let mut states = self.slot_states.write();
        if let Some(info) = states.get_mut(&slot) {
            info.complete_migration();
        }
    }

    /// Get slot owner
    pub fn get_slot_owner(&self, slot: u16) -> Option<NodeId> {
        self.slot_owners
            .read()
            .get(slot as usize)
            .cloned()
            .flatten()
    }

    /// Get slot for a key
    pub fn slot_for_key(&self, key: &[u8]) -> u16 {
        HashSlot::for_key(key)
    }

    /// Check if all slots are covered
    pub fn all_slots_covered(&self) -> bool {
        self.slot_owners.read().iter().all(|s| s.is_some())
    }

    /// Get number of covered slots
    pub fn covered_slot_count(&self) -> usize {
        self.slot_owners
            .read()
            .iter()
            .filter(|s| s.is_some())
            .count()
    }

    /// Get slots owned by a node
    pub fn get_node_slots(&self, node_id: &NodeId) -> Vec<u16> {
        self.slot_owners
            .read()
            .iter()
            .enumerate()
            .filter_map(|(slot, owner)| {
                if owner.as_ref() == Some(node_id) {
                    Some(slot as u16)
                } else {
                    None
                }
            })
            .collect()
    }

    /// Get slot ranges owned by a node (compressed representation)
    pub fn get_node_slot_ranges(&self, node_id: &NodeId) -> Vec<SlotRange> {
        let slots = self.get_node_slots(node_id);
        if slots.is_empty() {
            return Vec::new();
        }

        let mut ranges = Vec::new();
        let mut start = slots[0];
        let mut end = slots[0];

        for &slot in slots.iter().skip(1) {
            if slot == end + 1 {
                end = slot;
            } else {
                ranges.push(SlotRange::new(start, end));
                start = slot;
                end = slot;
            }
        }
        ranges.push(SlotRange::new(start, end));

        ranges
    }

    /// Update routing table from cluster nodes
    pub fn update_from_nodes(&self, nodes: &[ClusterNode]) {
        // Clear existing addresses and slots
        {
            let mut addrs = self.node_addrs.write();
            addrs.clear();

            let mut slot_owners = self.slot_owners.write();
            for owner in slot_owners.iter_mut() {
                *owner = None;
            }
        }

        // Add nodes and their slots
        for node in nodes {
            self.set_node_address(node.id.clone(), node.addr);

            for range in &node.slots {
                self.assign_slots(&node.id, range);
            }
        }

        // Update state based on coverage
        if self.all_slots_covered() {
            self.set_state(ClusterState::Ok);
        } else if self.covered_slot_count() > 0 {
            self.set_state(ClusterState::Degraded);
        } else {
            self.set_state(ClusterState::Fail);
        }
    }
}

/// Shared router reference
pub type SharedRouter = Arc<ClusterRouter>;

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_router() -> ClusterRouter {
        ClusterRouter::new("test-node-id".to_string())
    }

    #[test]
    fn test_route_local() {
        let router = create_test_router();
        let node_id = router.node_id().clone();

        // Assign all slots to ourselves
        router.assign_slots(&node_id, &SlotRange::new(0, CLUSTER_SLOTS - 1));
        router.set_state(ClusterState::Ok);

        // Any key should route locally
        let result = router.route(b"foo");
        assert!(matches!(result, RouteResult::Local));
    }

    #[test]
    fn test_route_moved() {
        let router = create_test_router();

        // Set up another node
        let other_node = "other-node".to_string();
        let other_addr: SocketAddr = "127.0.0.1:6380".parse().unwrap();
        router.set_node_address(other_node.clone(), other_addr);

        // Assign all slots to other node
        router.assign_slots(&other_node, &SlotRange::new(0, CLUSTER_SLOTS - 1));
        router.set_state(ClusterState::Ok);

        // Key should be redirected
        let result = router.route(b"foo");
        match result {
            RouteResult::Moved { addr, .. } => {
                assert_eq!(addr, other_addr);
            }
            _ => panic!("Expected Moved result"),
        }
    }

    #[test]
    fn test_route_cluster_down() {
        let router = create_test_router();
        router.set_state(ClusterState::Fail);

        let result = router.route(b"foo");
        assert!(matches!(result, RouteResult::ClusterDown));
    }

    #[test]
    fn test_route_multi_same_slot() {
        let router = create_test_router();
        let node_id = router.node_id().clone();

        router.assign_slots(&node_id, &SlotRange::new(0, CLUSTER_SLOTS - 1));
        router.set_state(ClusterState::Ok);

        // Keys with same hash tag should work
        let keys: Vec<&[u8]> = vec![b"{user}:1", b"{user}:2", b"{user}:3"];
        let result = router.route_multi(&keys);
        assert!(result.is_ok());
        assert!(matches!(result.unwrap(), RouteResult::Local));
    }

    #[test]
    fn test_route_multi_cross_slot() {
        let router = create_test_router();

        // Keys without hash tags will likely be on different slots
        let keys: Vec<&[u8]> = vec![b"foo", b"bar", b"baz"];
        let result = router.route_multi(&keys);
        // This might fail if they happen to be on the same slot
        // For test stability, use keys we know are on different slots
        let keys2: Vec<&[u8]> = vec![b"a", b"b"];
        let result2 = router.route_multi(&keys2);

        // At least one should fail (cross-slot)
        // Note: This test is probabilistic, but 'a' and 'b' should be on different slots
        assert!(
            result.is_err()
                || result2.is_err()
                || HashSlot::for_key(b"a") == HashSlot::for_key(b"b")
                || HashSlot::for_key(b"foo") == HashSlot::for_key(b"bar")
        );
    }

    #[test]
    fn test_slot_assignment() {
        let router = create_test_router();
        let node_id = "node1".to_string();

        router.assign_slots(&node_id, &SlotRange::new(0, 100));

        assert_eq!(router.get_slot_owner(0), Some(node_id.clone()));
        assert_eq!(router.get_slot_owner(100), Some(node_id.clone()));
        assert!(router.get_slot_owner(101).is_none());
    }

    #[test]
    fn test_get_node_slots() {
        let router = create_test_router();
        let node_id = "node1".to_string();

        router.assign_slots(&node_id, &SlotRange::new(0, 10));
        router.assign_slots(&node_id, &SlotRange::new(100, 110));

        let slots = router.get_node_slots(&node_id);
        assert_eq!(slots.len(), 22); // 11 + 11
    }

    #[test]
    fn test_get_node_slot_ranges() {
        let router = create_test_router();
        let node_id = "node1".to_string();

        router.assign_slots(&node_id, &SlotRange::new(0, 10));
        router.assign_slots(&node_id, &SlotRange::new(100, 110));

        let ranges = router.get_node_slot_ranges(&node_id);
        assert_eq!(ranges.len(), 2);
        assert_eq!(ranges[0], SlotRange::new(0, 10));
        assert_eq!(ranges[1], SlotRange::new(100, 110));
    }

    #[test]
    fn test_covered_slot_count() {
        let router = create_test_router();
        assert_eq!(router.covered_slot_count(), 0);

        let node_id = "node1".to_string();
        router.assign_slots(&node_id, &SlotRange::new(0, 999));

        assert_eq!(router.covered_slot_count(), 1000);
    }

    #[test]
    fn test_remove_node() {
        let router = create_test_router();
        let node_id = "node1".to_string();
        let addr: SocketAddr = "127.0.0.1:6379".parse().unwrap();

        router.set_node_address(node_id.clone(), addr);
        router.assign_slots(&node_id, &SlotRange::new(0, 100));

        assert_eq!(router.get_slot_owner(50), Some(node_id.clone()));

        router.remove_node(&node_id);

        assert!(router.get_slot_owner(50).is_none());
    }

    #[test]
    fn test_route_result_to_error_string() {
        let addr: SocketAddr = "127.0.0.1:6379".parse().unwrap();

        let moved = RouteResult::Moved { slot: 100, addr };
        assert_eq!(
            moved.to_error_string(),
            Some("MOVED 100 127.0.0.1:6379".to_string())
        );

        let ask = RouteResult::Ask { slot: 100, addr };
        assert_eq!(
            ask.to_error_string(),
            Some("ASK 100 127.0.0.1:6379".to_string())
        );

        let down = RouteResult::ClusterDown;
        assert_eq!(
            down.to_error_string(),
            Some("CLUSTERDOWN The cluster is down".to_string())
        );

        let local = RouteResult::Local;
        assert!(local.to_error_string().is_none());
    }

    #[test]
    fn test_migration_state() {
        let router = create_test_router();
        let node_id = router.node_id().clone();

        // Assign slot to ourselves
        router.set_slot_owner(100, Some(node_id.clone()));
        router.set_state(ClusterState::Ok);

        // Start migration
        router.start_migration(100, "target-node".to_string());

        // Should still route locally during migration
        let result = router.route_slot(100);
        assert!(matches!(result, RouteResult::Local));

        // Complete migration
        router.complete_migration(100, "target-node".to_string());

        // Now should redirect
        let target_addr: SocketAddr = "127.0.0.1:6380".parse().unwrap();
        router.set_node_address("target-node".to_string(), target_addr);

        let result2 = router.route_slot(100);
        match result2 {
            RouteResult::Moved { slot, addr } => {
                assert_eq!(slot, 100);
                assert_eq!(addr, target_addr);
            }
            _ => panic!("Expected Moved result"),
        }
    }

    #[test]
    fn test_cluster_state_display() {
        assert_eq!(ClusterState::Ok.to_string(), "ok");
        assert_eq!(ClusterState::Fail.to_string(), "fail");
        assert_eq!(ClusterState::Degraded.to_string(), "degraded");
        assert_eq!(ClusterState::Unknown.to_string(), "unknown");
    }
}
