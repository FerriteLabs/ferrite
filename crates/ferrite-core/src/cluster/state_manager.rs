//! Cluster state manager
//!
//! Provides a centralized, thread-safe cluster state manager that tracks node
//! topology, slot ownership, and migration state. Generates Redis-compatible
//! responses for CLUSTER INFO, CLUSTER NODES, and CLUSTER SLOTS commands, and
//! determines MOVED/ASK redirects for incoming keys.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};

use dashmap::DashMap;
use parking_lot::RwLock;
use rand::Rng;
use serde::{Deserialize, Serialize};

use super::slots::{HashSlot, SlotRange, CLUSTER_SLOTS};
use super::{NodeRole, NodeState};
use crate::protocol::Frame;

/// Link state between this node and a peer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LinkState {
    /// Gossip link is connected.
    Connected,
    /// Gossip link is disconnected.
    Disconnected,
}

impl std::fmt::Display for LinkState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Connected => write!(f, "connected"),
            Self::Disconnected => write!(f, "disconnected"),
        }
    }
}

/// Outcome of checking whether a key should be handled locally or redirected.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RedirectResult {
    /// Process locally.
    Ok,
    /// Permanent redirect – the slot belongs to another node.
    Moved {
        /// The hash slot that was looked up.
        slot: u16,
        /// Address of the node that owns the slot.
        addr: SocketAddr,
    },
    /// Temporary redirect – the slot is being migrated.
    Ask {
        /// The hash slot that was looked up.
        slot: u16,
        /// Address of the node importing the slot.
        addr: SocketAddr,
    },
}

/// Per-node information stored in the cluster state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterNodeInfo {
    /// 40-character hex node identifier.
    pub node_id: String,
    /// Network address.
    pub addr: SocketAddr,
    /// Role of this node.
    pub role: NodeRole,
    /// Health state.
    pub state: NodeState,
    /// If replica, the ID of its primary.
    pub primary_id: Option<String>,
    /// Slot ranges owned (only meaningful for primaries).
    pub slots: Vec<SlotRange>,
    /// Last PING sent timestamp (ms).
    pub ping_sent: u64,
    /// Last PONG received timestamp (ms).
    pub pong_recv: u64,
    /// Configuration epoch.
    pub config_epoch: u64,
    /// Link state to this node.
    pub link_state: LinkState,
    /// Additional flags (e.g. "myself", "noaddr").
    pub flags: Vec<String>,
}

/// Cluster state encoded as a single byte for atomic access.
const CLUSTER_STATE_OK: u8 = 0;
const CLUSTER_STATE_FAIL: u8 = 1;

/// Central, thread-safe cluster state manager.
pub struct ClusterStateManager {
    my_node_id: String,
    nodes: DashMap<String, ClusterNodeInfo>,
    slot_ownership: RwLock<[Option<String>; CLUSTER_SLOTS as usize]>,
    cluster_state: AtomicU8,
    current_epoch: AtomicU64,
    my_epoch: AtomicU64,
    migrating_slots: DashMap<u16, String>,
    importing_slots: DashMap<u16, String>,
}

/// Generate a random 40-character lowercase hex string.
fn generate_node_id() -> String {
    let mut rng = rand::thread_rng();
    (0..40)
        .map(|_| format!("{:x}", rng.gen::<u8>() % 16))
        .collect()
}

impl ClusterStateManager {
    /// Create a new manager for a node listening on `my_addr`.
    pub fn new(my_addr: SocketAddr) -> Self {
        let my_node_id = generate_node_id();

        let manager = Self {
            my_node_id: my_node_id.clone(),
            nodes: DashMap::new(),
            slot_ownership: RwLock::new([const { None }; CLUSTER_SLOTS as usize]),
            cluster_state: AtomicU8::new(CLUSTER_STATE_OK),
            current_epoch: AtomicU64::new(1),
            my_epoch: AtomicU64::new(1),
            migrating_slots: DashMap::new(),
            importing_slots: DashMap::new(),
        };

        // Register ourselves.
        let self_info = ClusterNodeInfo {
            node_id: my_node_id,
            addr: my_addr,
            role: NodeRole::Primary,
            state: NodeState::Online,
            primary_id: None,
            slots: Vec::new(),
            ping_sent: 0,
            pong_recv: 0,
            config_epoch: 1,
            link_state: LinkState::Connected,
            flags: vec!["myself".to_string()],
        };
        manager.nodes.insert(self_info.node_id.clone(), self_info);

        manager
    }

    /// Return this node's 40-char hex identifier.
    pub fn my_id(&self) -> &str {
        &self.my_node_id
    }

    /// Register (or update) a node in the cluster.
    pub fn add_node(&self, info: ClusterNodeInfo) {
        self.nodes.insert(info.node_id.clone(), info);
    }

    /// Remove a node and clear any slots it owned.
    pub fn remove_node(&self, node_id: &str) {
        self.nodes.remove(node_id);
        let mut ownership = self.slot_ownership.write();
        for slot_ref in ownership.iter_mut() {
            if slot_ref.as_deref() == Some(node_id) {
                *slot_ref = None;
            }
        }
    }

    /// Get a snapshot of a node's info.
    pub fn get_node(&self, node_id: &str) -> Option<ClusterNodeInfo> {
        self.nodes.get(node_id).map(|r| r.value().clone())
    }

    /// Get the node that owns a given slot.
    pub fn get_slot_owner(&self, slot: u16) -> Option<ClusterNodeInfo> {
        let ownership = self.slot_ownership.read();
        let owner_id = ownership.get(slot as usize)?.as_ref()?;
        self.get_node(owner_id)
    }

    /// Assign a set of slots to a node, updating both the ownership array and the
    /// node's stored `slots` field.
    pub fn assign_slots(&self, node_id: &str, slots: &[u16]) {
        {
            let mut ownership = self.slot_ownership.write();
            for &slot in slots {
                if (slot as usize) < CLUSTER_SLOTS as usize {
                    ownership[slot as usize] = Some(node_id.to_string());
                }
            }
        }
        self.rebuild_node_slot_ranges(node_id);
    }

    /// Remove slots from ownership (set them to unassigned).
    pub fn remove_slots(&self, slots: &[u16]) {
        {
            let mut ownership = self.slot_ownership.write();
            for &slot in slots {
                if (slot as usize) < CLUSTER_SLOTS as usize {
                    ownership[slot as usize] = None;
                }
            }
        }
        // Rebuild ranges for every node that might be affected.
        let node_ids: Vec<String> = self.nodes.iter().map(|r| r.key().clone()).collect();
        for nid in node_ids {
            self.rebuild_node_slot_ranges(&nid);
        }
    }

    /// Mark a slot as migrating **to** `target` (this node still owns it).
    pub fn set_slot_migrating(&self, slot: u16, target: &str) {
        self.migrating_slots.insert(slot, target.to_string());
    }

    /// Mark a slot as importing **from** `source`.
    pub fn set_slot_importing(&self, slot: u16, source: &str) {
        self.importing_slots.insert(slot, source.to_string());
    }

    /// Clear migration/import state for a slot.
    pub fn set_slot_stable(&self, slot: u16) {
        self.migrating_slots.remove(&slot);
        self.importing_slots.remove(&slot);
    }

    /// Return a snapshot of all known nodes.
    pub fn all_nodes(&self) -> Vec<ClusterNodeInfo> {
        self.nodes
            .iter()
            .map(|entry| entry.value().clone())
            .collect()
    }

    /// Determine whether a key should be processed locally, or redirected.
    pub fn check_redirect(&self, key: &[u8]) -> RedirectResult {
        let slot = HashSlot::for_key(key);

        // If the cluster is in FAIL state, we still compute the redirect so
        // callers can decide what to do.

        // Check migrating: we own the slot but it is moving elsewhere.
        if let Some(target_id) = self.migrating_slots.get(&slot) {
            if let Some(target) = self.get_node(target_id.value()) {
                return RedirectResult::Ask {
                    slot,
                    addr: target.addr,
                };
            }
        }

        // Check importing: another node still owns the slot, but we are
        // pulling it in. This path isn't a redirect from our perspective;
        // the *source* node would redirect with MOVED.

        let ownership = self.slot_ownership.read();
        let owner_id = match &ownership[slot as usize] {
            Some(id) => id.clone(),
            None => {
                // Slot unassigned – treat as local (no redirect).
                return RedirectResult::Ok;
            }
        };
        drop(ownership);

        if owner_id == self.my_node_id {
            RedirectResult::Ok
        } else {
            match self.get_node(&owner_id) {
                Some(owner) => RedirectResult::Moved {
                    slot,
                    addr: owner.addr,
                },
                // Owner node unknown – fall back to local.
                None => RedirectResult::Ok,
            }
        }
    }

    /// Generate Redis-compatible `CLUSTER INFO` output.
    pub fn format_cluster_info(&self) -> String {
        let state_str = if self.cluster_state.load(Ordering::Relaxed) == CLUSTER_STATE_OK {
            "ok"
        } else {
            "fail"
        };

        let assigned: usize = self
            .slot_ownership
            .read()
            .iter()
            .filter(|s| s.is_some())
            .count();

        let total_nodes = self.nodes.len();
        let primary_count = self
            .nodes
            .iter()
            .filter(|r| r.value().role == NodeRole::Primary)
            .count();

        let my_epoch = self.my_epoch.load(Ordering::Relaxed);
        let current_epoch = self.current_epoch.load(Ordering::Relaxed);

        format!(
            "cluster_enabled:1\r\n\
             cluster_state:{state_str}\r\n\
             cluster_slots_assigned:{assigned}\r\n\
             cluster_slots_ok:{assigned}\r\n\
             cluster_slots_pfail:0\r\n\
             cluster_slots_fail:0\r\n\
             cluster_known_nodes:{total_nodes}\r\n\
             cluster_size:{primary_count}\r\n\
             cluster_current_epoch:{current_epoch}\r\n\
             cluster_my_epoch:{my_epoch}\r\n\
             cluster_stats_messages_sent:0\r\n\
             cluster_stats_messages_received:0\r\n"
        )
    }

    /// Generate Redis-compatible `CLUSTER NODES` output.
    ///
    /// Each line follows the format:
    /// `<id> <ip>:<port>@<cport> <flags> <master> <ping-sent> <pong-recv> <config-epoch> <link-state> <slot>...`
    pub fn format_cluster_nodes(&self) -> String {
        let mut lines: Vec<String> = Vec::new();

        for entry in self.nodes.iter() {
            let node = entry.value();

            let flags = if node.flags.is_empty() {
                match node.role {
                    NodeRole::Primary => "master".to_string(),
                    NodeRole::Replica => "slave".to_string(),
                }
            } else {
                let role_flag = match node.role {
                    NodeRole::Primary => "master",
                    NodeRole::Replica => "slave",
                };
                let mut all: Vec<&str> = node.flags.iter().map(|s| s.as_str()).collect();
                if !all.contains(&role_flag) {
                    all.push(role_flag);
                }
                all.join(",")
            };

            let master_id = node.primary_id.as_deref().unwrap_or("-");
            let cport = node.addr.port() + 10000;

            let slot_strs: Vec<String> = node.slots.iter().map(|r| r.to_string()).collect();
            let slots_part = if slot_strs.is_empty() {
                String::new()
            } else {
                format!(" {}", slot_strs.join(" "))
            };

            lines.push(format!(
                "{id} {ip}:{port}@{cport} {flags} {master} {ping} {pong} {epoch} {link}{slots}",
                id = node.node_id,
                ip = node.addr.ip(),
                port = node.addr.port(),
                cport = cport,
                flags = flags,
                master = master_id,
                ping = node.ping_sent,
                pong = node.pong_recv,
                epoch = node.config_epoch,
                link = node.link_state,
                slots = slots_part,
            ));
        }

        lines.join("\n")
    }

    /// Generate Redis-compatible `CLUSTER SLOTS` output as a `Vec<Frame>`.
    ///
    /// Each entry is an array: `[start, end, [ip, port, id], ...]`
    pub fn format_cluster_slots(&self) -> Vec<Frame> {
        let mut result: Vec<Frame> = Vec::new();

        for entry in self.nodes.iter() {
            let node = entry.value();
            if node.role != NodeRole::Primary {
                continue;
            }
            for range in &node.slots {
                let node_info = Frame::array(vec![
                    Frame::bulk(node.addr.ip().to_string()),
                    Frame::integer(node.addr.port() as i64),
                    Frame::bulk(node.node_id.clone()),
                ]);

                let slot_entry = Frame::array(vec![
                    Frame::integer(range.start as i64),
                    Frame::integer(range.end as i64),
                    node_info,
                ]);

                result.push(slot_entry);
            }
        }

        result
    }

    /// Return the number of keys in a given slot.
    ///
    /// The state manager does not have direct access to the data store, so this
    /// always returns 0. Callers that need accurate counts should query the
    /// storage engine directly.
    pub fn count_keys_in_slot(&self, _slot: u16) -> u64 {
        0
    }

    /// Check whether the cluster considers itself healthy.
    pub fn cluster_is_ok(&self) -> bool {
        self.cluster_state.load(Ordering::Relaxed) == CLUSTER_STATE_OK
    }

    // ── private helpers ──────────────────────────────────────────────

    /// Rebuild the `slots` field on a node by scanning `slot_ownership`.
    fn rebuild_node_slot_ranges(&self, node_id: &str) {
        let ownership = self.slot_ownership.read();
        let mut owned: Vec<u16> = ownership
            .iter()
            .enumerate()
            .filter_map(|(i, owner)| {
                if owner.as_deref() == Some(node_id) {
                    Some(i as u16)
                } else {
                    None
                }
            })
            .collect();
        drop(ownership);

        owned.sort_unstable();

        let ranges = Self::compress_to_ranges(&owned);

        if let Some(mut entry) = self.nodes.get_mut(node_id) {
            entry.value_mut().slots = ranges;
        }
    }

    /// Compress a sorted list of slot numbers into contiguous `SlotRange`s.
    fn compress_to_ranges(slots: &[u16]) -> Vec<SlotRange> {
        if slots.is_empty() {
            return Vec::new();
        }
        let mut ranges = Vec::new();
        let mut start = slots[0];
        let mut end = slots[0];
        for &s in slots.iter().skip(1) {
            if s == end + 1 {
                end = s;
            } else {
                ranges.push(SlotRange::new(start, end));
                start = s;
                end = s;
            }
        }
        ranges.push(SlotRange::new(start, end));
        ranges
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_addr(port: u16) -> SocketAddr {
        SocketAddr::from(([127, 0, 0, 1], port))
    }

    fn make_manager() -> ClusterStateManager {
        ClusterStateManager::new(make_addr(6379))
    }

    // ── Slot assignment & lookup ─────────────────────────────────────

    #[test]
    fn test_assign_and_lookup_slots() {
        let mgr = make_manager();
        let my_id = mgr.my_id().to_string();

        mgr.assign_slots(&my_id, &[0, 1, 2, 3, 100]);

        let owner = mgr.get_slot_owner(2).expect("slot 2 should have owner");
        assert_eq!(owner.node_id, my_id);

        let owner100 = mgr.get_slot_owner(100).expect("slot 100 should have owner");
        assert_eq!(owner100.node_id, my_id);

        // Unassigned slot returns None.
        assert!(mgr.get_slot_owner(50).is_none());
    }

    #[test]
    fn test_remove_slots() {
        let mgr = make_manager();
        let my_id = mgr.my_id().to_string();

        mgr.assign_slots(&my_id, &[10, 11, 12]);
        assert!(mgr.get_slot_owner(11).is_some());

        mgr.remove_slots(&[11]);
        assert!(mgr.get_slot_owner(11).is_none());
        // Neighbours are still assigned.
        assert!(mgr.get_slot_owner(10).is_some());
        assert!(mgr.get_slot_owner(12).is_some());
    }

    // ── MOVED redirect ──────────────────────────────────────────────

    #[test]
    fn test_moved_redirect_when_another_node_owns_slot() {
        let mgr = make_manager();

        // Add a remote node.
        let remote_addr = make_addr(6380);
        let remote = ClusterNodeInfo {
            node_id: "remote000000000000000000000000000000000001".to_string(),
            addr: remote_addr,
            role: NodeRole::Primary,
            state: NodeState::Online,
            primary_id: None,
            slots: Vec::new(),
            ping_sent: 0,
            pong_recv: 0,
            config_epoch: 1,
            link_state: LinkState::Connected,
            flags: vec![],
        };
        let remote_id = remote.node_id.clone();
        mgr.add_node(remote);

        // Assign all slots to the remote node.
        let all_slots: Vec<u16> = (0..CLUSTER_SLOTS).collect();
        mgr.assign_slots(&remote_id, &all_slots);

        // Any key should produce a MOVED redirect.
        let result = mgr.check_redirect(b"hello");
        match result {
            RedirectResult::Moved { addr, .. } => assert_eq!(addr, remote_addr),
            other => panic!("Expected Moved, got {:?}", other),
        }
    }

    // ── ASK redirect during migration ───────────────────────────────

    #[test]
    fn test_ask_redirect_during_migration() {
        let mgr = make_manager();
        let my_id = mgr.my_id().to_string();

        // We own slot 866 (the slot for key "hello").
        let slot = HashSlot::for_key(b"hello");
        mgr.assign_slots(&my_id, &[slot]);

        // Add a target node for migration.
        let target_addr = make_addr(6381);
        let target = ClusterNodeInfo {
            node_id: "target00000000000000000000000000000000001".to_string(),
            addr: target_addr,
            role: NodeRole::Primary,
            state: NodeState::Online,
            primary_id: None,
            slots: Vec::new(),
            ping_sent: 0,
            pong_recv: 0,
            config_epoch: 1,
            link_state: LinkState::Connected,
            flags: vec![],
        };
        let target_id = target.node_id.clone();
        mgr.add_node(target);

        // Mark the slot as migrating.
        mgr.set_slot_migrating(slot, &target_id);

        let result = mgr.check_redirect(b"hello");
        match result {
            RedirectResult::Ask { addr, slot: s } => {
                assert_eq!(addr, target_addr);
                assert_eq!(s, slot);
            }
            other => panic!("Expected Ask, got {:?}", other),
        }

        // After stabilising, should be Ok again.
        mgr.set_slot_stable(slot);
        assert_eq!(mgr.check_redirect(b"hello"), RedirectResult::Ok);
    }

    // ── CLUSTER INFO formatting ─────────────────────────────────────

    #[test]
    fn test_format_cluster_info() {
        let mgr = make_manager();
        let my_id = mgr.my_id().to_string();

        mgr.assign_slots(&my_id, &[0, 1, 2]);

        let info = mgr.format_cluster_info();
        assert!(info.contains("cluster_enabled:1"));
        assert!(info.contains("cluster_state:ok"));
        assert!(info.contains("cluster_slots_assigned:3"));
        assert!(info.contains("cluster_known_nodes:1"));
        assert!(info.contains("cluster_size:1"));
    }

    // ── CLUSTER NODES formatting ────────────────────────────────────

    #[test]
    fn test_format_cluster_nodes() {
        let mgr = make_manager();
        let my_id = mgr.my_id().to_string();

        mgr.assign_slots(&my_id, &[0, 1, 2, 3, 4]);

        let nodes_str = mgr.format_cluster_nodes();
        // Should contain the node id and the "myself" flag.
        assert!(nodes_str.contains(&my_id));
        assert!(nodes_str.contains("myself"));
        // Should contain the slot range "0-4".
        assert!(nodes_str.contains("0-4"));
    }

    // ── CLUSTER SLOTS formatting ────────────────────────────────────

    #[test]
    fn test_format_cluster_slots() {
        let mgr = make_manager();
        let my_id = mgr.my_id().to_string();

        mgr.assign_slots(&my_id, &[0, 1, 2, 3, 4]);

        let frames = mgr.format_cluster_slots();
        // We assigned one contiguous range so expect one entry.
        assert_eq!(frames.len(), 1);

        // The entry should be an array.
        match &frames[0] {
            Frame::Array(Some(inner)) => {
                assert_eq!(inner.len(), 3); // start, end, node-info
                assert_eq!(inner[0], Frame::integer(0));
                assert_eq!(inner[1], Frame::integer(4));
            }
            other => panic!("Expected Array frame, got {:?}", other),
        }
    }

    // ── Misc ─────────────────────────────────────────────────────────

    #[test]
    fn test_cluster_is_ok_default() {
        let mgr = make_manager();
        assert!(mgr.cluster_is_ok());
    }

    #[test]
    fn test_node_id_length() {
        let mgr = make_manager();
        assert_eq!(mgr.my_id().len(), 40);
    }

    #[test]
    fn test_add_and_remove_node() {
        let mgr = make_manager();
        let info = ClusterNodeInfo {
            node_id: "abcd0000000000000000000000000000000000ef".to_string(),
            addr: make_addr(7000),
            role: NodeRole::Primary,
            state: NodeState::Online,
            primary_id: None,
            slots: Vec::new(),
            ping_sent: 0,
            pong_recv: 0,
            config_epoch: 1,
            link_state: LinkState::Connected,
            flags: vec![],
        };
        let nid = info.node_id.clone();

        mgr.add_node(info);
        assert!(mgr.get_node(&nid).is_some());

        mgr.remove_node(&nid);
        assert!(mgr.get_node(&nid).is_none());
    }

    #[test]
    fn test_local_key_returns_ok() {
        let mgr = make_manager();
        let my_id = mgr.my_id().to_string();

        let slot = HashSlot::for_key(b"mykey");
        mgr.assign_slots(&my_id, &[slot]);

        assert_eq!(mgr.check_redirect(b"mykey"), RedirectResult::Ok);
    }
}
