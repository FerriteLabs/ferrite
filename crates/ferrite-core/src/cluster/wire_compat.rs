//! Redis Cluster Wire Compatibility Layer
//!
//! Provides translation between Ferrite's internal cluster state and
//! Redis-compatible wire format for CLUSTER NODES, CLUSTER SLOTS,
//! CLUSTER SHARDS, and CLUSTER INFO responses.

#![allow(dead_code)]

use std::fmt::Write;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;

use super::routing::ClusterState;
use super::{ClusterNode, NodeRole, NodeState};
use crate::protocol::Frame;

// ---------------------------------------------------------------------------
// ClusterBusStats
// ---------------------------------------------------------------------------

/// Statistics for the cluster bus, tracking messages sent/received by type.
///
/// Used to populate the `CLUSTER INFO` response with bus-level counters.
#[derive(Debug, Clone, Default)]
pub struct ClusterBusStats {
    /// Total cluster bus messages sent.
    pub cluster_stats_messages_sent: u64,
    /// Total cluster bus messages received.
    pub cluster_stats_messages_received: u64,
    /// PING messages sent.
    pub cluster_stats_messages_ping_sent: u64,
    /// PING messages received.
    pub cluster_stats_messages_ping_received: u64,
    /// PONG messages sent.
    pub cluster_stats_messages_pong_sent: u64,
    /// PONG messages received.
    pub cluster_stats_messages_pong_received: u64,
    /// MEET messages sent.
    pub cluster_stats_messages_meet_sent: u64,
    /// MEET messages received.
    pub cluster_stats_messages_meet_received: u64,
    /// FAIL messages sent.
    pub cluster_stats_messages_fail_sent: u64,
    /// FAIL messages received.
    pub cluster_stats_messages_fail_received: u64,
    /// PUBLISH messages sent.
    pub cluster_stats_messages_publish_sent: u64,
    /// PUBLISH messages received.
    pub cluster_stats_messages_publish_received: u64,
    /// AUTH-REQ messages sent.
    pub cluster_stats_messages_auth_req_sent: u64,
    /// AUTH-REQ messages received.
    pub cluster_stats_messages_auth_req_received: u64,
    /// AUTH-ACK messages sent.
    pub cluster_stats_messages_auth_ack_sent: u64,
    /// AUTH-ACK messages received.
    pub cluster_stats_messages_auth_ack_received: u64,
    /// UPDATE messages sent.
    pub cluster_stats_messages_update_sent: u64,
    /// UPDATE messages received.
    pub cluster_stats_messages_update_received: u64,
    /// MFSTART messages sent.
    pub cluster_stats_messages_mfstart_sent: u64,
    /// MFSTART messages received.
    pub cluster_stats_messages_mfstart_received: u64,
}

// ---------------------------------------------------------------------------
// AtomicClusterBusStats
// ---------------------------------------------------------------------------

/// Thread-safe atomic counters backing [`ClusterBusStats`].
#[derive(Debug, Default)]
pub struct AtomicClusterBusStats {
    /// Total messages sent.
    pub messages_sent: AtomicU64,
    /// Total messages received.
    pub messages_received: AtomicU64,
    /// PING messages sent.
    pub ping_sent: AtomicU64,
    /// PING messages received.
    pub ping_received: AtomicU64,
    /// PONG messages sent.
    pub pong_sent: AtomicU64,
    /// PONG messages received.
    pub pong_received: AtomicU64,
    /// MEET messages sent.
    pub meet_sent: AtomicU64,
    /// MEET messages received.
    pub meet_received: AtomicU64,
    /// FAIL messages sent.
    pub fail_sent: AtomicU64,
    /// FAIL messages received.
    pub fail_received: AtomicU64,
    /// PUBLISH messages sent.
    pub publish_sent: AtomicU64,
    /// PUBLISH messages received.
    pub publish_received: AtomicU64,
    /// AUTH-REQ messages sent.
    pub auth_req_sent: AtomicU64,
    /// AUTH-REQ messages received.
    pub auth_req_received: AtomicU64,
    /// AUTH-ACK messages sent.
    pub auth_ack_sent: AtomicU64,
    /// AUTH-ACK messages received.
    pub auth_ack_received: AtomicU64,
    /// UPDATE messages sent.
    pub update_sent: AtomicU64,
    /// UPDATE messages received.
    pub update_received: AtomicU64,
    /// MFSTART messages sent.
    pub mfstart_sent: AtomicU64,
    /// MFSTART messages received.
    pub mfstart_received: AtomicU64,
}

impl AtomicClusterBusStats {
    /// Take a point-in-time snapshot of all counters.
    pub fn snapshot(&self) -> ClusterBusStats {
        ClusterBusStats {
            cluster_stats_messages_sent: self.messages_sent.load(Ordering::Relaxed),
            cluster_stats_messages_received: self.messages_received.load(Ordering::Relaxed),
            cluster_stats_messages_ping_sent: self.ping_sent.load(Ordering::Relaxed),
            cluster_stats_messages_ping_received: self.ping_received.load(Ordering::Relaxed),
            cluster_stats_messages_pong_sent: self.pong_sent.load(Ordering::Relaxed),
            cluster_stats_messages_pong_received: self.pong_received.load(Ordering::Relaxed),
            cluster_stats_messages_meet_sent: self.meet_sent.load(Ordering::Relaxed),
            cluster_stats_messages_meet_received: self.meet_received.load(Ordering::Relaxed),
            cluster_stats_messages_fail_sent: self.fail_sent.load(Ordering::Relaxed),
            cluster_stats_messages_fail_received: self.fail_received.load(Ordering::Relaxed),
            cluster_stats_messages_publish_sent: self.publish_sent.load(Ordering::Relaxed),
            cluster_stats_messages_publish_received: self.publish_received.load(Ordering::Relaxed),
            cluster_stats_messages_auth_req_sent: self.auth_req_sent.load(Ordering::Relaxed),
            cluster_stats_messages_auth_req_received: self.auth_req_received.load(Ordering::Relaxed),
            cluster_stats_messages_auth_ack_sent: self.auth_ack_sent.load(Ordering::Relaxed),
            cluster_stats_messages_auth_ack_received: self.auth_ack_received.load(Ordering::Relaxed),
            cluster_stats_messages_update_sent: self.update_sent.load(Ordering::Relaxed),
            cluster_stats_messages_update_received: self.update_received.load(Ordering::Relaxed),
            cluster_stats_messages_mfstart_sent: self.mfstart_sent.load(Ordering::Relaxed),
            cluster_stats_messages_mfstart_received: self.mfstart_received.load(Ordering::Relaxed),
        }
    }
}

// ---------------------------------------------------------------------------
// RedisClusterWireProtocol
// ---------------------------------------------------------------------------

/// Translates between Ferrite's internal cluster state and Redis-compatible
/// wire format for CLUSTER command responses.
pub struct RedisClusterWireProtocol;

impl RedisClusterWireProtocol {
    /// Format a `CLUSTER NODES` response matching Redis output exactly.
    ///
    /// Each line has the format:
    /// ```text
    /// <id> <ip:port@cport> <flags> <master> <ping-sent> <pong-recvd> <config-epoch> <link-state> <slot> <slot> ...
    /// ```
    pub fn encode_cluster_nodes_response(nodes: &[ClusterNode], self_id: &str) -> String {
        let mut output = String::new();

        for node in nodes {
            // Node ID
            let _ = write!(output, "{} ", node.id);

            // ip:port@cport
            let cport = node.addr.port() + 10000;
            let _ = write!(output, "{}:{}@{} ", node.addr.ip(), node.addr.port(), cport);

            // Flags
            let flags = Self::format_node_flags(node, self_id);
            let _ = write!(output, "{} ", flags);

            // Master node ID (or "-" if primary)
            match &node.primary_id {
                Some(primary) => {
                    let _ = write!(output, "{} ", primary);
                }
                None => {
                    let _ = write!(output, "- ");
                }
            }

            // ping-sent (ms timestamp)
            let ping_sent = node
                .last_ping
                .map(|t| t.elapsed().as_millis() as u64)
                .unwrap_or(0);
            let _ = write!(output, "{} ", ping_sent);

            // pong-recvd (ms timestamp)
            let pong_recvd = node
                .last_pong
                .map(|t| t.elapsed().as_millis() as u64)
                .unwrap_or(0);
            let _ = write!(output, "{} ", pong_recvd);

            // config-epoch
            let _ = write!(output, "{} ", node.config_epoch);

            // link-state
            let link_state = match node.state {
                NodeState::Online => "connected",
                _ => "disconnected",
            };
            let _ = write!(output, "{}", link_state);

            // Slot ranges (only for primaries)
            if node.role == NodeRole::Primary {
                for slot_range in &node.slots {
                    if slot_range.start == slot_range.end {
                        let _ = write!(output, " {}", slot_range.start);
                    } else {
                        let _ = write!(output, " {}-{}", slot_range.start, slot_range.end);
                    }
                }
            }

            output.push('\n');
        }

        output
    }

    /// Format a `CLUSTER SLOTS` response as a RESP array.
    ///
    /// Returns an array of arrays, each containing:
    /// `[start-slot, end-slot, [ip, port, node-id], [replica-ip, replica-port, replica-id] ...]`
    pub fn encode_cluster_slots_response(nodes: &[ClusterNode]) -> Vec<Frame> {
        let mut result = Vec::new();

        // Collect primary nodes with their slots
        let primaries: Vec<&ClusterNode> = nodes
            .iter()
            .filter(|n| n.role == NodeRole::Primary && !n.slots.is_empty())
            .collect();

        for primary in &primaries {
            for slot_range in &primary.slots {
                let mut entry = Vec::new();

                // Start slot
                entry.push(Frame::Integer(slot_range.start as i64));
                // End slot
                entry.push(Frame::Integer(slot_range.end as i64));

                // Primary node info: [ip, port, node-id]
                let primary_info = vec![
                    Frame::Bulk(Some(Bytes::from(primary.addr.ip().to_string()))),
                    Frame::Integer(primary.addr.port() as i64),
                    Frame::Bulk(Some(Bytes::from(primary.id.clone()))),
                ];
                entry.push(Frame::Array(Some(primary_info)));

                // Replica nodes for this primary
                let replicas: Vec<&ClusterNode> = nodes
                    .iter()
                    .filter(|n| {
                        n.role == NodeRole::Replica
                            && n.primary_id.as_deref() == Some(&primary.id)
                    })
                    .collect();

                for replica in replicas {
                    let replica_info = vec![
                        Frame::Bulk(Some(Bytes::from(replica.addr.ip().to_string()))),
                        Frame::Integer(replica.addr.port() as i64),
                        Frame::Bulk(Some(Bytes::from(replica.id.clone()))),
                    ];
                    entry.push(Frame::Array(Some(replica_info)));
                }

                result.push(Frame::Array(Some(entry)));
            }
        }

        result
    }

    /// Format a `CLUSTER SHARDS` response (Redis 7.0+).
    ///
    /// Returns an array of maps, each containing:
    /// ```text
    /// slots: [start, end, ...]
    /// nodes: [{id, port, ip, endpoint, role, replication-offset, health}, ...]
    /// ```
    pub fn encode_cluster_shards_response(nodes: &[ClusterNode]) -> Vec<Frame> {
        let mut result = Vec::new();

        // Group nodes by shard (primary + its replicas)
        let primaries: Vec<&ClusterNode> = nodes
            .iter()
            .filter(|n| n.role == NodeRole::Primary)
            .collect();

        for primary in &primaries {
            let mut shard_entry = Vec::new();

            // "slots" key
            shard_entry.push(Frame::Bulk(Some(Bytes::from_static(b"slots"))));

            // Slots array: [start1, end1, start2, end2, ...]
            let mut slots_array = Vec::new();
            for slot_range in &primary.slots {
                slots_array.push(Frame::Integer(slot_range.start as i64));
                slots_array.push(Frame::Integer(slot_range.end as i64));
            }
            shard_entry.push(Frame::Array(Some(slots_array)));

            // "nodes" key
            shard_entry.push(Frame::Bulk(Some(Bytes::from_static(b"nodes"))));

            // Nodes array
            let mut nodes_array = Vec::new();

            // Primary node entry
            nodes_array.push(Self::encode_shard_node_entry(primary));

            // Replica nodes
            let replicas: Vec<&ClusterNode> = nodes
                .iter()
                .filter(|n| {
                    n.role == NodeRole::Replica
                        && n.primary_id.as_deref() == Some(&primary.id)
                })
                .collect();

            for replica in replicas {
                nodes_array.push(Self::encode_shard_node_entry(replica));
            }

            shard_entry.push(Frame::Array(Some(nodes_array)));

            result.push(Frame::Array(Some(shard_entry)));
        }

        result
    }

    /// Format a `CLUSTER INFO` response matching Redis output.
    pub fn encode_cluster_info_response(
        state: &ClusterState,
        nodes: &[ClusterNode],
        self_node: &ClusterNode,
        stats: &ClusterBusStats,
    ) -> String {
        let cluster_state = match state {
            ClusterState::Ok | ClusterState::Degraded => "ok",
            _ => "fail",
        };

        // Count slots assigned
        let slots_assigned: usize = nodes
            .iter()
            .filter(|n| n.role == NodeRole::Primary)
            .map(|n| n.slot_count())
            .sum();

        let slots_ok = slots_assigned;
        let slots_pfail: usize = 0;
        let slots_fail: usize = 0;

        let known_nodes = nodes.len();
        let cluster_size = nodes
            .iter()
            .filter(|n| n.role == NodeRole::Primary && !n.slots.is_empty())
            .count();

        let my_epoch = self_node.config_epoch;
        let current_epoch = nodes.iter().map(|n| n.config_epoch).max().unwrap_or(0);

        let mut output = String::new();
        let _ = writeln!(output, "cluster_enabled:1");
        let _ = writeln!(output, "cluster_state:{}", cluster_state);
        let _ = writeln!(output, "cluster_slots_assigned:{}", slots_assigned);
        let _ = writeln!(output, "cluster_slots_ok:{}", slots_ok);
        let _ = writeln!(output, "cluster_slots_pfail:{}", slots_pfail);
        let _ = writeln!(output, "cluster_slots_fail:{}", slots_fail);
        let _ = writeln!(output, "cluster_known_nodes:{}", known_nodes);
        let _ = writeln!(output, "cluster_size:{}", cluster_size);
        let _ = writeln!(output, "cluster_current_epoch:{}", current_epoch);
        let _ = writeln!(output, "cluster_my_epoch:{}", my_epoch);
        let _ = writeln!(
            output,
            "cluster_stats_messages_sent:{}",
            stats.cluster_stats_messages_sent
        );
        let _ = writeln!(
            output,
            "cluster_stats_messages_received:{}",
            stats.cluster_stats_messages_received
        );
        let _ = writeln!(
            output,
            "cluster_stats_messages_ping_sent:{}",
            stats.cluster_stats_messages_ping_sent
        );
        let _ = writeln!(
            output,
            "cluster_stats_messages_ping_received:{}",
            stats.cluster_stats_messages_ping_received
        );
        let _ = writeln!(
            output,
            "cluster_stats_messages_pong_sent:{}",
            stats.cluster_stats_messages_pong_sent
        );
        let _ = writeln!(
            output,
            "cluster_stats_messages_pong_received:{}",
            stats.cluster_stats_messages_pong_received
        );
        let _ = writeln!(
            output,
            "cluster_stats_messages_meet_sent:{}",
            stats.cluster_stats_messages_meet_sent
        );
        let _ = writeln!(
            output,
            "cluster_stats_messages_meet_received:{}",
            stats.cluster_stats_messages_meet_received
        );
        let _ = writeln!(
            output,
            "cluster_stats_messages_fail_sent:{}",
            stats.cluster_stats_messages_fail_sent
        );
        let _ = writeln!(
            output,
            "cluster_stats_messages_fail_received:{}",
            stats.cluster_stats_messages_fail_received
        );
        let _ = writeln!(
            output,
            "cluster_stats_messages_publish_sent:{}",
            stats.cluster_stats_messages_publish_sent
        );
        let _ = writeln!(
            output,
            "cluster_stats_messages_publish_received:{}",
            stats.cluster_stats_messages_publish_received
        );
        let _ = writeln!(
            output,
            "cluster_stats_messages_auth_req_sent:{}",
            stats.cluster_stats_messages_auth_req_sent
        );
        let _ = writeln!(
            output,
            "cluster_stats_messages_auth_req_received:{}",
            stats.cluster_stats_messages_auth_req_received
        );
        let _ = writeln!(
            output,
            "cluster_stats_messages_auth_ack_sent:{}",
            stats.cluster_stats_messages_auth_ack_sent
        );
        let _ = writeln!(
            output,
            "cluster_stats_messages_auth_ack_received:{}",
            stats.cluster_stats_messages_auth_ack_received
        );
        let _ = writeln!(
            output,
            "cluster_stats_messages_update_sent:{}",
            stats.cluster_stats_messages_update_sent
        );
        let _ = writeln!(
            output,
            "cluster_stats_messages_update_received:{}",
            stats.cluster_stats_messages_update_received
        );
        let _ = writeln!(
            output,
            "cluster_stats_messages_mfstart_sent:{}",
            stats.cluster_stats_messages_mfstart_sent
        );
        let _ = write!(
            output,
            "cluster_stats_messages_mfstart_received:{}",
            stats.cluster_stats_messages_mfstart_received
        );

        output
    }

    // ── Private helpers ─────────────────────────────────────────────────

    /// Format node flags as a comma-separated string matching Redis output.
    fn format_node_flags(node: &ClusterNode, self_id: &str) -> String {
        let mut flags = Vec::new();

        if node.id == self_id {
            flags.push("myself");
        }

        match node.role {
            NodeRole::Primary => flags.push("master"),
            NodeRole::Replica => flags.push("slave"),
        }

        match node.state {
            NodeState::Fail => flags.push("fail"),
            NodeState::PFail => flags.push("fail?"),
            NodeState::Handshake => flags.push("handshake"),
            NodeState::NoAddr => flags.push("noaddr"),
            NodeState::Online => {}
        }

        if node.flags.noaddr && !flags.contains(&"noaddr") {
            flags.push("noaddr");
        }

        if flags.is_empty() {
            "noflags".to_string()
        } else {
            flags.join(",")
        }
    }

    /// Encode a single node entry for the CLUSTER SHARDS response.
    fn encode_shard_node_entry(node: &ClusterNode) -> Frame {
        let role_str = match node.role {
            NodeRole::Primary => "master",
            NodeRole::Replica => "replica",
        };
        let health = if node.is_healthy() { "online" } else { "offline" };

        let entry = vec![
            Frame::Bulk(Some(Bytes::from_static(b"id"))),
            Frame::Bulk(Some(Bytes::from(node.id.clone()))),
            Frame::Bulk(Some(Bytes::from_static(b"port"))),
            Frame::Integer(node.addr.port() as i64),
            Frame::Bulk(Some(Bytes::from_static(b"ip"))),
            Frame::Bulk(Some(Bytes::from(node.addr.ip().to_string()))),
            Frame::Bulk(Some(Bytes::from_static(b"endpoint"))),
            Frame::Bulk(Some(Bytes::from(node.addr.ip().to_string()))),
            Frame::Bulk(Some(Bytes::from_static(b"role"))),
            Frame::Bulk(Some(Bytes::from(role_str))),
            Frame::Bulk(Some(Bytes::from_static(b"replication-offset"))),
            Frame::Integer(0),
            Frame::Bulk(Some(Bytes::from_static(b"health"))),
            Frame::Bulk(Some(Bytes::from(health))),
        ];

        Frame::Array(Some(entry))
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use super::super::SlotRange;
    use super::*;

    fn make_primary(id: &str, addr: &str, slots: Vec<SlotRange>) -> ClusterNode {
        let mut node = ClusterNode::new(
            id.to_string(),
            addr.parse::<SocketAddr>().unwrap(),
        );
        node.role = NodeRole::Primary;
        node.slots = slots;
        node.config_epoch = 1;
        node
    }

    fn make_replica(id: &str, addr: &str, primary_id: &str) -> ClusterNode {
        let mut node = ClusterNode::new(
            id.to_string(),
            addr.parse::<SocketAddr>().unwrap(),
        );
        node.role = NodeRole::Replica;
        node.primary_id = Some(primary_id.to_string());
        node.config_epoch = 1;
        node
    }

    #[test]
    fn cluster_nodes_output_format() {
        let nodes = vec![
            make_primary(
                "node1-aaaa",
                "127.0.0.1:7000",
                vec![SlotRange::new(0, 5460)],
            ),
            make_primary(
                "node2-bbbb",
                "127.0.0.1:7001",
                vec![SlotRange::new(5461, 10922)],
            ),
        ];

        let output =
            RedisClusterWireProtocol::encode_cluster_nodes_response(&nodes, "node1-aaaa");

        assert!(output.contains("node1-aaaa"));
        assert!(output.contains("127.0.0.1:7000@17000"));
        assert!(output.contains("myself,master"));
        assert!(output.contains("0-5460"));
        assert!(output.contains("node2-bbbb"));
        assert!(output.contains("5461-10922"));
    }

    #[test]
    fn cluster_nodes_replica_shows_master_id() {
        let nodes = vec![
            make_primary(
                "primary1",
                "127.0.0.1:7000",
                vec![SlotRange::new(0, 16383)],
            ),
            make_replica("replica1", "127.0.0.1:7003", "primary1"),
        ];

        let output =
            RedisClusterWireProtocol::encode_cluster_nodes_response(&nodes, "primary1");

        let lines: Vec<&str> = output.trim().lines().collect();
        assert_eq!(lines.len(), 2);
        // Replica line should contain the primary ID
        let replica_line = lines.iter().find(|l| l.contains("replica1")).unwrap();
        assert!(replica_line.contains("slave"));
        assert!(replica_line.contains("primary1"));
    }

    #[test]
    fn cluster_slots_response_structure() {
        let nodes = vec![make_primary(
            "node1",
            "127.0.0.1:7000",
            vec![SlotRange::new(0, 5460)],
        )];

        let frames = RedisClusterWireProtocol::encode_cluster_slots_response(&nodes);
        assert_eq!(frames.len(), 1);

        if let Frame::Array(Some(entry)) = &frames[0] {
            // start slot, end slot, primary info
            assert!(entry.len() >= 3);
            assert_eq!(entry[0], Frame::Integer(0));
            assert_eq!(entry[1], Frame::Integer(5460));
        } else {
            panic!("expected array frame");
        }
    }

    #[test]
    fn cluster_shards_includes_replicas() {
        let nodes = vec![
            make_primary(
                "primary1",
                "127.0.0.1:7000",
                vec![SlotRange::new(0, 16383)],
            ),
            make_replica("replica1", "127.0.0.1:7003", "primary1"),
        ];

        let frames = RedisClusterWireProtocol::encode_cluster_shards_response(&nodes);
        assert_eq!(frames.len(), 1);

        if let Frame::Array(Some(shard)) = &frames[0] {
            // "slots", [slots...], "nodes", [nodes...]
            assert_eq!(shard.len(), 4);
            if let Frame::Array(Some(nodes_arr)) = &shard[3] {
                assert_eq!(nodes_arr.len(), 2); // primary + 1 replica
            } else {
                panic!("expected nodes array");
            }
        } else {
            panic!("expected shard array");
        }
    }

    #[test]
    fn cluster_info_contains_required_fields() {
        let nodes = vec![make_primary(
            "node1",
            "127.0.0.1:7000",
            vec![SlotRange::new(0, 16383)],
        )];
        let self_node = &nodes[0];
        let stats = ClusterBusStats::default();

        let output = RedisClusterWireProtocol::encode_cluster_info_response(
            &ClusterState::Ok,
            &nodes,
            self_node,
            &stats,
        );

        assert!(output.contains("cluster_enabled:1"));
        assert!(output.contains("cluster_state:ok"));
        assert!(output.contains("cluster_slots_assigned:16384"));
        assert!(output.contains("cluster_known_nodes:1"));
        assert!(output.contains("cluster_size:1"));
        assert!(output.contains("cluster_stats_messages_sent:0"));
        assert!(output.contains("cluster_stats_messages_received:0"));
    }

    #[test]
    fn cluster_info_fail_state() {
        let nodes = vec![];
        let self_node = make_primary("me", "127.0.0.1:7000", vec![]);
        let stats = ClusterBusStats::default();

        let output = RedisClusterWireProtocol::encode_cluster_info_response(
            &ClusterState::Fail,
            &nodes,
            &self_node,
            &stats,
        );

        assert!(output.contains("cluster_state:fail"));
    }

    #[test]
    fn format_node_flags_myself_master() {
        let node = make_primary(
            "self-node",
            "127.0.0.1:7000",
            vec![SlotRange::new(0, 100)],
        );
        let flags = RedisClusterWireProtocol::format_node_flags(&node, "self-node");
        assert_eq!(flags, "myself,master");
    }

    #[test]
    fn format_node_flags_slave() {
        let node = make_replica("replica1", "127.0.0.1:7001", "primary1");
        let flags = RedisClusterWireProtocol::format_node_flags(&node, "other");
        assert_eq!(flags, "slave");
    }

    #[test]
    fn format_node_flags_fail() {
        let mut node = make_primary("fail-node", "127.0.0.1:7002", vec![]);
        node.state = NodeState::Fail;
        let flags = RedisClusterWireProtocol::format_node_flags(&node, "other");
        assert_eq!(flags, "master,fail");
    }

    #[test]
    fn atomic_stats_snapshot() {
        let atomic = AtomicClusterBusStats::default();
        atomic.messages_sent.store(42, Ordering::Relaxed);
        atomic.ping_received.store(10, Ordering::Relaxed);

        let snap = atomic.snapshot();
        assert_eq!(snap.cluster_stats_messages_sent, 42);
        assert_eq!(snap.cluster_stats_messages_ping_received, 10);
        assert_eq!(snap.cluster_stats_messages_pong_sent, 0);
    }

    #[test]
    fn cluster_slots_with_replicas() {
        let nodes = vec![
            make_primary(
                "primary1",
                "127.0.0.1:7000",
                vec![SlotRange::new(0, 5460)],
            ),
            make_replica("replica1", "127.0.0.1:7003", "primary1"),
        ];

        let frames = RedisClusterWireProtocol::encode_cluster_slots_response(&nodes);
        assert_eq!(frames.len(), 1);

        if let Frame::Array(Some(entry)) = &frames[0] {
            // start, end, primary, replica = 4 entries
            assert_eq!(entry.len(), 4);
        } else {
            panic!("expected array frame");
        }
    }

    #[test]
    fn single_slot_range_no_dash() {
        let nodes = vec![make_primary(
            "node1",
            "127.0.0.1:7000",
            vec![SlotRange::single(42)],
        )];

        let output =
            RedisClusterWireProtocol::encode_cluster_nodes_response(&nodes, "other");
        // Single slot should appear as just "42" without a dash
        assert!(output.contains(" 42\n") || output.contains(" 42"));
        assert!(!output.contains("42-42"));
    }
}
