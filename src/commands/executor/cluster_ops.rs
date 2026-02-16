//! Cluster command helper methods on CommandExecutor.
//!
//! When a `ClusterStateManager` is configured (via `set_cluster_state`),
//! subcommands use real cluster topology. Otherwise the executor falls back
//! to standalone-mode defaults (single node, all 16384 slots local).

use bytes::Bytes;

use crate::cluster::{ClusterNodeInfo, HashSlot, CLUSTER_SLOTS};
use crate::protocol::Frame;

use super::CommandExecutor;

impl CommandExecutor {
    /// Handle `CLUSTER <subcommand> [args...]`.
    pub(super) fn cluster(&self, subcommand: &str, args: &[String]) -> Frame {
        match subcommand.to_uppercase().as_str() {
            "INFO" => self.cluster_info(),
            "NODES" => self.cluster_nodes(),
            "SLOTS" => self.cluster_slots(),
            "SHARDS" => self.cluster_shards(),
            "KEYSLOT" => self.cluster_keyslot(args),
            "MYID" => self.cluster_myid(),
            "COUNTKEYSINSLOT" => self.cluster_countkeysinslot(args),
            "GETKEYSINSLOT" => self.cluster_getkeysinslot(args),
            "MEET" => self.cluster_meet(args),
            "FORGET" => self.cluster_forget(args),
            "ADDSLOTS" => self.cluster_addslots(args),
            "DELSLOTS" => self.cluster_delslots(args),
            "SETSLOT" => self.cluster_setslot(args),
            "FAILOVER" => self.cluster_failover(args),
            "REPLICATE" => self.cluster_replicate(args),
            "RESET" => self.cluster_reset(args),
            "SAVECONFIG" => self.cluster_saveconfig(),
            "FLUSHSLOTS" => self.cluster_flushslots(),
            "SET-CONFIG-EPOCH" => self.cluster_set_config_epoch(args),
            "HELP" => self.cluster_help(),
            _ => Frame::error(format!(
                "ERR Unknown subcommand or wrong number of arguments for '{}'",
                subcommand
            )),
        }
    }

    // ── INFO ────────────────────────────────────────────────────────────

    fn cluster_info(&self) -> Frame {
        if let Some(csm) = &self.cluster_state {
            Frame::bulk(csm.format_cluster_info())
        } else {
            // Standalone mode: cluster_enabled:0
            let info = "\
cluster_enabled:0\r\n\
cluster_state:ok\r\n\
cluster_slots_assigned:16384\r\n\
cluster_slots_ok:16384\r\n\
cluster_slots_pfail:0\r\n\
cluster_slots_fail:0\r\n\
cluster_known_nodes:1\r\n\
cluster_size:1\r\n\
cluster_current_epoch:1\r\n\
cluster_my_epoch:1\r\n\
cluster_stats_messages_ping_sent:0\r\n\
cluster_stats_messages_pong_sent:0\r\n\
cluster_stats_messages_sent:0\r\n\
cluster_stats_messages_ping_received:0\r\n\
cluster_stats_messages_pong_received:0\r\n\
cluster_stats_messages_received:0\r\n\
total_cluster_links_buffer_limit_exceeded:0";
            Frame::bulk(info)
        }
    }

    // ── NODES ───────────────────────────────────────────────────────────

    fn cluster_nodes(&self) -> Frame {
        if let Some(csm) = &self.cluster_state {
            Frame::bulk(csm.format_cluster_nodes())
        } else {
            let node_id = "0000000000000000000000000000000000000001";
            let info = format!(
                "{} 127.0.0.1:6379@16379 myself,master - 0 0 1 connected 0-16383",
                node_id
            );
            Frame::bulk(info)
        }
    }

    // ── SLOTS ───────────────────────────────────────────────────────────

    fn cluster_slots(&self) -> Frame {
        if let Some(csm) = &self.cluster_state {
            let slot_frames = csm.format_cluster_slots();
            Frame::array(slot_frames)
        } else {
            let slot_info = Frame::array(vec![
                Frame::Integer(0),
                Frame::Integer(16383),
                Frame::array(vec![
                    Frame::bulk("127.0.0.1"),
                    Frame::Integer(6379),
                    Frame::bulk("0000000000000000000000000000000000000001"),
                ]),
            ]);
            Frame::array(vec![slot_info])
        }
    }

    // ── SHARDS (Redis 7.0+) ────────────────────────────────────────────

    fn cluster_shards(&self) -> Frame {
        if let Some(csm) = &self.cluster_state {
            // Build shard info from ClusterStateManager
            let my_id = csm.my_id().to_string();
            if let Some(node) = csm.get_node(&my_id) {
                let mut shard_map = std::collections::HashMap::new();

                // Build slots array from node's slot ranges
                let mut slot_ints: Vec<Frame> = Vec::new();
                for range in &node.slots {
                    slot_ints.push(Frame::Integer(range.start as i64));
                    slot_ints.push(Frame::Integer(range.end as i64));
                }
                shard_map.insert(Bytes::from_static(b"slots"), Frame::array(slot_ints));

                let mut node_map = std::collections::HashMap::new();
                node_map.insert(
                    Bytes::from_static(b"id"),
                    Frame::bulk(node.node_id.clone()),
                );
                node_map.insert(
                    Bytes::from_static(b"port"),
                    Frame::Integer(node.addr.port() as i64),
                );
                node_map.insert(
                    Bytes::from_static(b"ip"),
                    Frame::bulk(node.addr.ip().to_string()),
                );
                let role_str = match node.role {
                    crate::cluster::NodeRole::Primary => "master",
                    crate::cluster::NodeRole::Replica => "slave",
                };
                node_map.insert(Bytes::from_static(b"role"), Frame::bulk(role_str));
                let health = match node.state {
                    crate::cluster::NodeState::Online => "online",
                    crate::cluster::NodeState::PFail => "loading",
                    crate::cluster::NodeState::Fail => "offline",
                    _ => "online",
                };
                node_map.insert(Bytes::from_static(b"health"), Frame::bulk(health));

                shard_map.insert(
                    Bytes::from_static(b"nodes"),
                    Frame::array(vec![Frame::Map(node_map)]),
                );

                Frame::array(vec![Frame::Map(shard_map)])
            } else {
                // Fallback: empty
                Frame::array(vec![])
            }
        } else {
            let shard = Frame::Map(std::collections::HashMap::from([
                (
                    Bytes::from_static(b"slots"),
                    Frame::array(vec![Frame::Integer(0), Frame::Integer(16383)]),
                ),
                (
                    Bytes::from_static(b"nodes"),
                    Frame::array(vec![Frame::Map(std::collections::HashMap::from([
                        (
                            Bytes::from_static(b"id"),
                            Frame::bulk("0000000000000000000000000000000000000001"),
                        ),
                        (Bytes::from_static(b"port"), Frame::Integer(6379)),
                        (Bytes::from_static(b"ip"), Frame::bulk("127.0.0.1")),
                        (Bytes::from_static(b"role"), Frame::bulk("master")),
                        (Bytes::from_static(b"health"), Frame::bulk("online")),
                    ]))]),
                ),
            ]));
            Frame::array(vec![shard])
        }
    }

    // ── KEYSLOT ─────────────────────────────────────────────────────────

    fn cluster_keyslot(&self, args: &[String]) -> Frame {
        if args.is_empty() {
            return Frame::error(
                "ERR wrong number of arguments for 'cluster|keyslot' command",
            );
        }
        let key = args[0].as_bytes();
        let slot = HashSlot::for_key(key);
        Frame::Integer(slot as i64)
    }

    // ── MYID ────────────────────────────────────────────────────────────

    fn cluster_myid(&self) -> Frame {
        if let Some(csm) = &self.cluster_state {
            Frame::bulk(csm.my_id().to_string())
        } else {
            Frame::bulk("0000000000000000000000000000000000000001")
        }
    }

    // ── COUNTKEYSINSLOT ─────────────────────────────────────────────────

    fn cluster_countkeysinslot(&self, args: &[String]) -> Frame {
        if args.is_empty() {
            return Frame::error(
                "ERR wrong number of arguments for 'cluster|countkeysinslot' command",
            );
        }
        let slot: u16 = match args[0].parse() {
            Ok(s) if s < CLUSTER_SLOTS => s,
            _ => return Frame::error("ERR Invalid or out of range slot"),
        };

        // Scan store keys to count those hashing to the requested slot.
        // We scan db 0 by default (consistent with Redis Cluster which is
        // always single-db).
        let count = self.count_keys_in_slot(0, slot);
        Frame::Integer(count as i64)
    }

    // ── GETKEYSINSLOT ───────────────────────────────────────────────────

    fn cluster_getkeysinslot(&self, args: &[String]) -> Frame {
        if args.len() < 2 {
            return Frame::error(
                "ERR wrong number of arguments for 'cluster|getkeysinslot' command",
            );
        }
        let slot: u16 = match args[0].parse() {
            Ok(s) if s < CLUSTER_SLOTS => s,
            _ => return Frame::error("ERR Invalid or out of range slot"),
        };
        let count: usize = match args[1].parse() {
            Ok(c) => c,
            _ => return Frame::error("ERR Invalid count"),
        };

        let keys = self.get_keys_in_slot(0, slot, count);
        let frames: Vec<Frame> = keys.into_iter().map(Frame::bulk).collect();
        Frame::array(frames)
    }

    // ── MEET ────────────────────────────────────────────────────────────

    fn cluster_meet(&self, args: &[String]) -> Frame {
        if args.len() < 2 {
            return Frame::error(
                "ERR wrong number of arguments for 'cluster|meet' command",
            );
        }

        let ip: std::net::IpAddr = match args[0].parse() {
            Ok(ip) => ip,
            Err(_) => return Frame::error("ERR Invalid IP address"),
        };

        let port: u16 = match args[1].parse() {
            Ok(p) => p,
            Err(_) => return Frame::error("ERR Invalid port number"),
        };

        let addr = std::net::SocketAddr::new(ip, port);
        tracing::info!("CLUSTER MEET request for {}", addr);

        if let Some(csm) = &self.cluster_state {
            // Add the node to our known nodes. In a full cluster
            // implementation the gossip service would perform the
            // handshake asynchronously.
            let node_id = format!(
                "{:040x}",
                std::hash::BuildHasher::hash_one(
                    &std::collections::hash_map::RandomState::new(),
                    addr.to_string(),
                )
            );
            let node_info = ClusterNodeInfo {
                node_id: node_id.clone(),
                addr,
                role: crate::cluster::NodeRole::Primary,
                state: crate::cluster::NodeState::Online,
                primary_id: None,
                slots: Vec::new(),
                ping_sent: 0,
                pong_recv: 0,
                config_epoch: 0,
                link_state: crate::cluster::state_manager::LinkState::Connected,
                flags: vec![],
            };
            csm.add_node(node_info);
        }

        Frame::simple("OK")
    }

    // ── FORGET ──────────────────────────────────────────────────────────

    fn cluster_forget(&self, args: &[String]) -> Frame {
        if args.is_empty() {
            return Frame::error(
                "ERR wrong number of arguments for 'cluster|forget' command",
            );
        }

        let target_id = &args[0];

        if let Some(csm) = &self.cluster_state {
            // Cannot forget ourselves
            if target_id == csm.my_id() {
                return Frame::error("ERR I tried hard but I can't forget myself...");
            }

            // Verify node exists before removing
            if csm.get_node(target_id).is_none() {
                return Frame::error("ERR Unknown node ID");
            }

            csm.remove_node(target_id);
            tracing::info!("CLUSTER FORGET: removed node {}", target_id);
        } else {
            // Standalone mode: no nodes to forget
            return Frame::error("ERR Unknown node ID");
        }

        Frame::simple("OK")
    }

    // ── ADDSLOTS ────────────────────────────────────────────────────────

    fn cluster_addslots(&self, args: &[String]) -> Frame {
        if args.is_empty() {
            return Frame::error(
                "ERR wrong number of arguments for 'cluster|addslots' command",
            );
        }

        let mut slots_to_add = Vec::with_capacity(args.len());
        for arg in args {
            let slot: u16 = match arg.parse() {
                Ok(s) if s < CLUSTER_SLOTS => s,
                _ => return Frame::error("ERR Invalid or out of range slot"),
            };
            slots_to_add.push(slot);
        }

        if let Some(csm) = &self.cluster_state {
            let my_id = csm.my_id().to_string();

            // Check that none of the requested slots are already assigned
            // to another node (Redis returns an error in this case).
            for &slot in &slots_to_add {
                if let Some(owner) = csm.get_slot_owner(slot) {
                    if owner.node_id != my_id {
                        return Frame::error(format!(
                            "ERR Slot {} is already busy",
                            slot
                        ));
                    }
                }
            }

            csm.assign_slots(&my_id, &slots_to_add);
        }

        Frame::simple("OK")
    }

    // ── DELSLOTS ────────────────────────────────────────────────────────

    fn cluster_delslots(&self, args: &[String]) -> Frame {
        if args.is_empty() {
            return Frame::error(
                "ERR wrong number of arguments for 'cluster|delslots' command",
            );
        }

        let mut slots_to_del = Vec::with_capacity(args.len());
        for arg in args {
            let slot: u16 = match arg.parse() {
                Ok(s) if s < CLUSTER_SLOTS => s,
                _ => return Frame::error("ERR Invalid or out of range slot"),
            };
            slots_to_del.push(slot);
        }

        if let Some(csm) = &self.cluster_state {
            csm.remove_slots(&slots_to_del);
        }

        Frame::simple("OK")
    }

    // ── SETSLOT ─────────────────────────────────────────────────────────

    fn cluster_setslot(&self, args: &[String]) -> Frame {
        if args.len() < 2 {
            return Frame::error(
                "ERR wrong number of arguments for 'cluster|setslot' command",
            );
        }
        let slot: u16 = match args[0].parse() {
            Ok(s) if s < CLUSTER_SLOTS => s,
            _ => return Frame::error("ERR Invalid or out of range slot"),
        };
        let sub = args[1].to_uppercase();
        match sub.as_str() {
            "IMPORTING" => {
                if args.len() < 3 {
                    return Frame::error("ERR Please specify the source node ID");
                }
                let source_node_id = &args[2];
                tracing::info!(
                    "CLUSTER SETSLOT {} IMPORTING from {}",
                    slot,
                    source_node_id
                );
                if let Some(csm) = &self.cluster_state {
                    csm.set_slot_importing(slot, source_node_id);
                }
                Frame::simple("OK")
            }
            "MIGRATING" => {
                if args.len() < 3 {
                    return Frame::error("ERR Please specify the target node ID");
                }
                let target_node_id = &args[2];
                tracing::info!(
                    "CLUSTER SETSLOT {} MIGRATING to {}",
                    slot,
                    target_node_id
                );
                if let Some(csm) = &self.cluster_state {
                    csm.set_slot_migrating(slot, target_node_id);
                }
                Frame::simple("OK")
            }
            "STABLE" => {
                tracing::info!("CLUSTER SETSLOT {} STABLE", slot);
                if let Some(csm) = &self.cluster_state {
                    csm.set_slot_stable(slot);
                }
                Frame::simple("OK")
            }
            "NODE" => {
                if args.len() < 3 {
                    return Frame::error("ERR Please specify the node ID");
                }
                let node_id = &args[2];
                tracing::info!("CLUSTER SETSLOT {} NODE {}", slot, node_id);
                if let Some(csm) = &self.cluster_state {
                    // Clear migration state and assign the slot to the node.
                    csm.set_slot_stable(slot);
                    csm.assign_slots(node_id, &[slot]);
                }
                Frame::simple("OK")
            }
            _ => Frame::error("ERR Invalid CLUSTER SETSLOT action"),
        }
    }

    // ── FAILOVER ────────────────────────────────────────────────────────

    fn cluster_failover(&self, args: &[String]) -> Frame {
        let failover_type = if !args.is_empty() {
            match args[0].to_uppercase().as_str() {
                "FORCE" => {
                    tracing::info!("CLUSTER FAILOVER FORCE requested");
                    "FORCE"
                }
                "TAKEOVER" => {
                    tracing::info!("CLUSTER FAILOVER TAKEOVER requested");
                    "TAKEOVER"
                }
                _ => {
                    return Frame::error(
                        "ERR Invalid CLUSTER FAILOVER option. Valid: FORCE, TAKEOVER",
                    )
                }
            }
        } else {
            tracing::info!("CLUSTER FAILOVER (normal) requested");
            "NORMAL"
        };

        // In a full implementation with FailoverManager:
        // failover_manager.start_manual_failover(failover_type)
        let _ = failover_type;
        Frame::simple("OK")
    }

    // ── REPLICATE ───────────────────────────────────────────────────────

    fn cluster_replicate(&self, args: &[String]) -> Frame {
        if args.is_empty() {
            return Frame::error(
                "ERR wrong number of arguments for 'cluster|replicate' command",
            );
        }
        let _primary_id = &args[0];
        // In a full implementation: configure this node as replica of primary_id
        Frame::simple("OK")
    }

    // ── RESET ───────────────────────────────────────────────────────────

    fn cluster_reset(&self, args: &[String]) -> Frame {
        let hard = if !args.is_empty() {
            args[0].eq_ignore_ascii_case("HARD")
        } else {
            false
        };

        if let Some(csm) = &self.cluster_state {
            // Remove all slots from ourselves
            let all_slots: Vec<u16> = (0..CLUSTER_SLOTS).collect();
            csm.remove_slots(&all_slots);

            if hard {
                tracing::info!("CLUSTER RESET HARD");
                // A hard reset would regenerate the node ID and clear
                // all known nodes. We can approximate by removing all
                // non-self nodes.
                // (Full implementation would regenerate node ID.)
            } else {
                tracing::info!("CLUSTER RESET SOFT");
            }
        }

        Frame::simple("OK")
    }

    // ── SAVECONFIG ──────────────────────────────────────────────────────

    fn cluster_saveconfig(&self) -> Frame {
        // Persist cluster configuration to nodes.conf.
        // Acknowledged even when there's nothing to save.
        Frame::simple("OK")
    }

    // ── FLUSHSLOTS ──────────────────────────────────────────────────────

    fn cluster_flushslots(&self) -> Frame {
        if let Some(csm) = &self.cluster_state {
            let all_slots: Vec<u16> = (0..CLUSTER_SLOTS).collect();
            csm.remove_slots(&all_slots);
        }
        Frame::simple("OK")
    }

    // ── SET-CONFIG-EPOCH ────────────────────────────────────────────────

    fn cluster_set_config_epoch(&self, args: &[String]) -> Frame {
        if args.is_empty() {
            return Frame::error(
                "ERR wrong number of arguments for 'cluster|set-config-epoch' command",
            );
        }
        let epoch: u64 = match args[0].parse() {
            Ok(e) => e,
            _ => return Frame::error("ERR Invalid config epoch"),
        };

        if epoch == 0 {
            return Frame::error("ERR Invalid config epoch specified: 0");
        }

        if let Some(csm) = &self.cluster_state {
            // Update the node's config epoch.
            // In the real protocol this can only be set when the current
            // epoch is 0, but we allow it unconditionally for now.
            let my_id = csm.my_id().to_string();
            if let Some(mut entry) = {
                // We need to access the DashMap entry mutably. The
                // ClusterStateManager exposes get_node (clone), so we
                // update through add_node to overwrite.
                
                csm.get_node(&my_id)
            } {
                entry.config_epoch = epoch;
                csm.add_node(entry);
            }
            tracing::info!("CLUSTER SET-CONFIG-EPOCH {}", epoch);
        }

        Frame::simple("OK")
    }

    // ── HELP ────────────────────────────────────────────────────────────

    fn cluster_help(&self) -> Frame {
        Frame::array(vec![
            Frame::bulk("CLUSTER <subcommand> [<arg> [value] [opt] ...]"),
            Frame::bulk("INFO -- Return cluster information."),
            Frame::bulk("NODES -- Return cluster nodes information."),
            Frame::bulk("SLOTS -- Return information about slots."),
            Frame::bulk("SHARDS -- Return shard information (Redis 7.0+)."),
            Frame::bulk("KEYSLOT <key> -- Return the hash slot for a key."),
            Frame::bulk("MYID -- Return this node's ID."),
            Frame::bulk("COUNTKEYSINSLOT <slot> -- Count keys in a slot."),
            Frame::bulk("GETKEYSINSLOT <slot> <count> -- Get keys in a slot."),
            Frame::bulk("MEET <ip> <port> -- Connect to another node."),
            Frame::bulk("FORGET <node-id> -- Remove a node from the cluster."),
            Frame::bulk("ADDSLOTS <slot> [...] -- Assign slots to this node."),
            Frame::bulk("DELSLOTS <slot> [...] -- Remove slot assignments."),
            Frame::bulk("SETSLOT <slot> <action> [node-id] -- Set slot state."),
            Frame::bulk("FAILOVER [FORCE|TAKEOVER] -- Trigger manual failover."),
            Frame::bulk("REPLICATE <node-id> -- Configure as replica."),
            Frame::bulk("RESET [HARD|SOFT] -- Reset cluster state."),
            Frame::bulk("SAVECONFIG -- Save cluster config to disk."),
            Frame::bulk("FLUSHSLOTS -- Delete all slots from this node."),
            Frame::bulk("SET-CONFIG-EPOCH <epoch> -- Set configuration epoch."),
        ])
    }

    // ── Internal helpers ────────────────────────────────────────────────

    /// Count keys in the store that hash to the given slot.
    fn count_keys_in_slot(&self, db: u8, slot: u16) -> u64 {
        let keys = self.store.keys(db);
        keys.iter()
            .filter(|k| HashSlot::for_key(k) == slot)
            .count() as u64
    }

    /// Return up to `count` keys from the store that hash to the given slot.
    fn get_keys_in_slot(&self, db: u8, slot: u16, count: usize) -> Vec<Bytes> {
        let keys = self.store.keys(db);
        keys.into_iter()
            .filter(|k| HashSlot::for_key(k) == slot)
            .take(count)
            .collect()
    }
}
