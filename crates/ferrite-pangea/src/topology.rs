//! NUMA topology: route allocations across multiple per-node allocators.
//!
//! Real CXL pools expose multiple memory devices, each with its own
//! latency/bandwidth profile.  `NumaTopology` wraps N allocators and
//! routes each `allocate(key, payload)` to the node selected by a
//! pluggable [`RoutingPolicy`].

use crate::allocator::{AllocError, CxlAllocator, PageId};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RoutingPolicy {
    /// Hash the key, modulo the number of nodes.
    HashMod,
    /// Always pick the node with the most free pages.
    LeastUsed,
    /// Round-robin across nodes (useful for benchmarks).
    RoundRobin,
}

pub type NodeId = usize;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Locator {
    pub node: NodeId,
    pub page: PageId,
}

pub struct NumaTopology<A: CxlAllocator> {
    nodes: Vec<Arc<A>>,
    policy: RwLock<RoutingPolicy>,
    rr_cursor: parking_lot::Mutex<usize>,
    /// Maps user keys to where their data lives, so [`Self::read`]/[`Self::free`]
    /// can find it without callers tracking nodes manually.
    index: RwLock<HashMap<String, Locator>>,
}

impl<A: CxlAllocator> std::fmt::Debug for NumaTopology<A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NumaTopology")
            .field("nodes", &self.nodes.len())
            .field("policy", &*self.policy.read())
            .field("indexed_keys", &self.index.read().len())
            .finish()
    }
}

impl<A: CxlAllocator> NumaTopology<A> {
    pub fn new(nodes: Vec<Arc<A>>, policy: RoutingPolicy) -> Self {
        assert!(!nodes.is_empty(), "NumaTopology requires at least one node");
        Self {
            nodes,
            policy: RwLock::new(policy),
            rr_cursor: parking_lot::Mutex::new(0),
            index: RwLock::default(),
        }
    }

    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    /// Return the current routing policy.
    pub fn routing_policy(&self) -> RoutingPolicy {
        *self.policy.read()
    }

    /// Change the routing policy. Existing keys are NOT re-routed; only
    /// new allocations use the new policy.
    pub fn set_routing_policy(&self, policy: RoutingPolicy) {
        *self.policy.write() = policy;
    }

    /// Return all currently-allocated key names.
    pub fn keys(&self) -> Vec<String> {
        self.index.read().keys().cloned().collect()
    }

    /// Return allocated keys whose names start with `prefix`.
    pub fn keys_with_prefix(&self, prefix: &str) -> Vec<String> {
        self.index
            .read()
            .keys()
            .filter(|k| k.starts_with(prefix))
            .cloned()
            .collect()
    }

    /// Return per-node allocator stats for a specific node.
    pub fn node_stats(&self, node: NodeId) -> Option<crate::allocator::AllocStats> {
        self.nodes.get(node).map(|n| n.stats())
    }

    /// Return keys allocated on a specific node.
    pub fn node_keys(&self, node: NodeId) -> Vec<String> {
        self.index
            .read()
            .iter()
            .filter(|(_, loc)| loc.node == node)
            .map(|(k, _)| k.clone())
            .collect()
    }

    /// Allocate `payload` on a specific node, bypassing the routing policy.
    pub fn allocate_on_node(
        &self,
        key: &str,
        payload: &[u8],
        node: NodeId,
    ) -> Result<Locator, AllocError> {
        if node >= self.nodes.len() {
            return Err(AllocError::PageNotFound(node as PageId));
        }
        let page = self.nodes[node].allocate(payload)?;
        let locator = Locator { node, page };
        self.index.write().insert(key.to_string(), locator);
        Ok(locator)
    }

    /// Per-node free bytes.
    pub fn node_free_bytes(&self, node: NodeId) -> Option<usize> {
        self.nodes.get(node).map(|n| {
            let s = n.stats();
            s.bytes_total.saturating_sub(s.bytes_used)
        })
    }

    fn pick_node(&self, key: &str) -> NodeId {
        match *self.policy.read() {
            RoutingPolicy::HashMod => hash_str(key) % self.nodes.len(),
            RoutingPolicy::LeastUsed => self
                .nodes
                .iter()
                .enumerate()
                .min_by_key(|(_, n)| n.stats().pages_used)
                .map_or(0, |(i, _)| i),
            RoutingPolicy::RoundRobin => {
                let mut c = self.rr_cursor.lock();
                let n = *c % self.nodes.len();
                *c = c.wrapping_add(1);
                n
            }
        }
    }

    pub fn allocate(&self, key: &str, payload: &[u8]) -> Result<Locator, AllocError> {
        let node = self.pick_node(key);
        let page = self.nodes[node].allocate(payload)?;
        let locator = Locator { node, page };
        self.index.write().insert(key.to_string(), locator);
        Ok(locator)
    }

    pub fn read(&self, key: &str) -> Option<Vec<u8>> {
        let loc = *self.index.read().get(key)?;
        self.nodes[loc.node].read(loc.page).ok()
    }

    pub fn free(&self, key: &str) -> bool {
        let Some(loc) = self.index.write().remove(key) else {
            return false;
        };
        self.nodes[loc.node].free(loc.page).is_ok()
    }

    pub fn locator(&self, key: &str) -> Option<Locator> {
        self.index.read().get(key).copied()
    }

    /// Total free bytes across the topology.
    pub fn free_bytes(&self) -> usize {
        self.nodes
            .iter()
            .map(|n| {
                let s = n.stats();
                s.bytes_total.saturating_sub(s.bytes_used)
            })
            .sum()
    }

    /// Dump `(key, value)` for every currently-allocated key.  Returns
    /// `Err` if any allocator read fails.  Used for snapshots — the
    /// caller can replay these via [`Self::allocate`] on a fresh topology.
    pub fn snapshot_keys(&self) -> Vec<(String, Vec<u8>)> {
        let index = self.index.read();
        index
            .iter()
            .filter_map(|(k, loc)| {
                self.nodes[loc.node]
                    .read(loc.page)
                    .ok()
                    .map(|bytes| (k.clone(), bytes))
            })
            .collect()
    }

    /// Allocate every entry from a previous [`Self::snapshot_keys`] dump.
    /// Routing follows the current `RoutingPolicy` — keys may land on
    /// different nodes than they did before if topology changed.  Existing
    /// allocations for the same key are freed first.  Returns the count of
    /// successfully-restored entries.
    pub fn replay_keys(&self, entries: Vec<(String, Vec<u8>)>) -> usize {
        let mut restored = 0;
        for (k, v) in entries {
            self.free(&k);
            if self.allocate(&k, &v).is_ok() {
                restored += 1;
            }
        }
        restored
    }
}

fn hash_str(s: &str) -> usize {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash as _, Hasher};
    let mut h = DefaultHasher::new();
    s.hash(&mut h);
    #[allow(clippy::cast_possible_truncation)]
    let v = h.finish() as usize;
    v
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::allocator::InMemoryCxlAllocator;

    fn topology(nodes: usize, policy: RoutingPolicy) -> NumaTopology<InMemoryCxlAllocator> {
        let n: Vec<Arc<InMemoryCxlAllocator>> = (0..nodes)
            .map(|_| InMemoryCxlAllocator::shared(8 * 1024, 256))
            .collect();
        NumaTopology::new(n, policy)
    }

    #[test]
    fn allocate_and_read_round_trip() {
        let t = topology(2, RoutingPolicy::HashMod);
        t.allocate("k", b"v").unwrap();
        assert_eq!(t.read("k"), Some(b"v".to_vec()));
    }

    #[test]
    fn hash_mod_is_deterministic() {
        let t = topology(4, RoutingPolicy::HashMod);
        let a = t.allocate("alpha", b"x").unwrap();
        t.free("alpha");
        let b = t.allocate("alpha", b"y").unwrap();
        assert_eq!(a.node, b.node);
    }

    #[test]
    fn least_used_picks_emptiest_node() {
        let t = topology(3, RoutingPolicy::LeastUsed);
        // Pre-load node 0 with 5 pages, node 1 with 1 page.
        for i in 0..5 {
            t.nodes[0].allocate(format!("seed{i}").as_bytes()).unwrap();
        }
        t.nodes[1].allocate(b"seed").unwrap();
        let loc = t.allocate("new", b"v").unwrap();
        assert_eq!(loc.node, 2); // emptiest
    }

    #[test]
    fn round_robin_cycles_nodes() {
        let t = topology(3, RoutingPolicy::RoundRobin);
        let nodes: Vec<NodeId> = (0..6)
            .map(|i| t.allocate(&format!("k{i}"), b"v").unwrap().node)
            .collect();
        assert_eq!(nodes, vec![0, 1, 2, 0, 1, 2]);
    }

    #[test]
    fn free_releases_underlying_page() {
        let t = topology(2, RoutingPolicy::HashMod);
        let loc = t.allocate("k", b"v").unwrap();
        let used_before = t.nodes[loc.node].stats().pages_used;
        assert!(t.free("k"));
        let used_after = t.nodes[loc.node].stats().pages_used;
        assert_eq!(used_before - 1, used_after);
        assert!(t.read("k").is_none());
    }

    #[test]
    fn free_bytes_sums_across_nodes() {
        let t = topology(2, RoutingPolicy::HashMod);
        let total = t.free_bytes();
        assert_eq!(total, 2 * 8 * 1024);
        t.allocate("k", b"v").unwrap();
        assert!(t.free_bytes() < total);
    }

    #[test]
    fn snapshot_and_replay_round_trip() {
        let t1 = topology(2, RoutingPolicy::HashMod);
        t1.allocate("alpha", b"AAA").unwrap();
        t1.allocate("beta", b"BBB").unwrap();
        let dump = t1.snapshot_keys();
        assert_eq!(dump.len(), 2);

        let t2 = topology(2, RoutingPolicy::HashMod);
        let restored = t2.replay_keys(dump);
        assert_eq!(restored, 2);
        assert_eq!(t2.read("alpha"), Some(b"AAA".to_vec()));
        assert_eq!(t2.read("beta"), Some(b"BBB".to_vec()));
    }
}
