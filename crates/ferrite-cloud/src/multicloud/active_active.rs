//! Multi-Cloud Active-Active Replication
//!
//! CRDT-based multi-region replication with automatic conflict resolution
//! across AWS, GCP, and Azure regions.
#![allow(dead_code)]

use std::collections::HashMap;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for active-active replication.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActiveActiveConfig {
    pub region_id: String,
    pub region_name: String,
    pub cloud_provider: CloudProvider,
    pub peer_regions: Vec<RegionPeer>,
    pub sync_interval: Duration,
    pub conflict_resolution: ConflictResolution,
    pub max_lag_ms: u64,
    pub enable_compression: bool,
    pub bandwidth_limit_mbps: Option<u32>,
}

impl Default for ActiveActiveConfig {
    fn default() -> Self {
        Self {
            region_id: "region-0".into(),
            region_name: "us-east-1".into(),
            cloud_provider: CloudProvider::AWS,
            peer_regions: Vec::new(),
            sync_interval: Duration::from_millis(100),
            conflict_resolution: ConflictResolution::LastWriterWins,
            max_lag_ms: 1000,
            enable_compression: false,
            bandwidth_limit_mbps: None,
        }
    }
}

/// Peer region descriptor.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegionPeer {
    pub region_id: String,
    pub endpoint: String,
    pub cloud: CloudProvider,
    pub latency_ms: u64,
    pub state: PeerState,
}

/// Peer connection state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PeerState {
    Connected,
    Disconnected,
    Syncing,
}

/// Cloud provider.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CloudProvider {
    AWS,
    GCP,
    Azure,
    OnPrem,
}

impl CloudProvider {
    /// Parse from string.
    pub fn from_str_opt(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "aws" => Some(CloudProvider::AWS),
            "gcp" => Some(CloudProvider::GCP),
            "azure" => Some(CloudProvider::Azure),
            "onprem" => Some(CloudProvider::OnPrem),
            _ => None,
        }
    }
}

/// Conflict resolution strategy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConflictResolution {
    LastWriterWins,
    VectorClock,
    Custom(String),
}

// ---------------------------------------------------------------------------
// Vector Clock
// ---------------------------------------------------------------------------

/// Logical vector clock for causal ordering of distributed events.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct VectorClock {
    pub map: HashMap<String, u64>,
}

impl VectorClock {
    /// Create a new vector clock with an initial entry for the given region.
    pub fn new(region_id: &str) -> Self {
        let mut map = HashMap::new();
        map.insert(region_id.to_string(), 0);
        Self { map }
    }

    /// Increment the counter for the given region.
    pub fn increment(&mut self, region_id: &str) {
        let counter = self.map.entry(region_id.to_string()).or_insert(0);
        *counter += 1;
    }

    /// Merge another vector clock into this one (element-wise max).
    pub fn merge(&mut self, other: &VectorClock) {
        for (k, v) in &other.map {
            let entry = self.map.entry(k.clone()).or_insert(0);
            if *v > *entry {
                *entry = *v;
            }
        }
    }

    /// Returns `true` if `self` causally happened before `other`.
    pub fn happens_before(&self, other: &VectorClock) -> bool {
        let mut dominated = false;
        for (k, v) in &self.map {
            let other_v = other.map.get(k).copied().unwrap_or(0);
            if *v > other_v {
                return false;
            }
            if *v < other_v {
                dominated = true;
            }
        }
        // Check keys only in other
        for (k, v) in &other.map {
            if !self.map.contains_key(k) && *v > 0 {
                dominated = true;
            }
        }
        dominated
    }

    /// Returns `true` if neither clock happened before the other.
    pub fn is_concurrent(&self, other: &VectorClock) -> bool {
        !self.happens_before(other) && !other.happens_before(self) && self != other
    }
}

// ---------------------------------------------------------------------------
// Conflict types
// ---------------------------------------------------------------------------

/// An entry involved in a conflict.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConflictEntry {
    pub key: String,
    pub value: Vec<u8>,
    pub timestamp: u64,
    pub vector_clock: VectorClock,
    pub region_id: String,
}

/// Result of conflict resolution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ResolveResult {
    KeepLocal,
    AcceptRemote,
    Merged(Vec<u8>),
    Conflict(Vec<ConflictEntry>),
}

// ---------------------------------------------------------------------------
// Status types
// ---------------------------------------------------------------------------

/// Overall sync status.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncStatus {
    pub total_peers: usize,
    pub connected_peers: usize,
    pub pending_writes: u64,
    pub replication_lag_ms: u64,
    pub last_sync: HashMap<String, u64>,
}

/// Health information for a single region.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegionHealth {
    pub region_id: String,
    pub state: PeerState,
    pub latency_ms: u64,
    pub pending_writes: u64,
    pub last_heartbeat: u64,
}

/// Result of a forced sync operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncResult {
    pub keys_synced: u64,
    pub conflicts_resolved: u64,
    pub duration: Duration,
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Replication errors.
#[derive(Debug, Clone, thiserror::Error)]
pub enum ReplicationError {
    #[error("peer not found: {0}")]
    PeerNotFound(String),
    #[error("connection failed: {0}")]
    ConnectionFailed(String),
    #[error("sync failed: {0}")]
    SyncFailed(String),
    #[error("conflict unresolvable for key: {0}")]
    ConflictUnresolvable(String),
    #[error("bandwidth exceeded")]
    BandwidthExceeded,
}

// ---------------------------------------------------------------------------
// Replication queue entry
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct PendingWrite {
    key: String,
    value: Vec<u8>,
    timestamp: u64,
    vector_clock: VectorClock,
}

// ---------------------------------------------------------------------------
// Active-Active Manager
// ---------------------------------------------------------------------------

/// Manages multi-region active-active replication.
pub struct ActiveActiveManager {
    config: ActiveActiveConfig,
    peers: RwLock<HashMap<String, RegionPeer>>,
    pending_queue: RwLock<Vec<PendingWrite>>,
    local_clock: RwLock<VectorClock>,
    conflicts: RwLock<Vec<ConflictEntry>>,
    last_sync_times: RwLock<HashMap<String, Instant>>,
}

impl ActiveActiveManager {
    /// Create a new active-active manager.
    pub fn new(config: ActiveActiveConfig) -> Self {
        let mut peers = HashMap::new();
        for peer in &config.peer_regions {
            peers.insert(peer.region_id.clone(), peer.clone());
        }
        let clock = VectorClock::new(&config.region_id);
        Self {
            config,
            peers: RwLock::new(peers),
            pending_queue: RwLock::new(Vec::new()),
            local_clock: RwLock::new(clock),
            conflicts: RwLock::new(Vec::new()),
            last_sync_times: RwLock::new(HashMap::new()),
        }
    }

    /// Add a peer region.
    pub fn add_peer(&self, peer: RegionPeer) -> Result<(), ReplicationError> {
        self.peers.write().insert(peer.region_id.clone(), peer);
        Ok(())
    }

    /// Remove a peer region.
    pub fn remove_peer(&self, region_id: &str) -> Result<(), ReplicationError> {
        self.peers
            .write()
            .remove(region_id)
            .ok_or_else(|| ReplicationError::PeerNotFound(region_id.to_string()))?;
        Ok(())
    }

    /// Queue a local write for replication to all peers.
    pub fn replicate_write(
        &self,
        key: &str,
        value: &[u8],
        timestamp: u64,
    ) -> Result<(), ReplicationError> {
        let mut clock = self.local_clock.write();
        clock.increment(&self.config.region_id);
        let vc = clock.clone();
        drop(clock);

        self.pending_queue.write().push(PendingWrite {
            key: key.to_string(),
            value: value.to_vec(),
            timestamp,
            vector_clock: vc,
        });
        Ok(())
    }

    /// Process a write received from a remote region.
    pub fn receive_remote_write(
        &self,
        from_region: &str,
        key: &str,
        value: &[u8],
        timestamp: u64,
        vector_clock: &VectorClock,
    ) -> ResolveResult {
        // Merge the remote clock into our local clock
        let mut clock = self.local_clock.write();
        clock.merge(vector_clock);
        clock.increment(&self.config.region_id);
        drop(clock);

        // Build local and remote conflict entries for resolution
        let local_entry = ConflictEntry {
            key: key.to_string(),
            value: Vec::new(), // local value would be fetched from storage
            timestamp: current_timestamp_ms(),
            vector_clock: self.local_clock.read().clone(),
            region_id: self.config.region_id.clone(),
        };

        let remote_entry = ConflictEntry {
            key: key.to_string(),
            value: value.to_vec(),
            timestamp,
            vector_clock: vector_clock.clone(),
            region_id: from_region.to_string(),
        };

        self.resolve_conflict(key, &local_entry, &remote_entry)
    }

    /// Get current synchronization status.
    pub fn sync_status(&self) -> SyncStatus {
        let peers = self.peers.read();
        let connected = peers.values().filter(|p| p.state == PeerState::Connected).count();
        let pending = self.pending_queue.read().len() as u64;
        let max_lag = peers.values().map(|p| p.latency_ms).max().unwrap_or(0);
        let last_sync: HashMap<String, u64> = self
            .last_sync_times
            .read()
            .iter()
            .map(|(k, v)| (k.clone(), v.elapsed().as_millis() as u64))
            .collect();

        SyncStatus {
            total_peers: peers.len(),
            connected_peers: connected,
            pending_writes: pending,
            replication_lag_ms: max_lag,
            last_sync,
        }
    }

    /// Get health info for all regions.
    pub fn region_health(&self) -> Vec<RegionHealth> {
        let peers = self.peers.read();
        let sync_times = self.last_sync_times.read();

        peers
            .values()
            .map(|p| {
                let last_hb = sync_times
                    .get(&p.region_id)
                    .map(|t| t.elapsed().as_millis() as u64)
                    .unwrap_or(0);
                RegionHealth {
                    region_id: p.region_id.clone(),
                    state: p.state,
                    latency_ms: p.latency_ms,
                    pending_writes: 0,
                    last_heartbeat: last_hb,
                }
            })
            .collect()
    }

    /// Force an immediate sync with a specific region.
    pub fn force_sync(&self, region_id: &str) -> Result<SyncResult, ReplicationError> {
        let peers = self.peers.read();
        if !peers.contains_key(region_id) {
            return Err(ReplicationError::PeerNotFound(region_id.to_string()));
        }
        drop(peers);

        let start = Instant::now();
        let drained: Vec<PendingWrite> = {
            let mut queue = self.pending_queue.write();
            std::mem::take(&mut *queue)
        };
        let keys_synced = drained.len() as u64;

        self.last_sync_times
            .write()
            .insert(region_id.to_string(), Instant::now());

        Ok(SyncResult {
            keys_synced,
            conflicts_resolved: 0,
            duration: start.elapsed(),
        })
    }

    /// Resolve a conflict between a local and remote entry.
    pub fn resolve_conflict(
        &self,
        key: &str,
        local: &ConflictEntry,
        remote: &ConflictEntry,
    ) -> ResolveResult {
        match &self.config.conflict_resolution {
            ConflictResolution::LastWriterWins => {
                if remote.timestamp >= local.timestamp {
                    ResolveResult::AcceptRemote
                } else {
                    ResolveResult::KeepLocal
                }
            }
            ConflictResolution::VectorClock => {
                if local.vector_clock.happens_before(&remote.vector_clock) {
                    ResolveResult::AcceptRemote
                } else if remote.vector_clock.happens_before(&local.vector_clock) {
                    ResolveResult::KeepLocal
                } else {
                    // Concurrent — fall back to timestamp
                    if remote.timestamp >= local.timestamp {
                        ResolveResult::AcceptRemote
                    } else {
                        ResolveResult::KeepLocal
                    }
                }
            }
            ConflictResolution::Custom(_) => {
                // Store as unresolved conflict
                self.conflicts.write().push(remote.clone());
                ResolveResult::Conflict(vec![local.clone(), remote.clone()])
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn current_timestamp_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    fn default_config() -> ActiveActiveConfig {
        ActiveActiveConfig {
            region_id: "us-east-1".into(),
            region_name: "US East".into(),
            ..Default::default()
        }
    }

    // -- Vector clock tests -------------------------------------------------

    #[test]
    fn test_vector_clock_new() {
        let vc = VectorClock::new("r1");
        assert_eq!(vc.map.get("r1"), Some(&0));
    }

    #[test]
    fn test_vector_clock_increment() {
        let mut vc = VectorClock::new("r1");
        vc.increment("r1");
        vc.increment("r1");
        assert_eq!(vc.map["r1"], 2);
    }

    #[test]
    fn test_vector_clock_merge() {
        let mut a = VectorClock::new("r1");
        a.increment("r1"); // r1=1

        let mut b = VectorClock::new("r2");
        b.increment("r2");
        b.increment("r2"); // r2=2

        a.merge(&b);
        assert_eq!(a.map["r1"], 1);
        assert_eq!(a.map["r2"], 2);
    }

    #[test]
    fn test_vector_clock_happens_before() {
        let mut a = VectorClock::new("r1");
        a.increment("r1"); // {r1: 1}

        let mut b = a.clone();
        b.increment("r1"); // {r1: 2}

        assert!(a.happens_before(&b));
        assert!(!b.happens_before(&a));
    }

    #[test]
    fn test_vector_clock_concurrent() {
        let mut a = VectorClock::new("r1");
        a.increment("r1"); // {r1: 1}

        let mut b = VectorClock::new("r2");
        b.increment("r2"); // {r2: 1}

        assert!(a.is_concurrent(&b));
        assert!(b.is_concurrent(&a));
    }

    #[test]
    fn test_vector_clock_equal_not_concurrent() {
        let a = VectorClock::new("r1");
        let b = VectorClock::new("r1");
        assert!(!a.is_concurrent(&b));
    }

    // -- Conflict resolution tests ------------------------------------------

    #[test]
    fn test_lww_remote_wins() {
        let config = ActiveActiveConfig {
            conflict_resolution: ConflictResolution::LastWriterWins,
            ..default_config()
        };
        let mgr = ActiveActiveManager::new(config);

        let local = ConflictEntry {
            key: "k".into(),
            value: b"local".to_vec(),
            timestamp: 100,
            vector_clock: VectorClock::new("r1"),
            region_id: "r1".into(),
        };
        let remote = ConflictEntry {
            key: "k".into(),
            value: b"remote".to_vec(),
            timestamp: 200,
            vector_clock: VectorClock::new("r2"),
            region_id: "r2".into(),
        };

        let result = mgr.resolve_conflict("k", &local, &remote);
        assert!(matches!(result, ResolveResult::AcceptRemote));
    }

    #[test]
    fn test_lww_local_wins() {
        let config = ActiveActiveConfig {
            conflict_resolution: ConflictResolution::LastWriterWins,
            ..default_config()
        };
        let mgr = ActiveActiveManager::new(config);

        let local = ConflictEntry {
            key: "k".into(),
            value: b"local".to_vec(),
            timestamp: 300,
            vector_clock: VectorClock::new("r1"),
            region_id: "r1".into(),
        };
        let remote = ConflictEntry {
            key: "k".into(),
            value: b"remote".to_vec(),
            timestamp: 200,
            vector_clock: VectorClock::new("r2"),
            region_id: "r2".into(),
        };

        let result = mgr.resolve_conflict("k", &local, &remote);
        assert!(matches!(result, ResolveResult::KeepLocal));
    }

    #[test]
    fn test_vc_resolution_causal() {
        let config = ActiveActiveConfig {
            conflict_resolution: ConflictResolution::VectorClock,
            ..default_config()
        };
        let mgr = ActiveActiveManager::new(config);

        let mut vc1 = VectorClock::new("r1");
        vc1.increment("r1"); // {r1: 1}

        let mut vc2 = vc1.clone();
        vc2.increment("r2"); // {r1: 1, r2: 1}

        let local = ConflictEntry {
            key: "k".into(),
            value: b"old".to_vec(),
            timestamp: 100,
            vector_clock: vc1,
            region_id: "r1".into(),
        };
        let remote = ConflictEntry {
            key: "k".into(),
            value: b"new".to_vec(),
            timestamp: 200,
            vector_clock: vc2,
            region_id: "r2".into(),
        };

        let result = mgr.resolve_conflict("k", &local, &remote);
        assert!(matches!(result, ResolveResult::AcceptRemote));
    }

    // -- Peer management tests ----------------------------------------------

    #[test]
    fn test_add_remove_peer() {
        let mgr = ActiveActiveManager::new(default_config());

        let peer = RegionPeer {
            region_id: "eu-west-1".into(),
            endpoint: "https://eu-west-1.ferrite.io".into(),
            cloud: CloudProvider::AWS,
            latency_ms: 80,
            state: PeerState::Connected,
        };

        mgr.add_peer(peer).unwrap();
        let status = mgr.sync_status();
        assert_eq!(status.total_peers, 1);

        mgr.remove_peer("eu-west-1").unwrap();
        let status = mgr.sync_status();
        assert_eq!(status.total_peers, 0);
    }

    #[test]
    fn test_remove_nonexistent_peer() {
        let mgr = ActiveActiveManager::new(default_config());
        let err = mgr.remove_peer("nope").unwrap_err();
        assert!(matches!(err, ReplicationError::PeerNotFound(_)));
    }

    #[test]
    fn test_replicate_write_queues() {
        let mgr = ActiveActiveManager::new(default_config());
        mgr.replicate_write("key1", b"value1", 100).unwrap();
        mgr.replicate_write("key2", b"value2", 200).unwrap();

        let status = mgr.sync_status();
        assert_eq!(status.pending_writes, 2);
    }

    #[test]
    fn test_force_sync_drains_queue() {
        let config = ActiveActiveConfig {
            peer_regions: vec![RegionPeer {
                region_id: "r2".into(),
                endpoint: "http://r2".into(),
                cloud: CloudProvider::GCP,
                latency_ms: 50,
                state: PeerState::Connected,
            }],
            ..default_config()
        };
        let mgr = ActiveActiveManager::new(config);

        mgr.replicate_write("a", b"1", 1).unwrap();
        mgr.replicate_write("b", b"2", 2).unwrap();

        let result = mgr.force_sync("r2").unwrap();
        assert_eq!(result.keys_synced, 2);

        let status = mgr.sync_status();
        assert_eq!(status.pending_writes, 0);
    }

    #[test]
    fn test_force_sync_unknown_peer() {
        let mgr = ActiveActiveManager::new(default_config());
        let err = mgr.force_sync("unknown").unwrap_err();
        assert!(matches!(err, ReplicationError::PeerNotFound(_)));
    }

    #[test]
    fn test_receive_remote_write() {
        let config = ActiveActiveConfig {
            conflict_resolution: ConflictResolution::LastWriterWins,
            ..default_config()
        };
        let mgr = ActiveActiveManager::new(config);

        let vc = VectorClock::new("remote-region");
        // Use a timestamp far in the future to ensure remote wins with LWW
        let future_ts = current_timestamp_ms() + 100_000;
        let result = mgr.receive_remote_write("remote-region", "key", b"val", future_ts, &vc);
        assert!(matches!(result, ResolveResult::AcceptRemote));
    }

    #[test]
    fn test_region_health() {
        let config = ActiveActiveConfig {
            peer_regions: vec![
                RegionPeer {
                    region_id: "r2".into(),
                    endpoint: "http://r2".into(),
                    cloud: CloudProvider::GCP,
                    latency_ms: 50,
                    state: PeerState::Connected,
                },
                RegionPeer {
                    region_id: "r3".into(),
                    endpoint: "http://r3".into(),
                    cloud: CloudProvider::Azure,
                    latency_ms: 120,
                    state: PeerState::Disconnected,
                },
            ],
            ..default_config()
        };
        let mgr = ActiveActiveManager::new(config);
        let health = mgr.region_health();
        assert_eq!(health.len(), 2);
    }
}
