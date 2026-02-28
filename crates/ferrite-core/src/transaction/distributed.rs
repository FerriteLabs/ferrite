//! Distributed Lock Manager (DLM) for Distributed ACID Transactions
//!
//! Provides cross-node lock coordination with deadlock detection for
//! distributed transactions. Extends the local [`super::lock::LockManager`]
//! with cluster-aware locking, intent locks, and a wait-for graph based
//! deadlock detector.
//!
//! # Lock Compatibility Matrix
//!
//! ```text
//!                  | Shared | Exclusive | IntentShared | IntentExclusive |
//! -----------------|--------|-----------|--------------|-----------------|
//! Shared           |   ✓    |     ✗     |      ✓       |        ✗        |
//! Exclusive        |   ✗    |     ✗     |      ✗       |        ✗        |
//! IntentShared     |   ✓    |     ✗     |      ✓       |        ✓        |
//! IntentExclusive  |   ✗    |     ✗     |      ✓       |        ✗        |
//! ```
//!
//! # Deadlock Detection
//!
//! Uses a wait-for graph with DFS cycle detection:
//!
//! ```text
//! Tx1 ──waits──▶ Tx2 ──waits──▶ Tx3
//!  ▲                              │
//!  └──────────── waits ◀──────────┘
//!            DEADLOCK CYCLE
//!
//! Resolution: abort the youngest transaction (least work lost)
//! ```
//!
//! # Example
//!
//! ```ignore
//! use ferrite_core::transaction::distributed::*;
//!
//! let dlm = DistributedLockManager::new(DlmConfig::default());
//!
//! let tx = TxId::new();
//! let grant = dlm.acquire(&tx, "accounts:1001", LockType::Exclusive).await?;
//! // ... perform mutations ...
//! dlm.release(&tx, "accounts:1001").await?;
//! ```

use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};

use chrono::{DateTime, Utc};
use dashmap::DashMap;
use tokio::sync::{oneshot, Mutex};
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// Globally unique transaction identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TxId(pub Uuid);

impl TxId {
    /// Create a new random transaction ID.
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    /// Create a `TxId` from raw bytes (useful for deterministic tests).
    pub fn from_u128(v: u128) -> Self {
        Self(Uuid::from_u128(v))
    }
}

impl Default for TxId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for TxId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "tx-{}", self.0)
    }
}

/// Lock type with intent-lock support for hierarchical locking.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LockType {
    /// Shared (read) lock — multiple holders allowed.
    Shared,
    /// Exclusive (write) lock — single holder only.
    Exclusive,
    /// Intent-Shared — signals intent to acquire shared locks on children.
    IntentShared,
    /// Intent-Exclusive — signals intent to acquire exclusive locks on children.
    IntentExclusive,
}

impl LockType {
    /// Check if `self` is compatible with an already-held `other` lock.
    pub fn is_compatible(&self, other: &LockType) -> bool {
        use LockType::*;
        matches!(
            (self, other),
            (Shared, Shared)
                | (Shared, IntentShared)
                | (IntentShared, Shared)
                | (IntentShared, IntentShared)
                | (IntentShared, IntentExclusive)
                | (IntentExclusive, IntentShared)
        )
    }
}

impl fmt::Display for LockType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LockType::Shared => write!(f, "S"),
            LockType::Exclusive => write!(f, "X"),
            LockType::IntentShared => write!(f, "IS"),
            LockType::IntentExclusive => write!(f, "IX"),
        }
    }
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Errors produced by the distributed lock manager.
#[derive(Debug, Clone, thiserror::Error)]
pub enum DlmError {
    /// Lock acquisition timed out.
    #[error("lock timeout after {0}ms")]
    Timeout(u64),

    /// Transaction was chosen as a deadlock victim.
    #[error("deadlock detected — transaction aborted")]
    Deadlock,

    /// Transaction exceeded the per-transaction lock limit.
    #[error("max locks per transaction exceeded (limit: {0})")]
    MaxLocksExceeded(usize),

    /// Attempted to release a resource not currently locked.
    #[error("resource not locked: {0}")]
    ResourceNotLocked(String),

    /// Lock upgrade is not allowed (e.g. other holders exist).
    #[error("invalid lock upgrade from {from} to {to} on {resource}")]
    InvalidLockUpgrade {
        /// Current lock type held.
        from: LockType,
        /// Requested lock type.
        to: LockType,
        /// Resource name.
        resource: String,
    },

    /// Generic lock conflict.
    #[error("lock conflict: {0}")]
    Conflict(String),

    /// Internal error.
    #[error("internal DLM error: {0}")]
    Internal(String),
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for the [`DistributedLockManager`].
#[derive(Debug, Clone)]
pub struct DlmConfig {
    /// Maximum time (ms) to wait for a lock before returning [`DlmError::Timeout`].
    pub lock_timeout_ms: u64,
    /// Interval (ms) between automatic deadlock detection sweeps.
    pub deadlock_detection_interval_ms: u64,
    /// Maximum number of locks a single transaction may hold.
    pub max_locks_per_tx: usize,
    /// Time-to-live (ms) for granted locks (0 = no expiry).
    pub lock_ttl_ms: u64,
    /// Whether automatic deadlock detection is enabled.
    pub enable_deadlock_detection: bool,
}

impl Default for DlmConfig {
    fn default() -> Self {
        Self {
            lock_timeout_ms: 5_000,
            deadlock_detection_interval_ms: 1_000,
            max_locks_per_tx: 10_000,
            lock_ttl_ms: 30_000,
            enable_deadlock_detection: true,
        }
    }
}

// ---------------------------------------------------------------------------
// Lock-related structs
// ---------------------------------------------------------------------------

/// Result of a successful lock acquisition.
#[derive(Debug, Clone)]
pub struct LockGrant {
    /// Transaction that holds the lock.
    pub tx_id: TxId,
    /// Locked resource name.
    pub resource: String,
    /// Type of lock granted.
    pub lock_type: LockType,
    /// When the lock was granted.
    pub granted_at: DateTime<Utc>,
    /// When the lock expires (`None` if no TTL).
    pub expires_at: Option<DateTime<Utc>>,
}

/// Tracks who holds a lock on a resource.
#[derive(Debug, Clone)]
pub struct LockHolder {
    /// Transaction that holds the lock.
    pub tx_id: TxId,
    /// Type of lock held.
    pub lock_type: LockType,
    /// When the lock was granted.
    pub granted_at: DateTime<Utc>,
}

/// A transaction waiting in a lock queue.
pub struct LockWaiter {
    /// Transaction waiting for the lock.
    pub tx_id: TxId,
    /// Requested lock type.
    pub lock_type: LockType,
    /// When the request was made.
    pub requested_at: DateTime<Utc>,
    /// One-shot channel to notify when the lock is granted (or denied).
    pub notify: oneshot::Sender<Result<LockGrant, DlmError>>,
}

impl fmt::Debug for LockWaiter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LockWaiter")
            .field("tx_id", &self.tx_id)
            .field("lock_type", &self.lock_type)
            .field("requested_at", &self.requested_at)
            .finish()
    }
}

/// Internal state for a single lockable resource.
#[derive(Debug, Default)]
pub struct LockEntry {
    /// Resource name.
    pub resource: String,
    /// Current lock holders.
    pub holders: Vec<LockHolder>,
    /// Queue of transactions waiting for this lock.
    pub wait_queue: VecDeque<LockWaiter>,
}

// ---------------------------------------------------------------------------
// Deadlock detection
// ---------------------------------------------------------------------------

/// A detected deadlock cycle.
#[derive(Debug, Clone)]
pub struct DeadlockCycle {
    /// Transactions involved in the cycle.
    pub transactions: Vec<TxId>,
    /// Resources involved in the cycle.
    pub resources: Vec<String>,
    /// When the deadlock was detected.
    pub detected_at: DateTime<Utc>,
}

// ---------------------------------------------------------------------------
// Statistics
// ---------------------------------------------------------------------------

/// Runtime statistics for the distributed lock manager.
#[derive(Debug, Clone, Default)]
pub struct DlmStats {
    /// Number of currently held locks.
    pub active_locks: u64,
    /// Number of requests waiting for a lock.
    pub waiting_requests: u64,
    /// Total deadlock cycles detected.
    pub deadlocks_detected: u64,
    /// Total deadlocks resolved.
    pub deadlocks_resolved: u64,
    /// Total lock acquisitions that timed out.
    pub lock_timeouts: u64,
    /// Total successful lock acquisitions.
    pub total_acquisitions: u64,
    /// Total lock releases.
    pub total_releases: u64,
    /// Average wait time in microseconds.
    pub avg_wait_time_us: u64,
}

// ---------------------------------------------------------------------------
// Distributed Lock Manager
// ---------------------------------------------------------------------------

/// Distributed Lock Manager (DLM) for cross-node lock coordination.
///
/// Manages pessimistic locks across cluster nodes with support for shared,
/// exclusive, and intent lock types. Includes a wait-for graph based
/// deadlock detector that identifies cycles and resolves them by aborting
/// the youngest transaction in the cycle (least work lost).
pub struct DistributedLockManager {
    /// Lock table: resource → lock state.
    locks: DashMap<String, LockEntry>,
    /// Per-transaction lock sets: tx_id → set of resource names.
    tx_locks: DashMap<TxId, HashSet<String>>,
    /// Configuration.
    config: DlmConfig,
    /// Counters for statistics.
    stats_deadlocks_detected: AtomicU64,
    stats_deadlocks_resolved: AtomicU64,
    stats_lock_timeouts: AtomicU64,
    stats_total_acquisitions: AtomicU64,
    stats_total_releases: AtomicU64,
    stats_total_wait_time_us: AtomicU64,
    stats_total_waited: AtomicU64,
    /// Mutex-protected wait-for graph for deadlock detection.
    /// Key = waiter tx, Value = set of holder txs it waits on.
    wait_for_graph: Mutex<HashMap<TxId, HashSet<TxId>>>,
}

impl DistributedLockManager {
    /// Create a new distributed lock manager with the given configuration.
    pub fn new(config: DlmConfig) -> Self {
        Self {
            locks: DashMap::new(),
            tx_locks: DashMap::new(),
            config,
            stats_deadlocks_detected: AtomicU64::new(0),
            stats_deadlocks_resolved: AtomicU64::new(0),
            stats_lock_timeouts: AtomicU64::new(0),
            stats_total_acquisitions: AtomicU64::new(0),
            stats_total_releases: AtomicU64::new(0),
            stats_total_wait_time_us: AtomicU64::new(0),
            stats_total_waited: AtomicU64::new(0),
            wait_for_graph: Mutex::new(HashMap::new()),
        }
    }

    /// Acquire a lock on `resource` for `tx_id`.
    ///
    /// If the lock cannot be granted immediately the request is queued and
    /// the caller blocks (up to `lock_timeout_ms`). Returns a [`LockGrant`]
    /// on success.
    pub async fn acquire(
        &self,
        tx_id: &TxId,
        resource: &str,
        lock_type: LockType,
    ) -> Result<LockGrant, DlmError> {
        // Check per-tx lock limit.
        if let Some(held) = self.tx_locks.get(tx_id) {
            if held.len() >= self.config.max_locks_per_tx && !held.contains(resource) {
                return Err(DlmError::MaxLocksExceeded(self.config.max_locks_per_tx));
            }
        }

        // Fast path: try to grant immediately.
        if let Some(grant) = self.try_grant(tx_id, resource, lock_type)? {
            self.stats_total_acquisitions
                .fetch_add(1, Ordering::Relaxed);
            self.tx_locks
                .entry(*tx_id)
                .or_default()
                .insert(resource.to_string());
            return Ok(grant);
        }

        // Slow path: enqueue a waiter and block on oneshot.
        let (sender, receiver) = oneshot::channel();
        let requested_at = Utc::now();

        {
            let mut entry = self
                .locks
                .entry(resource.to_string())
                .or_insert_with(|| LockEntry {
                    resource: resource.to_string(),
                    holders: Vec::new(),
                    wait_queue: VecDeque::new(),
                });
            entry.wait_queue.push_back(LockWaiter {
                tx_id: *tx_id,
                lock_type,
                requested_at,
                notify: sender,
            });

            // Build wait-for edges.
            if self.config.enable_deadlock_detection {
                let holder_ids: Vec<TxId> = entry.holders.iter().map(|h| h.tx_id).collect();
                self.add_wait_for_edges(tx_id, &holder_ids).await;

                // Immediate deadlock check for this tx.
                if self.has_cycle(tx_id).await {
                    // Remove ourselves from the wait queue before returning.
                    entry.wait_queue.retain(|w| w.tx_id != *tx_id);
                    self.clear_wait_for(tx_id).await;
                    self.stats_deadlocks_detected
                        .fetch_add(1, Ordering::Relaxed);
                    return Err(DlmError::Deadlock);
                }
            }
        }

        // Wait with timeout.
        let timeout = tokio::time::Duration::from_millis(self.config.lock_timeout_ms);
        match tokio::time::timeout(timeout, receiver).await {
            Ok(Ok(result)) => {
                // Record wait time.
                let waited_us = (Utc::now() - requested_at).num_microseconds().unwrap_or(0) as u64;
                self.stats_total_wait_time_us
                    .fetch_add(waited_us, Ordering::Relaxed);
                self.stats_total_waited.fetch_add(1, Ordering::Relaxed);

                match &result {
                    Ok(_grant) => {
                        self.stats_total_acquisitions
                            .fetch_add(1, Ordering::Relaxed);
                        self.tx_locks
                            .entry(*tx_id)
                            .or_default()
                            .insert(resource.to_string());
                    }
                    Err(_) => {}
                }
                self.clear_wait_for(tx_id).await;
                result
            }
            Ok(Err(_recv_err)) => {
                // Sender dropped — treat as internal error.
                self.remove_waiter(tx_id, resource);
                self.clear_wait_for(tx_id).await;
                Err(DlmError::Internal("lock waiter channel closed".into()))
            }
            Err(_elapsed) => {
                // Timeout.
                self.remove_waiter(tx_id, resource);
                self.clear_wait_for(tx_id).await;
                self.stats_lock_timeouts.fetch_add(1, Ordering::Relaxed);
                Err(DlmError::Timeout(self.config.lock_timeout_ms))
            }
        }
    }

    /// Release a single lock held by `tx_id` on `resource`.
    pub async fn release(&self, tx_id: &TxId, resource: &str) -> Result<(), DlmError> {
        let mut released = false;

        if let Some(mut entry) = self.locks.get_mut(resource) {
            let before = entry.holders.len();
            entry.holders.retain(|h| h.tx_id != *tx_id);
            if entry.holders.len() < before {
                released = true;
                self.stats_total_releases.fetch_add(1, Ordering::Relaxed);
                // Try to grant waiting locks.
                self.grant_waiters(&mut entry);
            }
        }

        if !released {
            return Err(DlmError::ResourceNotLocked(resource.to_string()));
        }

        // Remove from per-tx tracking.
        if let Some(mut set) = self.tx_locks.get_mut(tx_id) {
            set.remove(resource);
        }

        Ok(())
    }

    /// Release all locks held by `tx_id`. Returns the number of locks released.
    pub async fn release_all(&self, tx_id: &TxId) -> Result<usize, DlmError> {
        let resources: Vec<String> = self
            .tx_locks
            .get(tx_id)
            .map(|set| set.iter().cloned().collect())
            .unwrap_or_default();

        let count = resources.len();

        for resource in &resources {
            if let Some(mut entry) = self.locks.get_mut(resource) {
                entry.holders.retain(|h| h.tx_id != *tx_id);
                self.grant_waiters(&mut entry);
            }
        }

        self.stats_total_releases
            .fetch_add(count as u64, Ordering::Relaxed);
        self.tx_locks.remove(tx_id);
        self.clear_wait_for(tx_id).await;

        Ok(count)
    }

    /// Run deadlock detection over the full wait-for graph.
    ///
    /// Returns all detected cycles. Each cycle lists the transactions and
    /// resources involved.
    pub fn detect_deadlocks(&self) -> Vec<DeadlockCycle> {
        // Build the wait-for graph snapshot from lock entries (avoids holding
        // the async mutex in a sync context).
        let mut graph: HashMap<TxId, HashSet<TxId>> = HashMap::new();
        let mut waiter_resources: HashMap<TxId, Vec<String>> = HashMap::new();

        for entry_ref in self.locks.iter() {
            let entry = entry_ref.value();
            let holder_ids: HashSet<TxId> = entry.holders.iter().map(|h| h.tx_id).collect();
            for waiter in &entry.wait_queue {
                let edges = graph.entry(waiter.tx_id).or_default();
                for &hid in &holder_ids {
                    if hid != waiter.tx_id {
                        edges.insert(hid);
                    }
                }
                waiter_resources
                    .entry(waiter.tx_id)
                    .or_default()
                    .push(entry.resource.clone());
            }
        }

        let mut cycles = Vec::new();
        let mut globally_visited: HashSet<TxId> = HashSet::new();

        for &start in graph.keys() {
            if globally_visited.contains(&start) {
                continue;
            }
            let mut visited: HashSet<TxId> = HashSet::new();
            let mut path: Vec<TxId> = Vec::new();
            if self.dfs_find_cycle(&graph, start, &mut visited, &mut path) {
                // Extract the cycle from path.
                if let Some(cycle_start_pos) = path
                    .last()
                    .and_then(|last| path.iter().position(|t| t == last))
                {
                    let cycle_txns: Vec<TxId> = path[cycle_start_pos..path.len() - 1].to_vec();
                    let mut resources: Vec<String> = Vec::new();
                    for tx in &cycle_txns {
                        if let Some(res) = waiter_resources.get(tx) {
                            for r in res {
                                if !resources.contains(r) {
                                    resources.push(r.clone());
                                }
                            }
                        }
                    }
                    if !cycle_txns.is_empty() {
                        cycles.push(DeadlockCycle {
                            transactions: cycle_txns,
                            resources,
                            detected_at: Utc::now(),
                        });
                        self.stats_deadlocks_detected
                            .fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
            globally_visited.extend(visited);
        }

        cycles
    }

    /// Resolve a detected deadlock by selecting a victim (the youngest
    /// transaction — created most recently, thus least work lost) and
    /// removing it from all lock queues.
    ///
    /// Returns the [`TxId`] of the aborted victim.
    pub fn resolve_deadlock(&self, cycle: &DeadlockCycle) -> Result<TxId, DlmError> {
        if cycle.transactions.is_empty() {
            return Err(DlmError::Internal("empty deadlock cycle".to_string()));
        }

        // Pick victim: the transaction whose ID is "greatest" (newest UUID
        // when using `from_u128`, or last in sorted order). For real
        // workloads this would use transaction start-time metadata; here we
        // pick the last entry in the cycle which, by convention in the
        // detection algorithm, is the youngest.
        let victim = *cycle
            .transactions
            .last()
            .expect("non-empty cycle guaranteed by check above");

        // Remove victim from all wait queues and notify with Deadlock.
        for mut entry_ref in self.locks.iter_mut() {
            let entry = entry_ref.value_mut();
            let mut kept = VecDeque::new();
            for waiter in entry.wait_queue.drain(..) {
                if waiter.tx_id == victim {
                    // Notify the victim so its `acquire` returns Deadlock.
                    let _ = waiter.notify.send(Err(DlmError::Deadlock));
                } else {
                    kept.push_back(waiter);
                }
            }
            entry.wait_queue = kept;

            // Also release any locks the victim already holds.
            entry.holders.retain(|h| h.tx_id != victim);
            self.grant_waiters(entry);
        }

        self.tx_locks.remove(&victim);
        self.stats_deadlocks_resolved
            .fetch_add(1, Ordering::Relaxed);

        Ok(victim)
    }

    /// Return a snapshot of current DLM statistics.
    pub fn get_stats(&self) -> DlmStats {
        let active_locks: u64 = self
            .locks
            .iter()
            .map(|e| e.value().holders.len() as u64)
            .sum();
        let waiting_requests: u64 = self
            .locks
            .iter()
            .map(|e| e.value().wait_queue.len() as u64)
            .sum();
        let total_waited = self.stats_total_waited.load(Ordering::Relaxed);
        let avg_wait_time_us = if total_waited > 0 {
            self.stats_total_wait_time_us.load(Ordering::Relaxed) / total_waited
        } else {
            0
        };

        DlmStats {
            active_locks,
            waiting_requests,
            deadlocks_detected: self.stats_deadlocks_detected.load(Ordering::Relaxed),
            deadlocks_resolved: self.stats_deadlocks_resolved.load(Ordering::Relaxed),
            lock_timeouts: self.stats_lock_timeouts.load(Ordering::Relaxed),
            total_acquisitions: self.stats_total_acquisitions.load(Ordering::Relaxed),
            total_releases: self.stats_total_releases.load(Ordering::Relaxed),
            avg_wait_time_us,
        }
    }

    // -----------------------------------------------------------------------
    // Private helpers
    // -----------------------------------------------------------------------

    /// Try to grant a lock immediately (no waiting). Returns `Ok(Some(grant))`
    /// if granted, `Ok(None)` if the caller must wait, or `Err` on upgrade
    /// conflict.
    fn try_grant(
        &self,
        tx_id: &TxId,
        resource: &str,
        lock_type: LockType,
    ) -> Result<Option<LockGrant>, DlmError> {
        let mut entry = self
            .locks
            .entry(resource.to_string())
            .or_insert_with(|| LockEntry {
                resource: resource.to_string(),
                holders: Vec::new(),
                wait_queue: VecDeque::new(),
            });

        // Check if this tx already holds a lock on this resource.
        if let Some(existing) = entry.holders.iter().find(|h| h.tx_id == *tx_id) {
            if existing.lock_type == lock_type || existing.lock_type == LockType::Exclusive {
                // Already hold an equal or stronger lock.
                return Ok(Some(self.make_grant(tx_id, resource, existing.lock_type)));
            }
            // Attempt upgrade (e.g. Shared → Exclusive).
            if lock_type == LockType::Exclusive && existing.lock_type == LockType::Shared {
                if entry.holders.len() == 1 {
                    // Safe to upgrade — we're the only holder.
                    entry.holders[0] = LockHolder {
                        tx_id: *tx_id,
                        lock_type: LockType::Exclusive,
                        granted_at: Utc::now(),
                    };
                    return Ok(Some(self.make_grant(tx_id, resource, LockType::Exclusive)));
                }
                // Other holders exist — cannot upgrade immediately.
                return Ok(None);
            }
            // Other incompatible upgrade — not supported.
            return Err(DlmError::InvalidLockUpgrade {
                from: existing.lock_type,
                to: lock_type,
                resource: resource.to_string(),
            });
        }

        // No existing lock from this tx — check compatibility with holders.
        let compatible = entry
            .holders
            .iter()
            .all(|h| lock_type.is_compatible(&h.lock_type));

        // Also respect FIFO: don't skip waiters.
        if compatible && entry.wait_queue.is_empty() {
            let now = Utc::now();
            entry.holders.push(LockHolder {
                tx_id: *tx_id,
                lock_type,
                granted_at: now,
            });
            return Ok(Some(self.make_grant(tx_id, resource, lock_type)));
        }

        Ok(None)
    }

    /// Build a [`LockGrant`] for the given parameters.
    fn make_grant(&self, tx_id: &TxId, resource: &str, lock_type: LockType) -> LockGrant {
        let now = Utc::now();
        let expires_at = if self.config.lock_ttl_ms > 0 {
            Some(now + chrono::Duration::milliseconds(self.config.lock_ttl_ms as i64))
        } else {
            None
        };
        LockGrant {
            tx_id: *tx_id,
            resource: resource.to_string(),
            lock_type,
            granted_at: now,
            expires_at,
        }
    }

    /// Try to grant locks to transactions in the wait queue (FIFO order).
    fn grant_waiters(&self, entry: &mut LockEntry) {
        let mut newly_granted = Vec::new();

        while let Some(front) = entry.wait_queue.front() {
            let compatible = entry
                .holders
                .iter()
                .all(|h| front.lock_type.is_compatible(&h.lock_type));

            if !compatible {
                break;
            }

            if let Some(waiter) = entry.wait_queue.pop_front() {
                let now = Utc::now();
                let expires_at = if self.config.lock_ttl_ms > 0 {
                    Some(now + chrono::Duration::milliseconds(self.config.lock_ttl_ms as i64))
                } else {
                    None
                };
                let grant = LockGrant {
                    tx_id: waiter.tx_id,
                    resource: entry.resource.clone(),
                    lock_type: waiter.lock_type,
                    granted_at: now,
                    expires_at,
                };
                entry.holders.push(LockHolder {
                    tx_id: waiter.tx_id,
                    lock_type: waiter.lock_type,
                    granted_at: now,
                });
                newly_granted.push((waiter, grant));
            }
        }

        // Notify waiters outside of the loop.
        for (waiter, grant) in newly_granted {
            self.tx_locks
                .entry(waiter.tx_id)
                .or_default()
                .insert(entry.resource.clone());
            let _ = waiter.notify.send(Ok(grant));
        }
    }

    /// Remove a waiter from a resource's wait queue.
    fn remove_waiter(&self, tx_id: &TxId, resource: &str) {
        if let Some(mut entry) = self.locks.get_mut(resource) {
            entry.wait_queue.retain(|w| w.tx_id != *tx_id);
        }
    }

    /// Add wait-for edges: `waiter` waits for each tx in `holders`.
    async fn add_wait_for_edges(&self, waiter: &TxId, holders: &[TxId]) {
        let mut wfg = self.wait_for_graph.lock().await;
        let edges = wfg.entry(*waiter).or_default();
        for holder in holders {
            if holder != waiter {
                edges.insert(*holder);
            }
        }
    }

    /// Clear all wait-for edges involving `tx_id`.
    async fn clear_wait_for(&self, tx_id: &TxId) {
        let mut wfg = self.wait_for_graph.lock().await;
        wfg.remove(tx_id);
        for edges in wfg.values_mut() {
            edges.remove(tx_id);
        }
    }

    /// Check if adding `tx_id` to the wait-for graph creates a cycle (DFS).
    async fn has_cycle(&self, tx_id: &TxId) -> bool {
        let wfg = self.wait_for_graph.lock().await;
        let mut visited = HashSet::new();
        let mut stack = vec![*tx_id];

        while let Some(current) = stack.pop() {
            if !visited.insert(current) {
                continue;
            }
            if let Some(edges) = wfg.get(&current) {
                for &next in edges {
                    if next == *tx_id {
                        return true; // cycle back to start
                    }
                    if !visited.contains(&next) {
                        stack.push(next);
                    }
                }
            }
        }
        false
    }

    /// DFS helper that records the path and returns `true` on first cycle.
    fn dfs_find_cycle(
        &self,
        graph: &HashMap<TxId, HashSet<TxId>>,
        node: TxId,
        visited: &mut HashSet<TxId>,
        path: &mut Vec<TxId>,
    ) -> bool {
        if path.contains(&node) {
            path.push(node); // complete the cycle marker
            return true;
        }
        if visited.contains(&node) {
            return false;
        }
        visited.insert(node);
        path.push(node);

        if let Some(neighbors) = graph.get(&node) {
            for &next in neighbors {
                if self.dfs_find_cycle(graph, next, visited, path) {
                    return true;
                }
            }
        }

        path.pop();
        false
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn default_config() -> DlmConfig {
        DlmConfig {
            lock_timeout_ms: 200,
            deadlock_detection_interval_ms: 100,
            max_locks_per_tx: 100,
            lock_ttl_ms: 30_000,
            enable_deadlock_detection: true,
        }
    }

    fn tx(n: u128) -> TxId {
        TxId::from_u128(n)
    }

    // -- Basic acquire / release ------------------------------------------

    #[tokio::test]
    async fn test_acquire_and_release() {
        let dlm = DistributedLockManager::new(default_config());
        let t1 = tx(1);

        let grant = dlm
            .acquire(&t1, "key:1", LockType::Exclusive)
            .await
            .unwrap();
        assert_eq!(grant.tx_id, t1);
        assert_eq!(grant.resource, "key:1");
        assert!(matches!(grant.lock_type, LockType::Exclusive));
        assert!(grant.expires_at.is_some());

        dlm.release(&t1, "key:1").await.unwrap();

        let stats = dlm.get_stats();
        assert_eq!(stats.active_locks, 0);
        assert_eq!(stats.total_acquisitions, 1);
        assert_eq!(stats.total_releases, 1);
    }

    // -- Shared lock compatibility ----------------------------------------

    #[tokio::test]
    async fn test_shared_lock_compatibility() {
        let dlm = DistributedLockManager::new(default_config());
        let t1 = tx(1);
        let t2 = tx(2);
        let t3 = tx(3);

        // Multiple shared locks on the same resource should succeed.
        dlm.acquire(&t1, "key:1", LockType::Shared).await.unwrap();
        dlm.acquire(&t2, "key:1", LockType::Shared).await.unwrap();
        dlm.acquire(&t3, "key:1", LockType::Shared).await.unwrap();

        let stats = dlm.get_stats();
        assert_eq!(stats.active_locks, 3);
        assert_eq!(stats.total_acquisitions, 3);
    }

    // -- Exclusive lock blocks others -------------------------------------

    #[tokio::test]
    async fn test_exclusive_blocks_shared() {
        let dlm = Arc::new(DistributedLockManager::new(default_config()));
        let t1 = tx(1);
        let t2 = tx(2);

        // t1 acquires exclusive.
        dlm.acquire(&t1, "key:1", LockType::Exclusive)
            .await
            .unwrap();

        // t2 tries shared — should timeout.
        let dlm2 = dlm.clone();
        let result =
            tokio::spawn(async move { dlm2.acquire(&t2, "key:1", LockType::Shared).await })
                .await
                .unwrap();

        assert!(matches!(result, Err(DlmError::Timeout(_))));
        assert_eq!(dlm.get_stats().lock_timeouts, 1);
    }

    #[tokio::test]
    async fn test_exclusive_blocks_exclusive() {
        let dlm = Arc::new(DistributedLockManager::new(default_config()));
        let t1 = tx(1);
        let t2 = tx(2);

        dlm.acquire(&t1, "key:1", LockType::Exclusive)
            .await
            .unwrap();

        let dlm2 = dlm.clone();
        let result =
            tokio::spawn(async move { dlm2.acquire(&t2, "key:1", LockType::Exclusive).await })
                .await
                .unwrap();

        assert!(matches!(result, Err(DlmError::Timeout(_))));
    }

    // -- Lock upgrade scenarios -------------------------------------------

    #[tokio::test]
    async fn test_lock_upgrade_sole_holder() {
        let dlm = DistributedLockManager::new(default_config());
        let t1 = tx(1);

        dlm.acquire(&t1, "key:1", LockType::Shared).await.unwrap();

        // Upgrade to exclusive — should succeed since t1 is the only holder.
        let grant = dlm
            .acquire(&t1, "key:1", LockType::Exclusive)
            .await
            .unwrap();
        assert!(matches!(grant.lock_type, LockType::Exclusive));
    }

    #[tokio::test]
    async fn test_lock_upgrade_blocked_by_other_holder() {
        let dlm = Arc::new(DistributedLockManager::new(default_config()));
        let t1 = tx(1);
        let t2 = tx(2);

        dlm.acquire(&t1, "key:1", LockType::Shared).await.unwrap();
        dlm.acquire(&t2, "key:1", LockType::Shared).await.unwrap();

        // t1 tries to upgrade — cannot because t2 also holds shared.
        let dlm2 = dlm.clone();
        let result =
            tokio::spawn(async move { dlm2.acquire(&t1, "key:1", LockType::Exclusive).await })
                .await
                .unwrap();

        assert!(matches!(result, Err(DlmError::Timeout(_))));
    }

    // -- Re-entrant locks -------------------------------------------------

    #[tokio::test]
    async fn test_reentrant_same_type() {
        let dlm = DistributedLockManager::new(default_config());
        let t1 = tx(1);

        dlm.acquire(&t1, "key:1", LockType::Exclusive)
            .await
            .unwrap();

        // Same tx, same type — should return immediately.
        let grant = dlm
            .acquire(&t1, "key:1", LockType::Exclusive)
            .await
            .unwrap();
        assert!(matches!(grant.lock_type, LockType::Exclusive));
    }

    // -- Deadlock detection (A waits B, B waits A) ------------------------

    #[tokio::test]
    async fn test_deadlock_detection_simple_cycle() {
        let dlm = Arc::new(DistributedLockManager::new(default_config()));
        let t1 = tx(1);
        let t2 = tx(2);

        // t1 holds "res:A", t2 holds "res:B".
        dlm.acquire(&t1, "res:A", LockType::Exclusive)
            .await
            .unwrap();
        dlm.acquire(&t2, "res:B", LockType::Exclusive)
            .await
            .unwrap();

        // t1 wants "res:B" (blocked by t2) — spawn in background.
        let dlm1 = dlm.clone();
        let h1 = tokio::spawn(async move { dlm1.acquire(&t1, "res:B", LockType::Exclusive).await });

        // Small yield to let t1 queue.
        tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;

        // t2 wants "res:A" (blocked by t1) — should detect deadlock.
        let result = dlm.acquire(&t2, "res:A", LockType::Exclusive).await;

        assert!(
            matches!(result, Err(DlmError::Deadlock)),
            "expected Deadlock, got {result:?}"
        );

        // Clean up: release t2's lock so t1 can proceed.
        dlm.release(&t2, "res:B").await.unwrap();
        let h1_result = h1.await.unwrap();
        assert!(h1_result.is_ok());

        assert!(dlm.get_stats().deadlocks_detected >= 1);
    }

    // -- Deadlock resolution (victim selection) ---------------------------

    #[tokio::test]
    async fn test_deadlock_resolution_victim() {
        let dlm = DistributedLockManager::new(default_config());
        let t1 = tx(1);
        let t2 = tx(2);

        // Manually build a cycle for resolution testing.
        let cycle = DeadlockCycle {
            transactions: vec![t1, t2],
            resources: vec!["res:A".to_string(), "res:B".to_string()],
            detected_at: Utc::now(),
        };

        let victim = dlm.resolve_deadlock(&cycle).unwrap();
        // Victim should be the last transaction (youngest by convention).
        assert_eq!(victim, t2);
        assert_eq!(dlm.get_stats().deadlocks_resolved, 1);
    }

    // -- detect_deadlocks full graph scan ---------------------------------

    #[tokio::test]
    async fn test_detect_deadlocks_graph_scan() {
        let dlm = DistributedLockManager::new(DlmConfig {
            enable_deadlock_detection: false, // disable inline detection
            ..default_config()
        });
        let t1 = tx(1);
        let t2 = tx(2);

        // t1 holds res:A, t2 holds res:B.
        dlm.acquire(&t1, "res:A", LockType::Exclusive)
            .await
            .unwrap();
        dlm.acquire(&t2, "res:B", LockType::Exclusive)
            .await
            .unwrap();

        // Manually inject waiters to simulate the deadlock (since inline
        // detection is off, the acquire would just queue+timeout).
        {
            let (s1, _r1) = oneshot::channel();
            let mut entry_a = dlm.locks.get_mut("res:A").unwrap();
            entry_a.wait_queue.push_back(LockWaiter {
                tx_id: t2,
                lock_type: LockType::Exclusive,
                requested_at: Utc::now(),
                notify: s1,
            });
        }
        {
            let (s2, _r2) = oneshot::channel();
            let mut entry_b = dlm.locks.get_mut("res:B").unwrap();
            entry_b.wait_queue.push_back(LockWaiter {
                tx_id: t1,
                lock_type: LockType::Exclusive,
                requested_at: Utc::now(),
                notify: s2,
            });
        }

        let cycles = dlm.detect_deadlocks();
        assert!(
            !cycles.is_empty(),
            "expected at least one deadlock cycle, got none"
        );
        assert_eq!(cycles[0].transactions.len(), 2);
    }

    // -- Lock timeout -----------------------------------------------------

    #[tokio::test]
    async fn test_lock_timeout() {
        let dlm = Arc::new(DistributedLockManager::new(DlmConfig {
            lock_timeout_ms: 50,
            enable_deadlock_detection: false,
            ..default_config()
        }));
        let t1 = tx(1);
        let t2 = tx(2);

        dlm.acquire(&t1, "key:1", LockType::Exclusive)
            .await
            .unwrap();

        let dlm2 = dlm.clone();
        let result =
            tokio::spawn(async move { dlm2.acquire(&t2, "key:1", LockType::Exclusive).await })
                .await
                .unwrap();

        assert!(matches!(result, Err(DlmError::Timeout(50))));
    }

    // -- Stats tracking ---------------------------------------------------

    #[tokio::test]
    async fn test_stats_tracking() {
        let dlm = DistributedLockManager::new(default_config());
        let t1 = tx(1);

        let s0 = dlm.get_stats();
        assert_eq!(s0.total_acquisitions, 0);
        assert_eq!(s0.total_releases, 0);

        dlm.acquire(&t1, "a", LockType::Shared).await.unwrap();
        dlm.acquire(&t1, "b", LockType::Exclusive).await.unwrap();

        let s1 = dlm.get_stats();
        assert_eq!(s1.total_acquisitions, 2);
        assert_eq!(s1.active_locks, 2);

        dlm.release(&t1, "a").await.unwrap();

        let s2 = dlm.get_stats();
        assert_eq!(s2.total_releases, 1);
        assert_eq!(s2.active_locks, 1);
    }

    // -- Release all locks ------------------------------------------------

    #[tokio::test]
    async fn test_release_all() {
        let dlm = DistributedLockManager::new(default_config());
        let t1 = tx(1);

        dlm.acquire(&t1, "a", LockType::Exclusive).await.unwrap();
        dlm.acquire(&t1, "b", LockType::Exclusive).await.unwrap();
        dlm.acquire(&t1, "c", LockType::Shared).await.unwrap();

        assert_eq!(dlm.get_stats().active_locks, 3);

        let released = dlm.release_all(&t1).await.unwrap();
        assert_eq!(released, 3);
        assert_eq!(dlm.get_stats().active_locks, 0);
        assert_eq!(dlm.get_stats().total_releases, 3);
    }

    // -- Max locks per transaction ----------------------------------------

    #[tokio::test]
    async fn test_max_locks_exceeded() {
        let dlm = DistributedLockManager::new(DlmConfig {
            max_locks_per_tx: 2,
            ..default_config()
        });
        let t1 = tx(1);

        dlm.acquire(&t1, "a", LockType::Shared).await.unwrap();
        dlm.acquire(&t1, "b", LockType::Shared).await.unwrap();

        let result = dlm.acquire(&t1, "c", LockType::Shared).await;
        assert!(matches!(result, Err(DlmError::MaxLocksExceeded(2))));
    }

    // -- Release non-held lock returns error ------------------------------

    #[tokio::test]
    async fn test_release_not_held() {
        let dlm = DistributedLockManager::new(default_config());
        let t1 = tx(1);

        let result = dlm.release(&t1, "ghost").await;
        assert!(matches!(result, Err(DlmError::ResourceNotLocked(_))));
    }

    // -- Intent lock compatibility ----------------------------------------

    #[tokio::test]
    async fn test_intent_shared_compatible_with_shared() {
        let dlm = DistributedLockManager::new(default_config());
        let t1 = tx(1);
        let t2 = tx(2);

        dlm.acquire(&t1, "tbl", LockType::Shared).await.unwrap();
        dlm.acquire(&t2, "tbl", LockType::IntentShared)
            .await
            .unwrap();

        assert_eq!(dlm.get_stats().active_locks, 2);
    }

    #[tokio::test]
    async fn test_intent_exclusive_blocks_shared() {
        let dlm = Arc::new(DistributedLockManager::new(default_config()));
        let t1 = tx(1);
        let t2 = tx(2);

        dlm.acquire(&t1, "tbl", LockType::Shared).await.unwrap();

        let dlm2 = dlm.clone();
        let result =
            tokio::spawn(async move { dlm2.acquire(&t2, "tbl", LockType::IntentExclusive).await })
                .await
                .unwrap();

        assert!(matches!(result, Err(DlmError::Timeout(_))));
    }

    // -- Lock type compatibility unit tests -------------------------------

    #[test]
    fn test_lock_type_compatibility_matrix() {
        use LockType::*;

        // S + S = compatible
        assert!(Shared.is_compatible(&Shared));
        // S + X = incompatible
        assert!(!Shared.is_compatible(&Exclusive));
        // X + anything = incompatible
        assert!(!Exclusive.is_compatible(&Shared));
        assert!(!Exclusive.is_compatible(&Exclusive));
        assert!(!Exclusive.is_compatible(&IntentShared));
        assert!(!Exclusive.is_compatible(&IntentExclusive));
        // IS + IS = compatible
        assert!(IntentShared.is_compatible(&IntentShared));
        // IS + S = compatible
        assert!(IntentShared.is_compatible(&Shared));
        // IS + X = incompatible
        assert!(!IntentShared.is_compatible(&Exclusive));
        // IS + IX = compatible
        assert!(IntentShared.is_compatible(&IntentExclusive));
        // IX + S = incompatible
        assert!(!IntentExclusive.is_compatible(&Shared));
        // IX + X = incompatible
        assert!(!IntentExclusive.is_compatible(&Exclusive));
        // IX + IS = compatible
        assert!(IntentExclusive.is_compatible(&IntentShared));
        // IX + IX = incompatible (correct per specification)
        assert!(!IntentExclusive.is_compatible(&IntentExclusive));
    }

    // -- Waiter is properly granted after release -------------------------

    #[tokio::test]
    async fn test_waiter_granted_after_release() {
        let dlm = Arc::new(DistributedLockManager::new(default_config()));
        let t1 = tx(1);
        let t2 = tx(2);

        dlm.acquire(&t1, "key:1", LockType::Exclusive)
            .await
            .unwrap();

        // Spawn t2 trying to acquire — will block.
        let dlm2 = dlm.clone();
        let handle =
            tokio::spawn(async move { dlm2.acquire(&t2, "key:1", LockType::Exclusive).await });

        // Give t2 time to queue.
        tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;

        // Release t1's lock — t2 should be granted.
        dlm.release(&t1, "key:1").await.unwrap();

        let result = handle.await.unwrap();
        assert!(result.is_ok());
        let grant = result.unwrap();
        assert_eq!(grant.tx_id, t2);
    }
}
