//! Distributed Lock Service
//!
//! Purpose-built distributed locking with fencing tokens, lease renewal,
//! lock queuing, deadlock detection, and automatic release.
#![allow(dead_code)]

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use thiserror::Error;

// ── Errors ───────────────────────────────────────────────────────────────────

/// Errors from the distributed lock service.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum LockError {
    /// The lock is already held by another owner.
    #[error("lock '{0}' is already held by '{1}'")]
    AlreadyLocked(String, String),

    /// The caller is not the owner of the lock.
    #[error("not the owner of lock '{0}'")]
    NotOwner(String),

    /// The fencing token is invalid or stale.
    #[error("invalid fencing token {0} for lock '{1}'")]
    InvalidToken(u64, String),

    /// The lock has expired.
    #[error("lock '{0}' has expired")]
    Expired(String),

    /// The wait queue is full.
    #[error("queue full for lock '{0}' (max {1})")]
    QueueFull(String, usize),

    /// A deadlock was detected among the participants.
    #[error("deadlock detected involving keys: {0:?}")]
    DeadlockDetected(Vec<String>),

    /// The TTL is invalid.
    #[error("invalid TTL: {0}")]
    InvalidTtl(String),
}

// ── Configuration ────────────────────────────────────────────────────────────

/// Configuration for the distributed lock manager.
#[derive(Debug, Clone)]
pub struct LockConfig {
    /// Default lock TTL if none specified.
    pub default_ttl: Duration,
    /// Maximum allowed lock TTL.
    pub max_ttl: Duration,
    /// Whether to enable wait-queue for contended locks.
    pub enable_queuing: bool,
    /// Maximum number of waiters per lock.
    pub max_queue_size: usize,
    /// Whether to run deadlock detection.
    pub enable_deadlock_detection: bool,
    /// Whether to issue fencing tokens.
    pub fencing_token_enabled: bool,
}

impl Default for LockConfig {
    fn default() -> Self {
        Self {
            default_ttl: Duration::from_secs(30),
            max_ttl: Duration::from_secs(300),
            enable_queuing: true,
            max_queue_size: 100,
            enable_deadlock_detection: true,
            fencing_token_enabled: true,
        }
    }
}

// ── Lock Types ───────────────────────────────────────────────────────────────

/// Type of lock to acquire.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum LockType {
    /// Exclusive lock — only one holder at a time.
    Exclusive,
    /// Reader-writer lock — multiple readers or one writer.
    ReadWrite {
        readers: u32,
        writer: Option<String>,
    },
}

// ── Lock Entry ───────────────────────────────────────────────────────────────

/// Internal representation of a held lock.
#[derive(Debug, Clone)]
pub struct LockEntry {
    /// The lock key.
    pub key: String,
    /// Current owner identifier.
    pub owner: String,
    /// Fencing token (monotonically increasing).
    pub token: u64,
    /// When the lock was acquired.
    pub acquired_at: Instant,
    /// When the lock expires.
    pub expires_at: Instant,
    /// Configured TTL.
    pub ttl: Duration,
    /// Type of lock held.
    pub lock_type: LockType,
    /// Number of times the lease has been renewed.
    pub renewals: u32,
}

// ── Lock Queue ───────────────────────────────────────────────────────────────

/// A waiter in the lock queue.
#[derive(Debug, Clone)]
pub struct LockWaiter {
    /// Client identifier.
    pub client_id: String,
    /// When the wait request was made.
    pub requested_at: Instant,
    /// Maximum time to wait.
    pub timeout: Duration,
    /// Desired lock type.
    pub lock_type: LockType,
}

/// Queue of waiters for a contended lock.
#[derive(Debug, Clone, Default)]
pub struct LockQueue {
    /// Ordered queue of waiters.
    pub waiters: VecDeque<LockWaiter>,
}

// ── Grant / Info / Stats ─────────────────────────────────────────────────────

/// Returned upon successful lock acquisition.
#[derive(Debug, Clone)]
pub struct LockGrant {
    /// The lock key.
    pub key: String,
    /// Fencing token for this grant.
    pub token: u64,
    /// When the lock expires.
    pub expires_at: Instant,
    /// Position in queue, if any.
    pub queue_position: Option<usize>,
}

/// Information about a held lock.
#[derive(Debug, Clone, Serialize)]
pub struct LockInfo {
    /// The lock key.
    pub key: String,
    /// Current owner.
    pub owner: String,
    /// Fencing token.
    pub token: u64,
    /// Milliseconds since acquisition.
    pub held_for_ms: u64,
    /// Milliseconds until expiry.
    pub expires_in_ms: u64,
    /// Type of lock held.
    pub lock_type: String,
    /// Number of waiters in queue.
    pub queue_depth: usize,
}

/// A detected deadlock cycle.
#[derive(Debug, Clone)]
pub struct DeadlockCycle {
    /// Owners involved in the cycle.
    pub participants: Vec<String>,
    /// Keys forming the cycle.
    pub keys: Vec<String>,
    /// When the deadlock was detected.
    pub detected_at: Instant,
}

/// Aggregate statistics for the lock manager.
#[derive(Debug, Clone, Serialize)]
pub struct LockStats {
    /// Number of currently held locks.
    pub active_locks: u64,
    /// Number of clients waiting in queues.
    pub queued_waiters: u64,
    /// Total locks ever acquired.
    pub total_acquired: u64,
    /// Total locks ever released.
    pub total_released: u64,
    /// Total locks that expired without release.
    pub total_expired: u64,
    /// Total deadlocks detected.
    pub deadlocks_detected: u64,
    /// Average hold time in milliseconds.
    pub avg_hold_time_ms: f64,
    /// Contention rate (locks that had to queue / total acquire attempts).
    pub contention_rate: f64,
}

// ── Distributed Lock Manager ─────────────────────────────────────────────────

/// Thread-safe distributed lock manager with fencing tokens and deadlock detection.
pub struct DistributedLockManager {
    config: LockConfig,
    locks: RwLock<HashMap<String, LockEntry>>,
    queues: RwLock<HashMap<String, LockQueue>>,
    token_counter: AtomicU64,

    // Statistics
    total_acquired: AtomicU64,
    total_released: AtomicU64,
    total_expired: AtomicU64,
    total_contended: AtomicU64,
    deadlocks_detected: AtomicU64,
    hold_time_total_ms: AtomicU64,
}

impl DistributedLockManager {
    /// Create a new lock manager with the given configuration.
    pub fn new(config: LockConfig) -> Self {
        Self {
            config,
            locks: RwLock::new(HashMap::new()),
            queues: RwLock::new(HashMap::new()),
            token_counter: AtomicU64::new(1),
            total_acquired: AtomicU64::new(0),
            total_released: AtomicU64::new(0),
            total_expired: AtomicU64::new(0),
            total_contended: AtomicU64::new(0),
            deadlocks_detected: AtomicU64::new(0),
            hold_time_total_ms: AtomicU64::new(0),
        }
    }

    /// Acquire a lock on the given key.
    ///
    /// Returns a `LockGrant` with a fencing token on success, or `LockError` if
    /// the lock is contended and queuing is disabled.
    pub fn acquire(
        &self,
        key: &str,
        owner: &str,
        ttl: Duration,
        lock_type: LockType,
    ) -> Result<LockGrant, LockError> {
        self.validate_ttl(ttl)?;

        let now = Instant::now();
        let mut locks = self.locks.write();

        // Snapshot existing lock state to avoid holding an immutable borrow
        // across subsequent mutations of the same HashMap.
        enum LockState {
            None,
            Expired { held_ms: u64 },
            Reentrant { token: u64 },
            Contended { current_owner: String },
        }

        let state = match locks.get(key) {
            None => LockState::None,
            Some(entry) if now >= entry.expires_at => LockState::Expired {
                held_ms: entry.ttl.as_millis() as u64,
            },
            Some(entry) if entry.owner == owner => LockState::Reentrant { token: entry.token },
            Some(entry) => LockState::Contended {
                current_owner: entry.owner.clone(),
            },
        };

        match state {
            LockState::Expired { held_ms } => {
                self.hold_time_total_ms
                    .fetch_add(held_ms, Ordering::Relaxed);
                self.total_expired.fetch_add(1, Ordering::Relaxed);
                locks.remove(key);
                // Fall through to acquire below
            }
            LockState::Reentrant { token: saved_token } => {
                return self.do_extend_inner(&mut locks, key, saved_token, ttl);
            }
            LockState::Contended { current_owner } => {
                if self.config.enable_queuing {
                    self.total_contended.fetch_add(1, Ordering::Relaxed);
                    let mut queues = self.queues.write();
                    let queue = queues.entry(key.to_string()).or_default();
                    if queue.waiters.len() >= self.config.max_queue_size {
                        return Err(LockError::QueueFull(
                            key.to_string(),
                            self.config.max_queue_size,
                        ));
                    }
                    let pos = queue.waiters.len();
                    queue.waiters.push_back(LockWaiter {
                        client_id: owner.to_string(),
                        requested_at: now,
                        timeout: ttl,
                        lock_type,
                    });
                    let token = self.next_token();
                    return Ok(LockGrant {
                        key: key.to_string(),
                        token,
                        expires_at: now + ttl,
                        queue_position: Some(pos),
                    });
                }
                return Err(LockError::AlreadyLocked(key.to_string(), current_owner));
            }
            LockState::None => {
                // Fall through to acquire below
            }
        }

        // Acquire the lock
        let token = self.next_token();
        let expires_at = now + ttl;
        locks.insert(
            key.to_string(),
            LockEntry {
                key: key.to_string(),
                owner: owner.to_string(),
                token,
                acquired_at: now,
                expires_at,
                ttl,
                lock_type,
                renewals: 0,
            },
        );
        self.total_acquired.fetch_add(1, Ordering::Relaxed);

        Ok(LockGrant {
            key: key.to_string(),
            token,
            expires_at,
            queue_position: None,
        })
    }

    /// Release a lock using its fencing token.
    pub fn release(&self, key: &str, token: u64) -> Result<(), LockError> {
        let mut locks = self.locks.write();
        let entry = locks
            .get(key)
            .ok_or_else(|| LockError::Expired(key.to_string()))?;

        if entry.token != token {
            return Err(LockError::InvalidToken(token, key.to_string()));
        }

        let held_ms = entry.acquired_at.elapsed().as_millis() as u64;
        self.hold_time_total_ms
            .fetch_add(held_ms, Ordering::Relaxed);
        locks.remove(key);
        self.total_released.fetch_add(1, Ordering::Relaxed);

        // Promote next waiter if queuing is enabled
        self.promote_next_waiter(&mut locks, key);

        Ok(())
    }

    /// Extend (renew) a lock's TTL.
    pub fn extend(
        &self,
        key: &str,
        token: u64,
        additional_ttl: Duration,
    ) -> Result<LockGrant, LockError> {
        self.validate_ttl(additional_ttl)?;
        let mut locks = self.locks.write();
        self.do_extend_inner(&mut locks, key, token, additional_ttl)
    }

    /// Non-blocking lock attempt. Returns `Ok(None)` if the lock is held.
    pub fn try_acquire(
        &self,
        key: &str,
        owner: &str,
        ttl: Duration,
    ) -> Result<Option<LockGrant>, LockError> {
        self.validate_ttl(ttl)?;

        let now = Instant::now();
        let mut locks = self.locks.write();

        if let Some(entry) = locks.get(key) {
            if now >= entry.expires_at {
                let held_ms = entry.ttl.as_millis() as u64;
                self.hold_time_total_ms
                    .fetch_add(held_ms, Ordering::Relaxed);
                self.total_expired.fetch_add(1, Ordering::Relaxed);
                locks.remove(key);
            } else {
                return Ok(None);
            }
        }

        let token = self.next_token();
        let expires_at = now + ttl;
        locks.insert(
            key.to_string(),
            LockEntry {
                key: key.to_string(),
                owner: owner.to_string(),
                token,
                acquired_at: now,
                expires_at,
                ttl,
                lock_type: LockType::Exclusive,
                renewals: 0,
            },
        );
        self.total_acquired.fetch_add(1, Ordering::Relaxed);

        Ok(Some(LockGrant {
            key: key.to_string(),
            token,
            expires_at,
            queue_position: None,
        }))
    }

    /// Check whether a key is currently locked.
    pub fn is_locked(&self, key: &str) -> bool {
        let locks = self.locks.read();
        if let Some(entry) = locks.get(key) {
            Instant::now() < entry.expires_at
        } else {
            false
        }
    }

    /// Get information about a specific lock.
    pub fn lock_info(&self, key: &str) -> Option<LockInfo> {
        let locks = self.locks.read();
        let queues = self.queues.read();
        locks.get(key).and_then(|entry| {
            let now = Instant::now();
            if now >= entry.expires_at {
                return None;
            }
            let queue_depth = queues.get(key).map(|q| q.waiters.len()).unwrap_or(0);
            Some(LockInfo {
                key: entry.key.clone(),
                owner: entry.owner.clone(),
                token: entry.token,
                held_for_ms: entry.acquired_at.elapsed().as_millis() as u64,
                expires_in_ms: entry.expires_at.duration_since(now).as_millis() as u64,
                lock_type: format!("{:?}", entry.lock_type),
                queue_depth,
            })
        })
    }

    /// Get all locks held by a specific owner.
    pub fn owner_locks(&self, owner: &str) -> Vec<LockInfo> {
        let locks = self.locks.read();
        let queues = self.queues.read();
        let now = Instant::now();

        locks
            .values()
            .filter(|e| e.owner == owner && now < e.expires_at)
            .map(|entry| {
                let queue_depth = queues.get(&entry.key).map(|q| q.waiters.len()).unwrap_or(0);
                LockInfo {
                    key: entry.key.clone(),
                    owner: entry.owner.clone(),
                    token: entry.token,
                    held_for_ms: entry.acquired_at.elapsed().as_millis() as u64,
                    expires_in_ms: entry.expires_at.duration_since(now).as_millis() as u64,
                    lock_type: format!("{:?}", entry.lock_type),
                    queue_depth,
                }
            })
            .collect()
    }

    /// Detect deadlocks by building a wait-for graph and finding cycles.
    pub fn detect_deadlocks(&self) -> Vec<DeadlockCycle> {
        if !self.config.enable_deadlock_detection {
            return Vec::new();
        }

        let locks = self.locks.read();
        let queues = self.queues.read();

        // Build wait-for graph: waiter_owner -> lock_key -> lock_owner
        // Edge: waiter -> current holder (the waiter is waiting for the holder)
        let mut graph: HashMap<String, Vec<(String, String)>> = HashMap::new();

        for (key, queue) in queues.iter() {
            if let Some(holder) = locks.get(key) {
                for waiter in &queue.waiters {
                    graph
                        .entry(waiter.client_id.clone())
                        .or_default()
                        .push((key.clone(), holder.owner.clone()));
                }
            }
        }

        // DFS cycle detection
        let mut cycles = Vec::new();
        let mut visited = HashSet::new();
        let mut path = Vec::new();
        let mut path_set = HashSet::new();

        for start_node in graph.keys() {
            if visited.contains(start_node) {
                continue;
            }
            Self::dfs_detect_cycle(
                start_node,
                &graph,
                &mut visited,
                &mut path,
                &mut path_set,
                &mut cycles,
            );
        }

        let count = cycles.len() as u64;
        if count > 0 {
            self.deadlocks_detected.fetch_add(count, Ordering::Relaxed);
        }

        cycles
    }

    /// Remove all expired locks and return the count.
    pub fn expire_stale(&self) -> usize {
        let now = Instant::now();
        let mut locks = self.locks.write();
        let before = locks.len();

        let expired_keys: Vec<String> = locks
            .iter()
            .filter(|(_, e)| now >= e.expires_at)
            .map(|(k, _)| k.clone())
            .collect();

        for key in &expired_keys {
            if let Some(entry) = locks.remove(key) {
                let held_ms = entry.ttl.as_millis() as u64;
                self.hold_time_total_ms
                    .fetch_add(held_ms, Ordering::Relaxed);
            }
        }

        let expired = before - locks.len();
        self.total_expired
            .fetch_add(expired as u64, Ordering::Relaxed);

        // Try to promote waiters for expired keys
        for key in &expired_keys {
            self.promote_next_waiter(&mut locks, key);
        }

        expired
    }

    /// Return aggregate lock statistics.
    pub fn stats(&self) -> LockStats {
        let locks = self.locks.read();
        let queues = self.queues.read();

        let active = locks
            .values()
            .filter(|e| Instant::now() < e.expires_at)
            .count() as u64;
        let queued: u64 = queues.values().map(|q| q.waiters.len() as u64).sum();
        let acquired = self.total_acquired.load(Ordering::Relaxed);
        let released = self.total_released.load(Ordering::Relaxed);
        let expired = self.total_expired.load(Ordering::Relaxed);
        let deadlocks = self.deadlocks_detected.load(Ordering::Relaxed);
        let hold_total = self.hold_time_total_ms.load(Ordering::Relaxed);
        let completed = released + expired;
        let avg_hold = if completed > 0 {
            hold_total as f64 / completed as f64
        } else {
            0.0
        };
        let contended = self.total_contended.load(Ordering::Relaxed);
        let contention = if acquired > 0 {
            contended as f64 / acquired as f64
        } else {
            0.0
        };

        LockStats {
            active_locks: active,
            queued_waiters: queued,
            total_acquired: acquired,
            total_released: released,
            total_expired: expired,
            deadlocks_detected: deadlocks,
            avg_hold_time_ms: avg_hold,
            contention_rate: contention,
        }
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    fn next_token(&self) -> u64 {
        self.token_counter.fetch_add(1, Ordering::SeqCst)
    }

    fn validate_ttl(&self, ttl: Duration) -> Result<(), LockError> {
        if ttl.is_zero() {
            return Err(LockError::InvalidTtl("TTL must be > 0".to_string()));
        }
        if ttl > self.config.max_ttl {
            return Err(LockError::InvalidTtl(format!(
                "TTL {}ms exceeds max {}ms",
                ttl.as_millis(),
                self.config.max_ttl.as_millis()
            )));
        }
        Ok(())
    }

    fn do_extend_inner(
        &self,
        locks: &mut HashMap<String, LockEntry>,
        key: &str,
        token: u64,
        additional_ttl: Duration,
    ) -> Result<LockGrant, LockError> {
        let entry = locks
            .get_mut(key)
            .ok_or_else(|| LockError::Expired(key.to_string()))?;

        if entry.token != token {
            return Err(LockError::InvalidToken(token, key.to_string()));
        }

        let now = Instant::now();
        if now >= entry.expires_at {
            locks.remove(key);
            return Err(LockError::Expired(key.to_string()));
        }

        let new_expires = now + additional_ttl;
        entry.expires_at = new_expires;
        entry.ttl = additional_ttl;
        entry.renewals += 1;

        Ok(LockGrant {
            key: key.to_string(),
            token,
            expires_at: new_expires,
            queue_position: None,
        })
    }

    fn promote_next_waiter(&self, locks: &mut HashMap<String, LockEntry>, key: &str) {
        if !self.config.enable_queuing {
            return;
        }

        let mut queues = self.queues.write();
        if let Some(queue) = queues.get_mut(key) {
            let now = Instant::now();
            // Remove timed-out waiters
            queue
                .waiters
                .retain(|w| now.duration_since(w.requested_at) < w.timeout);

            if let Some(waiter) = queue.waiters.pop_front() {
                let token = self.next_token();
                let expires_at = now + waiter.timeout;
                locks.insert(
                    key.to_string(),
                    LockEntry {
                        key: key.to_string(),
                        owner: waiter.client_id,
                        token,
                        acquired_at: now,
                        expires_at,
                        ttl: waiter.timeout,
                        lock_type: waiter.lock_type,
                        renewals: 0,
                    },
                );
                self.total_acquired.fetch_add(1, Ordering::Relaxed);
            }

            if queue.waiters.is_empty() {
                queues.remove(key);
            }
        }
    }

    fn dfs_detect_cycle(
        node: &str,
        graph: &HashMap<String, Vec<(String, String)>>,
        visited: &mut HashSet<String>,
        path: &mut Vec<(String, String)>,
        path_set: &mut HashSet<String>,
        cycles: &mut Vec<DeadlockCycle>,
    ) {
        if path_set.contains(node) {
            // Found a cycle — extract it
            let cycle_start = path.iter().position(|(_, n)| n == node);
            if let Some(start) = cycle_start {
                let cycle_edges = &path[start..];
                let participants: Vec<String> =
                    cycle_edges.iter().map(|(_, n)| n.clone()).collect();
                let keys: Vec<String> = cycle_edges.iter().map(|(k, _)| k.clone()).collect();
                cycles.push(DeadlockCycle {
                    participants,
                    keys,
                    detected_at: Instant::now(),
                });
            }
            return;
        }

        if visited.contains(node) {
            return;
        }

        visited.insert(node.to_string());
        path_set.insert(node.to_string());

        if let Some(edges) = graph.get(node) {
            for (key, target) in edges {
                path.push((key.clone(), node.to_string()));
                Self::dfs_detect_cycle(target, graph, visited, path, path_set, cycles);
                path.pop();
            }
        }

        path_set.remove(node);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn default_manager() -> DistributedLockManager {
        DistributedLockManager::new(LockConfig::default())
    }

    #[test]
    fn test_basic_acquire_release() {
        let mgr = default_manager();
        let grant = mgr
            .acquire(
                "key1",
                "owner1",
                Duration::from_secs(10),
                LockType::Exclusive,
            )
            .expect("acquire");
        assert_eq!(grant.key, "key1");
        assert!(grant.token > 0);
        assert!(grant.queue_position.is_none());

        mgr.release("key1", grant.token).expect("release");
        assert!(!mgr.is_locked("key1"));
    }

    #[test]
    fn test_fencing_tokens_monotonic() {
        let mgr = default_manager();
        let g1 = mgr
            .acquire("k1", "o1", Duration::from_secs(10), LockType::Exclusive)
            .expect("acquire");
        mgr.release("k1", g1.token).expect("release");

        let g2 = mgr
            .acquire("k1", "o2", Duration::from_secs(10), LockType::Exclusive)
            .expect("acquire");
        assert!(g2.token > g1.token);
        mgr.release("k1", g2.token).expect("release");
    }

    #[test]
    fn test_contention_with_queue() {
        let mgr = default_manager();
        let _g1 = mgr
            .acquire(
                "key1",
                "owner1",
                Duration::from_secs(30),
                LockType::Exclusive,
            )
            .expect("acquire");

        // Second acquire should queue
        let g2 = mgr
            .acquire(
                "key1",
                "owner2",
                Duration::from_secs(30),
                LockType::Exclusive,
            )
            .expect("queued");
        assert!(g2.queue_position.is_some());
    }

    #[test]
    fn test_contention_without_queue() {
        let config = LockConfig {
            enable_queuing: false,
            ..LockConfig::default()
        };
        let mgr = DistributedLockManager::new(config);
        let _g1 = mgr
            .acquire(
                "key1",
                "owner1",
                Duration::from_secs(30),
                LockType::Exclusive,
            )
            .expect("acquire");

        let result = mgr.acquire(
            "key1",
            "owner2",
            Duration::from_secs(30),
            LockType::Exclusive,
        );
        assert!(matches!(result, Err(LockError::AlreadyLocked(_, _))));
    }

    #[test]
    fn test_ttl_expiry() {
        let mgr = default_manager();
        let _g = mgr
            .acquire(
                "key1",
                "owner1",
                Duration::from_millis(1),
                LockType::Exclusive,
            )
            .expect("acquire");

        std::thread::sleep(Duration::from_millis(5));
        assert!(!mgr.is_locked("key1"));
    }

    #[test]
    fn test_extend_lease() {
        let mgr = default_manager();
        let grant = mgr
            .acquire(
                "key1",
                "owner1",
                Duration::from_secs(5),
                LockType::Exclusive,
            )
            .expect("acquire");
        let original_expires = grant.expires_at;

        let extended = mgr
            .extend("key1", grant.token, Duration::from_secs(30))
            .expect("extend");
        assert!(extended.expires_at > original_expires);
    }

    #[test]
    fn test_invalid_token_release() {
        let mgr = default_manager();
        let _g = mgr
            .acquire(
                "key1",
                "owner1",
                Duration::from_secs(10),
                LockType::Exclusive,
            )
            .expect("acquire");

        let result = mgr.release("key1", 99999);
        assert!(matches!(result, Err(LockError::InvalidToken(99999, _))));
    }

    #[test]
    fn test_try_acquire() {
        let mgr = default_manager();

        let result = mgr
            .try_acquire("key1", "owner1", Duration::from_secs(10))
            .expect("try_acquire");
        assert!(result.is_some());

        let result2 = mgr
            .try_acquire("key1", "owner2", Duration::from_secs(10))
            .expect("try_acquire");
        assert!(result2.is_none());
    }

    #[test]
    fn test_lock_info() {
        let mgr = default_manager();
        let grant = mgr
            .acquire(
                "key1",
                "owner1",
                Duration::from_secs(30),
                LockType::Exclusive,
            )
            .expect("acquire");

        let info = mgr.lock_info("key1").expect("info");
        assert_eq!(info.key, "key1");
        assert_eq!(info.owner, "owner1");
        assert_eq!(info.token, grant.token);
    }

    #[test]
    fn test_owner_locks() {
        let mgr = default_manager();
        mgr.acquire("k1", "owner1", Duration::from_secs(30), LockType::Exclusive)
            .expect("acquire");
        mgr.acquire("k2", "owner1", Duration::from_secs(30), LockType::Exclusive)
            .expect("acquire");
        mgr.acquire("k3", "owner2", Duration::from_secs(30), LockType::Exclusive)
            .expect("acquire");

        let locks = mgr.owner_locks("owner1");
        assert_eq!(locks.len(), 2);
    }

    #[test]
    fn test_expire_stale() {
        let mgr = default_manager();
        mgr.acquire("k1", "o1", Duration::from_millis(1), LockType::Exclusive)
            .expect("acquire");
        mgr.acquire("k2", "o2", Duration::from_millis(1), LockType::Exclusive)
            .expect("acquire");
        mgr.acquire("k3", "o3", Duration::from_secs(60), LockType::Exclusive)
            .expect("acquire");

        std::thread::sleep(Duration::from_millis(5));
        let expired = mgr.expire_stale();
        assert_eq!(expired, 2);
    }

    #[test]
    fn test_queue_full() {
        let config = LockConfig {
            max_queue_size: 1,
            ..LockConfig::default()
        };
        let mgr = DistributedLockManager::new(config);
        mgr.acquire("k1", "o1", Duration::from_secs(30), LockType::Exclusive)
            .expect("acquire");
        mgr.acquire("k1", "o2", Duration::from_secs(30), LockType::Exclusive)
            .expect("queued");

        let result = mgr.acquire("k1", "o3", Duration::from_secs(30), LockType::Exclusive);
        assert!(matches!(result, Err(LockError::QueueFull(_, 1))));
    }

    #[test]
    fn test_invalid_ttl() {
        let mgr = default_manager();
        let result = mgr.acquire("k1", "o1", Duration::ZERO, LockType::Exclusive);
        assert!(matches!(result, Err(LockError::InvalidTtl(_))));

        let result = mgr.acquire("k1", "o1", Duration::from_secs(999), LockType::Exclusive);
        assert!(matches!(result, Err(LockError::InvalidTtl(_))));
    }

    #[test]
    fn test_stats() {
        let mgr = default_manager();
        mgr.acquire("k1", "o1", Duration::from_secs(30), LockType::Exclusive)
            .expect("acquire");

        let stats = mgr.stats();
        assert_eq!(stats.active_locks, 1);
        assert_eq!(stats.total_acquired, 1);
    }

    #[test]
    fn test_reader_writer_lock_type() {
        let mgr = default_manager();
        let lock_type = LockType::ReadWrite {
            readers: 3,
            writer: None,
        };
        let grant = mgr
            .acquire("k1", "o1", Duration::from_secs(10), lock_type)
            .expect("acquire");
        assert!(grant.token > 0);
        mgr.release("k1", grant.token).expect("release");
    }

    #[test]
    fn test_deadlock_detection_no_deadlock() {
        let mgr = default_manager();
        mgr.acquire("k1", "o1", Duration::from_secs(30), LockType::Exclusive)
            .expect("acquire");
        let cycles = mgr.detect_deadlocks();
        assert!(cycles.is_empty());
    }
}
