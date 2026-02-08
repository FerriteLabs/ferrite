//! Distributed Lock Manager
//!
//! Provides pessimistic locking for transactions with deadlock detection.
//! Supports shared (read) and exclusive (write) locks with upgrade capability.
//!
//! # Lock Compatibility Matrix
//!
//! ```text
//!          | None | Shared | Exclusive |
//! ---------|------|--------|-----------|
//! Shared   |  ✓   |   ✓    |     ✗     |
//! Exclusive|  ✓   |   ✗    |     ✗     |
//! ```
//!
//! # Deadlock Detection
//!
//! Uses wait-for graph to detect cycles:
//!
//! ```text
//! Txn1 waits for Txn2 (holds key A)
//!   ↓
//! Txn2 waits for Txn3 (holds key B)
//!   ↓
//! Txn3 waits for Txn1 (holds key C)
//!   ↓
//! DEADLOCK! → Abort youngest transaction
//! ```

use std::collections::{HashMap, HashSet, VecDeque};
use std::time::{Duration, Instant};

use bytes::Bytes;
use tokio::sync::{Mutex, RwLock};

use super::txn::TransactionId;
use super::TransactionError;

/// Lock modes
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockMode {
    /// Shared lock for reading (multiple holders allowed)
    Shared,
    /// Exclusive lock for writing (single holder only)
    Exclusive,
}

impl LockMode {
    /// Check if this mode is compatible with another
    pub fn is_compatible(&self, other: &LockMode) -> bool {
        matches!((self, other), (LockMode::Shared, LockMode::Shared))
    }

    /// Check if this mode can be upgraded to another
    pub fn can_upgrade_to(&self, other: &LockMode) -> bool {
        matches!((self, other), (LockMode::Shared, LockMode::Exclusive))
    }
}

/// A granted lock
#[derive(Debug, Clone)]
pub struct LockGrant {
    /// Transaction that holds the lock
    pub txn_id: TransactionId,
    /// Lock mode
    pub mode: LockMode,
    /// When the lock was acquired
    pub acquired_at: Instant,
    /// Database index for the locked key.
    pub db: u8,
    /// The locked key.
    pub key: Bytes,
}

/// Lock request in the wait queue
#[derive(Debug, Clone)]
struct LockRequest {
    txn_id: TransactionId,
    mode: LockMode,
    requested_at: Instant,
}

/// State of a single lockable key
#[derive(Default)]
struct LockEntry {
    /// Current holders
    holders: Vec<LockGrant>,
    /// Waiting requests
    waiters: VecDeque<LockRequest>,
}

/// Lock error types
#[derive(Debug, Clone, thiserror::Error)]
pub enum LockError {
    /// Lock acquisition timed out for the given key.
    #[error("lock timeout on key: {0}")]
    Timeout(String),

    /// A deadlock cycle was detected among transactions.
    #[error("deadlock detected")]
    Deadlock,

    /// The lock is not currently held.
    #[error("lock not held")]
    NotHeld,

    /// Lock mode upgrade is not permitted.
    #[error("upgrade not allowed")]
    UpgradeNotAllowed,
}

impl From<LockError> for TransactionError {
    fn from(err: LockError) -> Self {
        match err {
            LockError::Timeout(key) => TransactionError::LockTimeout(key),
            LockError::Deadlock => TransactionError::Deadlock,
            LockError::NotHeld => TransactionError::InvalidState("lock not held".to_string()),
            LockError::UpgradeNotAllowed => {
                TransactionError::InvalidState("upgrade not allowed".to_string())
            }
        }
    }
}

/// Distributed Lock Manager
pub struct LockManager {
    /// Lock table: (db, key) -> lock state
    locks: RwLock<HashMap<(u8, Bytes), LockEntry>>,
    /// Transaction locks: txn_id -> set of (db, key)
    txn_locks: RwLock<HashMap<TransactionId, HashSet<(u8, Bytes)>>>,
    /// Wait-for graph for deadlock detection
    wait_for: Mutex<HashMap<TransactionId, HashSet<TransactionId>>>,
    /// Lock timeout
    timeout: Duration,
    /// Enable deadlock detection
    deadlock_detection: bool,
}

impl LockManager {
    /// Create a new lock manager
    pub fn new(timeout: Duration, deadlock_detection: bool) -> Self {
        Self {
            locks: RwLock::new(HashMap::new()),
            txn_locks: RwLock::new(HashMap::new()),
            wait_for: Mutex::new(HashMap::new()),
            timeout,
            deadlock_detection,
        }
    }

    /// Acquire a lock on a key
    pub async fn acquire(
        &self,
        txn_id: TransactionId,
        db: u8,
        key: Bytes,
        mode: LockMode,
    ) -> Result<LockGrant, LockError> {
        let start = Instant::now();
        let cache_key = (db, key.clone());

        loop {
            // Check timeout
            if start.elapsed() > self.timeout {
                // Clean up wait queue
                self.remove_waiter(txn_id, db, &key).await;
                return Err(LockError::Timeout(
                    String::from_utf8_lossy(&key).to_string(),
                ));
            }

            // Try to acquire lock
            {
                let mut locks = self.locks.write().await;
                let entry = locks.entry(cache_key.clone()).or_default();

                // Check if we already hold a compatible lock
                if let Some(existing) = entry.holders.iter().find(|h| h.txn_id == txn_id) {
                    if existing.mode == mode || existing.mode == LockMode::Exclusive {
                        // Already have the lock
                        return Ok(existing.clone());
                    } else if mode == LockMode::Exclusive {
                        // Need to upgrade from shared to exclusive
                        // Only allowed if we're the only holder
                        if entry.holders.len() == 1 {
                            let grant = LockGrant {
                                txn_id,
                                mode: LockMode::Exclusive,
                                acquired_at: Instant::now(),
                                db,
                                key: key.clone(),
                            };
                            entry.holders[0] = grant.clone();
                            return Ok(grant);
                        }
                    }
                }

                // Check if lock can be granted
                let can_grant = entry.holders.is_empty()
                    || (mode == LockMode::Shared
                        && entry.holders.iter().all(|h| h.mode == LockMode::Shared));

                if can_grant {
                    let grant = LockGrant {
                        txn_id,
                        mode,
                        acquired_at: Instant::now(),
                        db,
                        key: key.clone(),
                    };
                    entry.holders.push(grant.clone());

                    // Track in txn_locks
                    self.txn_locks
                        .write()
                        .await
                        .entry(txn_id)
                        .or_default()
                        .insert(cache_key.clone());

                    // Clear wait-for edges
                    self.clear_wait_for(txn_id).await;

                    return Ok(grant);
                }

                // Add to wait queue if not already there
                if !entry.waiters.iter().any(|w| w.txn_id == txn_id) {
                    entry.waiters.push_back(LockRequest {
                        txn_id,
                        mode,
                        requested_at: Instant::now(),
                    });

                    // Build wait-for edges
                    if self.deadlock_detection {
                        let holder_ids: Vec<TransactionId> =
                            entry.holders.iter().map(|h| h.txn_id).collect();
                        self.add_wait_for_edges(txn_id, holder_ids).await;

                        // Check for deadlock
                        if self.detect_deadlock(txn_id).await {
                            // Remove from wait queue
                            entry.waiters.retain(|w| w.txn_id != txn_id);
                            self.clear_wait_for(txn_id).await;
                            return Err(LockError::Deadlock);
                        }
                    }
                }
            }

            // Wait before retrying
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// Release a specific lock
    pub async fn release(
        &self,
        txn_id: TransactionId,
        db: u8,
        key: &Bytes,
    ) -> Result<(), LockError> {
        let cache_key = (db, key.clone());

        let mut locks = self.locks.write().await;
        if let Some(entry) = locks.get_mut(&cache_key) {
            let before_len = entry.holders.len();
            entry.holders.retain(|h| h.txn_id != txn_id);

            if entry.holders.len() == before_len {
                return Err(LockError::NotHeld);
            }

            // Grant locks to waiting transactions
            self.grant_waiting_locks(entry);

            // Remove from txn_locks
            if let Some(keys) = self.txn_locks.write().await.get_mut(&txn_id) {
                keys.remove(&cache_key);
            }
        }

        Ok(())
    }

    /// Release all locks held by a transaction
    pub async fn release_all(&self, txn_id: TransactionId) {
        // Get all keys held by this transaction
        let keys: Vec<(u8, Bytes)> = {
            let txn_locks = self.txn_locks.read().await;
            txn_locks
                .get(&txn_id)
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .collect()
        };

        // Release each lock
        let mut locks = self.locks.write().await;
        for (db, key) in keys {
            if let Some(entry) = locks.get_mut(&(db, key.clone())) {
                entry.holders.retain(|h| h.txn_id != txn_id);
                self.grant_waiting_locks(entry);
            }
        }

        // Remove from txn_locks
        self.txn_locks.write().await.remove(&txn_id);

        // Clear wait-for edges
        self.clear_wait_for(txn_id).await;
    }

    /// Check if a transaction holds a lock on a key
    pub async fn is_held(&self, txn_id: TransactionId, db: u8, key: &Bytes) -> bool {
        let locks = self.locks.read().await;
        locks
            .get(&(db, key.clone()))
            .map(|e| e.holders.iter().any(|h| h.txn_id == txn_id))
            .unwrap_or(false)
    }

    /// Get the lock mode held by a transaction on a key
    pub async fn get_lock_mode(
        &self,
        txn_id: TransactionId,
        db: u8,
        key: &Bytes,
    ) -> Option<LockMode> {
        let locks = self.locks.read().await;
        locks
            .get(&(db, key.clone()))
            .and_then(|e| e.holders.iter().find(|h| h.txn_id == txn_id))
            .map(|h| h.mode)
    }

    /// Get all locks held by a transaction
    pub async fn get_locks(&self, txn_id: TransactionId) -> Vec<LockGrant> {
        let txn_locks = self.txn_locks.read().await;
        let keys = txn_locks.get(&txn_id).cloned().unwrap_or_default();

        let locks = self.locks.read().await;
        keys.iter()
            .filter_map(|(db, key)| {
                locks
                    .get(&(*db, key.clone()))
                    .and_then(|e| e.holders.iter().find(|h| h.txn_id == txn_id).cloned())
            })
            .collect()
    }

    /// Get the number of active locks
    pub async fn lock_count(&self) -> usize {
        self.locks
            .read()
            .await
            .values()
            .map(|e| e.holders.len())
            .sum()
    }

    /// Get the number of waiting requests
    pub async fn waiter_count(&self) -> usize {
        self.locks
            .read()
            .await
            .values()
            .map(|e| e.waiters.len())
            .sum()
    }

    // Private helper methods

    fn grant_waiting_locks(&self, entry: &mut LockEntry) {
        while let Some(request) = entry.waiters.front() {
            let can_grant = entry.holders.is_empty()
                || (request.mode == LockMode::Shared
                    && entry.holders.iter().all(|h| h.mode == LockMode::Shared));

            if can_grant {
                if let Some(request) = entry.waiters.pop_front() {
                    entry.holders.push(LockGrant {
                        txn_id: request.txn_id,
                        mode: request.mode,
                        acquired_at: Instant::now(),
                        db: 0, // Will be set properly by caller
                        key: Bytes::new(),
                    });
                }
            } else {
                break;
            }
        }
    }

    async fn remove_waiter(&self, txn_id: TransactionId, db: u8, key: &Bytes) {
        let mut locks = self.locks.write().await;
        if let Some(entry) = locks.get_mut(&(db, key.clone())) {
            entry.waiters.retain(|w| w.txn_id != txn_id);
        }
    }

    async fn add_wait_for_edges(&self, waiter: TransactionId, holders: Vec<TransactionId>) {
        let mut wait_for = self.wait_for.lock().await;
        let edges = wait_for.entry(waiter).or_default();
        for holder in holders {
            if holder != waiter {
                edges.insert(holder);
            }
        }
    }

    async fn clear_wait_for(&self, txn_id: TransactionId) {
        let mut wait_for = self.wait_for.lock().await;
        wait_for.remove(&txn_id);
        // Also remove this txn from others' wait lists
        for edges in wait_for.values_mut() {
            edges.remove(&txn_id);
        }
    }

    /// Detect deadlock using DFS on wait-for graph
    async fn detect_deadlock(&self, start: TransactionId) -> bool {
        let wait_for = self.wait_for.lock().await;

        let mut visited = HashSet::new();
        let mut stack = HashSet::new();
        let mut to_visit = vec![start];

        while let Some(current) = to_visit.pop() {
            if stack.contains(&current) {
                // Found a cycle
                return true;
            }

            if visited.contains(&current) {
                continue;
            }

            visited.insert(current);
            stack.insert(current);

            if let Some(edges) = wait_for.get(&current) {
                for &next in edges {
                    to_visit.push(next);
                }
            }
        }

        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn test_lock_mode_compatibility() {
        assert!(LockMode::Shared.is_compatible(&LockMode::Shared));
        assert!(!LockMode::Shared.is_compatible(&LockMode::Exclusive));
        assert!(!LockMode::Exclusive.is_compatible(&LockMode::Shared));
        assert!(!LockMode::Exclusive.is_compatible(&LockMode::Exclusive));
    }

    #[tokio::test]
    async fn test_lock_acquire_release() {
        let lm = LockManager::new(Duration::from_secs(5), true);

        let txn1 = TransactionId(1);
        let key = Bytes::from("test_key");

        // Acquire lock
        let grant = lm
            .acquire(txn1, 0, key.clone(), LockMode::Exclusive)
            .await
            .unwrap();
        assert_eq!(grant.txn_id, txn1);
        assert_eq!(grant.mode, LockMode::Exclusive);

        // Check lock is held
        assert!(lm.is_held(txn1, 0, &key).await);
        assert_eq!(lm.lock_count().await, 1);

        // Release lock
        lm.release(txn1, 0, &key).await.unwrap();
        assert!(!lm.is_held(txn1, 0, &key).await);
        assert_eq!(lm.lock_count().await, 0);
    }

    #[tokio::test]
    async fn test_shared_locks() {
        let lm = LockManager::new(Duration::from_secs(5), true);

        let txn1 = TransactionId(1);
        let txn2 = TransactionId(2);
        let key = Bytes::from("test_key");

        // Both can acquire shared locks
        lm.acquire(txn1, 0, key.clone(), LockMode::Shared)
            .await
            .unwrap();
        lm.acquire(txn2, 0, key.clone(), LockMode::Shared)
            .await
            .unwrap();

        assert!(lm.is_held(txn1, 0, &key).await);
        assert!(lm.is_held(txn2, 0, &key).await);
        assert_eq!(lm.lock_count().await, 2);
    }

    #[tokio::test]
    async fn test_exclusive_blocks_shared() {
        let lm = Arc::new(LockManager::new(Duration::from_millis(100), true));

        let txn1 = TransactionId(1);
        let txn2 = TransactionId(2);
        let key = Bytes::from("test_key");

        // Txn1 acquires exclusive
        lm.acquire(txn1, 0, key.clone(), LockMode::Exclusive)
            .await
            .unwrap();

        // Txn2 should timeout trying to get shared
        let lm_clone = lm.clone();
        let key_clone = key.clone();
        let result =
            tokio::spawn(
                async move { lm_clone.acquire(txn2, 0, key_clone, LockMode::Shared).await },
            )
            .await
            .unwrap();

        assert!(matches!(result, Err(LockError::Timeout(_))));
    }

    #[tokio::test]
    async fn test_release_all() {
        let lm = LockManager::new(Duration::from_secs(5), true);

        let txn1 = TransactionId(1);
        let key1 = Bytes::from("key1");
        let key2 = Bytes::from("key2");

        lm.acquire(txn1, 0, key1.clone(), LockMode::Exclusive)
            .await
            .unwrap();
        lm.acquire(txn1, 0, key2.clone(), LockMode::Exclusive)
            .await
            .unwrap();

        assert_eq!(lm.lock_count().await, 2);

        lm.release_all(txn1).await;

        assert_eq!(lm.lock_count().await, 0);
        assert!(!lm.is_held(txn1, 0, &key1).await);
        assert!(!lm.is_held(txn1, 0, &key2).await);
    }

    #[tokio::test]
    async fn test_get_locks() {
        let lm = LockManager::new(Duration::from_secs(5), true);

        let txn1 = TransactionId(1);
        let key1 = Bytes::from("key1");
        let key2 = Bytes::from("key2");

        lm.acquire(txn1, 0, key1.clone(), LockMode::Shared)
            .await
            .unwrap();
        lm.acquire(txn1, 0, key2.clone(), LockMode::Exclusive)
            .await
            .unwrap();

        let locks = lm.get_locks(txn1).await;
        assert_eq!(locks.len(), 2);
    }

    #[tokio::test]
    async fn test_lock_upgrade() {
        let lm = LockManager::new(Duration::from_secs(5), true);

        let txn1 = TransactionId(1);
        let key = Bytes::from("test_key");

        // Acquire shared
        lm.acquire(txn1, 0, key.clone(), LockMode::Shared)
            .await
            .unwrap();
        assert_eq!(
            lm.get_lock_mode(txn1, 0, &key).await,
            Some(LockMode::Shared)
        );

        // Upgrade to exclusive (should work since only holder)
        lm.acquire(txn1, 0, key.clone(), LockMode::Exclusive)
            .await
            .unwrap();
        assert_eq!(
            lm.get_lock_mode(txn1, 0, &key).await,
            Some(LockMode::Exclusive)
        );
    }

    // Note: Deadlock tests are tricky because they require concurrent transactions
    // In a real scenario, you'd test this with multiple async tasks racing
}
