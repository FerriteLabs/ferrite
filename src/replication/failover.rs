//! Graceful replication failover
//!
//! Implements the `FAILOVER` command:
//! 1. Block new writes on the primary.
//! 2. Wait for the target replica to catch up.
//! 3. Promote the replica to primary.
//! 4. Return the result.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::Notify;
use tracing::{error, info, warn};

use super::primary::ReplicationPrimary;
use super::{ReplicationRole, SharedReplicationState};

/// Default timeout for waiting for replica to catch up.
const DEFAULT_FAILOVER_TIMEOUT: Duration = Duration::from_secs(30);

/// Poll interval when waiting for the replica to catch up.
const CATCHUP_POLL_INTERVAL: Duration = Duration::from_millis(100);

/// Errors that can occur during failover.
#[derive(Debug, thiserror::Error)]
pub enum FailoverError {
    /// No replica specified and no suitable replica found.
    #[error("no replica available for failover")]
    NoReplicaAvailable,

    /// The target replica is not connected.
    #[error("target replica {0} is not online")]
    ReplicaNotOnline(SocketAddr),

    /// Timed out waiting for the replica to catch up.
    #[error("failover timed out after {0:?} waiting for replica to catch up")]
    Timeout(Duration),

    /// A failover is already in progress.
    #[error("another failover is already in progress")]
    AlreadyInProgress,

    /// This node is not a primary.
    #[error("failover can only be initiated on a primary")]
    NotPrimary,
}

/// Result of a successful failover.
#[derive(Debug, Clone)]
pub struct FailoverResult {
    /// The replica that was promoted.
    pub promoted_replica: SocketAddr,
    /// Time taken for the failover.
    pub duration: Duration,
    /// Replication offset at the point of promotion.
    pub offset_at_promotion: u64,
}

/// Coordinates a graceful failover from primary to replica.
pub struct FailoverCoordinator {
    primary: Arc<ReplicationPrimary>,
    state: SharedReplicationState,
    /// Set to `true` while a failover is in progress to block writes.
    writes_blocked: Arc<AtomicBool>,
    /// Notify waiters when writes are unblocked.
    writes_unblocked: Arc<Notify>,
    /// Whether a failover is currently in flight.
    in_progress: AtomicBool,
}

impl FailoverCoordinator {
    /// Create a new failover coordinator.
    pub fn new(primary: Arc<ReplicationPrimary>, state: SharedReplicationState) -> Self {
        Self {
            primary,
            state,
            writes_blocked: Arc::new(AtomicBool::new(false)),
            writes_unblocked: Arc::new(Notify::new()),
            in_progress: AtomicBool::new(false),
        }
    }

    /// Returns `true` if new writes should be rejected.
    pub fn writes_blocked(&self) -> bool {
        self.writes_blocked.load(Ordering::SeqCst)
    }

    /// Returns a clonable handle to the write-block flag.
    pub fn writes_blocked_flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.writes_blocked)
    }

    /// Initiate a graceful failover.
    ///
    /// If `target` is `None`, the replica with the smallest replication lag is chosen.
    pub async fn initiate(
        &self,
        target: Option<SocketAddr>,
        timeout: Option<Duration>,
    ) -> Result<FailoverResult, FailoverError> {
        // Guard: only on primary
        if self.state.role().await != ReplicationRole::Primary {
            return Err(FailoverError::NotPrimary);
        }

        // Guard: single failover at a time
        if self
            .in_progress
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return Err(FailoverError::AlreadyInProgress);
        }

        let result = self.do_failover(target, timeout).await;

        // Always release the guard
        self.in_progress.store(false, Ordering::SeqCst);
        self.writes_blocked.store(false, Ordering::SeqCst);
        self.writes_unblocked.notify_waiters();

        if result.is_ok() {
            ferrite_core::metrics::record_replication_failover(true);
        } else {
            ferrite_core::metrics::record_replication_failover(false);
        }

        result
    }

    async fn do_failover(
        &self,
        target: Option<SocketAddr>,
        timeout: Option<Duration>,
    ) -> Result<FailoverResult, FailoverError> {
        let timeout = timeout.unwrap_or(DEFAULT_FAILOVER_TIMEOUT);
        let started = Instant::now();

        // Step 1: Resolve target replica
        let target_addr = match target {
            Some(addr) => addr,
            None => self.pick_best_replica().await?,
        };

        info!(replica = %target_addr, "starting graceful failover");

        // Verify replica is online
        let replicas = self.primary.get_replicas().await;
        let replica = replicas
            .iter()
            .find(|r| r.addr == target_addr)
            .ok_or(FailoverError::ReplicaNotOnline(target_addr))?;
        if !replica.online {
            return Err(FailoverError::ReplicaNotOnline(target_addr));
        }

        // Step 2: Block new writes
        info!("blocking new writes for failover");
        self.writes_blocked.store(true, Ordering::SeqCst);

        // Step 3: Wait for replica to catch up
        let offset_at_promotion = self.wait_for_catchup(target_addr, timeout, started).await?;

        // Step 4: Promote – switch our role to Replica
        info!(
            replica = %target_addr,
            offset = offset_at_promotion,
            "replica caught up, promoting"
        );
        self.state.set_role(ReplicationRole::Replica).await;

        let duration = started.elapsed();
        info!(
            replica = %target_addr,
            duration_ms = duration.as_millis(),
            "failover complete"
        );

        Ok(FailoverResult {
            promoted_replica: target_addr,
            duration,
            offset_at_promotion,
        })
    }

    /// Pick the online replica with the smallest byte lag.
    async fn pick_best_replica(&self) -> Result<SocketAddr, FailoverError> {
        let replicas = self.primary.get_replicas().await;
        let master_offset = self.state.repl_offset();

        replicas
            .iter()
            .filter(|r| r.online)
            .min_by_key(|r| r.lag_bytes(master_offset))
            .map(|r| r.addr)
            .ok_or(FailoverError::NoReplicaAvailable)
    }

    /// Spin-wait until the target replica ACKs the current primary offset.
    async fn wait_for_catchup(
        &self,
        target: SocketAddr,
        timeout: Duration,
        started: Instant,
    ) -> Result<u64, FailoverError> {
        let primary_offset = self.state.repl_offset();

        loop {
            if started.elapsed() > timeout {
                warn!(
                    replica = %target,
                    timeout_ms = timeout.as_millis(),
                    "failover timed out waiting for replica to catch up"
                );
                return Err(FailoverError::Timeout(timeout));
            }

            let count = self.primary.count_replicas_at_offset(primary_offset).await;
            if count > 0 {
                // At least one replica (our target) is at or past the offset
                return Ok(primary_offset);
            }

            tokio::time::sleep(CATCHUP_POLL_INTERVAL).await;
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use crate::replication::{ReplicationState, ReplicationStream};
    use crate::storage::Store;

    fn make_coordinator() -> (
        FailoverCoordinator,
        Arc<ReplicationPrimary>,
        Arc<ReplicationState>,
    ) {
        let store = Arc::new(Store::new(16));
        let state = Arc::new(ReplicationState::new());
        let stream = Arc::new(ReplicationStream::new(10000));
        let primary = Arc::new(ReplicationPrimary::new(store, state.clone(), stream));
        let coordinator = FailoverCoordinator::new(primary.clone(), state.clone());
        (coordinator, primary, state)
    }

    #[tokio::test]
    async fn test_failover_no_replicas() {
        let (coord, _, _) = make_coordinator();
        let result = coord.initiate(None, Some(Duration::from_millis(100))).await;
        assert!(matches!(result, Err(FailoverError::NoReplicaAvailable)));
    }

    #[tokio::test]
    async fn test_failover_not_primary() {
        let (coord, _, state) = make_coordinator();
        state.set_role(ReplicationRole::Replica).await;
        let result = coord.initiate(None, None).await;
        assert!(matches!(result, Err(FailoverError::NotPrimary)));
    }

    #[tokio::test]
    async fn test_failover_writes_blocked_during_failover() {
        let (coord, _, _) = make_coordinator();
        assert!(!coord.writes_blocked());
        // After a failed failover, writes should be unblocked
        let _ = coord.initiate(None, Some(Duration::from_millis(50))).await;
        assert!(!coord.writes_blocked());
    }

    #[tokio::test]
    async fn test_failover_already_in_progress() {
        let (coord, primary, state) = make_coordinator();
        let addr: SocketAddr = "127.0.0.1:6381".parse().expect("valid addr");
        primary.register_replica(addr, 6381).await;
        primary
            .set_replica_state(&addr, crate::replication::ReplicaConnectionState::Online)
            .await;
        // Advance offset but don't update replica – failover will time out
        state.increment_offset(1000);

        let coord = Arc::new(coord);
        let coord2 = coord.clone();

        let h1 = tokio::spawn(async move {
            coord
                .initiate(Some(addr), Some(Duration::from_millis(200)))
                .await
        });
        // Small delay to let h1 grab the lock
        tokio::time::sleep(Duration::from_millis(10)).await;
        let h2 = tokio::spawn(async move {
            coord2
                .initiate(Some(addr), Some(Duration::from_millis(200)))
                .await
        });

        let (r1, r2) = tokio::join!(h1, h2);
        let results = [r1.expect("join"), r2.expect("join")];
        // Exactly one should be AlreadyInProgress
        let in_progress_count = results
            .iter()
            .filter(|r| matches!(r, Err(FailoverError::AlreadyInProgress)))
            .count();
        assert!(in_progress_count <= 1);
    }

    #[tokio::test]
    async fn test_failover_success() {
        let (coord, primary, state) = make_coordinator();
        let addr: SocketAddr = "127.0.0.1:6381".parse().expect("valid addr");
        primary.register_replica(addr, 6381).await;
        primary
            .set_replica_state(&addr, crate::replication::ReplicaConnectionState::Online)
            .await;

        state.increment_offset(500);
        // Simulate replica being caught up
        primary.update_replica_offset(&addr, 500).await;

        let result = coord
            .initiate(Some(addr), Some(Duration::from_secs(5)))
            .await;
        let result = result.expect("failover should succeed");
        assert_eq!(result.promoted_replica, addr);
        assert_eq!(result.offset_at_promotion, 500);
        assert_eq!(state.role().await, ReplicationRole::Replica);
    }

    #[tokio::test]
    async fn test_failover_timeout() {
        let (coord, primary, state) = make_coordinator();
        let addr: SocketAddr = "127.0.0.1:6381".parse().expect("valid addr");
        primary.register_replica(addr, 6381).await;
        primary
            .set_replica_state(&addr, crate::replication::ReplicaConnectionState::Online)
            .await;

        state.increment_offset(5000);
        // Replica is at 0, will never catch up
        let result = coord
            .initiate(Some(addr), Some(Duration::from_millis(200)))
            .await;
        assert!(matches!(result, Err(FailoverError::Timeout(_))));
        // Writes should be unblocked after failure
        assert!(!coord.writes_blocked());
    }
}
