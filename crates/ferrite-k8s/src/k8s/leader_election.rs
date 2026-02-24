//! Leader Election for High-Availability Operator Deployment
//!
//! Implements leader election using Kubernetes Lease objects so that only
//! one operator replica is actively reconciling at any given time.
//!
//! # Design
//!
//! - Uses a `Lease` resource in the operator namespace as the lock.
//! - The holder periodically renews the lease; if a renewal is missed
//!   (e.g. pod crash) another replica can acquire it after the TTL expires.
//! - Transitions are signalled via [`LeaderEvent`] callbacks.

use super::operator::OperatorError;
use parking_lot::RwLock;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Configuration for leader election.
#[derive(Debug, Clone)]
pub struct LeaderElectionConfig {
    /// Name of the Lease resource used as the election lock.
    pub lease_name: String,
    /// Namespace of the Lease resource.
    pub lease_namespace: String,
    /// How long a lease is valid before it must be renewed.
    pub lease_duration: Duration,
    /// How often the holder attempts to renew the lease.
    pub renew_interval: Duration,
    /// How often a non-leader checks whether the lease has expired.
    pub retry_interval: Duration,
    /// Identity of this operator instance (typically pod name).
    pub identity: String,
}

impl Default for LeaderElectionConfig {
    fn default() -> Self {
        Self {
            lease_name: "ferrite-operator-leader".to_string(),
            lease_namespace: "ferrite-system".to_string(),
            lease_duration: Duration::from_secs(15),
            renew_interval: Duration::from_secs(10),
            retry_interval: Duration::from_secs(5),
            identity: uuid::Uuid::new_v4().to_string(),
        }
    }
}

/// Events emitted by the leader elector.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LeaderEvent {
    /// This instance became the leader.
    BecameLeader,
    /// This instance lost leadership (lease expired or was taken over).
    LostLeadership,
    /// Leadership renewal succeeded.
    RenewedLease,
    /// Leadership renewal failed (will retry).
    RenewFailed { reason: String },
}

/// In-memory representation of a Kubernetes Lease object.
#[derive(Debug, Clone)]
pub struct LeaseRecord {
    /// Identity of the current holder.
    pub holder_identity: String,
    /// When the lease was acquired.
    pub acquire_time: Instant,
    /// When the lease was last renewed.
    pub renew_time: Instant,
    /// Duration of the lease.
    pub lease_duration: Duration,
    /// Monotonic lease transitions counter.
    pub transitions: u64,
}

impl LeaseRecord {
    /// Check whether the lease has expired.
    pub fn is_expired(&self) -> bool {
        self.renew_time.elapsed() > self.lease_duration
    }
}

/// State of the leader elector.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ElectorState {
    /// Not yet started.
    Idle,
    /// Actively attempting to acquire the lease.
    Waiting,
    /// This instance holds the lease.
    Leading,
    /// The elector has been stopped.
    Stopped,
}

/// Leader elector that manages a Kubernetes Lease for HA deployments.
///
/// In production this would interact with the K8s API; here we use an
/// in-memory `Arc<RwLock<Option<LeaseRecord>>>` so the rest of the
/// operator logic can be exercised without a live cluster.
pub struct LeaderElector {
    config: LeaderElectionConfig,
    state: RwLock<ElectorState>,
    lease: Arc<RwLock<Option<LeaseRecord>>>,
    events: RwLock<Vec<LeaderEvent>>,
}

impl LeaderElector {
    /// Create a new leader elector with the given configuration.
    pub fn new(config: LeaderElectionConfig) -> Self {
        Self {
            config,
            state: RwLock::new(ElectorState::Idle),
            lease: Arc::new(RwLock::new(None)),
            events: RwLock::new(Vec::new()),
        }
    }

    /// Try to acquire the lease. Returns `true` if this instance is now the leader.
    pub async fn try_acquire(&self) -> Result<bool, OperatorError> {
        let mut lease_guard = self.lease.write();

        match lease_guard.as_ref() {
            Some(record) if !record.is_expired() => {
                if record.holder_identity == self.config.identity {
                    // Already the leader — renew
                    let mut updated = record.clone();
                    updated.renew_time = Instant::now();
                    *lease_guard = Some(updated);
                    self.push_event(LeaderEvent::RenewedLease);
                    Ok(true)
                } else {
                    // Someone else holds the lease
                    *self.state.write() = ElectorState::Waiting;
                    Ok(false)
                }
            }
            _ => {
                // Lease is vacant or expired — acquire
                let transitions = lease_guard.as_ref().map(|r| r.transitions + 1).unwrap_or(0);

                let now = Instant::now();
                *lease_guard = Some(LeaseRecord {
                    holder_identity: self.config.identity.clone(),
                    acquire_time: now,
                    renew_time: now,
                    lease_duration: self.config.lease_duration,
                    transitions,
                });

                *self.state.write() = ElectorState::Leading;
                self.push_event(LeaderEvent::BecameLeader);

                tracing::info!(
                    identity = %self.config.identity,
                    lease = %self.config.lease_name,
                    "Acquired leadership"
                );

                Ok(true)
            }
        }
    }

    /// Renew the lease. Only succeeds if this instance currently holds it.
    pub async fn renew(&self) -> Result<bool, OperatorError> {
        let mut lease_guard = self.lease.write();

        match lease_guard.as_mut() {
            Some(record) if record.holder_identity == self.config.identity => {
                record.renew_time = Instant::now();
                self.push_event(LeaderEvent::RenewedLease);
                Ok(true)
            }
            _ => {
                *self.state.write() = ElectorState::Waiting;
                self.push_event(LeaderEvent::LostLeadership);
                tracing::warn!(
                    identity = %self.config.identity,
                    "Lost leadership — lease held by another instance"
                );
                Ok(false)
            }
        }
    }

    /// Release the lease voluntarily (e.g. during graceful shutdown).
    pub async fn release(&self) -> Result<(), OperatorError> {
        let mut lease_guard = self.lease.write();
        if let Some(record) = lease_guard.as_ref() {
            if record.holder_identity == self.config.identity {
                *lease_guard = None;
                *self.state.write() = ElectorState::Stopped;
                self.push_event(LeaderEvent::LostLeadership);
                tracing::info!(
                    identity = %self.config.identity,
                    "Released leadership"
                );
            }
        }
        Ok(())
    }

    /// Check whether this instance is currently the leader.
    pub fn is_leader(&self) -> bool {
        *self.state.read() == ElectorState::Leading
    }

    /// Get the current elector state.
    pub fn state(&self) -> ElectorState {
        self.state.read().clone()
    }

    /// Get the current lease holder identity, if any.
    pub fn current_leader(&self) -> Option<String> {
        self.lease
            .read()
            .as_ref()
            .filter(|r| !r.is_expired())
            .map(|r| r.holder_identity.clone())
    }

    /// Get the configuration.
    pub fn config(&self) -> &LeaderElectionConfig {
        &self.config
    }

    /// Drain all recorded events (for testing / observability).
    pub fn drain_events(&self) -> Vec<LeaderEvent> {
        std::mem::take(&mut *self.events.write())
    }

    /// Get a shared reference to the lease store (for multi-instance tests).
    pub fn lease_store(&self) -> Arc<RwLock<Option<LeaseRecord>>> {
        self.lease.clone()
    }

    /// Create a new elector that shares the same lease store (simulates
    /// a second operator pod competing for the same Lease resource).
    pub fn new_competing(
        config: LeaderElectionConfig,
        lease_store: Arc<RwLock<Option<LeaseRecord>>>,
    ) -> Self {
        Self {
            config,
            state: RwLock::new(ElectorState::Idle),
            lease: lease_store,
            events: RwLock::new(Vec::new()),
        }
    }

    fn push_event(&self, event: LeaderEvent) {
        self.events.write().push(event);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config(identity: &str) -> LeaderElectionConfig {
        LeaderElectionConfig {
            identity: identity.to_string(),
            lease_duration: Duration::from_millis(200),
            renew_interval: Duration::from_millis(100),
            retry_interval: Duration::from_millis(50),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn test_acquire_leadership() {
        let elector = LeaderElector::new(test_config("pod-1"));

        assert!(!elector.is_leader());
        assert_eq!(elector.state(), ElectorState::Idle);

        let acquired = elector.try_acquire().await.unwrap();
        assert!(acquired);
        assert!(elector.is_leader());
        assert_eq!(elector.state(), ElectorState::Leading);
        assert_eq!(elector.current_leader(), Some("pod-1".to_string()));
    }

    #[tokio::test]
    async fn test_renew_leadership() {
        let elector = LeaderElector::new(test_config("pod-1"));
        elector.try_acquire().await.unwrap();

        let renewed = elector.renew().await.unwrap();
        assert!(renewed);
        assert!(elector.is_leader());
    }

    #[tokio::test]
    async fn test_release_leadership() {
        let elector = LeaderElector::new(test_config("pod-1"));
        elector.try_acquire().await.unwrap();
        assert!(elector.is_leader());

        elector.release().await.unwrap();
        assert!(!elector.is_leader());
        assert_eq!(elector.state(), ElectorState::Stopped);
        assert!(elector.current_leader().is_none());
    }

    #[tokio::test]
    async fn test_competing_electors() {
        let elector1 = LeaderElector::new(test_config("pod-1"));
        elector1.try_acquire().await.unwrap();
        assert!(elector1.is_leader());

        // Pod-2 shares the same lease store
        let store = elector1.lease_store();
        let elector2 = LeaderElector::new_competing(test_config("pod-2"), store);

        // Pod-2 cannot acquire while pod-1 holds the lease
        let acquired = elector2.try_acquire().await.unwrap();
        assert!(!acquired);
        assert!(!elector2.is_leader());
    }

    #[tokio::test]
    async fn test_takeover_after_expiry() {
        let config1 = LeaderElectionConfig {
            identity: "pod-1".to_string(),
            lease_duration: Duration::from_millis(50),
            ..test_config("pod-1")
        };
        let elector1 = LeaderElector::new(config1);
        elector1.try_acquire().await.unwrap();
        assert!(elector1.is_leader());

        let store = elector1.lease_store();

        // Wait for lease to expire
        tokio::time::sleep(Duration::from_millis(100)).await;

        let elector2 = LeaderElector::new_competing(test_config("pod-2"), store);
        let acquired = elector2.try_acquire().await.unwrap();
        assert!(acquired);
        assert!(elector2.is_leader());
        assert_eq!(elector2.current_leader(), Some("pod-2".to_string()));
    }

    #[tokio::test]
    async fn test_events() {
        let elector = LeaderElector::new(test_config("pod-1"));
        elector.try_acquire().await.unwrap();
        elector.renew().await.unwrap();
        elector.release().await.unwrap();

        let events = elector.drain_events();
        assert_eq!(events.len(), 3);
        assert_eq!(events[0], LeaderEvent::BecameLeader);
        assert_eq!(events[1], LeaderEvent::RenewedLease);
        assert_eq!(events[2], LeaderEvent::LostLeadership);
    }

    #[tokio::test]
    async fn test_renew_after_lost() {
        let elector1 = LeaderElector::new(LeaderElectionConfig {
            identity: "pod-1".to_string(),
            lease_duration: Duration::from_millis(50),
            ..test_config("pod-1")
        });
        elector1.try_acquire().await.unwrap();
        let store = elector1.lease_store();

        // Wait for expiry, then pod-2 takes over
        tokio::time::sleep(Duration::from_millis(100)).await;
        let elector2 = LeaderElector::new_competing(test_config("pod-2"), store.clone());
        elector2.try_acquire().await.unwrap();

        // Pod-1 tries to renew — should fail
        let renewed = elector1.renew().await.unwrap();
        assert!(!renewed);
        assert!(!elector1.is_leader());

        let events = elector1.drain_events();
        assert!(events.contains(&LeaderEvent::LostLeadership));
    }

    #[tokio::test]
    async fn test_lease_transitions_counter() {
        let elector = LeaderElector::new(LeaderElectionConfig {
            identity: "pod-1".to_string(),
            lease_duration: Duration::from_millis(50),
            ..test_config("pod-1")
        });
        elector.try_acquire().await.unwrap();
        let store = elector.lease_store();

        // Wait for expiry
        tokio::time::sleep(Duration::from_millis(100)).await;

        let elector2 = LeaderElector::new_competing(test_config("pod-2"), store.clone());
        elector2.try_acquire().await.unwrap();

        let lease = store.read();
        let record = lease.as_ref().unwrap();
        assert_eq!(record.transitions, 1);
        assert_eq!(record.holder_identity, "pod-2");
    }
}
