//! Replication health monitoring
//!
//! Tracks replication lag (bytes and seconds) per replica and emits
//! Prometheus-compatible metrics via the `metrics` crate.

use std::sync::Arc;
use std::time::Duration;

use tokio::time::interval;
use tracing::{debug, warn};

use super::primary::ReplicationPrimary;
use super::SharedReplicationState;

/// Periodically samples replication lag and publishes metrics.
pub struct ReplicationHealthMonitor {
    primary: Arc<ReplicationPrimary>,
    state: SharedReplicationState,
    /// How often to sample lag metrics
    sample_interval: Duration,
}

impl ReplicationHealthMonitor {
    /// Create a new health monitor.
    pub fn new(
        primary: Arc<ReplicationPrimary>,
        state: SharedReplicationState,
        sample_interval: Duration,
    ) -> Self {
        Self {
            primary,
            state,
            sample_interval,
        }
    }

    /// Run the monitoring loop. Call from a spawned task.
    pub async fn run(&self) {
        let mut tick = interval(self.sample_interval);
        loop {
            tick.tick().await;
            self.sample().await;
        }
    }

    /// Take one sample of all replica lag metrics.
    pub async fn sample(&self) {
        let replicas = self.primary.get_replicas().await;
        let master_offset = self.state.repl_offset();

        ferrite_core::metrics::record_replication_offset(master_offset as f64);
        ferrite_core::metrics::record_connected_replicas(
            replicas.iter().filter(|r| r.online).count() as f64,
        );

        for replica in &replicas {
            if !replica.online {
                continue;
            }
            let label = format!("{}:{}", replica.addr.ip(), replica.listening_port);
            let lag_bytes = replica.lag_bytes(master_offset);
            let lag_secs = replica.lag_seconds();

            ferrite_core::metrics::record_replication_lag_bytes(&label, lag_bytes as f64);
            ferrite_core::metrics::record_replication_lag_seconds(
                &label,
                lag_secs as f64,
            );

            if lag_bytes > 10 * 1024 * 1024 {
                warn!(
                    replica = %label,
                    lag_bytes,
                    "replica replication lag exceeds 10 MiB"
                );
            }
            debug!(
                replica = %label,
                lag_bytes,
                lag_secs,
                "replication lag sample"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replication::{ReplicationState, ReplicationStream};
    use crate::storage::Store;

    #[tokio::test]
    async fn test_health_monitor_sample_empty() {
        let store = Arc::new(Store::new(16));
        let state = Arc::new(ReplicationState::new());
        let stream = Arc::new(ReplicationStream::new(10000));
        let primary = Arc::new(ReplicationPrimary::new(store, state.clone(), stream));

        let monitor =
            ReplicationHealthMonitor::new(primary, state, Duration::from_millis(100));
        // Should not panic with no replicas
        monitor.sample().await;
    }

    #[tokio::test]
    async fn test_health_monitor_sample_with_replica() {
        let store = Arc::new(Store::new(16));
        let state = Arc::new(ReplicationState::new());
        let stream = Arc::new(ReplicationStream::new(10000));
        let primary = Arc::new(ReplicationPrimary::new(
            store,
            state.clone(),
            stream,
        ));

        let addr = "127.0.0.1:6380".parse().expect("valid addr");
        primary.register_replica(addr, 6380).await;
        primary
            .set_replica_state(&addr, crate::replication::ReplicaConnectionState::Online)
            .await;

        state.increment_offset(5000);
        primary.update_replica_offset(&addr, 2000).await;

        let monitor =
            ReplicationHealthMonitor::new(primary.clone(), state, Duration::from_millis(100));
        monitor.sample().await;

        // Verify lag via primary
        let max_lag = primary.max_replica_lag().await;
        assert_eq!(max_lag, 3000);
    }
}
