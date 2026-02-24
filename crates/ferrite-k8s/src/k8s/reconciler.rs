//! Reconciliation Framework for Ferrite Operator
//!
//! Provides the core reconciliation logic and abstractions.

use super::controller::*;
use super::crd::*;
use super::operator::OperatorError;
use super::resources::*;
use super::*;
use std::time::Duration;

/// Reconciler trait for resources
pub trait Reconciler {
    /// Resource type
    type Resource;

    /// Reconcile the resource
    fn reconcile(
        &self,
        resource: &Self::Resource,
    ) -> impl std::future::Future<Output = Result<ReconcileResult, OperatorError>> + Send;

    /// Finalize the resource (cleanup before deletion)
    fn finalize(
        &self,
        resource: &Self::Resource,
    ) -> impl std::future::Future<Output = Result<(), OperatorError>> + Send;
}

/// Actions to be taken during reconciliation
#[derive(Debug)]
pub enum ReconcileAction {
    /// Create a StatefulSet
    CreateStatefulSet(StatefulSetSpec),
    /// Update a StatefulSet
    UpdateStatefulSet(StatefulSetSpec),
    /// Scale a StatefulSet
    ScaleStatefulSet { replicas: i32 },
    /// Create a Service
    CreateService(ServiceSpec),
    /// Create a ConfigMap
    CreateConfigMap(ConfigMapSpec),
    /// Create a PVC
    CreatePVC(PvcSpec),
    /// Create a PodDisruptionBudget
    CreatePDB(PodDisruptionBudgetSpec),
    /// Create a ServiceMonitor for Prometheus Operator
    CreateServiceMonitor(ServiceMonitorSpec),
    /// Create a CronJob for scheduled backups
    CreateCronJob(CronJobSpec),
    /// Delete a Pod
    DeletePod(String),
    /// Update status
    UpdateStatus(FerriteClusterStatus),
    /// Requeue after duration
    Requeue(Duration),
}

/// Result of reconciliation
#[derive(Debug, Clone)]
pub struct ReconcileResult {
    /// Whether to requeue
    pub requeue: bool,
    /// Requeue delay
    pub requeue_after: Option<Duration>,
    /// Error occurred
    pub error: Option<String>,
}

impl ReconcileResult {
    /// Reconciliation complete, no requeue needed
    pub fn done() -> Self {
        Self {
            requeue: false,
            requeue_after: None,
            error: None,
        }
    }

    /// Requeue after duration
    pub fn requeue(after: Duration) -> Self {
        Self {
            requeue: true,
            requeue_after: Some(after),
            error: None,
        }
    }

    /// Requeue immediately
    pub fn requeue_immediate() -> Self {
        Self {
            requeue: true,
            requeue_after: None,
            error: None,
        }
    }

    /// Requeue with error
    pub fn requeue_with_error(error: impl Into<String>, after: Duration) -> Self {
        Self {
            requeue: true,
            requeue_after: Some(after),
            error: Some(error.into()),
        }
    }

    /// Check if this result indicates an error
    pub fn is_error(&self) -> bool {
        self.error.is_some()
    }
}

/// State machine for cluster reconciliation
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClusterReconcileState {
    /// Initial state
    Initial,
    /// Creating ConfigMap
    CreatingConfigMap,
    /// Creating Services
    CreatingServices,
    /// Creating StatefulSet
    CreatingStatefulSet,
    /// Waiting for pods
    WaitingForPods,
    /// Configuring replication
    ConfiguringReplication,
    /// Running health checks
    HealthCheck,
    /// Ready
    Ready,
    /// Scaling
    Scaling,
    /// Upgrading
    Upgrading,
    /// FailingOver
    FailingOver,
    /// Error
    Error,
}

impl ClusterReconcileState {
    /// Get next state based on current state and conditions
    pub fn next(&self, cluster: &FerriteCluster) -> Self {
        match self {
            ClusterReconcileState::Initial => ClusterReconcileState::CreatingConfigMap,
            ClusterReconcileState::CreatingConfigMap => ClusterReconcileState::CreatingServices,
            ClusterReconcileState::CreatingServices => ClusterReconcileState::CreatingStatefulSet,
            ClusterReconcileState::CreatingStatefulSet => ClusterReconcileState::WaitingForPods,
            ClusterReconcileState::WaitingForPods => {
                if cluster.ready_replicas() >= cluster.spec.replicas {
                    ClusterReconcileState::ConfiguringReplication
                } else {
                    ClusterReconcileState::WaitingForPods
                }
            }
            ClusterReconcileState::ConfiguringReplication => ClusterReconcileState::HealthCheck,
            ClusterReconcileState::HealthCheck => ClusterReconcileState::Ready,
            ClusterReconcileState::Ready => ClusterReconcileState::Ready,
            ClusterReconcileState::Scaling => {
                if cluster.ready_replicas() >= cluster.spec.replicas {
                    ClusterReconcileState::Ready
                } else {
                    ClusterReconcileState::Scaling
                }
            }
            ClusterReconcileState::Upgrading => ClusterReconcileState::WaitingForPods,
            ClusterReconcileState::FailingOver => ClusterReconcileState::WaitingForPods,
            ClusterReconcileState::Error => ClusterReconcileState::Initial,
        }
    }

    /// Convert to cluster phase
    pub fn to_phase(&self) -> ClusterPhase {
        match self {
            ClusterReconcileState::Initial
            | ClusterReconcileState::CreatingConfigMap
            | ClusterReconcileState::CreatingServices
            | ClusterReconcileState::CreatingStatefulSet => ClusterPhase::Creating,
            ClusterReconcileState::WaitingForPods
            | ClusterReconcileState::ConfiguringReplication
            | ClusterReconcileState::HealthCheck => ClusterPhase::Creating,
            ClusterReconcileState::Ready => ClusterPhase::Running,
            ClusterReconcileState::Scaling => ClusterPhase::Scaling,
            ClusterReconcileState::Upgrading => ClusterPhase::Upgrading,
            ClusterReconcileState::FailingOver => ClusterPhase::FailingOver,
            ClusterReconcileState::Error => ClusterPhase::Failed,
        }
    }
}

/// Retry policy for reconciliation
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    /// Maximum retries
    pub max_retries: u32,
    /// Base delay
    pub base_delay: Duration,
    /// Maximum delay
    pub max_delay: Duration,
    /// Exponential backoff factor
    pub backoff_factor: f64,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_retries: MAX_RECONCILE_RETRIES,
            base_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(300),
            backoff_factor: 2.0,
        }
    }
}

impl RetryPolicy {
    /// Calculate delay for retry attempt
    pub fn delay_for_attempt(&self, attempt: u32) -> Duration {
        let delay_secs = self.base_delay.as_secs_f64() * self.backoff_factor.powi(attempt as i32);
        let delay = Duration::from_secs_f64(delay_secs);
        delay.min(self.max_delay)
    }

    /// Check if should retry
    pub fn should_retry(&self, attempt: u32) -> bool {
        attempt < self.max_retries
    }
}

/// Reconciliation context
#[derive(Default)]
pub struct ReconcileContext {
    /// Current attempt number
    pub attempt: u32,
    /// Retry policy
    pub retry_policy: RetryPolicy,
    /// Last error
    pub last_error: Option<String>,
}

impl ReconcileContext {
    /// Record an error and get next action
    pub fn on_error(&mut self, error: impl Into<String>) -> ReconcileResult {
        self.attempt += 1;
        self.last_error = Some(error.into());

        if self.retry_policy.should_retry(self.attempt) {
            let delay = self.retry_policy.delay_for_attempt(self.attempt);
            ReconcileResult::requeue_with_error(self.last_error.clone().unwrap_or_default(), delay)
        } else {
            ReconcileResult {
                requeue: false,
                requeue_after: None,
                error: self.last_error.clone(),
            }
        }
    }

    /// Reset context on success
    pub fn on_success(&mut self) {
        self.attempt = 0;
        self.last_error = None;
    }
}

/// Event type for reconciliation
#[derive(Debug, Clone)]
pub enum ReconcileEvent {
    /// Resource created
    Created,
    /// Resource updated
    Updated,
    /// Resource deleted
    Deleted,
    /// Timer triggered
    Timer,
    /// Child resource changed
    ChildChanged { kind: String, name: String },
}

/// Queue for reconciliation requests
pub struct ReconcileQueue<T> {
    /// Items to reconcile
    items: std::collections::VecDeque<(T, ReconcileEvent)>,
    /// Maximum queue size
    max_size: usize,
}

impl<T> ReconcileQueue<T> {
    /// Create a new queue
    pub fn new(max_size: usize) -> Self {
        Self {
            items: std::collections::VecDeque::new(),
            max_size,
        }
    }

    /// Add item to queue
    pub fn push(&mut self, item: T, event: ReconcileEvent) -> bool {
        if self.items.len() >= self.max_size {
            return false;
        }
        self.items.push_back((item, event));
        true
    }

    /// Get next item
    pub fn pop(&mut self) -> Option<(T, ReconcileEvent)> {
        self.items.pop_front()
    }

    /// Check if queue is empty
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// Get queue length
    pub fn len(&self) -> usize {
        self.items.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_reconcile_result() {
        let done = ReconcileResult::done();
        assert!(!done.requeue);
        assert!(done.requeue_after.is_none());

        let requeue = ReconcileResult::requeue(Duration::from_secs(30));
        assert!(requeue.requeue);
        assert_eq!(requeue.requeue_after, Some(Duration::from_secs(30)));
    }

    #[test]
    fn test_retry_policy() {
        let policy = RetryPolicy::default();

        assert!(policy.should_retry(0));
        assert!(policy.should_retry(4));
        assert!(!policy.should_retry(5));

        let delay0 = policy.delay_for_attempt(0);
        let delay1 = policy.delay_for_attempt(1);
        let delay2 = policy.delay_for_attempt(2);

        assert!(delay1 > delay0);
        assert!(delay2 > delay1);
    }

    #[test]
    fn test_reconcile_context() {
        let mut ctx = ReconcileContext::default();

        let result = ctx.on_error("test error");
        assert!(result.requeue);
        assert_eq!(ctx.attempt, 1);

        ctx.on_success();
        assert_eq!(ctx.attempt, 0);
    }

    #[test]
    fn test_cluster_reconcile_state() {
        let cluster = FerriteCluster::new("test", FerriteClusterSpec::default());

        let state = ClusterReconcileState::Initial;
        assert_eq!(
            state.next(&cluster),
            ClusterReconcileState::CreatingConfigMap
        );

        let ready_state = ClusterReconcileState::Ready;
        assert_eq!(ready_state.to_phase(), ClusterPhase::Running);
    }

    #[test]
    fn test_reconcile_queue() {
        let mut queue = ReconcileQueue::<String>::new(10);

        assert!(queue.push("item1".to_string(), ReconcileEvent::Created));
        assert!(queue.push("item2".to_string(), ReconcileEvent::Updated));

        assert_eq!(queue.len(), 2);

        let (item, event) = queue.pop().unwrap();
        assert_eq!(item, "item1");
        assert!(matches!(event, ReconcileEvent::Created));
    }
}
