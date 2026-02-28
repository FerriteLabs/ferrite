//! Fault injection for distributed systems testing
//!
//! Provides a rich set of fault types (network partitions, process crashes,
//! disk slowdowns, clock skew, etc.) that can be injected into and healed
//! from a running Ferrite cluster during Jepsen-style tests.

#![allow(dead_code)]

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// Fault description types
// ---------------------------------------------------------------------------

/// Describes *what* kind of fault to inject.
#[derive(Debug, Clone, PartialEq)]
pub enum FaultType {
    /// Split the cluster into isolated network partitions.
    /// Each inner `Vec<String>` is a group of node IDs that can communicate.
    NetworkPartition { partitions: Vec<Vec<String>> },

    /// Add artificial latency (with optional jitter) to all packets.
    NetworkDelay { latency_ms: u64, jitter_ms: u64 },

    /// Randomly drop a fraction of network packets.
    NetworkDrop { drop_rate: f64 },

    /// Kill the target process outright.
    ProcessCrash,

    /// Pause the process via SIGSTOP; resume later with SIGCONT.
    ProcessPause,

    /// Add artificial latency to disk I/O operations.
    DiskSlow { latency_ms: u64 },

    /// Shift the node's clock by `skew_ms` milliseconds (may be negative).
    ClockSkew { skew_ms: i64 },

    /// Consume memory until the target percentage of RAM is used.
    MemoryPressure { target_pct: f64 },
}

/// Describes *which* node(s) the fault targets.
#[derive(Debug, Clone, PartialEq)]
pub enum FaultTarget {
    /// A specific node identified by ID.
    Node(String),
    /// Every node in the cluster.
    AllNodes,
    /// A randomly chosen node.
    RandomNode,
    /// A strict majority of nodes.
    Majority,
    /// A strict minority of nodes.
    Minority,
}

/// A scheduled fault event — when, what, where, and for how long.
#[derive(Debug, Clone)]
pub struct FaultEvent {
    /// Offset from the start of the test at which the fault should fire.
    pub timestamp: Duration,
    /// Kind of fault to inject.
    pub fault_type: FaultType,
    /// Which node(s) are affected.
    pub target: FaultTarget,
    /// How long the fault persists. `None` means it stays active until
    /// explicitly healed.
    pub duration: Option<Duration>,
}

// ---------------------------------------------------------------------------
// Fault handle & errors
// ---------------------------------------------------------------------------

/// Opaque handle returned after a fault is successfully injected.
/// Use it to heal (remove) the fault later.
#[derive(Debug, Clone)]
pub struct FaultHandle {
    /// Unique identifier for this injection.
    pub id: u64,
    /// The type of fault that was injected.
    pub fault_type: FaultType,
    /// The target of the fault.
    pub target: FaultTarget,
    /// Wall-clock instant when the fault was injected.
    pub injected_at: Instant,
}

/// Errors that can occur during fault injection or healing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FaultError {
    /// The specified node was not found in the cluster.
    NodeNotFound { node_id: String },
    /// A fault of this type is already active on the target.
    AlreadyInjected { message: String },
    /// Attempted to heal a fault that is not currently active.
    NotActive { message: String },
    /// Insufficient permissions to perform the requested fault injection.
    PermissionDenied { message: String },
}

impl std::fmt::Display for FaultError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NodeNotFound { node_id } => write!(f, "node not found: {node_id}"),
            Self::AlreadyInjected { message } => write!(f, "already injected: {message}"),
            Self::NotActive { message } => write!(f, "fault not active: {message}"),
            Self::PermissionDenied { message } => write!(f, "permission denied: {message}"),
        }
    }
}

impl std::error::Error for FaultError {}

// ---------------------------------------------------------------------------
// Fault injector
// ---------------------------------------------------------------------------

/// Counter used to generate unique fault handle IDs.
static NEXT_FAULT_ID: AtomicU64 = AtomicU64::new(1);

/// Manages the lifecycle of injected faults.
///
/// Tracks all currently-active faults so they can be individually healed
/// or bulk-cleared at the end of a test.
pub struct FaultInjector {
    /// Currently active faults.
    active: Mutex<Vec<FaultHandle>>,
}

impl FaultInjector {
    /// Create a new, empty fault injector.
    pub fn new() -> Self {
        Self {
            active: Mutex::new(Vec::new()),
        }
    }

    /// Inject a fault described by `event` into the cluster.
    ///
    /// Returns a [`FaultHandle`] that can later be passed to [`heal`] to
    /// remove the fault.
    pub fn inject(&self, event: &FaultEvent) -> Result<FaultHandle, FaultError> {
        let mut active = self.active.lock().unwrap();

        // Prevent duplicate injection of the exact same fault type + target.
        for handle in active.iter() {
            if handle.fault_type == event.fault_type && handle.target == event.target {
                return Err(FaultError::AlreadyInjected {
                    message: format!(
                        "fault {:?} already active on target {:?}",
                        event.fault_type, event.target
                    ),
                });
            }
        }

        let handle = FaultHandle {
            id: NEXT_FAULT_ID.fetch_add(1, Ordering::Relaxed),
            fault_type: event.fault_type.clone(),
            target: event.target.clone(),
            injected_at: Instant::now(),
        };

        active.push(handle.clone());
        Ok(handle)
    }

    /// Remove (heal) a previously injected fault identified by `handle`.
    pub fn heal(&self, handle: &FaultHandle) -> Result<(), FaultError> {
        let mut active = self.active.lock().unwrap();
        let pos = active.iter().position(|h| h.id == handle.id);
        match pos {
            Some(idx) => {
                active.remove(idx);
                Ok(())
            }
            None => Err(FaultError::NotActive {
                message: format!("fault handle {} is not active", handle.id),
            }),
        }
    }

    /// Heal all currently active faults. Returns the number of faults healed.
    pub fn heal_all(&self) -> Result<usize, FaultError> {
        let mut active = self.active.lock().unwrap();
        let count = active.len();
        active.clear();
        Ok(count)
    }

    /// Return a snapshot of all currently active fault handles.
    pub fn active_faults(&self) -> Vec<FaultHandle> {
        self.active.lock().unwrap().clone()
    }
}

impl Default for FaultInjector {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn partition_event() -> FaultEvent {
        FaultEvent {
            timestamp: Duration::from_secs(5),
            fault_type: FaultType::NetworkPartition {
                partitions: vec![vec!["n1".into(), "n2".into()], vec!["n3".into()]],
            },
            target: FaultTarget::AllNodes,
            duration: Some(Duration::from_secs(10)),
        }
    }

    #[test]
    fn inject_and_heal() {
        let injector = FaultInjector::new();
        let event = partition_event();

        let handle = injector.inject(&event).expect("inject should succeed");
        assert_eq!(injector.active_faults().len(), 1);

        injector.heal(&handle).expect("heal should succeed");
        assert!(injector.active_faults().is_empty());
    }

    #[test]
    fn duplicate_inject_rejected() {
        let injector = FaultInjector::new();
        let event = partition_event();

        let _h = injector.inject(&event).unwrap();
        let result = injector.inject(&event);
        assert!(matches!(result, Err(FaultError::AlreadyInjected { .. })));
    }

    #[test]
    fn heal_all_clears_everything() {
        let injector = FaultInjector::new();

        injector
            .inject(&FaultEvent {
                timestamp: Duration::ZERO,
                fault_type: FaultType::ProcessCrash,
                target: FaultTarget::Node("n1".into()),
                duration: None,
            })
            .unwrap();

        injector
            .inject(&FaultEvent {
                timestamp: Duration::ZERO,
                fault_type: FaultType::DiskSlow { latency_ms: 200 },
                target: FaultTarget::Node("n2".into()),
                duration: None,
            })
            .unwrap();

        assert_eq!(injector.active_faults().len(), 2);
        let healed = injector.heal_all().unwrap();
        assert_eq!(healed, 2);
        assert!(injector.active_faults().is_empty());
    }

    #[test]
    fn heal_unknown_handle_errors() {
        let injector = FaultInjector::new();
        let bogus = FaultHandle {
            id: 9999,
            fault_type: FaultType::ProcessCrash,
            target: FaultTarget::AllNodes,
            injected_at: Instant::now(),
        };
        let result = injector.heal(&bogus);
        assert!(matches!(result, Err(FaultError::NotActive { .. })));
    }

    #[test]
    fn fault_error_display() {
        let err = FaultError::NodeNotFound {
            node_id: "n42".into(),
        };
        assert!(err.to_string().contains("n42"));
    }
}
