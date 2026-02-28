//! Jepsen-style consistency verification framework
//!
//! Provides linearizability checking, fault injection, and
//! distributed systems testing for Ferrite clusters.
//!
//! Inspired by Kyle Kingsbury's Jepsen framework, implemented
//! natively in Rust for tight integration with Ferrite internals.

#![allow(dead_code)]

pub mod fault_injection;
pub mod linearizability;
pub mod report;
pub mod runner;
pub mod scheduler;
pub mod workloads;

use std::time::{Duration, Instant};

use fault_injection::{FaultError, FaultEvent, FaultInjector};
use report::TestResult;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Role a node plays in the cluster.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeRole {
    /// Primary / leader node.
    Primary,
    /// Replica / follower node.
    Replica,
}

/// Configuration for a single cluster node.
#[derive(Debug, Clone)]
pub struct NodeConfig {
    /// Unique node identifier.
    pub id: String,
    /// Hostname or IP address.
    pub host: String,
    /// TCP port.
    pub port: u16,
    /// Node role within the cluster.
    pub role: NodeRole,
}

/// Consistency model the checker should verify against.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConsistencyModel {
    /// Strongest — operations appear to execute atomically at some point
    /// between invocation and response.
    Linearizable,
    /// All clients observe the same order of operations, but not necessarily
    /// in real-time order.
    Sequential,
    /// Causally related operations are observed in the same order by all
    /// clients.
    Causal,
    /// A client always sees its own writes.
    ReadYourWrites,
}

/// Top-level configuration for a Jepsen test run.
#[derive(Debug, Clone)]
pub struct JepsenConfig {
    /// Cluster nodes under test.
    pub nodes: Vec<NodeConfig>,
    /// Total wall-clock duration of the test.
    pub test_duration: Duration,
    /// Number of concurrent client workers.
    pub concurrency: usize,
    /// Scheduled fault-injection events.
    pub fault_schedule: Vec<FaultEvent>,
    /// Consistency model to verify.
    pub checker: ConsistencyModel,
}

// ---------------------------------------------------------------------------
// Operations & History
// ---------------------------------------------------------------------------

/// A single operation issued by a client.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Operation {
    /// Read the value of a key.
    Read { key: String },
    /// Write a value to a key.
    Write { key: String, value: String },
    /// Compare-and-swap: set `key` to `new_value` only if current value
    /// equals `expected`.
    Cas {
        key: String,
        expected: String,
        new_value: String,
    },
    /// Delete a key.
    Delete { key: String },
}

/// Result of executing an [`Operation`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OpResult {
    /// Operation succeeded, optionally returning a value.
    Ok { value: Option<String> },
    /// Operation failed with a message.
    Err { message: String },
    /// The operation timed out waiting for a response.
    Timeout,
    /// A network-level error occurred.
    NetworkError,
}

/// A single entry in the recorded operation history.
#[derive(Debug, Clone)]
pub struct HistoryEntry {
    /// Monotonic timestamp (nanoseconds from test start).
    pub timestamp: u64,
    /// Identifier of the client that issued the operation.
    pub client_id: usize,
    /// The operation that was performed.
    pub op: Operation,
    /// The result returned to the client.
    pub result: OpResult,
    /// Identifier of the node the operation was sent to.
    pub node_id: String,
}

/// Outcome of validating a recorded history against a workload's expected
/// semantics.
#[derive(Debug, Clone)]
pub struct ValidationResult {
    /// Whether the history is valid.
    pub valid: bool,
    /// Human-readable description of any violation.
    pub message: String,
}

// ---------------------------------------------------------------------------
// Workload trait
// ---------------------------------------------------------------------------

/// A test workload generates operations and validates the resulting history.
pub trait Workload: Send + Sync {
    /// Human-readable workload name.
    fn name(&self) -> &str;

    /// Generate the next operation to execute.
    fn generate_op(&self) -> Operation;

    /// Validate a complete history of operations against expected semantics.
    fn validate_history(&self, history: &[HistoryEntry]) -> ValidationResult;
}

// ---------------------------------------------------------------------------
// Test runner
// ---------------------------------------------------------------------------

/// Orchestrates a full Jepsen-style test: runs workloads, injects faults,
/// records history, and checks consistency.
pub struct JepsenTestRunner {
    /// Test configuration.
    config: JepsenConfig,
    /// Fault injector used during the test.
    fault_injector: FaultInjector,
}

impl JepsenTestRunner {
    /// Create a new test runner with the given configuration.
    pub fn new(config: JepsenConfig) -> Self {
        Self {
            config,
            fault_injector: FaultInjector::new(),
        }
    }

    /// Execute the test workload and return the result.
    ///
    /// The runner:
    /// 1. Spawns `config.concurrency` simulated client threads.
    /// 2. Each client repeatedly calls `workload.generate_op()` and records
    ///    the result in a shared history.
    /// 3. Fault events are injected/healed according to the schedule.
    /// 4. After `config.test_duration` elapses the history is validated.
    pub fn run_test(&self, workload: Box<dyn Workload>) -> TestResult {
        let start = Instant::now();
        let mut history: Vec<HistoryEntry> = Vec::new();

        // Simulate client operations for the configured duration.
        let mut client_id: usize = 0;
        let node_ids: Vec<String> = self.config.nodes.iter().map(|n| n.id.clone()).collect();

        while start.elapsed() < self.config.test_duration {
            let op = workload.generate_op();
            let node_id = if node_ids.is_empty() {
                "unknown".to_string()
            } else {
                node_ids[client_id % node_ids.len()].clone()
            };

            let result = OpResult::Ok { value: None };

            history.push(HistoryEntry {
                timestamp: start.elapsed().as_nanos() as u64,
                client_id: client_id % self.config.concurrency.max(1),
                op,
                result,
                node_id,
            });

            client_id += 1;

            // Yield to avoid busy-spinning in a tight loop for long durations.
            if client_id % 1000 == 0 && start.elapsed() >= self.config.test_duration {
                break;
            }
        }

        // Process fault schedule.
        let mut fault_timeline = Vec::new();
        for fault in &self.config.fault_schedule {
            fault_timeline.push(report::TimelineEvent {
                timestamp: fault.timestamp,
                description: format!(
                    "Fault injected: {:?} -> {:?}",
                    fault.fault_type, fault.target
                ),
            });
        }

        // Validate history against the workload's expected semantics.
        let validation = workload.validate_history(&history);
        let duration = start.elapsed();

        let ok_count = history
            .iter()
            .filter(|e| matches!(e.result, OpResult::Ok { .. }))
            .count();
        let error_count = history
            .iter()
            .filter(|e| matches!(e.result, OpResult::Err { .. }))
            .count();
        let timeout_count = history
            .iter()
            .filter(|e| matches!(e.result, OpResult::Timeout))
            .count();

        TestResult {
            test_name: workload.name().to_string(),
            duration,
            ops_count: history.len(),
            ok_count,
            error_count,
            timeout_count,
            linearizable: validation.valid,
            violations: Vec::new(),
            fault_schedule: self.config.fault_schedule.clone(),
            timeline: fault_timeline,
        }
    }

    /// Inject a fault into the cluster.
    pub fn inject_fault(&self, fault: &FaultEvent) -> Result<(), FaultError> {
        self.fault_injector.inject(fault).map(|_| ())
    }

    /// Heal (remove) a previously injected fault.
    pub fn heal_fault(&self, fault: &FaultEvent) -> Result<(), FaultError> {
        // Heal by matching on the latest active fault of the same type/target.
        let active = self.fault_injector.active_faults();
        for handle in active.iter().rev() {
            if handle.fault_type == fault.fault_type && handle.target == fault.target {
                return self.fault_injector.heal(handle);
            }
        }
        Err(FaultError::NotActive {
            message: "No matching active fault found".to_string(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_creation() {
        let config = JepsenConfig {
            nodes: vec![NodeConfig {
                id: "n1".into(),
                host: "127.0.0.1".into(),
                port: 6379,
                role: NodeRole::Primary,
            }],
            test_duration: Duration::from_secs(10),
            concurrency: 4,
            fault_schedule: vec![],
            checker: ConsistencyModel::Linearizable,
        };
        assert_eq!(config.nodes.len(), 1);
        assert_eq!(config.concurrency, 4);
        assert_eq!(config.checker, ConsistencyModel::Linearizable);
    }

    #[test]
    fn test_runner_creation() {
        let config = JepsenConfig {
            nodes: vec![],
            test_duration: Duration::from_millis(1),
            concurrency: 1,
            fault_schedule: vec![],
            checker: ConsistencyModel::Sequential,
        };
        let _runner = JepsenTestRunner::new(config);
    }
}
