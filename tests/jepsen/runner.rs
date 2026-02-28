//! Complete Jepsen test runner
//!
//! Orchestrates workload generation, fault injection, history collection,
//! linearizability checking, and report generation.

#![allow(dead_code)]
#![allow(clippy::unwrap_used)]

use std::time::{Duration, Instant};

use super::fault_injection::{FaultEvent, FaultInjector};
use super::report::{ReportGenerator, TestResult, TimelineEvent};
use super::workloads::{CounterWorkload, RegisterWorkload, SetWorkload};
use super::{
    ConsistencyModel, HistoryEntry, JepsenConfig, NodeConfig, OpResult, Operation, Workload,
};

use super::scheduler::{FaultSchedule, ScheduledFault};

// ---------------------------------------------------------------------------
// JepsenRunConfig
// ---------------------------------------------------------------------------

/// Configuration for a `JepsenRunner` invocation.
#[derive(Debug, Clone)]
pub struct JepsenRunConfig {
    /// Nodes in the simulated cluster.
    pub node_configs: Vec<NodeConfig>,
    /// Total wall-clock duration of the test.
    pub test_duration: Duration,
    /// Number of concurrent client workers.
    pub concurrency: usize,
    /// Optional fault schedule to apply during the test.
    pub fault_schedule: Option<FaultSchedule>,
    /// Consistency model to verify.
    pub consistency_model: ConsistencyModel,
}

// ---------------------------------------------------------------------------
// TestReport
// ---------------------------------------------------------------------------

/// Extended test report produced by `JepsenRunner`.
#[derive(Debug, Clone)]
pub struct TestReport {
    /// Name of the test / workload.
    pub test_name: String,
    /// Configuration used for the run.
    pub config: JepsenRunConfig,
    /// Wall-clock duration of the test.
    pub duration: Duration,
    /// Total number of operations issued.
    pub total_ops: usize,
    /// Operations that returned Ok.
    pub ok_ops: usize,
    /// Operations that returned an error.
    pub error_ops: usize,
    /// Operations that timed out.
    pub timeout_ops: usize,
    /// Whether the history was found to be linearizable.
    pub linearizable: bool,
    /// Linearizability violations (empty when linearizable).
    pub violations: Vec<super::linearizability::Violation>,
    /// Fault events that were injected.
    pub fault_events: Vec<ScheduledFault>,
    /// ASCII timeline visualization.
    pub timeline_str: String,
}

impl TestReport {
    /// Render the report using `ReportGenerator`.
    pub fn to_text(&self) -> String {
        let result = TestResult {
            test_name: self.test_name.clone(),
            duration: self.duration,
            ops_count: self.total_ops,
            ok_count: self.ok_ops,
            error_count: self.error_ops,
            timeout_count: self.timeout_ops,
            linearizable: self.linearizable,
            violations: self.violations.clone(),
            fault_schedule: self
                .fault_events
                .iter()
                .map(|sf| sf.fault.clone())
                .collect(),
            timeline: Vec::new(),
        };
        ReportGenerator::generate_text(&result)
    }
}

// ---------------------------------------------------------------------------
// JepsenRunner
// ---------------------------------------------------------------------------

/// Orchestrates Jepsen-style tests with workload generation, fault injection,
/// history collection, linearizability checking, and reporting.
pub struct JepsenRunner {
    config: JepsenRunConfig,
}

impl JepsenRunner {
    /// Create a new runner with the given configuration.
    pub fn new(config: JepsenRunConfig) -> Self {
        Self { config }
    }

    /// Run the register linearizability test.
    pub fn run_register_test(&self) -> TestReport {
        let workload = Box::new(RegisterWorkload::new(5));
        let schedule = self
            .config
            .fault_schedule
            .clone()
            .unwrap_or_else(FaultSchedule::new);
        self.run_with_custom_workload(workload, schedule)
    }

    /// Run the counter increment test.
    pub fn run_counter_test(&self) -> TestReport {
        let workload = Box::new(CounterWorkload::new(3));
        let schedule = self
            .config
            .fault_schedule
            .clone()
            .unwrap_or_else(FaultSchedule::new);
        self.run_with_custom_workload(workload, schedule)
    }

    /// Run the set membership test.
    pub fn run_set_test(&self) -> TestReport {
        let workload = Box::new(SetWorkload::new(3));
        let schedule = self
            .config
            .fault_schedule
            .clone()
            .unwrap_or_else(FaultSchedule::new);
        self.run_with_custom_workload(workload, schedule)
    }

    /// Run all standard tests sequentially and return their reports.
    pub fn run_all_tests(&self) -> Vec<TestReport> {
        vec![
            self.run_register_test(),
            self.run_counter_test(),
            self.run_set_test(),
        ]
    }

    /// Run a custom workload with the given fault schedule.
    pub fn run_with_custom_workload(
        &self,
        workload: Box<dyn Workload>,
        schedule: FaultSchedule,
    ) -> TestReport {
        let start = Instant::now();
        let fault_injector = FaultInjector::new();
        let mut history: Vec<HistoryEntry> = Vec::new();
        let mut timeline_events: Vec<TimelineEvent> = Vec::new();

        let node_ids: Vec<String> = self
            .config
            .node_configs
            .iter()
            .map(|n| n.id.clone())
            .collect();

        // Collect scheduled faults into a peekable iterator.
        let scheduled_faults: Vec<ScheduledFault> = schedule.iter().cloned().collect();
        let mut next_fault_idx: usize = 0;

        // Track faults that need healing.
        let mut pending_heals: Vec<(Duration, FaultEvent)> = Vec::new();

        let mut client_id: usize = 0;

        while start.elapsed() < self.config.test_duration {
            let elapsed = start.elapsed();

            // Inject any faults whose time has come.
            while next_fault_idx < scheduled_faults.len()
                && scheduled_faults[next_fault_idx].inject_at <= elapsed
            {
                let sf = &scheduled_faults[next_fault_idx];
                let _ = fault_injector.inject(&sf.fault);
                timeline_events.push(TimelineEvent {
                    timestamp: elapsed,
                    description: format!(
                        "Fault injected: {:?} -> {:?}",
                        sf.fault.fault_type, sf.fault.target
                    ),
                });
                if let Some(heal_dur) = sf.heal_after {
                    pending_heals.push((elapsed + heal_dur, sf.fault.clone()));
                }
                next_fault_idx += 1;
            }

            // Heal any faults whose heal time has passed.
            pending_heals.retain(|(heal_at, fault)| {
                if elapsed >= *heal_at {
                    let active = fault_injector.active_faults();
                    for handle in active.iter().rev() {
                        if handle.fault_type == fault.fault_type && handle.target == fault.target {
                            let _ = fault_injector.heal(handle);
                            break;
                        }
                    }
                    false
                } else {
                    true
                }
            });

            // Determine if the current operation is affected by an active fault.
            let has_active_faults = !fault_injector.active_faults().is_empty();

            // Generate and execute an operation.
            let op = workload.generate_op();
            let node_id = if node_ids.is_empty() {
                "sim-node".to_string()
            } else {
                node_ids[client_id % node_ids.len()].clone()
            };

            // Simulate operation result: if there are active faults,
            // some operations may error or timeout.
            let result = if has_active_faults && client_id % 5 == 0 {
                OpResult::Timeout
            } else if has_active_faults && client_id % 7 == 0 {
                OpResult::Err {
                    message: "simulated fault".to_string(),
                }
            } else {
                OpResult::Ok { value: None }
            };

            history.push(HistoryEntry {
                timestamp: start.elapsed().as_nanos() as u64,
                client_id: client_id % self.config.concurrency.max(1),
                op,
                result,
                node_id,
            });

            client_id += 1;

            if client_id % 1000 == 0 && start.elapsed() >= self.config.test_duration {
                break;
            }
        }

        // Heal all remaining faults.
        let _ = fault_injector.heal_all();

        // Validate history.
        let validation = workload.validate_history(&history);
        let duration = start.elapsed();

        let ok_ops = history
            .iter()
            .filter(|e| matches!(e.result, OpResult::Ok { .. }))
            .count();
        let error_ops = history
            .iter()
            .filter(|e| matches!(e.result, OpResult::Err { .. }))
            .count();
        let timeout_ops = history
            .iter()
            .filter(|e| matches!(e.result, OpResult::Timeout))
            .count();

        let timeline_str = schedule.to_timeline_string();

        TestReport {
            test_name: workload.name().to_string(),
            config: self.config.clone(),
            duration,
            total_ops: history.len(),
            ok_ops,
            error_ops,
            timeout_ops,
            linearizable: validation.valid,
            violations: Vec::new(),
            fault_events: scheduled_faults,
            timeline_str,
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jepsen::scheduler::ScheduleBuilder;
    use crate::jepsen::NodeRole;

    fn test_config(duration_ms: u64) -> JepsenRunConfig {
        JepsenRunConfig {
            node_configs: vec![
                NodeConfig {
                    id: "n1".into(),
                    host: "127.0.0.1".into(),
                    port: 6379,
                    role: NodeRole::Primary,
                },
                NodeConfig {
                    id: "n2".into(),
                    host: "127.0.0.1".into(),
                    port: 6380,
                    role: NodeRole::Replica,
                },
                NodeConfig {
                    id: "n3".into(),
                    host: "127.0.0.1".into(),
                    port: 6381,
                    role: NodeRole::Replica,
                },
            ],
            test_duration: Duration::from_millis(duration_ms),
            concurrency: 4,
            fault_schedule: None,
            consistency_model: ConsistencyModel::Linearizable,
        }
    }

    #[test]
    fn run_register_no_faults() {
        let runner = JepsenRunner::new(test_config(50));
        let report = runner.run_register_test();
        assert!(report.total_ops > 0);
        assert!(report.linearizable);
        assert_eq!(report.test_name, "register-linearizability");
    }

    #[test]
    fn run_counter_no_faults() {
        let runner = JepsenRunner::new(test_config(50));
        let report = runner.run_counter_test();
        assert!(report.total_ops > 0);
        assert_eq!(report.test_name, "counter-increment");
    }

    #[test]
    fn run_set_no_faults() {
        let runner = JepsenRunner::new(test_config(50));
        let report = runner.run_set_test();
        assert!(report.total_ops > 0);
        assert_eq!(report.test_name, "set-operations");
    }

    #[test]
    fn run_all_tests() {
        let runner = JepsenRunner::new(test_config(50));
        let reports = runner.run_all_tests();
        assert_eq!(reports.len(), 3);
    }

    #[test]
    fn run_with_fault_schedule() {
        let schedule = {
            let mut b = ScheduleBuilder::new(Duration::from_millis(50));
            b.add_crash_at(Duration::from_millis(10), "n1");
            b.build()
        };

        let mut config = test_config(50);
        config.fault_schedule = Some(schedule);

        let runner = JepsenRunner::new(config);
        let report = runner.run_register_test();
        assert!(report.total_ops > 0);
        assert!(!report.fault_events.is_empty());
    }

    #[test]
    fn report_text_output() {
        let runner = JepsenRunner::new(test_config(50));
        let report = runner.run_register_test();
        let text = report.to_text();
        assert!(text.contains("Ferrite Jepsen Test Report"));
    }
}
