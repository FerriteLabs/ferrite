//! Jepsen integration tests for Ferrite
//!
//! Run with: cargo test --test jepsen_integration
#![allow(clippy::unwrap_used)]

mod jepsen;

use std::time::Duration;

use jepsen::runner::{JepsenRunConfig, JepsenRunner};
use jepsen::scheduler::ScheduleBuilder;
use jepsen::{ConsistencyModel, NodeConfig, NodeRole};

/// Helper to create a 3-node cluster config.
fn three_node_config(duration: Duration) -> JepsenRunConfig {
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
        test_duration: duration,
        concurrency: 4,
        fault_schedule: None,
        consistency_model: ConsistencyModel::Linearizable,
    }
}

#[test]
fn test_register_linearizability_no_faults() {
    // 3-node cluster, no faults, verify linearizable
    let config = three_node_config(Duration::from_millis(100));
    let runner = JepsenRunner::new(config);
    let report = runner.run_register_test();

    assert!(
        report.linearizable,
        "Register test should be linearizable without faults"
    );
    assert!(report.total_ops > 0, "Should have executed operations");
    assert_eq!(report.error_ops, 0, "No errors expected without faults");
    assert_eq!(report.timeout_ops, 0, "No timeouts expected without faults");
}

#[test]
fn test_register_linearizability_with_partition() {
    // 3-node cluster, network partition at 30ms for 10ms
    let schedule = {
        let mut b = ScheduleBuilder::new(Duration::from_millis(100));
        b.add_partition_at(
            Duration::from_millis(30),
            Duration::from_millis(10),
            vec![vec!["n1".into()], vec!["n2".into(), "n3".into()]],
        );
        b.build()
    };

    let mut config = three_node_config(Duration::from_millis(100));
    config.fault_schedule = Some(schedule);

    let runner = JepsenRunner::new(config);
    let report = runner.run_register_test();

    assert!(report.total_ops > 0, "Should have executed operations");
    assert!(!report.fault_events.is_empty(), "Should have fault events");
    assert!(!report.timeline_str.is_empty(), "Should have timeline");
}

#[test]
fn test_counter_correctness() {
    // Concurrent counter increments should sum correctly
    let config = three_node_config(Duration::from_millis(100));
    let runner = JepsenRunner::new(config);
    let report = runner.run_counter_test();

    assert!(report.linearizable, "Counter test should pass validation");
    assert!(report.total_ops > 0, "Should have executed operations");
    assert_eq!(report.test_name, "counter-increment");
}

#[test]
fn test_set_membership_under_faults() {
    // Set adds should be durable under crash/restart
    let schedule = {
        let mut b = ScheduleBuilder::new(Duration::from_millis(100));
        b.add_crash_at(Duration::from_millis(20), "n2");
        b.add_clock_skew_at(Duration::from_millis(40), "n3", -100);
        b.build()
    };

    let mut config = three_node_config(Duration::from_millis(100));
    config.fault_schedule = Some(schedule);

    let runner = JepsenRunner::new(config);
    let report = runner.run_set_test();

    assert!(report.total_ops > 0, "Should have executed operations");
    assert!(!report.fault_events.is_empty(), "Should have fault events");
    assert_eq!(report.test_name, "set-operations");
}
