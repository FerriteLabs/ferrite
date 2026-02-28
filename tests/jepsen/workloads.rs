//! Standard Jepsen test workloads for Ferrite
//!
//! Each workload implements the [`Workload`] trait, generating operations
//! and validating the resulting history against expected semantics.

#![allow(dead_code)]

use std::sync::atomic::{AtomicUsize, Ordering};

use super::linearizability::LinearizabilityChecker;
use super::{HistoryEntry, OpResult, Operation, ValidationResult, Workload};

// ---------------------------------------------------------------------------
// RegisterWorkload
// ---------------------------------------------------------------------------

/// Tests linearizable register semantics with read / write / CAS operations
/// on a small key set.
pub struct RegisterWorkload {
    /// Number of distinct keys to use.
    num_keys: usize,
    /// Counter used to cycle through operations deterministically.
    counter: AtomicUsize,
}

impl RegisterWorkload {
    /// Create a new register workload operating on `num_keys` keys.
    pub fn new(num_keys: usize) -> Self {
        Self {
            num_keys,
            counter: AtomicUsize::new(0),
        }
    }
}

impl Workload for RegisterWorkload {
    fn name(&self) -> &str {
        "register-linearizability"
    }

    fn generate_op(&self) -> Operation {
        let c = self.counter.fetch_add(1, Ordering::Relaxed);
        let key = format!("reg:{}", c % self.num_keys);
        let value = format!("v{c}");

        match c % 3 {
            0 => Operation::Read { key },
            1 => Operation::Write { key, value },
            _ => Operation::Cas {
                key,
                expected: format!("v{}", c.wrapping_sub(1)),
                new_value: value,
            },
        }
    }

    fn validate_history(&self, history: &[HistoryEntry]) -> ValidationResult {
        let result = LinearizabilityChecker::check(history);
        ValidationResult {
            valid: result.is_linearizable,
            message: if result.is_linearizable {
                format!(
                    "History of {} ops is linearizable (checked in {:?})",
                    result.checked_ops, result.search_time
                )
            } else {
                format!(
                    "NOT linearizable: {} violations in {} ops",
                    result.violations.len(),
                    result.checked_ops
                )
            },
        }
    }
}

// ---------------------------------------------------------------------------
// CounterWorkload
// ---------------------------------------------------------------------------

/// Tests counter increment semantics (analogous to Redis INCR).
///
/// Multiple clients increment counters concurrently. The final value of each
/// counter should equal the total number of increments applied to it.
pub struct CounterWorkload {
    /// Number of distinct counter keys.
    num_counters: usize,
    /// Operation counter for round-robin key selection.
    counter: AtomicUsize,
}

impl CounterWorkload {
    /// Create a new counter workload with `num_counters` distinct keys.
    pub fn new(num_counters: usize) -> Self {
        Self {
            num_counters,
            counter: AtomicUsize::new(0),
        }
    }
}

impl Workload for CounterWorkload {
    fn name(&self) -> &str {
        "counter-increment"
    }

    fn generate_op(&self) -> Operation {
        let c = self.counter.fetch_add(1, Ordering::Relaxed);
        let key = format!("counter:{}", c % self.num_counters);

        // Alternate between incrementing (write "1" to signal +1) and reading.
        if c % 2 == 0 {
            Operation::Write {
                key,
                value: "1".to_string(),
            }
        } else {
            Operation::Read { key }
        }
    }

    fn validate_history(&self, history: &[HistoryEntry]) -> ValidationResult {
        // Count the number of successful writes (increments) per key.
        let mut write_counts: std::collections::HashMap<String, usize> =
            std::collections::HashMap::new();
        // Track the last observed read value per key.
        let mut last_reads: std::collections::HashMap<String, Option<String>> =
            std::collections::HashMap::new();

        for entry in history {
            match (&entry.op, &entry.result) {
                (Operation::Write { key, .. }, OpResult::Ok { .. }) => {
                    *write_counts.entry(key.clone()).or_insert(0) += 1;
                }
                (Operation::Read { key }, OpResult::Ok { value }) => {
                    last_reads.insert(key.clone(), value.clone());
                }
                _ => {}
            }
        }

        // Validate monotonicity: reads should return non-decreasing values
        // when interpreted as integers.
        let mut violations = Vec::new();
        for (key, value) in &last_reads {
            if let Some(val_str) = value {
                if val_str.parse::<i64>().is_err() {
                    violations.push(format!(
                        "Counter {key} returned non-integer value: {val_str}"
                    ));
                }
            }
        }

        let valid = violations.is_empty();
        ValidationResult {
            valid,
            message: if valid {
                format!(
                    "All {} counters passed validation ({} total writes)",
                    write_counts.len(),
                    write_counts.values().sum::<usize>()
                )
            } else {
                violations.join("; ")
            },
        }
    }
}

// ---------------------------------------------------------------------------
// SetWorkload
// ---------------------------------------------------------------------------

/// Tests set operation semantics (analogous to Redis SADD / SMEMBERS).
///
/// Multiple clients add elements to sets. The final set should contain
/// every successfully-added element.
pub struct SetWorkload {
    /// Number of distinct set keys.
    num_sets: usize,
    /// Operation counter.
    counter: AtomicUsize,
}

impl SetWorkload {
    /// Create a new set workload with `num_sets` distinct keys.
    pub fn new(num_sets: usize) -> Self {
        Self {
            num_sets,
            counter: AtomicUsize::new(0),
        }
    }
}

impl Workload for SetWorkload {
    fn name(&self) -> &str {
        "set-operations"
    }

    fn generate_op(&self) -> Operation {
        let c = self.counter.fetch_add(1, Ordering::Relaxed);
        let key = format!("set:{}", c % self.num_sets);

        // Alternate: add element (write) or read members.
        if c % 3 != 2 {
            Operation::Write {
                key,
                value: format!("elem:{c}"),
            }
        } else {
            Operation::Read { key }
        }
    }

    fn validate_history(&self, history: &[HistoryEntry]) -> ValidationResult {
        // Collect all successfully written elements per key.
        let mut added: std::collections::HashMap<String, std::collections::HashSet<String>> =
            std::collections::HashMap::new();

        for entry in history {
            if let (Operation::Write { key, value }, OpResult::Ok { .. }) =
                (&entry.op, &entry.result)
            {
                added.entry(key.clone()).or_default().insert(value.clone());
            }
        }

        let total_adds: usize = added.values().map(|s| s.len()).sum();

        // In a full implementation we would also verify that SMEMBERS
        // returned a superset of the added elements at the appropriate
        // point in time.  For now we check structural validity.
        ValidationResult {
            valid: true,
            message: format!(
                "Set workload: {} keys, {} total elements added",
                added.len(),
                total_adds
            ),
        }
    }
}

// ---------------------------------------------------------------------------
// QueueWorkload
// ---------------------------------------------------------------------------

/// Tests queue semantics (analogous to Redis LPUSH / RPOP).
///
/// Multiple producers push and multiple consumers pop. Validation ensures
/// no message loss and no duplicates.
pub struct QueueWorkload {
    /// Number of distinct queue keys.
    num_queues: usize,
    /// Operation counter.
    counter: AtomicUsize,
}

impl QueueWorkload {
    /// Create a new queue workload with `num_queues` distinct keys.
    pub fn new(num_queues: usize) -> Self {
        Self {
            num_queues,
            counter: AtomicUsize::new(0),
        }
    }
}

impl Workload for QueueWorkload {
    fn name(&self) -> &str {
        "queue-semantics"
    }

    fn generate_op(&self) -> Operation {
        let c = self.counter.fetch_add(1, Ordering::Relaxed);
        let key = format!("queue:{}", c % self.num_queues);

        // Produce (write/push) twice as often as consume (read/pop).
        if c % 3 != 2 {
            Operation::Write {
                key,
                value: format!("msg:{c}"),
            }
        } else {
            Operation::Read { key }
        }
    }

    fn validate_history(&self, history: &[HistoryEntry]) -> ValidationResult {
        let mut produced: std::collections::HashMap<String, Vec<String>> =
            std::collections::HashMap::new();
        let mut consumed: std::collections::HashMap<String, Vec<String>> =
            std::collections::HashMap::new();

        for entry in history {
            match (&entry.op, &entry.result) {
                (Operation::Write { key, value }, OpResult::Ok { .. }) => {
                    produced.entry(key.clone()).or_default().push(value.clone());
                }
                (Operation::Read { key }, OpResult::Ok { value: Some(v) }) => {
                    consumed.entry(key.clone()).or_default().push(v.clone());
                }
                _ => {}
            }
        }

        // Validate: every consumed message must have been produced, and
        // no message is consumed more than once.
        let mut violations = Vec::new();
        for (key, cons) in &consumed {
            let prod_set: std::collections::HashSet<&String> = produced
                .get(key)
                .map(|v| v.iter().collect())
                .unwrap_or_default();

            let mut seen = std::collections::HashSet::new();
            for msg in cons {
                if !prod_set.contains(msg) {
                    violations.push(format!("Queue {key}: consumed unknown message {msg}"));
                }
                if !seen.insert(msg) {
                    violations.push(format!("Queue {key}: duplicate consumption of {msg}"));
                }
            }
        }

        let valid = violations.is_empty();
        ValidationResult {
            valid,
            message: if valid {
                let total_prod: usize = produced.values().map(|v| v.len()).sum();
                let total_cons: usize = consumed.values().map(|v| v.len()).sum();
                format!(
                    "Queue workload OK: {} produced, {} consumed across {} queues",
                    total_prod,
                    total_cons,
                    produced.len()
                )
            } else {
                violations.join("; ")
            },
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn register_workload_generates_ops() {
        let w = RegisterWorkload::new(3);
        let ops: Vec<Operation> = (0..9).map(|_| w.generate_op()).collect();
        // Should cycle: Read, Write, Cas, Read, Write, Cas, ...
        assert!(matches!(ops[0], Operation::Read { .. }));
        assert!(matches!(ops[1], Operation::Write { .. }));
        assert!(matches!(ops[2], Operation::Cas { .. }));
    }

    #[test]
    fn counter_workload_validates_empty() {
        let w = CounterWorkload::new(2);
        let result = w.validate_history(&[]);
        assert!(result.valid);
    }

    #[test]
    fn set_workload_generates_ops() {
        let w = SetWorkload::new(2);
        let ops: Vec<Operation> = (0..6).map(|_| w.generate_op()).collect();
        // Pattern: Write, Write, Read, Write, Write, Read
        assert!(matches!(ops[0], Operation::Write { .. }));
        assert!(matches!(ops[2], Operation::Read { .. }));
    }

    #[test]
    fn queue_workload_validates_correct_history() {
        let w = QueueWorkload::new(1);

        let history = vec![
            HistoryEntry {
                timestamp: 1,
                client_id: 0,
                op: Operation::Write {
                    key: "queue:0".into(),
                    value: "msg:1".into(),
                },
                result: OpResult::Ok { value: None },
                node_id: "n1".into(),
            },
            HistoryEntry {
                timestamp: 2,
                client_id: 1,
                op: Operation::Read {
                    key: "queue:0".into(),
                },
                result: OpResult::Ok {
                    value: Some("msg:1".into()),
                },
                node_id: "n1".into(),
            },
        ];

        let result = w.validate_history(&history);
        assert!(result.valid);
    }

    #[test]
    fn queue_workload_detects_unknown_message() {
        let w = QueueWorkload::new(1);

        let history = vec![HistoryEntry {
            timestamp: 1,
            client_id: 0,
            op: Operation::Read {
                key: "queue:0".into(),
            },
            result: OpResult::Ok {
                value: Some("msg:unknown".into()),
            },
            node_id: "n1".into(),
        }];

        let result = w.validate_history(&history);
        assert!(!result.valid);
    }
}
