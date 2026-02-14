//! Shadow-mode result comparator for Redis proxy.
//!
//! Compares responses from Redis and Ferrite to detect divergences
//! during shadow mode operation.

use std::collections::VecDeque;
use std::time::Duration;

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

/// Maximum number of divergence records kept in memory.
const MAX_DIVERGENCE_HISTORY: usize = 10_000;

/// Represents the result of a command execution against one backend.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommandResult {
    /// The backend that produced this result.
    pub backend: Backend,
    /// The response bytes (RESP-encoded).
    pub response: Vec<u8>,
    /// Time taken to execute the command.
    pub latency: Duration,
    /// Whether the command succeeded.
    pub success: bool,
}

/// Which backend produced a result.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Backend {
    Redis,
    Ferrite,
}

/// A recorded divergence between Redis and Ferrite responses.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Divergence {
    /// The command that caused the divergence.
    pub command: String,
    /// The key involved (if applicable).
    pub key: Option<String>,
    /// Redis response summary.
    pub redis_response: String,
    /// Ferrite response summary.
    pub ferrite_response: String,
    /// When the divergence was detected.
    pub timestamp: chrono::DateTime<chrono::Utc>,
    /// Classification of the divergence.
    pub severity: DivergenceSeverity,
}

/// Severity levels for divergences.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DivergenceSeverity {
    /// Responses are semantically equivalent (e.g., ordering differences).
    Benign,
    /// Minor differences that may not affect application behavior.
    Warning,
    /// Significant differences that could affect application behavior.
    Error,
    /// Data loss or corruption detected.
    Critical,
}

/// Compares Redis and Ferrite responses and tracks divergences.
pub struct ResultComparator {
    divergences: Mutex<VecDeque<Divergence>>,
    comparison_count: std::sync::atomic::AtomicU64,
    divergence_count: std::sync::atomic::AtomicU64,
}

impl ResultComparator {
    pub fn new() -> Self {
        Self {
            divergences: Mutex::new(VecDeque::with_capacity(MAX_DIVERGENCE_HISTORY)),
            comparison_count: std::sync::atomic::AtomicU64::new(0),
            divergence_count: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Compares two command results and records any divergence.
    pub fn compare(
        &self,
        command: &str,
        key: Option<&str>,
        redis_result: &CommandResult,
        ferrite_result: &CommandResult,
    ) -> ComparisonOutcome {
        self.comparison_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        // Both failed the same way
        if !redis_result.success && !ferrite_result.success {
            return ComparisonOutcome::BothFailed;
        }

        // One succeeded, one failed
        if redis_result.success != ferrite_result.success {
            let divergence = Divergence {
                command: command.to_string(),
                key: key.map(|k| k.to_string()),
                redis_response: format!(
                    "success={}, len={}",
                    redis_result.success,
                    redis_result.response.len()
                ),
                ferrite_response: format!(
                    "success={}, len={}",
                    ferrite_result.success,
                    ferrite_result.response.len()
                ),
                timestamp: chrono::Utc::now(),
                severity: DivergenceSeverity::Error,
            };
            self.record_divergence(divergence);
            return ComparisonOutcome::Diverged(DivergenceSeverity::Error);
        }

        // Both succeeded — compare responses
        if redis_result.response == ferrite_result.response {
            return ComparisonOutcome::Match;
        }

        // Responses differ — classify severity
        let severity =
            self.classify_divergence(command, &redis_result.response, &ferrite_result.response);
        let divergence = Divergence {
            command: command.to_string(),
            key: key.map(|k| k.to_string()),
            redis_response: String::from_utf8_lossy(&redis_result.response).to_string(),
            ferrite_response: String::from_utf8_lossy(&ferrite_result.response).to_string(),
            timestamp: chrono::Utc::now(),
            severity,
        };
        self.record_divergence(divergence);
        ComparisonOutcome::Diverged(severity)
    }

    /// Returns the most recent divergences (up to `limit`).
    pub fn recent_divergences(&self, limit: usize) -> Vec<Divergence> {
        let divergences = self.divergences.lock();
        divergences.iter().rev().take(limit).cloned().collect()
    }

    /// Returns total comparison count.
    pub fn comparison_count(&self) -> u64 {
        self.comparison_count
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Returns total divergence count.
    pub fn divergence_count(&self) -> u64 {
        self.divergence_count
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Clears all recorded divergences.
    pub fn clear(&self) {
        self.divergences.lock().clear();
    }

    fn record_divergence(&self, divergence: Divergence) {
        self.divergence_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let mut divergences = self.divergences.lock();
        if divergences.len() >= MAX_DIVERGENCE_HISTORY {
            divergences.pop_front();
        }
        divergences.push_back(divergence);
    }

    fn classify_divergence(
        &self,
        command: &str,
        _redis: &[u8],
        _ferrite: &[u8],
    ) -> DivergenceSeverity {
        // Commands where ordering doesn't matter
        let unordered_commands = ["SMEMBERS", "HGETALL", "KEYS", "SINTER", "SUNION", "SDIFF"];
        let cmd_upper = command.to_uppercase();
        if unordered_commands.iter().any(|c| cmd_upper.starts_with(c)) {
            return DivergenceSeverity::Benign;
        }

        // Default to warning for other divergences
        DivergenceSeverity::Warning
    }
}

impl Default for ResultComparator {
    fn default() -> Self {
        Self::new()
    }
}

/// Outcome of comparing Redis and Ferrite responses.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ComparisonOutcome {
    /// Responses are identical.
    Match,
    /// Both backends returned errors.
    BothFailed,
    /// Responses differ with the given severity.
    Diverged(DivergenceSeverity),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_result(response: &[u8], success: bool) -> CommandResult {
        CommandResult {
            backend: Backend::Redis,
            response: response.to_vec(),
            latency: Duration::from_micros(100),
            success,
        }
    }

    #[test]
    fn test_matching_responses() {
        let comparator = ResultComparator::new();
        let redis = make_result(b"+OK\r\n", true);
        let ferrite = make_result(b"+OK\r\n", true);

        let outcome = comparator.compare("SET", Some("key"), &redis, &ferrite);
        assert_eq!(outcome, ComparisonOutcome::Match);
        assert_eq!(comparator.divergence_count(), 0);
    }

    #[test]
    fn test_diverging_responses() {
        let comparator = ResultComparator::new();
        let redis = make_result(b"+OK\r\n", true);
        let ferrite = make_result(b"-ERR\r\n", true);

        let outcome = comparator.compare("SET", Some("key"), &redis, &ferrite);
        assert!(matches!(outcome, ComparisonOutcome::Diverged(_)));
        assert_eq!(comparator.divergence_count(), 1);
    }

    #[test]
    fn test_both_failed() {
        let comparator = ResultComparator::new();
        let redis = make_result(b"-ERR\r\n", false);
        let ferrite = make_result(b"-ERR\r\n", false);

        let outcome = comparator.compare("SET", Some("key"), &redis, &ferrite);
        assert_eq!(outcome, ComparisonOutcome::BothFailed);
    }

    #[test]
    fn test_one_failed() {
        let comparator = ResultComparator::new();
        let redis = make_result(b"+OK\r\n", true);
        let ferrite = make_result(b"-ERR\r\n", false);

        let outcome = comparator.compare("SET", Some("key"), &redis, &ferrite);
        assert_eq!(
            outcome,
            ComparisonOutcome::Diverged(DivergenceSeverity::Error)
        );
    }

    #[test]
    fn test_unordered_command_benign() {
        let comparator = ResultComparator::new();
        let redis = make_result(b"*2\r\n$1\r\na\r\n$1\r\nb\r\n", true);
        let ferrite = make_result(b"*2\r\n$1\r\nb\r\n$1\r\na\r\n", true);

        let outcome = comparator.compare("SMEMBERS", Some("myset"), &redis, &ferrite);
        assert_eq!(
            outcome,
            ComparisonOutcome::Diverged(DivergenceSeverity::Benign)
        );
    }

    #[test]
    fn test_recent_divergences_limit() {
        let comparator = ResultComparator::new();
        for i in 0..20 {
            let redis = make_result(b"+OK\r\n", true);
            let ferrite = make_result(format!("-ERR {}\r\n", i).as_bytes(), true);
            comparator.compare("GET", Some(&format!("key:{}", i)), &redis, &ferrite);
        }
        let recent = comparator.recent_divergences(5);
        assert_eq!(recent.len(), 5);
    }

    #[test]
    fn test_clear_divergences() {
        let comparator = ResultComparator::new();
        let redis = make_result(b"+OK\r\n", true);
        let ferrite = make_result(b"-ERR\r\n", true);
        comparator.compare("SET", Some("key"), &redis, &ferrite);
        assert_eq!(comparator.recent_divergences(100).len(), 1);

        comparator.clear();
        assert_eq!(comparator.recent_divergences(100).len(), 0);
    }
}
