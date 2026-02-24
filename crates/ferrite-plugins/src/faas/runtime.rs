#![forbid(unsafe_code)]

//! FaaS function runtime — manages sandboxed function execution environments.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Errors from the FaaS runtime.
#[derive(Debug, Error)]
pub enum FaaSRuntimeError {
    #[error("function not found: {0}")]
    FunctionNotFound(String),
    #[error("execution timeout after {0}ms")]
    Timeout(u64),
    #[error("execution error: {0}")]
    ExecutionError(String),
    #[error("resource limit exceeded: {0}")]
    ResourceLimitExceeded(String),
}

/// Resource limits for a function execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FaaSResourceLimits {
    /// Maximum memory in bytes (default 64 MB).
    pub max_memory_bytes: u64,
    /// Maximum CPU time in milliseconds (default 5000).
    pub max_cpu_time_ms: u64,
    /// Maximum network calls allowed (default 10).
    pub max_network_calls: u32,
}

impl Default for FaaSResourceLimits {
    fn default() -> Self {
        Self {
            max_memory_bytes: 64 * 1024 * 1024,
            max_cpu_time_ms: 5000,
            max_network_calls: 10,
        }
    }
}

/// Result of a function execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunctionResult {
    /// Raw output bytes.
    pub output: Vec<u8>,
    /// Wall-clock execution time in milliseconds.
    pub execution_time_ms: u64,
    /// Approximate memory used in bytes.
    pub memory_used_bytes: u64,
    /// Log lines emitted during execution.
    pub logs: Vec<String>,
}

/// Execution context passed into a function invocation.
#[derive(Debug, Clone, Default)]
pub struct ExecutionContext {
    /// Caller identity (optional).
    pub caller: Option<String>,
    /// Additional metadata key-value pairs.
    pub metadata: Vec<(String, String)>,
}

/// Manages sandboxed function execution.
pub struct FunctionRuntime {
    limits: FaaSResourceLimits,
    total_invocations: AtomicU64,
    logs: Arc<Mutex<Vec<(String, Vec<String>)>>>,
}

impl FunctionRuntime {
    /// Create a new runtime with the given resource limits.
    pub fn new(limits: FaaSResourceLimits) -> Self {
        Self {
            limits,
            total_invocations: AtomicU64::new(0),
            logs: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Execute a function by name with the given input and context.
    ///
    /// Uses `tokio::time::timeout` for CPU-time enforcement.
    pub async fn execute(
        &self,
        fn_name: &str,
        input: &[u8],
        _context: &ExecutionContext,
    ) -> Result<FunctionResult, FaaSRuntimeError> {
        let timeout_dur = std::time::Duration::from_millis(self.limits.max_cpu_time_ms);
        let start = Instant::now();

        let result = tokio::time::timeout(timeout_dur, async {
            // Simulated WASM execution: echo input back.
            // In production this delegates to WasmExecutor.
            let output = input.to_vec();
            let memory_used = output.len() as u64;

            if memory_used > self.limits.max_memory_bytes {
                return Err(FaaSRuntimeError::ResourceLimitExceeded(format!(
                    "memory {} exceeds limit {}",
                    memory_used, self.limits.max_memory_bytes
                )));
            }

            Ok((output, memory_used))
        })
        .await
        .map_err(|_| FaaSRuntimeError::Timeout(self.limits.max_cpu_time_ms))?;

        let (output, memory_used) = result?;
        let elapsed = start.elapsed().as_millis() as u64;

        self.total_invocations.fetch_add(1, Ordering::Relaxed);

        let log_line = format!("executed {} in {}ms", fn_name, elapsed);
        {
            let mut logs = self.logs.lock();
            logs.push((fn_name.to_string(), vec![log_line.clone()]));
            // Keep bounded
            if logs.len() > 10_000 {
                logs.drain(..5_000);
            }
        }

        Ok(FunctionResult {
            output,
            execution_time_ms: elapsed,
            memory_used_bytes: memory_used,
            logs: vec![log_line],
        })
    }

    /// Retrieve recent logs for a specific function.
    pub fn logs(&self, fn_name: &str, count: usize) -> Vec<String> {
        let logs = self.logs.lock();
        logs.iter()
            .rev()
            .filter(|(name, _)| name == fn_name)
            .take(count)
            .flat_map(|(_, lines)| lines.clone())
            .collect()
    }

    /// Total invocations across all functions.
    pub fn total_invocations(&self) -> u64 {
        self.total_invocations.load(Ordering::Relaxed)
    }

    /// Current resource limits.
    pub fn limits(&self) -> &FaaSResourceLimits {
        &self.limits
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_execute_basic() {
        let rt = FunctionRuntime::new(FaaSResourceLimits::default());
        let ctx = ExecutionContext::default();
        let result = rt.execute("hello", b"world", &ctx).await.unwrap();
        assert_eq!(result.output, b"world");
        assert_eq!(rt.total_invocations(), 1);
    }

    #[tokio::test]
    async fn test_execute_timeout() {
        let limits = FaaSResourceLimits {
            max_cpu_time_ms: 1, // 1ms — extremely tight
            ..Default::default()
        };
        let rt = FunctionRuntime::new(limits);
        let ctx = ExecutionContext::default();
        // Fast execution should still succeed in most cases
        let _ = rt.execute("fast", b"x", &ctx).await;
    }

    #[test]
    fn test_logs_retrieval() {
        let rt = FunctionRuntime::new(FaaSResourceLimits::default());
        {
            let mut logs = rt.logs.lock();
            logs.push(("fn1".to_string(), vec!["log1".to_string()]));
            logs.push(("fn2".to_string(), vec!["log2".to_string()]));
            logs.push(("fn1".to_string(), vec!["log3".to_string()]));
        }
        let fn1_logs = rt.logs("fn1", 10);
        assert_eq!(fn1_logs.len(), 2);
    }

    #[test]
    fn test_default_limits() {
        let limits = FaaSResourceLimits::default();
        assert_eq!(limits.max_memory_bytes, 64 * 1024 * 1024);
        assert_eq!(limits.max_cpu_time_ms, 5000);
        assert_eq!(limits.max_network_calls, 10);
    }
}
