#![forbid(unsafe_code)]

//! FaaS registry — central registry for deployed serverless functions.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::runtime::{ExecutionContext, FaaSResourceLimits, FunctionResult, FunctionRuntime};
use super::scheduler::{FunctionScheduler, ScheduleEntry};

/// Errors from the FaaS registry.
#[derive(Debug, Error)]
pub enum FaaSRegistryError {
    #[error("function not found: {0}")]
    NotFound(String),
    #[error("function already exists: {0}")]
    AlreadyExists(String),
    #[error("deploy error: {0}")]
    DeployError(String),
    #[error("invocation error: {0}")]
    InvocationError(String),
    #[error("scheduler error: {0}")]
    SchedulerError(String),
}

/// Supported function languages/runtimes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FunctionLanguage {
    Wasm,
}

impl std::fmt::Display for FunctionLanguage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FunctionLanguage::Wasm => write!(f, "wasm"),
        }
    }
}

/// Status of a deployed function.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FunctionStatus {
    Active,
    Disabled,
    Error,
}

impl std::fmt::Display for FunctionStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FunctionStatus::Active => write!(f, "active"),
            FunctionStatus::Disabled => write!(f, "disabled"),
            FunctionStatus::Error => write!(f, "error"),
        }
    }
}

/// Metadata for a deployed function.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunctionMeta {
    pub name: String,
    pub language: FunctionLanguage,
    pub source_hash: String,
    pub deployed_at: u64,
    pub invocation_count: u64,
    pub avg_latency_ms: f64,
    pub status: FunctionStatus,
}

/// Configuration for deploying a function.
#[derive(Debug, Clone, Default)]
pub struct DeployConfig {
    pub limits: Option<FaaSResourceLimits>,
}

/// Internal stored function data.
struct StoredFunction {
    meta: FunctionMeta,
    #[allow(dead_code)]
    source: Vec<u8>,
    total_latency_ms: u64,
}

/// Central registry for deployed FaaS functions.
pub struct FaaSRegistry {
    functions: DashMap<String, StoredFunction>,
    runtime: FunctionRuntime,
    scheduler: FunctionScheduler,
    total_invocations: AtomicU64,
}

impl FaaSRegistry {
    /// Create a new registry with default resource limits.
    pub fn new() -> Self {
        Self {
            functions: DashMap::new(),
            runtime: FunctionRuntime::new(FaaSResourceLimits::default()),
            scheduler: FunctionScheduler::new(),
            total_invocations: AtomicU64::new(0),
        }
    }

    /// Deploy a function from source bytes.
    pub fn deploy(
        &self,
        name: &str,
        source_bytes: Vec<u8>,
        _config: DeployConfig,
    ) -> Result<FunctionMeta, FaaSRegistryError> {
        if self.functions.contains_key(name) {
            return Err(FaaSRegistryError::AlreadyExists(name.to_string()));
        }

        let source_hash = simple_hash(&source_bytes);
        let now = now_epoch_secs();

        let meta = FunctionMeta {
            name: name.to_string(),
            language: FunctionLanguage::Wasm,
            source_hash,
            deployed_at: now,
            invocation_count: 0,
            avg_latency_ms: 0.0,
            status: FunctionStatus::Active,
        };

        self.functions.insert(
            name.to_string(),
            StoredFunction {
                meta: meta.clone(),
                source: source_bytes,
                total_latency_ms: 0,
            },
        );

        tracing::info!(function = name, "FaaS function deployed");
        Ok(meta)
    }

    /// Invoke a deployed function.
    pub async fn invoke(
        &self,
        name: &str,
        args: &[Vec<u8>],
    ) -> Result<FunctionResult, FaaSRegistryError> {
        if !self.functions.contains_key(name) {
            return Err(FaaSRegistryError::NotFound(name.to_string()));
        }

        // Check status
        {
            let entry = self
                .functions
                .get(name)
                .expect("function existence already checked");
            if entry.meta.status != FunctionStatus::Active {
                return Err(FaaSRegistryError::InvocationError(format!(
                    "function '{}' is {}",
                    name, entry.meta.status
                )));
            }
        }

        let input: Vec<u8> = if args.is_empty() {
            Vec::new()
        } else {
            args.concat()
        };

        let ctx = ExecutionContext::default();
        let result = self
            .runtime
            .execute(name, &input, &ctx)
            .await
            .map_err(|e| FaaSRegistryError::InvocationError(e.to_string()))?;

        // Update stats
        {
            let mut entry = self
                .functions
                .get_mut(name)
                .expect("function existence already checked");
            entry.meta.invocation_count += 1;
            entry.total_latency_ms += result.execution_time_ms;
            entry.meta.avg_latency_ms =
                entry.total_latency_ms as f64 / entry.meta.invocation_count as f64;
        }

        self.total_invocations.fetch_add(1, Ordering::Relaxed);
        Ok(result)
    }

    /// Remove a deployed function.
    pub fn undeploy(&self, name: &str) -> Result<(), FaaSRegistryError> {
        self.functions
            .remove(name)
            .map(|_| {
                tracing::info!(function = name, "FaaS function undeployed");
            })
            .ok_or_else(|| FaaSRegistryError::NotFound(name.to_string()))
    }

    /// List all deployed functions.
    pub fn list(&self) -> Vec<FunctionMeta> {
        self.functions.iter().map(|e| e.meta.clone()).collect()
    }

    /// Get metadata for a specific function.
    pub fn info(&self, name: &str) -> Result<FunctionMeta, FaaSRegistryError> {
        self.functions
            .get(name)
            .map(|e| e.meta.clone())
            .ok_or_else(|| FaaSRegistryError::NotFound(name.to_string()))
    }

    /// Get recent logs for a function.
    pub fn logs(&self, name: &str, count: usize) -> Vec<String> {
        self.runtime.logs(name, count)
    }

    /// Schedule periodic execution of a function.
    pub fn schedule(
        &self,
        function_name: &str,
        schedule_name: &str,
        cron_expr: &str,
    ) -> Result<(), FaaSRegistryError> {
        if !self.functions.contains_key(function_name) {
            return Err(FaaSRegistryError::NotFound(function_name.to_string()));
        }
        let entry = ScheduleEntry {
            name: schedule_name.to_string(),
            function_name: function_name.to_string(),
            cron_expr: cron_expr.to_string(),
            enabled: true,
            last_run: 0,
            next_run: 0,
        };
        self.scheduler
            .schedule(entry)
            .map_err(|e| FaaSRegistryError::SchedulerError(e.to_string()))
    }

    /// Cancel a scheduled function.
    pub fn unschedule(&self, name: &str) -> Result<(), FaaSRegistryError> {
        self.scheduler
            .cancel(name)
            .map_err(|e| FaaSRegistryError::SchedulerError(e.to_string()))
    }

    /// List all schedules.
    pub fn schedules(&self) -> Vec<ScheduleEntry> {
        self.scheduler.list()
    }

    /// Overall FaaS statistics.
    pub fn stats(&self) -> FaaSStats {
        let functions = self.list();
        let total_functions = functions.len();
        let total_invocations = self.total_invocations.load(Ordering::Relaxed);
        let avg_latency_ms = if total_functions > 0 {
            functions.iter().map(|f| f.avg_latency_ms).sum::<f64>() / total_functions as f64
        } else {
            0.0
        };
        FaaSStats {
            total_functions,
            total_invocations,
            avg_latency_ms,
            active_schedules: self.scheduler.len(),
        }
    }
}

impl Default for FaaSRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Aggregate FaaS statistics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FaaSStats {
    pub total_functions: usize,
    pub total_invocations: u64,
    pub avg_latency_ms: f64,
    pub active_schedules: usize,
}

fn simple_hash(data: &[u8]) -> String {
    // Simple FNV-1a-inspired hash for source identification
    let mut hash: u64 = 0xcbf29ce484222325;
    for &byte in data {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("{:016x}", hash)
}

fn now_epoch_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deploy_and_list() {
        let registry = FaaSRegistry::new();
        let meta = registry
            .deploy("test_fn", b"wasm-bytes".to_vec(), DeployConfig::default())
            .unwrap();
        assert_eq!(meta.name, "test_fn");
        assert_eq!(meta.language, FunctionLanguage::Wasm);
        assert_eq!(meta.status, FunctionStatus::Active);
        assert_eq!(registry.list().len(), 1);
    }

    #[test]
    fn test_deploy_duplicate() {
        let registry = FaaSRegistry::new();
        registry
            .deploy("fn1", b"bytes".to_vec(), DeployConfig::default())
            .unwrap();
        assert!(registry
            .deploy("fn1", b"bytes".to_vec(), DeployConfig::default())
            .is_err());
    }

    #[tokio::test]
    async fn test_invoke() {
        let registry = FaaSRegistry::new();
        registry
            .deploy("fn1", b"source".to_vec(), DeployConfig::default())
            .unwrap();
        let result = registry.invoke("fn1", &[b"hello".to_vec()]).await.unwrap();
        assert_eq!(result.output, b"hello");

        let meta = registry.info("fn1").unwrap();
        assert_eq!(meta.invocation_count, 1);
    }

    #[tokio::test]
    async fn test_invoke_not_found() {
        let registry = FaaSRegistry::new();
        assert!(registry.invoke("nope", &[]).await.is_err());
    }

    #[test]
    fn test_undeploy() {
        let registry = FaaSRegistry::new();
        registry
            .deploy("fn1", b"bytes".to_vec(), DeployConfig::default())
            .unwrap();
        registry.undeploy("fn1").unwrap();
        assert!(registry.info("fn1").is_err());
    }

    #[test]
    fn test_undeploy_not_found() {
        let registry = FaaSRegistry::new();
        assert!(registry.undeploy("nope").is_err());
    }

    #[test]
    fn test_schedule_and_unschedule() {
        let registry = FaaSRegistry::new();
        registry
            .deploy("fn1", b"bytes".to_vec(), DeployConfig::default())
            .unwrap();
        registry.schedule("fn1", "job1", "@every 5m").unwrap();
        assert_eq!(registry.schedules().len(), 1);
        registry.unschedule("job1").unwrap();
        assert!(registry.schedules().is_empty());
    }

    #[test]
    fn test_schedule_nonexistent_function() {
        let registry = FaaSRegistry::new();
        assert!(registry.schedule("nope", "job1", "@hourly").is_err());
    }

    #[test]
    fn test_stats() {
        let registry = FaaSRegistry::new();
        registry
            .deploy("fn1", b"bytes".to_vec(), DeployConfig::default())
            .unwrap();
        let stats = registry.stats();
        assert_eq!(stats.total_functions, 1);
        assert_eq!(stats.total_invocations, 0);
    }

    #[test]
    fn test_info() {
        let registry = FaaSRegistry::new();
        registry
            .deploy("fn1", b"bytes".to_vec(), DeployConfig::default())
            .unwrap();
        let meta = registry.info("fn1").unwrap();
        assert_eq!(meta.name, "fn1");
    }

    #[test]
    fn test_logs_empty() {
        let registry = FaaSRegistry::new();
        let logs = registry.logs("fn1", 10);
        assert!(logs.is_empty());
    }

    #[test]
    fn test_simple_hash() {
        let h1 = simple_hash(b"hello");
        let h2 = simple_hash(b"world");
        assert_ne!(h1, h2);
        assert_eq!(h1.len(), 16);
    }

    #[test]
    fn test_function_status_display() {
        assert_eq!(FunctionStatus::Active.to_string(), "active");
        assert_eq!(FunctionStatus::Disabled.to_string(), "disabled");
        assert_eq!(FunctionStatus::Error.to_string(), "error");
    }

    #[test]
    fn test_function_language_display() {
        assert_eq!(FunctionLanguage::Wasm.to_string(), "wasm");
    }
}
