//! CDC-Triggered Inference
//!
//! Trigger ML inference automatically when data changes.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use super::{InferenceInput, InferenceOutput};

/// Trigger configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TriggerConfig {
    /// Trigger name
    pub name: String,
    /// Key pattern to match (glob-style)
    pub pattern: String,
    /// Model to run inference with
    pub model: String,
    /// Input field from data (JSONPath or field name)
    pub input_field: String,
    /// Output key (where to store result)
    pub output_key: Option<String>,
    /// Operations to trigger on
    pub on_operations: Vec<TriggerOperation>,
    /// Whether trigger is enabled
    pub enabled: bool,
    /// Debounce time in milliseconds
    pub debounce_ms: u64,
    /// Maximum retries on failure
    pub max_retries: u32,
}

impl Default for TriggerConfig {
    fn default() -> Self {
        Self {
            name: String::new(),
            pattern: "*".to_string(),
            model: String::new(),
            input_field: "content".to_string(),
            output_key: None,
            on_operations: vec![TriggerOperation::Set, TriggerOperation::Update],
            enabled: true,
            debounce_ms: 0,
            max_retries: 3,
        }
    }
}

impl TriggerConfig {
    /// Create a new trigger config
    pub fn new(
        name: impl Into<String>,
        pattern: impl Into<String>,
        model: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            pattern: pattern.into(),
            model: model.into(),
            ..Default::default()
        }
    }

    /// Set input field
    pub fn with_input_field(mut self, field: impl Into<String>) -> Self {
        self.input_field = field.into();
        self
    }

    /// Set output key
    pub fn with_output_key(mut self, key: impl Into<String>) -> Self {
        self.output_key = Some(key.into());
        self
    }

    /// Set debounce time
    pub fn with_debounce(mut self, ms: u64) -> Self {
        self.debounce_ms = ms;
        self
    }
}

/// Operations that can trigger inference
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum TriggerOperation {
    /// Key was set
    Set,
    /// Key was updated
    Update,
    /// Key was deleted
    Delete,
    /// Key expired
    Expire,
}

/// Event that triggered inference
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TriggerEvent {
    /// Trigger name
    pub trigger_name: String,
    /// Key that triggered
    pub key: String,
    /// Operation type
    pub operation: TriggerOperation,
    /// Value (for set/update)
    pub value: Option<String>,
    /// Old value (for update)
    pub old_value: Option<String>,
    /// Timestamp
    pub timestamp: u64,
    /// Database number
    pub db: u8,
}

/// Result of triggered inference
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TriggerResult {
    /// Trigger name
    pub trigger_name: String,
    /// Key that was processed
    pub key: String,
    /// Model used
    pub model: String,
    /// Inference output
    pub output: InferenceOutput,
    /// Processing latency
    pub latency_ms: u64,
    /// Whether output was stored
    pub output_stored: bool,
    /// Output key if stored
    pub output_key: Option<String>,
}

/// Trigger statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TriggerStats {
    /// Total events processed
    pub events_processed: u64,
    /// Total inferences run
    pub inferences_run: u64,
    /// Failures
    pub failures: u64,
    /// Average latency
    pub avg_latency_ms: f64,
}

/// Manages inference triggers
pub struct InferenceTrigger {
    triggers: RwLock<HashMap<String, TriggerConfig>>,
    // Stats per trigger
    stats: RwLock<HashMap<String, TriggerStats>>,
    // Global stats
    total_events: AtomicU64,
    total_inferences: AtomicU64,
    total_failures: AtomicU64,
    total_latency_ms: AtomicU64,
}

impl InferenceTrigger {
    /// Create a new trigger manager
    pub fn new() -> Self {
        Self {
            triggers: RwLock::new(HashMap::new()),
            stats: RwLock::new(HashMap::new()),
            total_events: AtomicU64::new(0),
            total_inferences: AtomicU64::new(0),
            total_failures: AtomicU64::new(0),
            total_latency_ms: AtomicU64::new(0),
        }
    }

    /// Register a trigger
    pub fn register(&self, config: TriggerConfig) {
        let name = config.name.clone();
        self.triggers.write().insert(name.clone(), config);
        self.stats.write().insert(name, TriggerStats::default());
    }

    /// Unregister a trigger
    pub fn unregister(&self, name: &str) -> bool {
        self.triggers.write().remove(name).is_some()
    }

    /// Get trigger config
    pub fn get(&self, name: &str) -> Option<TriggerConfig> {
        self.triggers.read().get(name).cloned()
    }

    /// List all triggers
    pub fn list(&self) -> Vec<String> {
        self.triggers.read().keys().cloned().collect()
    }

    /// Check if key matches any trigger
    pub fn find_matching(&self, key: &str, operation: &TriggerOperation) -> Vec<TriggerConfig> {
        self.triggers
            .read()
            .values()
            .filter(|t| {
                t.enabled
                    && t.on_operations.contains(operation)
                    && self.pattern_matches(&t.pattern, key)
            })
            .cloned()
            .collect()
    }

    /// Simple glob pattern matching
    fn pattern_matches(&self, pattern: &str, key: &str) -> bool {
        if pattern == "*" {
            return true;
        }

        if let Some(prefix) = pattern.strip_suffix('*') {
            return key.starts_with(prefix);
        }

        if let Some(suffix) = pattern.strip_prefix('*') {
            return key.ends_with(suffix);
        }

        pattern == key
    }

    /// Process an event
    pub async fn process_event<F, Fut>(
        &self,
        event: TriggerEvent,
        inference_fn: F,
    ) -> Vec<TriggerResult>
    where
        F: Fn(&str, InferenceInput) -> Fut,
        Fut: std::future::Future<Output = Result<InferenceOutput, String>>,
    {
        self.total_events.fetch_add(1, Ordering::Relaxed);

        let matching = self.find_matching(&event.key, &event.operation);
        let mut results = Vec::new();

        for trigger in matching {
            let start = std::time::Instant::now();

            // Extract input from event
            let input = match &event.value {
                Some(v) => InferenceInput::Text(v.clone()),
                None => continue,
            };

            // Run inference
            match inference_fn(&trigger.model, input).await {
                Ok(output) => {
                    let latency = start.elapsed().as_millis() as u64;
                    self.total_inferences.fetch_add(1, Ordering::Relaxed);
                    self.total_latency_ms.fetch_add(latency, Ordering::Relaxed);

                    results.push(TriggerResult {
                        trigger_name: trigger.name.clone(),
                        key: event.key.clone(),
                        model: trigger.model.clone(),
                        output,
                        latency_ms: latency,
                        output_stored: trigger.output_key.is_some(),
                        output_key: trigger.output_key.clone(),
                    });
                }
                Err(_) => {
                    self.total_failures.fetch_add(1, Ordering::Relaxed);
                }
            }
        }

        results
    }

    /// Get trigger stats
    pub fn get_stats(&self, name: &str) -> Option<TriggerStats> {
        self.stats.read().get(name).cloned()
    }

    /// Get global stats
    pub fn global_stats(&self) -> TriggerStats {
        let total = self.total_inferences.load(Ordering::Relaxed);
        let latency = self.total_latency_ms.load(Ordering::Relaxed);

        TriggerStats {
            events_processed: self.total_events.load(Ordering::Relaxed),
            inferences_run: total,
            failures: self.total_failures.load(Ordering::Relaxed),
            avg_latency_ms: if total > 0 {
                latency as f64 / total as f64
            } else {
                0.0
            },
        }
    }

    /// Enable/disable a trigger
    pub fn set_enabled(&self, name: &str, enabled: bool) -> bool {
        if let Some(trigger) = self.triggers.write().get_mut(name) {
            trigger.enabled = enabled;
            true
        } else {
            false
        }
    }
}

impl Default for InferenceTrigger {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_trigger_config() {
        let config = TriggerConfig::new("test", "users:*", "sentiment")
            .with_input_field("text")
            .with_output_key("users:*:sentiment")
            .with_debounce(100);

        assert_eq!(config.name, "test");
        assert_eq!(config.pattern, "users:*");
        assert_eq!(config.model, "sentiment");
        assert_eq!(config.debounce_ms, 100);
    }

    #[test]
    fn test_pattern_matching() {
        let trigger = InferenceTrigger::new();

        assert!(trigger.pattern_matches("*", "anything"));
        assert!(trigger.pattern_matches("users:*", "users:123"));
        assert!(!trigger.pattern_matches("users:*", "orders:123"));
        assert!(trigger.pattern_matches("*.json", "file.json"));
        assert!(trigger.pattern_matches("exact", "exact"));
        assert!(!trigger.pattern_matches("exact", "different"));
    }

    #[test]
    fn test_trigger_registration() {
        let manager = InferenceTrigger::new();

        let config = TriggerConfig::new("test", "users:*", "model");
        manager.register(config);

        assert!(manager.get("test").is_some());
        assert!(manager.list().contains(&"test".to_string()));

        manager.unregister("test");
        assert!(manager.get("test").is_none());
    }

    #[test]
    fn test_find_matching() {
        let manager = InferenceTrigger::new();

        manager.register(TriggerConfig::new("t1", "users:*", "model1"));
        manager.register(TriggerConfig::new("t2", "orders:*", "model2"));

        let matches = manager.find_matching("users:123", &TriggerOperation::Set);
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].name, "t1");

        let no_matches = manager.find_matching("products:123", &TriggerOperation::Set);
        assert!(no_matches.is_empty());
    }

    #[test]
    fn test_enable_disable() {
        let manager = InferenceTrigger::new();

        let config = TriggerConfig::new("test", "*", "model");
        manager.register(config);

        assert!(manager.get("test").unwrap().enabled);

        manager.set_enabled("test", false);
        assert!(!manager.get("test").unwrap().enabled);

        // Disabled trigger should not match
        let matches = manager.find_matching("key", &TriggerOperation::Set);
        assert!(matches.is_empty());
    }
}
