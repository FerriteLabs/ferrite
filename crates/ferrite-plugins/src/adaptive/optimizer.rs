//! Optimization Components
//!
//! Configuration and resource optimization for adaptive tuning.

use super::engine::{Adaptation, AdaptationType};
use super::AdaptiveError;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Configuration optimizer for tuning system parameters
pub struct ConfigOptimizer {
    /// Current configuration values
    current_config: HashMap<String, ConfigValue>,
    /// Optimization history
    history: Vec<ConfigChange>,
    /// Constraints on configuration values
    constraints: HashMap<String, ConfigConstraint>,
}

impl ConfigOptimizer {
    /// Create a new config optimizer
    pub fn new() -> Self {
        let mut optimizer = Self {
            current_config: HashMap::new(),
            history: Vec::new(),
            constraints: HashMap::new(),
        };
        optimizer.setup_default_constraints();
        optimizer
    }

    /// Setup default constraints
    fn setup_default_constraints(&mut self) {
        // Memory-related constraints
        self.constraints.insert(
            "maxmemory-percent".to_string(),
            ConfigConstraint {
                min: Some(ConfigValue::Float(10.0)),
                max: Some(ConfigValue::Float(95.0)),
                allowed_values: None,
            },
        );

        // Thread pool constraints
        self.constraints.insert(
            "io-threads".to_string(),
            ConfigConstraint {
                min: Some(ConfigValue::Integer(1)),
                max: Some(ConfigValue::Integer(128)),
                allowed_values: None,
            },
        );

        // Eviction policy constraints
        self.constraints.insert(
            "eviction-policy".to_string(),
            ConfigConstraint {
                min: None,
                max: None,
                allowed_values: Some(vec![
                    "noeviction".to_string(),
                    "allkeys-lru".to_string(),
                    "allkeys-lfu".to_string(),
                    "volatile-lru".to_string(),
                    "volatile-lfu".to_string(),
                    "allkeys-random".to_string(),
                    "volatile-random".to_string(),
                    "volatile-ttl".to_string(),
                ]),
            },
        );
    }

    /// Apply an eviction policy change
    pub fn apply_eviction_change(&mut self, adaptation: &Adaptation) -> Result<(), AdaptiveError> {
        if let AdaptationType::EvictionPolicyChange { policy } = &adaptation.adaptation_type {
            // Validate against constraints
            if let Some(constraint) = self.constraints.get("eviction-policy") {
                if let Some(allowed) = &constraint.allowed_values {
                    if !allowed.contains(policy) {
                        return Err(AdaptiveError::ConstraintViolation(format!(
                            "Invalid eviction policy: {}",
                            policy
                        )));
                    }
                }
            }

            // Record the change
            let old_value = self.current_config.get("eviction-policy").cloned();
            self.current_config.insert(
                "eviction-policy".to_string(),
                ConfigValue::String(policy.clone()),
            );

            self.history.push(ConfigChange {
                parameter: "eviction-policy".to_string(),
                old_value,
                new_value: ConfigValue::String(policy.clone()),
                timestamp: current_timestamp_ms(),
                adaptation_id: adaptation.id.clone(),
            });

            Ok(())
        } else {
            Err(AdaptiveError::Internal(
                "Invalid adaptation type".to_string(),
            ))
        }
    }

    /// Apply a configuration change
    pub fn apply_config_change(&mut self, adaptation: &Adaptation) -> Result<(), AdaptiveError> {
        if let AdaptationType::ConfigurationChange { parameter, value } =
            &adaptation.adaptation_type
        {
            // Parse the value
            let config_value = self.parse_value(value);

            // Validate against constraints
            if let Some(constraint) = self.constraints.get(parameter) {
                self.validate_constraint(&config_value, constraint)?;
            }

            // Record the change
            let old_value = self.current_config.get(parameter).cloned();
            self.current_config
                .insert(parameter.clone(), config_value.clone());

            self.history.push(ConfigChange {
                parameter: parameter.clone(),
                old_value,
                new_value: config_value,
                timestamp: current_timestamp_ms(),
                adaptation_id: adaptation.id.clone(),
            });

            Ok(())
        } else {
            Err(AdaptiveError::Internal(
                "Invalid adaptation type".to_string(),
            ))
        }
    }

    /// Parse a string value into ConfigValue
    fn parse_value(&self, value: &str) -> ConfigValue {
        // Try integer
        if let Ok(i) = value.parse::<i64>() {
            return ConfigValue::Integer(i);
        }
        // Try float
        if let Ok(f) = value.parse::<f64>() {
            return ConfigValue::Float(f);
        }
        // Try boolean
        if value.eq_ignore_ascii_case("true") {
            return ConfigValue::Boolean(true);
        }
        if value.eq_ignore_ascii_case("false") {
            return ConfigValue::Boolean(false);
        }
        // Default to string
        ConfigValue::String(value.to_string())
    }

    /// Validate a value against a constraint
    fn validate_constraint(
        &self,
        value: &ConfigValue,
        constraint: &ConfigConstraint,
    ) -> Result<(), AdaptiveError> {
        // Check min
        if let Some(min) = &constraint.min {
            if !value.gte(min) {
                return Err(AdaptiveError::ConstraintViolation(format!(
                    "Value {:?} is below minimum {:?}",
                    value, min
                )));
            }
        }

        // Check max
        if let Some(max) = &constraint.max {
            if !value.lte(max) {
                return Err(AdaptiveError::ConstraintViolation(format!(
                    "Value {:?} is above maximum {:?}",
                    value, max
                )));
            }
        }

        // Check allowed values
        if let Some(allowed) = &constraint.allowed_values {
            if let ConfigValue::String(s) = value {
                if !allowed.contains(s) {
                    return Err(AdaptiveError::ConstraintViolation(format!(
                        "Value {} is not in allowed values",
                        s
                    )));
                }
            }
        }

        Ok(())
    }

    /// Get current configuration
    pub fn current_config(&self) -> &HashMap<String, ConfigValue> {
        &self.current_config
    }

    /// Get configuration history
    pub fn history(&self) -> &[ConfigChange] {
        &self.history
    }

    /// Rollback to a previous configuration
    pub fn rollback(&mut self, adaptation_id: &str) -> Result<(), AdaptiveError> {
        let changes: Vec<_> = self
            .history
            .iter()
            .filter(|c| c.adaptation_id == adaptation_id)
            .cloned()
            .collect();

        for change in changes {
            if let Some(old_value) = change.old_value {
                self.current_config.insert(change.parameter, old_value);
            } else {
                self.current_config.remove(&change.parameter);
            }
        }

        Ok(())
    }
}

impl Default for ConfigOptimizer {
    fn default() -> Self {
        Self::new()
    }
}

/// Resource optimizer for memory and compute resources
pub struct ResourceOptimizer {
    /// Memory tier allocations
    memory_tiers: HashMap<String, TierAllocation>,
    /// Thread pool sizes
    thread_pools: HashMap<String, usize>,
    /// Optimization history
    history: Vec<ResourceChange>,
    /// Resource constraints
    constraints: ResourceConstraints,
}

impl ResourceOptimizer {
    /// Create a new resource optimizer
    pub fn new() -> Self {
        Self {
            memory_tiers: Self::default_tiers(),
            thread_pools: Self::default_pools(),
            history: Vec::new(),
            constraints: ResourceConstraints::default(),
        }
    }

    /// Default memory tier allocations
    fn default_tiers() -> HashMap<String, TierAllocation> {
        let mut tiers = HashMap::new();
        tiers.insert(
            "hot".to_string(),
            TierAllocation {
                name: "hot".to_string(),
                allocation_percent: 20.0,
                current_usage_percent: 0.0,
            },
        );
        tiers.insert(
            "warm".to_string(),
            TierAllocation {
                name: "warm".to_string(),
                allocation_percent: 50.0,
                current_usage_percent: 0.0,
            },
        );
        tiers.insert(
            "cold".to_string(),
            TierAllocation {
                name: "cold".to_string(),
                allocation_percent: 30.0,
                current_usage_percent: 0.0,
            },
        );
        tiers
    }

    /// Default thread pool sizes
    fn default_pools() -> HashMap<String, usize> {
        let mut pools = HashMap::new();
        let cpus = num_cpus::get();
        pools.insert("io".to_string(), cpus);
        pools.insert("compute".to_string(), cpus);
        pools.insert("background".to_string(), 2);
        pools
    }

    /// Apply a memory tier adjustment
    pub fn apply_memory_adjustment(
        &mut self,
        adaptation: &Adaptation,
    ) -> Result<(), AdaptiveError> {
        if let AdaptationType::MemoryTierAdjustment {
            tier,
            allocation_percent,
        } = &adaptation.adaptation_type
        {
            // Validate tier exists
            if !self.memory_tiers.contains_key(tier) {
                return Err(AdaptiveError::ConstraintViolation(format!(
                    "Unknown memory tier: {}",
                    tier
                )));
            }

            // Validate allocation is within bounds
            if *allocation_percent < 5.0 || *allocation_percent > 80.0 {
                return Err(AdaptiveError::ConstraintViolation(format!(
                    "Allocation {}% is out of bounds (5-80%)",
                    allocation_percent
                )));
            }

            // Calculate if total would exceed 100%
            let current_total: f64 = self
                .memory_tiers
                .iter()
                .filter(|(k, _)| *k != tier)
                .map(|(_, v)| v.allocation_percent)
                .sum();

            if current_total + allocation_percent > 100.0 {
                return Err(AdaptiveError::ConstraintViolation(
                    "Total allocation would exceed 100%".to_string(),
                ));
            }

            // Apply the change
            let old_allocation = self.memory_tiers.get(tier).map(|t| t.allocation_percent);
            if let Some(tier_data) = self.memory_tiers.get_mut(tier) {
                tier_data.allocation_percent = *allocation_percent;
            }

            // Record the change
            self.history.push(ResourceChange {
                resource_type: ResourceType::MemoryTier,
                resource_name: tier.clone(),
                old_value: old_allocation.map(|v| v.to_string()).unwrap_or_default(),
                new_value: allocation_percent.to_string(),
                timestamp: current_timestamp_ms(),
                adaptation_id: adaptation.id.clone(),
            });

            Ok(())
        } else {
            Err(AdaptiveError::Internal(
                "Invalid adaptation type".to_string(),
            ))
        }
    }

    /// Apply a thread pool resize
    pub fn apply_thread_resize(&mut self, adaptation: &Adaptation) -> Result<(), AdaptiveError> {
        if let AdaptationType::ThreadPoolResize { pool, new_size } = &adaptation.adaptation_type {
            // Validate pool exists
            if !self.thread_pools.contains_key(pool) {
                return Err(AdaptiveError::ConstraintViolation(format!(
                    "Unknown thread pool: {}",
                    pool
                )));
            }

            // Validate size
            if *new_size < 1 || *new_size > self.constraints.max_threads {
                return Err(AdaptiveError::ConstraintViolation(format!(
                    "Thread count {} is out of bounds (1-{})",
                    new_size, self.constraints.max_threads
                )));
            }

            // Apply the change
            let old_size = self.thread_pools.get(pool).copied();
            self.thread_pools.insert(pool.clone(), *new_size);

            // Record the change
            self.history.push(ResourceChange {
                resource_type: ResourceType::ThreadPool,
                resource_name: pool.clone(),
                old_value: old_size.map(|v| v.to_string()).unwrap_or_default(),
                new_value: new_size.to_string(),
                timestamp: current_timestamp_ms(),
                adaptation_id: adaptation.id.clone(),
            });

            Ok(())
        } else {
            Err(AdaptiveError::Internal(
                "Invalid adaptation type".to_string(),
            ))
        }
    }

    /// Get current memory tier allocations
    pub fn memory_tiers(&self) -> &HashMap<String, TierAllocation> {
        &self.memory_tiers
    }

    /// Get current thread pool sizes
    pub fn thread_pools(&self) -> &HashMap<String, usize> {
        &self.thread_pools
    }

    /// Get resource change history
    pub fn history(&self) -> &[ResourceChange] {
        &self.history
    }

    /// Update tier usage
    pub fn update_tier_usage(&mut self, tier: &str, usage_percent: f64) {
        if let Some(tier_data) = self.memory_tiers.get_mut(tier) {
            tier_data.current_usage_percent = usage_percent;
        }
    }

    /// Get optimization target based on current state
    pub fn get_optimization_target(&self) -> OptimizationTarget {
        // Check for memory pressure
        let hot_tier = self.memory_tiers.get("hot");
        if let Some(tier) = hot_tier {
            if tier.current_usage_percent > 90.0 {
                return OptimizationTarget::MemoryPressure;
            }
        }

        // Check for underutilized resources
        let total_allocation: f64 = self
            .memory_tiers
            .values()
            .map(|t| t.allocation_percent)
            .sum();
        if total_allocation < 80.0 {
            return OptimizationTarget::ResourceEfficiency;
        }

        OptimizationTarget::Throughput
    }
}

impl Default for ResourceOptimizer {
    fn default() -> Self {
        Self::new()
    }
}

/// Configuration value types
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum ConfigValue {
    /// Integer value
    Integer(i64),
    /// Float value
    Float(f64),
    /// String value
    String(String),
    /// Boolean value
    Boolean(bool),
}

impl ConfigValue {
    /// Check if this value is greater than or equal to another
    fn gte(&self, other: &ConfigValue) -> bool {
        match (self, other) {
            (ConfigValue::Integer(a), ConfigValue::Integer(b)) => a >= b,
            (ConfigValue::Float(a), ConfigValue::Float(b)) => a >= b,
            (ConfigValue::Integer(a), ConfigValue::Float(b)) => (*a as f64) >= *b,
            (ConfigValue::Float(a), ConfigValue::Integer(b)) => *a >= (*b as f64),
            _ => true,
        }
    }

    /// Check if this value is less than or equal to another
    fn lte(&self, other: &ConfigValue) -> bool {
        match (self, other) {
            (ConfigValue::Integer(a), ConfigValue::Integer(b)) => a <= b,
            (ConfigValue::Float(a), ConfigValue::Float(b)) => a <= b,
            (ConfigValue::Integer(a), ConfigValue::Float(b)) => (*a as f64) <= *b,
            (ConfigValue::Float(a), ConfigValue::Integer(b)) => *a <= (*b as f64),
            _ => true,
        }
    }
}

/// Configuration constraint
#[derive(Clone, Debug)]
pub struct ConfigConstraint {
    /// Minimum value
    pub min: Option<ConfigValue>,
    /// Maximum value
    pub max: Option<ConfigValue>,
    /// Allowed values (for enums)
    pub allowed_values: Option<Vec<String>>,
}

/// Record of a configuration change
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ConfigChange {
    /// Parameter name
    pub parameter: String,
    /// Old value
    pub old_value: Option<ConfigValue>,
    /// New value
    pub new_value: ConfigValue,
    /// Timestamp
    pub timestamp: u64,
    /// Associated adaptation ID
    pub adaptation_id: String,
}

/// Memory tier allocation
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TierAllocation {
    /// Tier name
    pub name: String,
    /// Allocation percentage
    pub allocation_percent: f64,
    /// Current usage percentage
    pub current_usage_percent: f64,
}

/// Resource change record
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ResourceChange {
    /// Resource type
    pub resource_type: ResourceType,
    /// Resource name
    pub resource_name: String,
    /// Old value
    pub old_value: String,
    /// New value
    pub new_value: String,
    /// Timestamp
    pub timestamp: u64,
    /// Associated adaptation ID
    pub adaptation_id: String,
}

/// Type of resource
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ResourceType {
    /// Memory tier
    MemoryTier,
    /// Thread pool
    ThreadPool,
    /// Buffer pool
    BufferPool,
    /// Connection pool
    ConnectionPool,
}

/// Resource constraints
#[derive(Clone, Debug)]
pub struct ResourceConstraints {
    /// Maximum total memory percentage
    pub max_memory_percent: f64,
    /// Maximum threads
    pub max_threads: usize,
    /// Minimum threads
    pub min_threads: usize,
}

impl Default for ResourceConstraints {
    fn default() -> Self {
        Self {
            max_memory_percent: 95.0,
            max_threads: 256,
            min_threads: 1,
        }
    }
}

/// Optimization target
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum OptimizationTarget {
    /// Optimize for throughput
    Throughput,
    /// Optimize for latency
    Latency,
    /// Reduce memory pressure
    MemoryPressure,
    /// Improve resource efficiency
    ResourceEfficiency,
    /// Balance all metrics
    Balanced,
}

/// Get current timestamp in milliseconds
fn current_timestamp_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_optimizer_creation() {
        let optimizer = ConfigOptimizer::new();
        assert!(optimizer.constraints.contains_key("eviction-policy"));
    }

    #[test]
    fn test_resource_optimizer_default_tiers() {
        let optimizer = ResourceOptimizer::new();
        assert!(optimizer.memory_tiers.contains_key("hot"));
        assert!(optimizer.memory_tiers.contains_key("warm"));
        assert!(optimizer.memory_tiers.contains_key("cold"));

        let total: f64 = optimizer
            .memory_tiers
            .values()
            .map(|t| t.allocation_percent)
            .sum();
        assert_eq!(total, 100.0);
    }

    #[test]
    fn test_config_value_comparison() {
        let int_val = ConfigValue::Integer(50);
        let float_val = ConfigValue::Float(50.0);

        assert!(int_val.gte(&ConfigValue::Integer(40)));
        assert!(int_val.lte(&ConfigValue::Integer(60)));
        assert!(float_val.gte(&ConfigValue::Float(40.0)));
    }

    #[test]
    fn test_optimization_target() {
        let mut optimizer = ResourceOptimizer::new();

        // Default should not be memory pressure
        let target = optimizer.get_optimization_target();
        assert_ne!(target, OptimizationTarget::MemoryPressure);

        // Simulate high usage
        optimizer.update_tier_usage("hot", 95.0);
        let target = optimizer.get_optimization_target();
        assert_eq!(target, OptimizationTarget::MemoryPressure);
    }
}
