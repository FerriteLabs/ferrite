//! Smart Data Lifecycle Manager
//!
//! Orchestrates automated hot→cold→archive→delete transitions for keys
//! based on configurable policies with age, access-frequency, size, and
//! idle-time triggers. Integrates with the tiering engine to provide
//! cost-aware lifecycle management.
//!
//! ## Architecture
//!
//! The lifecycle manager operates in three phases:
//! 1. **Policy Evaluation** — matches keys against namespace patterns and
//!    evaluates stage triggers to determine the next action.
//! 2. **Action Planning** — selects the highest-priority applicable policy
//!    and computes the lifecycle action for each key.
//! 3. **Cycle Execution** — applies planned actions in bulk, respecting
//!    per-cycle limits and dry-run mode.
//!
//! ## Usage
//!
//! ```rust,ignore
//! use ferrite::tiering::lifecycle::{LifecycleManager, LifecycleConfig, LifecyclePolicy};
//!
//! let manager = LifecycleManager::new(LifecycleConfig::default());
//!
//! // Add a policy
//! let policy = LifecyclePolicy {
//!     name: "expire-sessions".into(),
//!     namespace_pattern: "session:*".into(),
//!     stages: vec![/* ... */],
//!     priority: 10,
//!     enabled: true,
//!     description: None,
//! };
//! let id = manager.add_policy(policy).unwrap();
//!
//! // Evaluate a key
//! let action = manager.evaluate_key("session:abc", &metadata);
//!
//! // Run a full cycle
//! let result = manager.execute_cycle();
//! ```

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use chrono::{DateTime, Utc};
use dashmap::DashMap;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Default evaluation interval (5 minutes).
const DEFAULT_EVALUATION_INTERVAL_SECS: u64 = 300;

/// Default maximum transitions per cycle.
const DEFAULT_MAX_TRANSITIONS: usize = 10_000;

/// Maximum number of policies allowed.
const MAX_POLICIES: usize = 1_000;

/// Cost per GB/month by tier (USD) — sensible defaults.
const COST_HOT_PER_GB: f64 = 0.10;
const COST_WARM_PER_GB: f64 = 0.05;
const COST_COLD_PER_GB: f64 = 0.01;
const COST_ARCHIVE_PER_GB: f64 = 0.004;

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Errors returned by lifecycle operations.
#[derive(Debug, Error, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum LifecycleError {
    /// The requested policy was not found.
    #[error("policy not found")]
    PolicyNotFound,

    /// A policy with the given name already exists.
    #[error("policy already exists: {0}")]
    PolicyAlreadyExists(String),

    /// The policy definition is invalid.
    #[error("invalid policy: {0}")]
    InvalidPolicy(String),

    /// A data migration operation failed.
    #[error("migration failed: {0}")]
    MigrationFailed(String),

    /// An archival operation failed.
    #[error("archival failed: {0}")]
    ArchivalFailed(String),

    /// The maximum number of policies has been reached.
    #[error("maximum number of policies exceeded")]
    MaxPoliciesExceeded,

    /// An internal lifecycle error occurred.
    #[error("internal error: {0}")]
    Internal(String),
}

// ---------------------------------------------------------------------------
// PolicyId
// ---------------------------------------------------------------------------

/// Unique identifier for a lifecycle policy.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PolicyId(pub String);

impl PolicyId {
    /// Generate a new random policy id.
    pub fn new() -> Self {
        Self(Uuid::new_v4().to_string())
    }
}

impl Default for PolicyId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for PolicyId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

// ---------------------------------------------------------------------------
// LifecycleTier
// ---------------------------------------------------------------------------

/// Logical lifecycle tier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
pub enum LifecycleTier {
    /// In-memory hot tier with lowest latency.
    Hot,
    /// Warm tier with slightly reduced performance.
    Warm,
    /// Cold tier optimized for infrequent access.
    Cold,
    /// Archive tier for long-term, low-cost retention.
    Archive,
    /// Terminal stage indicating the key should be removed.
    Delete,
}

impl LifecycleTier {
    /// Cost per GB/month for this tier.
    pub fn cost_per_gb(&self) -> f64 {
        match self {
            LifecycleTier::Hot => COST_HOT_PER_GB,
            LifecycleTier::Warm => COST_WARM_PER_GB,
            LifecycleTier::Cold => COST_COLD_PER_GB,
            LifecycleTier::Archive => COST_ARCHIVE_PER_GB,
            LifecycleTier::Delete => 0.0,
        }
    }

    /// Display name.
    pub fn name(&self) -> &'static str {
        match self {
            LifecycleTier::Hot => "hot",
            LifecycleTier::Warm => "warm",
            LifecycleTier::Cold => "cold",
            LifecycleTier::Archive => "archive",
            LifecycleTier::Delete => "delete",
        }
    }
}

impl std::fmt::Display for LifecycleTier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

// ---------------------------------------------------------------------------
// TriggerType & StageTrigger
// ---------------------------------------------------------------------------

/// Type of trigger that initiates a stage transition.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum TriggerType {
    /// Key age in days since creation.
    AgeDays,
    /// Access frequency (accesses per day).
    AccessFrequency,
    /// Key size in bytes.
    SizeBytes,
    /// Idle time in seconds since last access.
    IdleTimeSecs,
    /// Manual trigger (always evaluates to true when threshold is 0).
    Manual,
}

/// Trigger condition for a lifecycle stage.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StageTrigger {
    /// The type of trigger.
    pub trigger_type: TriggerType,
    /// Threshold value. Transition fires when the metric exceeds this value.
    /// For `AccessFrequency`, transition fires when frequency drops *below*
    /// the threshold (indicating the key has become cold).
    pub threshold: f64,
}

impl StageTrigger {
    /// Evaluate whether this trigger fires for the given key metadata.
    pub fn evaluate(&self, metadata: &KeyMetadata) -> bool {
        let now = Utc::now();
        match self.trigger_type {
            TriggerType::AgeDays => {
                let age_days = (now - metadata.created_at).num_seconds() as f64 / 86_400.0;
                age_days >= self.threshold
            }
            TriggerType::AccessFrequency => {
                let age_days = (now - metadata.created_at).num_seconds().max(1) as f64 / 86_400.0;
                let freq = metadata.access_count as f64 / age_days;
                // Low frequency → should move to colder tier
                freq <= self.threshold
            }
            TriggerType::SizeBytes => metadata.size_bytes as f64 >= self.threshold,
            TriggerType::IdleTimeSecs => {
                let idle = (now - metadata.last_accessed).num_seconds() as f64;
                idle >= self.threshold
            }
            TriggerType::Manual => self.threshold == 0.0,
        }
    }
}

// ---------------------------------------------------------------------------
// StageAction
// ---------------------------------------------------------------------------

/// Action to perform when a stage transition fires.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum StageAction {
    /// Migrate data to the target tier.
    Migrate,
    /// Compress data in place.
    Compress,
    /// Archive data to external storage.
    Archive {
        /// The destination path or URI for archived data.
        destination: String,
    },
    /// Delete the key.
    Delete,
    /// Send a notification.
    Notify {
        /// The notification channel name.
        channel: String,
    },
}

// ---------------------------------------------------------------------------
// LifecycleStage
// ---------------------------------------------------------------------------

/// A single stage in a lifecycle policy.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LifecycleStage {
    /// Target tier for this stage.
    pub tier: LifecycleTier,
    /// Trigger condition.
    pub trigger: StageTrigger,
    /// Action to perform.
    pub action: StageAction,
    /// Optional retention period in days before the next stage.
    pub retention_days: Option<u32>,
}

// ---------------------------------------------------------------------------
// LifecyclePolicy
// ---------------------------------------------------------------------------

/// A named lifecycle policy that applies to keys matching a namespace pattern.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LifecyclePolicy {
    /// Human-readable name.
    pub name: String,
    /// Glob-style namespace pattern (e.g. `session:*`, `cache:user:*`).
    pub namespace_pattern: String,
    /// Ordered stages from hottest to coldest.
    pub stages: Vec<LifecycleStage>,
    /// Priority (higher = evaluated first).
    pub priority: u32,
    /// Whether this policy is active.
    pub enabled: bool,
    /// Optional description.
    pub description: Option<String>,
}

// ---------------------------------------------------------------------------
// KeyMetadata
// ---------------------------------------------------------------------------

/// Metadata about a key used for lifecycle evaluation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeyMetadata {
    /// The key name.
    pub key: String,
    /// Size in bytes.
    pub size_bytes: u64,
    /// When the key was created.
    pub created_at: DateTime<Utc>,
    /// When the key was last accessed.
    pub last_accessed: DateTime<Utc>,
    /// Total access count.
    pub access_count: u64,
    /// Current lifecycle tier.
    pub current_tier: LifecycleTier,
    /// Remaining TTL in milliseconds, if set.
    pub ttl_remaining_ms: Option<i64>,
}

// ---------------------------------------------------------------------------
// LifecycleAction
// ---------------------------------------------------------------------------

/// Action determined by evaluating a key against lifecycle policies.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum LifecycleAction {
    /// No transition needed.
    NoAction,
    /// Move the key to a different tier.
    MoveToTier(LifecycleTier),
    /// Compress the key in place.
    Compress,
    /// Archive the key to an external destination.
    Archive {
        /// The destination path or URI for archived data.
        destination: String,
    },
    /// Delete the key.
    Delete,
    /// Set a TTL on the key.
    SetTtl {
        /// Time-to-live in milliseconds.
        ttl_ms: u64,
    },
}

// ---------------------------------------------------------------------------
// CycleResult & ActionTaken
// ---------------------------------------------------------------------------

/// Result of executing one lifecycle cycle.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CycleResult {
    /// Number of keys evaluated.
    pub keys_evaluated: u64,
    /// Actions that were taken (or would be taken in dry-run mode).
    pub actions_taken: Vec<ActionTaken>,
    /// Errors encountered during the cycle.
    pub errors: Vec<String>,
    /// Wall-clock duration in milliseconds.
    pub duration_ms: u64,
    /// Estimated cost savings from this cycle (USD).
    pub cost_savings_usd: f64,
}

/// A single action taken during a lifecycle cycle.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActionTaken {
    /// The key pattern or specific key affected.
    pub key_pattern: String,
    /// The action performed.
    pub action: LifecycleAction,
    /// Number of keys affected.
    pub keys_affected: u64,
    /// Total bytes affected.
    pub bytes_affected: u64,
}

// ---------------------------------------------------------------------------
// CostProjection & TierCostBreakdown
// ---------------------------------------------------------------------------

/// Cost projection over a given period.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CostProjection {
    /// Current estimated monthly cost (USD).
    pub current_monthly_cost_usd: f64,
    /// Projected monthly cost after lifecycle optimizations (USD).
    pub projected_monthly_cost_usd: f64,
    /// Absolute savings (USD).
    pub savings_usd: f64,
    /// Savings as a percentage of current cost.
    pub savings_percent: f64,
    /// Per-tier breakdown.
    pub breakdown: Vec<TierCostBreakdown>,
}

/// Cost breakdown for a single tier.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TierCostBreakdown {
    /// The tier.
    pub tier: LifecycleTier,
    /// Number of keys in this tier.
    pub keys: u64,
    /// Total bytes in this tier.
    pub bytes: u64,
    /// Monthly cost for this tier (USD).
    pub monthly_cost_usd: f64,
}

// ---------------------------------------------------------------------------
// PolicyInfo
// ---------------------------------------------------------------------------

/// Summary information about a registered policy.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolicyInfo {
    /// Policy identifier.
    pub id: PolicyId,
    /// Human-readable name.
    pub name: String,
    /// Namespace pattern.
    pub namespace_pattern: String,
    /// Number of stages.
    pub stages_count: usize,
    /// Priority.
    pub priority: u32,
    /// Whether the policy is enabled.
    pub enabled: bool,
    /// Number of keys this policy has matched.
    pub keys_matched: u64,
    /// When this policy was last evaluated.
    pub last_evaluated: Option<DateTime<Utc>>,
}

// ---------------------------------------------------------------------------
// LifecycleStats
// ---------------------------------------------------------------------------

/// Aggregate statistics for the lifecycle manager.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LifecycleStats {
    /// Total number of registered policies.
    pub total_policies: u64,
    /// Number of currently enabled policies.
    pub active_policies: u64,
    /// Total evaluation cycles executed.
    pub total_cycles: u64,
    /// Total keys migrated between tiers.
    pub keys_migrated: u64,
    /// Total keys archived to external storage.
    pub keys_archived: u64,
    /// Total keys deleted.
    pub keys_deleted: u64,
    /// Total bytes reclaimed through lifecycle actions.
    pub bytes_saved: u64,
    /// Cumulative estimated cost savings in USD.
    pub total_cost_savings_usd: f64,
}

// ---------------------------------------------------------------------------
// LifecycleConfig
// ---------------------------------------------------------------------------

/// Configuration for the lifecycle manager.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LifecycleConfig {
    /// How often to run evaluation cycles (seconds).
    pub evaluation_interval_secs: u64,
    /// Maximum key transitions per cycle.
    pub max_transitions_per_cycle: usize,
    /// When true, evaluate but do not apply changes.
    pub dry_run: bool,
    /// Enable archival transitions.
    pub enable_archival: bool,
    /// Enable automatic deletion.
    pub enable_auto_delete: bool,
    /// Enable cost-optimisation heuristics.
    pub cost_optimization_enabled: bool,
}

impl Default for LifecycleConfig {
    fn default() -> Self {
        Self {
            evaluation_interval_secs: DEFAULT_EVALUATION_INTERVAL_SECS,
            max_transitions_per_cycle: DEFAULT_MAX_TRANSITIONS,
            dry_run: false,
            enable_archival: true,
            enable_auto_delete: false,
            cost_optimization_enabled: true,
        }
    }
}

// ---------------------------------------------------------------------------
// Internal state
// ---------------------------------------------------------------------------

/// Per-policy runtime state stored alongside the policy definition.
struct PolicyEntry {
    id: PolicyId,
    policy: LifecyclePolicy,
    keys_matched: AtomicU64,
    last_evaluated: RwLock<Option<DateTime<Utc>>>,
}

// ---------------------------------------------------------------------------
// LifecycleManager
// ---------------------------------------------------------------------------

/// Automated data lifecycle manager.
///
/// Thread-safe: all public methods take `&self` and use interior mutability
/// via [`DashMap`], [`AtomicU64`], and [`parking_lot::RwLock`].
pub struct LifecycleManager {
    config: LifecycleConfig,
    policies: DashMap<String, PolicyEntry>,
    // global stats (atomics)
    total_cycles: AtomicU64,
    keys_migrated: AtomicU64,
    keys_archived: AtomicU64,
    keys_deleted: AtomicU64,
    bytes_saved: AtomicU64,
    cost_savings: RwLock<f64>,
}

impl LifecycleManager {
    /// Create a new lifecycle manager with the given configuration.
    pub fn new(config: LifecycleConfig) -> Self {
        Self {
            config,
            policies: DashMap::new(),
            total_cycles: AtomicU64::new(0),
            keys_migrated: AtomicU64::new(0),
            keys_archived: AtomicU64::new(0),
            keys_deleted: AtomicU64::new(0),
            bytes_saved: AtomicU64::new(0),
            cost_savings: RwLock::new(0.0),
        }
    }

    // -- Policy management --------------------------------------------------

    /// Register a new lifecycle policy.
    pub fn add_policy(&self, policy: LifecyclePolicy) -> Result<PolicyId, LifecycleError> {
        if policy.name.is_empty() {
            return Err(LifecycleError::InvalidPolicy(
                "policy name must not be empty".into(),
            ));
        }
        if policy.stages.is_empty() {
            return Err(LifecycleError::InvalidPolicy(
                "policy must have at least one stage".into(),
            ));
        }
        if policy.namespace_pattern.is_empty() {
            return Err(LifecycleError::InvalidPolicy(
                "namespace pattern must not be empty".into(),
            ));
        }
        if self.policies.len() >= MAX_POLICIES {
            return Err(LifecycleError::MaxPoliciesExceeded);
        }
        // Check for duplicate names
        for entry in self.policies.iter() {
            if entry.value().policy.name == policy.name {
                return Err(LifecycleError::PolicyAlreadyExists(policy.name.clone()));
            }
        }

        let id = PolicyId::new();
        let entry = PolicyEntry {
            id: id.clone(),
            policy,
            keys_matched: AtomicU64::new(0),
            last_evaluated: RwLock::new(None),
        };
        self.policies.insert(id.0.clone(), entry);
        Ok(id)
    }

    /// Remove a policy by id.
    pub fn remove_policy(&self, id: &PolicyId) -> Result<(), LifecycleError> {
        self.policies
            .remove(&id.0)
            .map(|_| ())
            .ok_or(LifecycleError::PolicyNotFound)
    }

    /// List all registered policies.
    pub fn list_policies(&self) -> Vec<PolicyInfo> {
        let mut infos: Vec<PolicyInfo> = self
            .policies
            .iter()
            .map(|entry| {
                let e = entry.value();
                PolicyInfo {
                    id: e.id.clone(),
                    name: e.policy.name.clone(),
                    namespace_pattern: e.policy.namespace_pattern.clone(),
                    stages_count: e.policy.stages.len(),
                    priority: e.policy.priority,
                    enabled: e.policy.enabled,
                    keys_matched: e.keys_matched.load(Ordering::Relaxed),
                    last_evaluated: *e.last_evaluated.read(),
                }
            })
            .collect();
        // Sort by priority descending (highest first)
        infos.sort_by(|a, b| b.priority.cmp(&a.priority));
        infos
    }

    // -- Key evaluation -----------------------------------------------------

    /// Evaluate a single key against all active policies and return the
    /// highest-priority applicable action.
    pub fn evaluate_key(&self, key: &str, metadata: &KeyMetadata) -> LifecycleAction {
        // Collect matching policies sorted by priority (descending).
        let mut matches: Vec<_> = self
            .policies
            .iter()
            .filter(|e| {
                let p = &e.value().policy;
                p.enabled && namespace_matches(&p.namespace_pattern, key)
            })
            .map(|e| {
                let v = e.value();
                (v.id.clone(), v.policy.clone())
            })
            .collect();

        matches.sort_by(|a, b| b.1.priority.cmp(&a.1.priority));

        for (_id, policy) in &matches {
            // Update match counter
            if let Some(entry) = self.policies.get(&_id.0) {
                entry.value().keys_matched.fetch_add(1, Ordering::Relaxed);
                *entry.value().last_evaluated.write() = Some(Utc::now());
            }

            if let Some(action) = self.evaluate_policy(policy, metadata) {
                return action;
            }
        }

        LifecycleAction::NoAction
    }

    /// Evaluate a single policy's stages against the key metadata.
    fn evaluate_policy(
        &self,
        policy: &LifecyclePolicy,
        metadata: &KeyMetadata,
    ) -> Option<LifecycleAction> {
        // Walk stages in order; find the first stage whose tier is *colder*
        // than the key's current tier and whose trigger fires.
        for stage in &policy.stages {
            // Only consider transitions to colder tiers (or delete).
            if stage.tier <= metadata.current_tier && stage.tier != LifecycleTier::Delete {
                continue;
            }

            if !stage.trigger.evaluate(metadata) {
                continue;
            }

            // Check config-level gates
            if stage.tier == LifecycleTier::Archive && !self.config.enable_archival {
                continue;
            }
            if stage.tier == LifecycleTier::Delete && !self.config.enable_auto_delete {
                continue;
            }

            return Some(self.stage_to_action(stage));
        }
        None
    }

    /// Convert a lifecycle stage into a concrete action.
    fn stage_to_action(&self, stage: &LifecycleStage) -> LifecycleAction {
        match &stage.action {
            StageAction::Migrate => LifecycleAction::MoveToTier(stage.tier),
            StageAction::Compress => LifecycleAction::Compress,
            StageAction::Archive { destination } => LifecycleAction::Archive {
                destination: destination.clone(),
            },
            StageAction::Delete => LifecycleAction::Delete,
            StageAction::Notify { .. } => LifecycleAction::NoAction,
        }
    }

    // -- Cycle execution ----------------------------------------------------

    /// Execute a full evaluation cycle over a set of tracked keys.
    ///
    /// In a real deployment this would iterate over the storage engine; here
    /// we expose it as a method that callers feed with key metadata.
    pub fn execute_cycle(&self) -> CycleResult {
        self.execute_cycle_with_keys(&[])
    }

    /// Execute a lifecycle cycle for the provided keys.
    pub fn execute_cycle_with_keys(&self, keys: &[KeyMetadata]) -> CycleResult {
        let start = Instant::now();
        let mut actions_taken: Vec<ActionTaken> = Vec::new();
        let errors: Vec<String> = Vec::new();
        let mut transitions: usize = 0;
        let mut cost_savings = 0.0_f64;

        for meta in keys {
            if transitions >= self.config.max_transitions_per_cycle {
                break;
            }

            let action = self.evaluate_key(&meta.key, meta);
            if action == LifecycleAction::NoAction {
                continue;
            }

            // Estimate per-key cost saving
            let saving = self.estimate_saving(meta, &action);

            if !self.config.dry_run {
                // In a real implementation we would perform the migration here.
                self.record_action(&action, meta);
            }

            actions_taken.push(ActionTaken {
                key_pattern: meta.key.clone(),
                action,
                keys_affected: 1,
                bytes_affected: meta.size_bytes,
            });

            cost_savings += saving;
            transitions += 1;
        }

        self.total_cycles.fetch_add(1, Ordering::Relaxed);
        if !self.config.dry_run {
            let mut cs = self.cost_savings.write();
            *cs += cost_savings;
        }

        CycleResult {
            keys_evaluated: keys.len() as u64,
            actions_taken,
            errors,
            duration_ms: start.elapsed().as_millis() as u64,
            cost_savings_usd: cost_savings,
        }
    }

    /// Record stats for an executed action.
    fn record_action(&self, action: &LifecycleAction, meta: &KeyMetadata) {
        match action {
            LifecycleAction::MoveToTier(_) => {
                self.keys_migrated.fetch_add(1, Ordering::Relaxed);
                self.bytes_saved.fetch_add(0, Ordering::Relaxed);
            }
            LifecycleAction::Archive { .. } => {
                self.keys_archived.fetch_add(1, Ordering::Relaxed);
                self.bytes_saved
                    .fetch_add(meta.size_bytes, Ordering::Relaxed);
            }
            LifecycleAction::Delete => {
                self.keys_deleted.fetch_add(1, Ordering::Relaxed);
                self.bytes_saved
                    .fetch_add(meta.size_bytes, Ordering::Relaxed);
            }
            _ => {}
        }
    }

    /// Rough per-key monthly saving estimate when moving to a colder tier.
    fn estimate_saving(&self, meta: &KeyMetadata, action: &LifecycleAction) -> f64 {
        let size_gb = meta.size_bytes as f64 / (1024.0 * 1024.0 * 1024.0);
        let current_cost = meta.current_tier.cost_per_gb() * size_gb;
        let new_cost = match action {
            LifecycleAction::MoveToTier(t) => t.cost_per_gb() * size_gb,
            LifecycleAction::Archive { .. } => LifecycleTier::Archive.cost_per_gb() * size_gb,
            LifecycleAction::Delete => 0.0,
            _ => current_cost,
        };
        (current_cost - new_cost).max(0.0)
    }

    // -- Cost projection ----------------------------------------------------

    /// Project cost savings over a number of days using current policy set
    /// and the supplied key metadata snapshot.
    pub fn get_cost_projection(&self, days: u32) -> CostProjection {
        self.get_cost_projection_with_keys(days, &[])
    }

    /// Project costs with an explicit set of keys.
    pub fn get_cost_projection_with_keys(&self, days: u32, keys: &[KeyMetadata]) -> CostProjection {
        let months = days as f64 / 30.0;

        let mut current_cost = 0.0_f64;
        let mut projected_cost = 0.0_f64;
        let mut breakdown_map: std::collections::HashMap<LifecycleTier, (u64, u64, f64)> =
            std::collections::HashMap::new();

        for meta in keys {
            let size_gb = meta.size_bytes as f64 / (1024.0 * 1024.0 * 1024.0);
            let cur = meta.current_tier.cost_per_gb() * size_gb * months;
            current_cost += cur;

            let action = self.evaluate_key(&meta.key, meta);
            let target_tier = match &action {
                LifecycleAction::MoveToTier(t) => *t,
                LifecycleAction::Archive { .. } => LifecycleTier::Archive,
                LifecycleAction::Delete => LifecycleTier::Delete,
                _ => meta.current_tier,
            };

            let proj = target_tier.cost_per_gb() * size_gb * months;
            projected_cost += proj;

            let entry = breakdown_map.entry(target_tier).or_insert((0, 0, 0.0));
            entry.0 += 1;
            entry.1 += meta.size_bytes;
            entry.2 += proj;
        }

        let savings = current_cost - projected_cost;
        let savings_percent = if current_cost > 0.0 {
            (savings / current_cost) * 100.0
        } else {
            0.0
        };

        let breakdown = breakdown_map
            .into_iter()
            .map(|(tier, (k, b, c))| TierCostBreakdown {
                tier,
                keys: k,
                bytes: b,
                monthly_cost_usd: c,
            })
            .collect();

        CostProjection {
            current_monthly_cost_usd: current_cost,
            projected_monthly_cost_usd: projected_cost,
            savings_usd: savings,
            savings_percent,
            breakdown,
        }
    }

    // -- Stats --------------------------------------------------------------

    /// Get aggregate lifecycle statistics.
    pub fn get_stats(&self) -> LifecycleStats {
        let total_policies = self.policies.len() as u64;
        let active_policies = self
            .policies
            .iter()
            .filter(|e| e.value().policy.enabled)
            .count() as u64;

        LifecycleStats {
            total_policies,
            active_policies,
            total_cycles: self.total_cycles.load(Ordering::Relaxed),
            keys_migrated: self.keys_migrated.load(Ordering::Relaxed),
            keys_archived: self.keys_archived.load(Ordering::Relaxed),
            keys_deleted: self.keys_deleted.load(Ordering::Relaxed),
            bytes_saved: self.bytes_saved.load(Ordering::Relaxed),
            total_cost_savings_usd: *self.cost_savings.read(),
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Simple glob-style pattern matching supporting `*` as a wildcard.
fn namespace_matches(pattern: &str, key: &str) -> bool {
    if pattern == "*" {
        return true;
    }

    let parts: Vec<&str> = pattern.split('*').collect();
    if parts.len() == 1 {
        // No wildcard — exact match
        return pattern == key;
    }

    let mut pos = 0usize;
    for (i, part) in parts.iter().enumerate() {
        if part.is_empty() {
            continue;
        }
        if i == 0 {
            // Must match at start
            if !key.starts_with(part) {
                return false;
            }
            pos = part.len();
        } else if i == parts.len() - 1 {
            // Must match at end
            if !key[pos..].ends_with(part) {
                return false;
            }
            pos = key.len();
        } else {
            // Must match somewhere after pos
            match key[pos..].find(part) {
                Some(idx) => pos += idx + part.len(),
                None => return false,
            }
        }
    }

    true
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;

    // -- helpers ------------------------------------------------------------

    fn default_manager() -> LifecycleManager {
        LifecycleManager::new(LifecycleConfig {
            enable_auto_delete: true,
            ..LifecycleConfig::default()
        })
    }

    fn make_metadata(
        key: &str,
        tier: LifecycleTier,
        age_days: i64,
        access_count: u64,
    ) -> KeyMetadata {
        let now = Utc::now();
        KeyMetadata {
            key: key.to_string(),
            size_bytes: 1024,
            created_at: now - Duration::days(age_days),
            last_accessed: now - Duration::hours(age_days * 2),
            access_count,
            current_tier: tier,
            ttl_remaining_ms: None,
        }
    }

    fn age_policy(name: &str, pattern: &str, priority: u32) -> LifecyclePolicy {
        LifecyclePolicy {
            name: name.to_string(),
            namespace_pattern: pattern.to_string(),
            stages: vec![
                LifecycleStage {
                    tier: LifecycleTier::Warm,
                    trigger: StageTrigger {
                        trigger_type: TriggerType::AgeDays,
                        threshold: 7.0,
                    },
                    action: StageAction::Migrate,
                    retention_days: Some(30),
                },
                LifecycleStage {
                    tier: LifecycleTier::Cold,
                    trigger: StageTrigger {
                        trigger_type: TriggerType::AgeDays,
                        threshold: 30.0,
                    },
                    action: StageAction::Migrate,
                    retention_days: Some(90),
                },
                LifecycleStage {
                    tier: LifecycleTier::Archive,
                    trigger: StageTrigger {
                        trigger_type: TriggerType::AgeDays,
                        threshold: 90.0,
                    },
                    action: StageAction::Archive {
                        destination: "s3://archive-bucket".into(),
                    },
                    retention_days: None,
                },
                LifecycleStage {
                    tier: LifecycleTier::Delete,
                    trigger: StageTrigger {
                        trigger_type: TriggerType::AgeDays,
                        threshold: 365.0,
                    },
                    action: StageAction::Delete,
                    retention_days: None,
                },
            ],
            priority,
            enabled: true,
            description: Some("Standard age-based lifecycle".into()),
        }
    }

    // -- policy management --------------------------------------------------

    #[test]
    fn test_add_and_list_policies() {
        let mgr = default_manager();
        let _id = mgr.add_policy(age_policy("p1", "session:*", 10)).unwrap();
        let _ = mgr.add_policy(age_policy("p2", "cache:*", 20)).unwrap();

        let list = mgr.list_policies();
        assert_eq!(list.len(), 2);
        // Higher priority first
        assert_eq!(list[0].name, "p2");
        assert_eq!(list[1].name, "p1");
    }

    #[test]
    fn test_remove_policy() {
        let mgr = default_manager();
        let id = mgr.add_policy(age_policy("p1", "session:*", 10)).unwrap();
        assert_eq!(mgr.list_policies().len(), 1);
        mgr.remove_policy(&id).unwrap();
        assert_eq!(mgr.list_policies().len(), 0);
    }

    #[test]
    fn test_remove_nonexistent_policy() {
        let mgr = default_manager();
        let err = mgr
            .remove_policy(&PolicyId("nonexistent".into()))
            .unwrap_err();
        assert_eq!(err, LifecycleError::PolicyNotFound);
    }

    #[test]
    fn test_duplicate_policy_name() {
        let mgr = default_manager();
        mgr.add_policy(age_policy("p1", "session:*", 10)).unwrap();
        let err = mgr.add_policy(age_policy("p1", "cache:*", 20)).unwrap_err();
        assert!(matches!(err, LifecycleError::PolicyAlreadyExists(_)));
    }

    #[test]
    fn test_invalid_policy_empty_name() {
        let mgr = default_manager();
        let mut policy = age_policy("", "session:*", 10);
        policy.name = String::new();
        let err = mgr.add_policy(policy).unwrap_err();
        assert!(matches!(err, LifecycleError::InvalidPolicy(_)));
    }

    #[test]
    fn test_invalid_policy_no_stages() {
        let mgr = default_manager();
        let policy = LifecyclePolicy {
            name: "empty".into(),
            namespace_pattern: "test:*".into(),
            stages: vec![],
            priority: 1,
            enabled: true,
            description: None,
        };
        let err = mgr.add_policy(policy).unwrap_err();
        assert!(matches!(err, LifecycleError::InvalidPolicy(_)));
    }

    #[test]
    fn test_invalid_policy_empty_pattern() {
        let mgr = default_manager();
        let mut policy = age_policy("p1", "", 10);
        policy.namespace_pattern = String::new();
        let err = mgr.add_policy(policy).unwrap_err();
        assert!(matches!(err, LifecycleError::InvalidPolicy(_)));
    }

    // -- key evaluation: age trigger ----------------------------------------

    #[test]
    fn test_evaluate_key_young_key_no_action() {
        let mgr = default_manager();
        mgr.add_policy(age_policy("p1", "session:*", 10)).unwrap();

        let meta = make_metadata("session:abc", LifecycleTier::Hot, 1, 100);
        assert_eq!(
            mgr.evaluate_key("session:abc", &meta),
            LifecycleAction::NoAction
        );
    }

    #[test]
    fn test_evaluate_key_age_triggers_warm() {
        let mgr = default_manager();
        mgr.add_policy(age_policy("p1", "session:*", 10)).unwrap();

        let meta = make_metadata("session:abc", LifecycleTier::Hot, 10, 100);
        assert_eq!(
            mgr.evaluate_key("session:abc", &meta),
            LifecycleAction::MoveToTier(LifecycleTier::Warm)
        );
    }

    #[test]
    fn test_evaluate_key_age_triggers_cold() {
        let mgr = default_manager();
        mgr.add_policy(age_policy("p1", "session:*", 10)).unwrap();

        let meta = make_metadata("session:abc", LifecycleTier::Warm, 35, 50);
        assert_eq!(
            mgr.evaluate_key("session:abc", &meta),
            LifecycleAction::MoveToTier(LifecycleTier::Cold)
        );
    }

    #[test]
    fn test_evaluate_key_age_triggers_archive() {
        let mgr = default_manager();
        mgr.add_policy(age_policy("p1", "session:*", 10)).unwrap();

        let meta = make_metadata("session:abc", LifecycleTier::Cold, 100, 10);
        assert_eq!(
            mgr.evaluate_key("session:abc", &meta),
            LifecycleAction::Archive {
                destination: "s3://archive-bucket".into()
            }
        );
    }

    #[test]
    fn test_evaluate_key_age_triggers_delete() {
        let mgr = default_manager();
        mgr.add_policy(age_policy("p1", "session:*", 10)).unwrap();

        let meta = make_metadata("session:abc", LifecycleTier::Archive, 400, 0);
        assert_eq!(
            mgr.evaluate_key("session:abc", &meta),
            LifecycleAction::Delete
        );
    }

    // -- key evaluation: frequency trigger ----------------------------------

    #[test]
    fn test_evaluate_key_frequency_trigger() {
        let mgr = default_manager();
        let policy = LifecyclePolicy {
            name: "freq".into(),
            namespace_pattern: "cache:*".into(),
            stages: vec![LifecycleStage {
                tier: LifecycleTier::Cold,
                trigger: StageTrigger {
                    trigger_type: TriggerType::AccessFrequency,
                    threshold: 1.0, // less than 1 access/day → cold
                },
                action: StageAction::Migrate,
                retention_days: None,
            }],
            priority: 10,
            enabled: true,
            description: None,
        };
        mgr.add_policy(policy).unwrap();

        // 2 accesses over 30 days = 0.067/day → below threshold
        let meta = make_metadata("cache:user:1", LifecycleTier::Hot, 30, 2);
        assert_eq!(
            mgr.evaluate_key("cache:user:1", &meta),
            LifecycleAction::MoveToTier(LifecycleTier::Cold)
        );
    }

    #[test]
    fn test_evaluate_key_frequency_high_no_action() {
        let mgr = default_manager();
        let policy = LifecyclePolicy {
            name: "freq".into(),
            namespace_pattern: "cache:*".into(),
            stages: vec![LifecycleStage {
                tier: LifecycleTier::Cold,
                trigger: StageTrigger {
                    trigger_type: TriggerType::AccessFrequency,
                    threshold: 1.0,
                },
                action: StageAction::Migrate,
                retention_days: None,
            }],
            priority: 10,
            enabled: true,
            description: None,
        };
        mgr.add_policy(policy).unwrap();

        // 100 accesses over 10 days = 10/day → above threshold → no action
        let meta = make_metadata("cache:user:1", LifecycleTier::Hot, 10, 100);
        assert_eq!(
            mgr.evaluate_key("cache:user:1", &meta),
            LifecycleAction::NoAction
        );
    }

    // -- key evaluation: size trigger ---------------------------------------

    #[test]
    fn test_evaluate_key_size_trigger() {
        let mgr = default_manager();
        let policy = LifecyclePolicy {
            name: "large-blobs".into(),
            namespace_pattern: "blob:*".into(),
            stages: vec![LifecycleStage {
                tier: LifecycleTier::Cold,
                trigger: StageTrigger {
                    trigger_type: TriggerType::SizeBytes,
                    threshold: 1_000_000.0, // 1 MB
                },
                action: StageAction::Compress,
                retention_days: None,
            }],
            priority: 5,
            enabled: true,
            description: None,
        };
        mgr.add_policy(policy).unwrap();

        let now = Utc::now();
        let meta = KeyMetadata {
            key: "blob:img:42".into(),
            size_bytes: 5_000_000,
            created_at: now - Duration::days(1),
            last_accessed: now,
            access_count: 10,
            current_tier: LifecycleTier::Hot,
            ttl_remaining_ms: None,
        };
        assert_eq!(
            mgr.evaluate_key("blob:img:42", &meta),
            LifecycleAction::Compress
        );
    }

    // -- key evaluation: idle time trigger -----------------------------------

    #[test]
    fn test_evaluate_key_idle_trigger() {
        let mgr = default_manager();
        let policy = LifecyclePolicy {
            name: "idle".into(),
            namespace_pattern: "temp:*".into(),
            stages: vec![LifecycleStage {
                tier: LifecycleTier::Delete,
                trigger: StageTrigger {
                    trigger_type: TriggerType::IdleTimeSecs,
                    threshold: 3600.0, // 1 hour idle → delete
                },
                action: StageAction::Delete,
                retention_days: None,
            }],
            priority: 100,
            enabled: true,
            description: None,
        };
        mgr.add_policy(policy).unwrap();

        let now = Utc::now();
        let meta = KeyMetadata {
            key: "temp:work:99".into(),
            size_bytes: 256,
            created_at: now - Duration::hours(5),
            last_accessed: now - Duration::hours(2),
            access_count: 3,
            current_tier: LifecycleTier::Hot,
            ttl_remaining_ms: None,
        };
        assert_eq!(
            mgr.evaluate_key("temp:work:99", &meta),
            LifecycleAction::Delete
        );
    }

    // -- namespace matching -------------------------------------------------

    #[test]
    fn test_namespace_exact_match() {
        assert!(namespace_matches("hello", "hello"));
        assert!(!namespace_matches("hello", "world"));
    }

    #[test]
    fn test_namespace_wildcard_prefix() {
        assert!(namespace_matches("session:*", "session:abc"));
        assert!(namespace_matches("session:*", "session:"));
        assert!(!namespace_matches("session:*", "cache:abc"));
    }

    #[test]
    fn test_namespace_wildcard_middle() {
        assert!(namespace_matches("user:*:profile", "user:123:profile"));
        assert!(!namespace_matches("user:*:profile", "user:123:settings"));
    }

    #[test]
    fn test_namespace_wildcard_all() {
        assert!(namespace_matches("*", "anything"));
        assert!(namespace_matches("*", ""));
    }

    #[test]
    fn test_unmatched_namespace_no_action() {
        let mgr = default_manager();
        mgr.add_policy(age_policy("p1", "session:*", 10)).unwrap();

        let meta = make_metadata("other:abc", LifecycleTier::Hot, 100, 0);
        assert_eq!(
            mgr.evaluate_key("other:abc", &meta),
            LifecycleAction::NoAction
        );
    }

    // -- priority ordering --------------------------------------------------

    #[test]
    fn test_priority_ordering() {
        let mgr = default_manager();

        // Low-priority policy: migrate to warm at 7 days
        let low = LifecyclePolicy {
            name: "low-prio".into(),
            namespace_pattern: "data:*".into(),
            stages: vec![LifecycleStage {
                tier: LifecycleTier::Warm,
                trigger: StageTrigger {
                    trigger_type: TriggerType::AgeDays,
                    threshold: 7.0,
                },
                action: StageAction::Migrate,
                retention_days: None,
            }],
            priority: 1,
            enabled: true,
            description: None,
        };

        // High-priority policy: delete at 7 days
        let high = LifecyclePolicy {
            name: "high-prio".into(),
            namespace_pattern: "data:*".into(),
            stages: vec![LifecycleStage {
                tier: LifecycleTier::Delete,
                trigger: StageTrigger {
                    trigger_type: TriggerType::AgeDays,
                    threshold: 7.0,
                },
                action: StageAction::Delete,
                retention_days: None,
            }],
            priority: 100,
            enabled: true,
            description: None,
        };

        mgr.add_policy(low).unwrap();
        mgr.add_policy(high).unwrap();

        let meta = make_metadata("data:x", LifecycleTier::Hot, 10, 5);
        // Should get the high-priority policy action
        assert_eq!(mgr.evaluate_key("data:x", &meta), LifecycleAction::Delete);
    }

    // -- disabled policy ----------------------------------------------------

    #[test]
    fn test_disabled_policy_ignored() {
        let mgr = default_manager();
        let mut policy = age_policy("disabled", "session:*", 10);
        policy.enabled = false;
        mgr.add_policy(policy).unwrap();

        let meta = make_metadata("session:abc", LifecycleTier::Hot, 100, 0);
        assert_eq!(
            mgr.evaluate_key("session:abc", &meta),
            LifecycleAction::NoAction
        );
    }

    // -- cycle execution ----------------------------------------------------

    #[test]
    fn test_execute_cycle_empty() {
        let mgr = default_manager();
        let result = mgr.execute_cycle();
        assert_eq!(result.keys_evaluated, 0);
        assert!(result.actions_taken.is_empty());
    }

    #[test]
    fn test_execute_cycle_with_keys() {
        let mgr = default_manager();
        mgr.add_policy(age_policy("p1", "session:*", 10)).unwrap();

        let keys = vec![
            make_metadata("session:a", LifecycleTier::Hot, 1, 100), // young → no action
            make_metadata("session:b", LifecycleTier::Hot, 10, 50), // 10 days → warm
            make_metadata("session:c", LifecycleTier::Warm, 35, 20), // 35 days → cold
        ];

        let result = mgr.execute_cycle_with_keys(&keys);
        assert_eq!(result.keys_evaluated, 3);
        assert_eq!(result.actions_taken.len(), 2);
        assert!(result.errors.is_empty());

        let stats = mgr.get_stats();
        assert_eq!(stats.total_cycles, 1);
        assert_eq!(stats.keys_migrated, 2);
    }

    #[test]
    fn test_execute_cycle_max_transitions() {
        let mgr = LifecycleManager::new(LifecycleConfig {
            max_transitions_per_cycle: 1,
            enable_auto_delete: true,
            ..LifecycleConfig::default()
        });
        mgr.add_policy(age_policy("p1", "session:*", 10)).unwrap();

        let keys = vec![
            make_metadata("session:a", LifecycleTier::Hot, 10, 50),
            make_metadata("session:b", LifecycleTier::Hot, 10, 50),
        ];

        let result = mgr.execute_cycle_with_keys(&keys);
        // Only 1 transition allowed
        assert_eq!(result.actions_taken.len(), 1);
    }

    // -- dry run mode -------------------------------------------------------

    #[test]
    fn test_dry_run_mode() {
        let mgr = LifecycleManager::new(LifecycleConfig {
            dry_run: true,
            enable_auto_delete: true,
            ..LifecycleConfig::default()
        });
        mgr.add_policy(age_policy("p1", "session:*", 10)).unwrap();

        let keys = vec![make_metadata("session:a", LifecycleTier::Hot, 10, 50)];

        let result = mgr.execute_cycle_with_keys(&keys);
        // Actions are planned but stats should NOT be updated
        assert_eq!(result.actions_taken.len(), 1);

        let stats = mgr.get_stats();
        assert_eq!(stats.keys_migrated, 0);
        assert_eq!(stats.total_cycles, 1);
    }

    // -- archival / delete config gates -------------------------------------

    #[test]
    fn test_archival_disabled() {
        let mgr = LifecycleManager::new(LifecycleConfig {
            enable_archival: false,
            enable_auto_delete: true,
            ..LifecycleConfig::default()
        });
        mgr.add_policy(age_policy("p1", "session:*", 10)).unwrap();

        // 100 days old, in Cold → would trigger archive but archival is disabled
        let meta = make_metadata("session:x", LifecycleTier::Cold, 100, 0);
        let action = mgr.evaluate_key("session:x", &meta);
        // Should skip archive and go to delete (365+ days check won't fire)
        // Since the key is only 100 days old, delete threshold (365) won't fire either
        assert_eq!(action, LifecycleAction::NoAction);
    }

    #[test]
    fn test_auto_delete_disabled() {
        let mgr = LifecycleManager::new(LifecycleConfig {
            enable_auto_delete: false,
            ..LifecycleConfig::default()
        });
        mgr.add_policy(age_policy("p1", "session:*", 10)).unwrap();

        // 400 days old, in Archive → would trigger delete but auto-delete disabled
        let meta = make_metadata("session:x", LifecycleTier::Archive, 400, 0);
        let action = mgr.evaluate_key("session:x", &meta);
        assert_eq!(action, LifecycleAction::NoAction);
    }

    // -- cost projection ----------------------------------------------------

    #[test]
    fn test_cost_projection_empty() {
        let mgr = default_manager();
        let proj = mgr.get_cost_projection(30);
        assert_eq!(proj.current_monthly_cost_usd, 0.0);
        assert_eq!(proj.projected_monthly_cost_usd, 0.0);
        assert_eq!(proj.savings_usd, 0.0);
    }

    #[test]
    fn test_cost_projection_with_keys() {
        let mgr = default_manager();
        mgr.add_policy(age_policy("p1", "data:*", 10)).unwrap();

        let keys = vec![
            make_metadata("data:a", LifecycleTier::Hot, 10, 5), // → warm
        ];

        let proj = mgr.get_cost_projection_with_keys(30, &keys);
        // Hot costs more than Warm, so savings should be positive
        assert!(proj.savings_usd >= 0.0);
        assert!(proj.savings_percent >= 0.0);
        assert!(!proj.breakdown.is_empty());
    }

    // -- stats tracking -----------------------------------------------------

    #[test]
    fn test_stats_initial() {
        let mgr = default_manager();
        let stats = mgr.get_stats();
        assert_eq!(stats.total_policies, 0);
        assert_eq!(stats.active_policies, 0);
        assert_eq!(stats.total_cycles, 0);
        assert_eq!(stats.keys_migrated, 0);
    }

    #[test]
    fn test_stats_after_operations() {
        let mgr = default_manager();
        mgr.add_policy(age_policy("p1", "session:*", 10)).unwrap();

        let mut disabled = age_policy("p2", "cache:*", 5);
        disabled.enabled = false;
        mgr.add_policy(disabled).unwrap();

        let stats = mgr.get_stats();
        assert_eq!(stats.total_policies, 2);
        assert_eq!(stats.active_policies, 1);
    }

    #[test]
    fn test_stats_after_cycle() {
        let mgr = default_manager();
        mgr.add_policy(age_policy("p1", "session:*", 10)).unwrap();

        let keys = vec![
            make_metadata("session:a", LifecycleTier::Hot, 10, 50),
            make_metadata("session:b", LifecycleTier::Archive, 400, 0),
        ];

        mgr.execute_cycle_with_keys(&keys);
        let stats = mgr.get_stats();
        assert_eq!(stats.total_cycles, 1);
        assert_eq!(stats.keys_migrated, 1); // session:a → warm
        assert_eq!(stats.keys_deleted, 1); // session:b → delete
    }

    // -- lifecycle stage transitions ----------------------------------------

    #[test]
    fn test_full_lifecycle_progression() {
        let mgr = default_manager();
        mgr.add_policy(age_policy("p1", "key:*", 10)).unwrap();

        // Hot → Warm (10 days)
        let meta = make_metadata("key:1", LifecycleTier::Hot, 10, 50);
        assert_eq!(
            mgr.evaluate_key("key:1", &meta),
            LifecycleAction::MoveToTier(LifecycleTier::Warm)
        );

        // Warm → Cold (35 days)
        let meta = make_metadata("key:1", LifecycleTier::Warm, 35, 30);
        assert_eq!(
            mgr.evaluate_key("key:1", &meta),
            LifecycleAction::MoveToTier(LifecycleTier::Cold)
        );

        // Cold → Archive (100 days)
        let meta = make_metadata("key:1", LifecycleTier::Cold, 100, 10);
        assert_eq!(
            mgr.evaluate_key("key:1", &meta),
            LifecycleAction::Archive {
                destination: "s3://archive-bucket".into()
            }
        );

        // Archive → Delete (400 days)
        let meta = make_metadata("key:1", LifecycleTier::Archive, 400, 0);
        assert_eq!(mgr.evaluate_key("key:1", &meta), LifecycleAction::Delete);
    }

    // -- PolicyId -----------------------------------------------------------

    #[test]
    fn test_policy_id_uniqueness() {
        let a = PolicyId::new();
        let b = PolicyId::new();
        assert_ne!(a, b);
    }

    #[test]
    fn test_policy_id_display() {
        let id = PolicyId("abc-123".into());
        assert_eq!(format!("{}", id), "abc-123");
    }

    // -- StageTrigger evaluate edge cases -----------------------------------

    #[test]
    fn test_manual_trigger() {
        let trigger = StageTrigger {
            trigger_type: TriggerType::Manual,
            threshold: 0.0,
        };
        let meta = make_metadata("x", LifecycleTier::Hot, 1, 1);
        assert!(trigger.evaluate(&meta));

        let trigger_nonzero = StageTrigger {
            trigger_type: TriggerType::Manual,
            threshold: 1.0,
        };
        assert!(!trigger_nonzero.evaluate(&meta));
    }

    // -- LifecycleTier ordering ---------------------------------------------

    #[test]
    fn test_tier_ordering() {
        assert!(LifecycleTier::Hot < LifecycleTier::Warm);
        assert!(LifecycleTier::Warm < LifecycleTier::Cold);
        assert!(LifecycleTier::Cold < LifecycleTier::Archive);
        assert!(LifecycleTier::Archive < LifecycleTier::Delete);
    }

    #[test]
    fn test_tier_cost_ordering() {
        assert!(LifecycleTier::Hot.cost_per_gb() > LifecycleTier::Warm.cost_per_gb());
        assert!(LifecycleTier::Warm.cost_per_gb() > LifecycleTier::Cold.cost_per_gb());
        assert!(LifecycleTier::Cold.cost_per_gb() > LifecycleTier::Archive.cost_per_gb());
        assert_eq!(LifecycleTier::Delete.cost_per_gb(), 0.0);
    }

    // -- LifecycleConfig default --------------------------------------------

    #[test]
    fn test_config_defaults() {
        let cfg = LifecycleConfig::default();
        assert_eq!(
            cfg.evaluation_interval_secs,
            DEFAULT_EVALUATION_INTERVAL_SECS
        );
        assert_eq!(cfg.max_transitions_per_cycle, DEFAULT_MAX_TRANSITIONS);
        assert!(!cfg.dry_run);
        assert!(cfg.enable_archival);
        assert!(!cfg.enable_auto_delete);
        assert!(cfg.cost_optimization_enabled);
    }
}
