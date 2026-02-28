//! Real-Time Feature Store
//!
//! ML feature serving with point-in-time correctness, versioning,
//! and automatic drift detection.
#![allow(dead_code)]

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for the feature store.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeatureStoreConfig {
    /// Maximum number of feature definitions.
    pub max_features: usize,
    /// Maximum number of unique entities tracked.
    pub max_entities: usize,
    /// Enable automatic drift detection.
    pub enable_drift_detection: bool,
    /// Time window for drift comparison.
    pub drift_window: Duration,
    /// Maximum acceptable freshness SLA.
    pub freshness_sla: Duration,
}

impl Default for FeatureStoreConfig {
    fn default() -> Self {
        Self {
            max_features: 10_000,
            max_entities: 1_000_000,
            enable_drift_detection: true,
            drift_window: Duration::from_secs(3600),
            freshness_sla: Duration::from_secs(5),
        }
    }
}

// ---------------------------------------------------------------------------
// Feature types
// ---------------------------------------------------------------------------

/// Value type for a feature.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum FeatureValueType {
    /// Numeric (f64) feature.
    Numeric,
    /// Categorical (string) feature.
    Categorical,
    /// Fixed-size vector feature.
    Vector(usize),
    /// Boolean feature.
    Boolean,
    /// Free-form text feature.
    Text,
    /// Timestamp feature.
    Timestamp,
}

impl std::fmt::Display for FeatureValueType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Numeric => write!(f, "Numeric"),
            Self::Categorical => write!(f, "Categorical"),
            Self::Vector(n) => write!(f, "Vector({})", n),
            Self::Boolean => write!(f, "Boolean"),
            Self::Text => write!(f, "Text"),
            Self::Timestamp => write!(f, "Timestamp"),
        }
    }
}

/// Definition of a feature in the store.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeatureDefinition {
    /// Feature name (unique identifier).
    pub name: String,
    /// The entity type this feature belongs to (e.g. "user", "product").
    pub entity_type: String,
    /// Expected value type.
    pub value_type: FeatureValueType,
    /// Data source identifier.
    pub source: String,
    /// Feature-level freshness SLA.
    pub freshness_sla: Duration,
    /// Human-readable description.
    pub description: String,
    /// Searchable tags.
    pub tags: Vec<String>,
    /// Creation timestamp (unix seconds).
    pub created_at: u64,
    /// Schema version.
    pub version: u32,
}

/// A stored feature value with metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeatureValue {
    /// The serialized value.
    pub value: serde_json::Value,
    /// Timestamp when the value was written (unix seconds).
    pub timestamp: u64,
    /// Version at write time.
    pub version: u32,
    /// Source that produced this value.
    pub source: String,
}

// ---------------------------------------------------------------------------
// Freshness / Drift
// ---------------------------------------------------------------------------

/// Freshness status of a feature value.
#[derive(Debug, Clone, PartialEq)]
pub enum FreshnessStatus {
    /// The value is within the freshness SLA.
    Fresh,
    /// The value is stale by the given duration.
    Stale(Duration),
    /// No value exists for this entity/feature.
    Unknown,
}

/// Report of drift detection for a feature.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DriftReport {
    /// Feature name.
    pub feature: String,
    /// Drift score (simplified z-score).
    pub drift_score: f64,
    /// Whether drift was detected (score > 2.0).
    pub is_drifted: bool,
    /// Baseline mean.
    pub baseline_mean: f64,
    /// Current window mean.
    pub current_mean: f64,
    /// Baseline standard deviation.
    pub baseline_stddev: f64,
    /// Current window standard deviation.
    pub current_stddev: f64,
    /// Number of samples used.
    pub samples: u64,
}

/// Summary information about a feature.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeatureInfo {
    /// Feature definition.
    pub definition: FeatureDefinition,
    /// Number of entities with a value for this feature.
    pub entity_count: usize,
    /// Most recent update timestamp.
    pub latest_update: Option<u64>,
    /// Average freshness in milliseconds.
    pub avg_freshness_ms: f64,
}

/// Aggregate statistics for the feature store.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeatureStoreStats {
    /// Number of defined features.
    pub features_defined: usize,
    /// Number of unique entities.
    pub entities: usize,
    /// Total stored values.
    pub total_values: u64,
    /// Number of stale values.
    pub stale_values: u64,
    /// Number of drift alerts.
    pub drift_alerts: u64,
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Errors returned by the feature store.
#[derive(Debug, thiserror::Error)]
pub enum FeatureStoreError {
    /// Feature has not been defined.
    #[error("feature '{0}' is not defined")]
    FeatureNotDefined(String),
    /// Feature already exists.
    #[error("feature '{0}' already exists")]
    FeatureExists(String),
    /// Entity was not found.
    #[error("entity '{0}' not found")]
    EntityNotFound(String),
    /// Value type does not match the feature definition.
    #[error("type mismatch for feature '{0}'")]
    TypeMismatch(String),
    /// Feature freshness SLA exceeded.
    #[error("staleness exceeded for feature '{0}'")]
    StalenessExceeded(String),
    /// Maximum number of features reached.
    #[error("maximum number of features reached ({0})")]
    MaxFeatures(usize),
}

// ---------------------------------------------------------------------------
// Internal types
// ---------------------------------------------------------------------------

/// Per-entity storage: feature_name -> BTreeMap<timestamp, FeatureValue>.
/// The BTreeMap enables efficient point-in-time lookups.
type EntityStore = HashMap<String, BTreeMap<u64, FeatureValue>>;

struct InternalStats {
    total_values: AtomicU64,
    drift_alerts: AtomicU64,
}

impl InternalStats {
    fn new() -> Self {
        Self {
            total_values: AtomicU64::new(0),
            drift_alerts: AtomicU64::new(0),
        }
    }
}

// ---------------------------------------------------------------------------
// FeatureStore
// ---------------------------------------------------------------------------

/// Real-time feature store with point-in-time correctness and drift detection.
pub struct FeatureStore {
    config: FeatureStoreConfig,
    /// Feature definitions keyed by name.
    definitions: RwLock<HashMap<String, FeatureDefinition>>,
    /// Per-entity feature values: entity_id -> feature_name -> timeline.
    data: RwLock<HashMap<String, EntityStore>>,
    stats: InternalStats,
}

impl FeatureStore {
    /// Create a new feature store.
    pub fn new(config: FeatureStoreConfig) -> Self {
        Self {
            config,
            definitions: RwLock::new(HashMap::new()),
            data: RwLock::new(HashMap::new()),
            stats: InternalStats::new(),
        }
    }

    // -- helpers --

    fn now_secs() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0)
    }

    // -- public API --

    /// Define a new feature in the store.
    pub fn define_feature(&self, def: FeatureDefinition) -> Result<(), FeatureStoreError> {
        let mut defs = self.definitions.write();
        if defs.contains_key(&def.name) {
            return Err(FeatureStoreError::FeatureExists(def.name.clone()));
        }
        if defs.len() >= self.config.max_features {
            return Err(FeatureStoreError::MaxFeatures(self.config.max_features));
        }
        defs.insert(def.name.clone(), def);
        Ok(())
    }

    /// Set a feature value for an entity.
    pub fn set_feature(
        &self,
        entity_id: &str,
        feature: &str,
        value: serde_json::Value,
    ) -> Result<(), FeatureStoreError> {
        let defs = self.definitions.read();
        let def = defs
            .get(feature)
            .ok_or_else(|| FeatureStoreError::FeatureNotDefined(feature.to_string()))?;

        let ts = Self::now_secs();
        let fv = FeatureValue {
            value,
            timestamp: ts,
            version: def.version,
            source: def.source.clone(),
        };

        drop(defs);

        let mut data = self.data.write();
        let entity = data.entry(entity_id.to_string()).or_default();
        let timeline = entity.entry(feature.to_string()).or_default();
        timeline.insert(ts, fv);

        self.stats.total_values.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Get the latest value of a feature for an entity.
    pub fn get_feature(
        &self,
        entity_id: &str,
        feature: &str,
    ) -> Result<Option<FeatureValue>, FeatureStoreError> {
        let defs = self.definitions.read();
        if !defs.contains_key(feature) {
            return Err(FeatureStoreError::FeatureNotDefined(feature.to_string()));
        }
        drop(defs);

        let data = self.data.read();
        let result = data
            .get(entity_id)
            .and_then(|e| e.get(feature))
            .and_then(|timeline| timeline.values().last().cloned());
        Ok(result)
    }

    /// Get the value of a feature for an entity at a specific point in time.
    pub fn get_feature_at(
        &self,
        entity_id: &str,
        feature: &str,
        timestamp: u64,
    ) -> Result<Option<FeatureValue>, FeatureStoreError> {
        let defs = self.definitions.read();
        if !defs.contains_key(feature) {
            return Err(FeatureStoreError::FeatureNotDefined(feature.to_string()));
        }
        drop(defs);

        let data = self.data.read();
        let result = data
            .get(entity_id)
            .and_then(|e| e.get(feature))
            .and_then(|timeline| {
                // Find the latest entry at or before the requested timestamp.
                timeline
                    .range(..=timestamp)
                    .next_back()
                    .map(|(_, v)| v.clone())
            });
        Ok(result)
    }

    /// Batch-get multiple features for an entity.
    pub fn get_features(
        &self,
        entity_id: &str,
        features: &[&str],
    ) -> HashMap<String, Option<FeatureValue>> {
        let data = self.data.read();
        features
            .iter()
            .map(|&f| {
                let val = data
                    .get(entity_id)
                    .and_then(|e| e.get(f))
                    .and_then(|timeline| timeline.values().last().cloned());
                (f.to_string(), val)
            })
            .collect()
    }

    /// Check the freshness of a feature value for an entity.
    pub fn check_freshness(&self, entity_id: &str, feature: &str) -> FreshnessStatus {
        let data = self.data.read();
        let latest = data
            .get(entity_id)
            .and_then(|e| e.get(feature))
            .and_then(|timeline| timeline.values().last());

        match latest {
            Some(fv) => {
                let now = Self::now_secs();
                let age_secs = now.saturating_sub(fv.timestamp);
                let sla = self
                    .definitions
                    .read()
                    .get(feature)
                    .map(|d| d.freshness_sla)
                    .unwrap_or(self.config.freshness_sla);
                if age_secs <= sla.as_secs() {
                    FreshnessStatus::Fresh
                } else {
                    FreshnessStatus::Stale(Duration::from_secs(age_secs))
                }
            }
            None => FreshnessStatus::Unknown,
        }
    }

    /// Detect drift for a feature by comparing recent vs historical distribution.
    ///
    /// Drift score = |current_mean - baseline_mean| / baseline_stddev (z-score).
    /// Drift is flagged if score > 2.0.
    pub fn detect_drift(&self, feature: &str) -> DriftReport {
        let data = self.data.read();
        let now = Self::now_secs();
        let window_secs = self.config.drift_window.as_secs();

        let mut baseline_vals: Vec<f64> = Vec::new();
        let mut current_vals: Vec<f64> = Vec::new();

        for entity_store in data.values() {
            if let Some(timeline) = entity_store.get(feature) {
                for (ts, fv) in timeline.iter() {
                    if let Some(n) = fv.value.as_f64() {
                        if now.saturating_sub(*ts) > window_secs {
                            baseline_vals.push(n);
                        } else {
                            current_vals.push(n);
                        }
                    }
                }
            }
        }

        let (baseline_mean, baseline_stddev) = mean_stddev(&baseline_vals);
        let (current_mean, current_stddev) = mean_stddev(&current_vals);

        let drift_score = if baseline_stddev > 0.0 {
            ((current_mean - baseline_mean) / baseline_stddev).abs()
        } else if (current_mean - baseline_mean).abs() > f64::EPSILON {
            // No variance in baseline but means differ — flag as drifted.
            3.0
        } else {
            0.0
        };

        let is_drifted = drift_score > 2.0;
        if is_drifted {
            self.stats.drift_alerts.fetch_add(1, Ordering::Relaxed);
        }

        DriftReport {
            feature: feature.to_string(),
            drift_score,
            is_drifted,
            baseline_mean,
            current_mean,
            baseline_stddev,
            current_stddev,
            samples: (baseline_vals.len() + current_vals.len()) as u64,
        }
    }

    /// List all defined features.
    pub fn list_features(&self) -> Vec<FeatureDefinition> {
        self.definitions.read().values().cloned().collect()
    }

    /// Get information about a specific feature.
    pub fn feature_info(&self, name: &str) -> Option<FeatureInfo> {
        let defs = self.definitions.read();
        let def = defs.get(name)?.clone();
        drop(defs);

        let data = self.data.read();
        let now = Self::now_secs();
        let mut entity_count = 0usize;
        let mut latest_update: Option<u64> = None;
        let mut freshness_sum = 0u64;

        for entity_store in data.values() {
            if let Some(timeline) = entity_store.get(name) {
                if let Some((&ts, _)) = timeline.iter().next_back() {
                    entity_count += 1;
                    latest_update = Some(latest_update.map_or(ts, |prev: u64| prev.max(ts)));
                    freshness_sum += now.saturating_sub(ts);
                }
            }
        }

        let avg_freshness_ms = if entity_count > 0 {
            (freshness_sum as f64 / entity_count as f64) * 1000.0
        } else {
            0.0
        };

        Some(FeatureInfo {
            definition: def,
            entity_count,
            latest_update,
            avg_freshness_ms,
        })
    }

    /// Return all feature values for an entity.
    pub fn entity_features(&self, entity_id: &str) -> HashMap<String, FeatureValue> {
        let data = self.data.read();
        let mut result = HashMap::new();
        if let Some(entity_store) = data.get(entity_id) {
            for (feature, timeline) in entity_store.iter() {
                if let Some(val) = timeline.values().last() {
                    result.insert(feature.clone(), val.clone());
                }
            }
        }
        result
    }

    /// Return aggregate statistics.
    pub fn stats(&self) -> FeatureStoreStats {
        let defs = self.definitions.read();
        let data = self.data.read();
        let now = Self::now_secs();

        let mut stale_count = 0u64;
        for entity_store in data.values() {
            for (feature, timeline) in entity_store.iter() {
                if let Some(fv) = timeline.values().last() {
                    let sla = defs
                        .get(feature)
                        .map(|d| d.freshness_sla)
                        .unwrap_or(self.config.freshness_sla);
                    if now.saturating_sub(fv.timestamp) > sla.as_secs() {
                        stale_count += 1;
                    }
                }
            }
        }

        FeatureStoreStats {
            features_defined: defs.len(),
            entities: data.len(),
            total_values: self.stats.total_values.load(Ordering::Relaxed),
            stale_values: stale_count,
            drift_alerts: self.stats.drift_alerts.load(Ordering::Relaxed),
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn mean_stddev(vals: &[f64]) -> (f64, f64) {
    if vals.is_empty() {
        return (0.0, 0.0);
    }
    let n = vals.len() as f64;
    let mean = vals.iter().sum::<f64>() / n;
    let variance = vals.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / n;
    (mean, variance.sqrt())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn default_store() -> FeatureStore {
        FeatureStore::new(FeatureStoreConfig::default())
    }

    fn sample_def(name: &str) -> FeatureDefinition {
        FeatureDefinition {
            name: name.to_string(),
            entity_type: "user".to_string(),
            value_type: FeatureValueType::Numeric,
            source: "test".to_string(),
            freshness_sla: Duration::from_secs(60),
            description: "test feature".to_string(),
            tags: vec!["test".to_string()],
            created_at: 0,
            version: 1,
        }
    }

    #[test]
    fn test_define_feature() {
        let store = default_store();
        store.define_feature(sample_def("age")).expect("define");
        let list = store.list_features();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].name, "age");
    }

    #[test]
    fn test_define_duplicate() {
        let store = default_store();
        store.define_feature(sample_def("dup")).expect("first");
        let err = store.define_feature(sample_def("dup")).unwrap_err();
        assert!(matches!(err, FeatureStoreError::FeatureExists(_)));
    }

    #[test]
    fn test_set_and_get_feature() {
        let store = default_store();
        store.define_feature(sample_def("score")).expect("define");
        store
            .set_feature("user-1", "score", serde_json::json!(42.0))
            .expect("set");

        let val = store
            .get_feature("user-1", "score")
            .expect("get")
            .expect("some");
        assert_eq!(val.value, serde_json::json!(42.0));
        assert_eq!(val.version, 1);
    }

    #[test]
    fn test_get_undefined_feature() {
        let store = default_store();
        let err = store.get_feature("user-1", "nope").unwrap_err();
        assert!(matches!(err, FeatureStoreError::FeatureNotDefined(_)));
    }

    #[test]
    fn test_set_undefined_feature() {
        let store = default_store();
        let err = store
            .set_feature("user-1", "nope", serde_json::json!(1))
            .unwrap_err();
        assert!(matches!(err, FeatureStoreError::FeatureNotDefined(_)));
    }

    #[test]
    fn test_get_features_batch() {
        let store = default_store();
        store.define_feature(sample_def("f1")).expect("def f1");
        store.define_feature(sample_def("f2")).expect("def f2");
        store
            .set_feature("e1", "f1", serde_json::json!(1.0))
            .expect("set f1");
        store
            .set_feature("e1", "f2", serde_json::json!(2.0))
            .expect("set f2");

        let batch = store.get_features("e1", &["f1", "f2", "f3"]);
        assert!(batch.get("f1").expect("f1").is_some());
        assert!(batch.get("f2").expect("f2").is_some());
        assert!(batch.get("f3").expect("f3").is_none());
    }

    #[test]
    fn test_point_in_time_lookup() {
        let store = default_store();
        store.define_feature(sample_def("val")).expect("define");

        // Manually insert values at known timestamps.
        {
            let mut data = store.data.write();
            let entity = data.entry("e1".to_string()).or_default();
            let timeline = entity.entry("val".to_string()).or_default();
            timeline.insert(
                100,
                FeatureValue {
                    value: serde_json::json!(10.0),
                    timestamp: 100,
                    version: 1,
                    source: "test".to_string(),
                },
            );
            timeline.insert(
                200,
                FeatureValue {
                    value: serde_json::json!(20.0),
                    timestamp: 200,
                    version: 1,
                    source: "test".to_string(),
                },
            );
        }

        let at_150 = store
            .get_feature_at("e1", "val", 150)
            .expect("get_at")
            .expect("some");
        assert_eq!(at_150.value, serde_json::json!(10.0));

        let at_200 = store
            .get_feature_at("e1", "val", 200)
            .expect("get_at")
            .expect("some");
        assert_eq!(at_200.value, serde_json::json!(20.0));
    }

    #[test]
    fn test_freshness_unknown() {
        let store = default_store();
        store.define_feature(sample_def("fresh")).expect("define");
        let status = store.check_freshness("no-entity", "fresh");
        assert_eq!(status, FreshnessStatus::Unknown);
    }

    #[test]
    fn test_drift_detection() {
        let store = default_store();
        store.define_feature(sample_def("metric")).expect("define");

        let now = FeatureStore::now_secs();
        let old_ts = now.saturating_sub(7200); // 2 hours ago (outside drift window)

        {
            let mut data = store.data.write();
            // Baseline: values around 10.0
            for i in 0..20 {
                let entity = data.entry(format!("baseline-{}", i)).or_default();
                let timeline = entity.entry("metric".to_string()).or_default();
                timeline.insert(
                    old_ts + i as u64,
                    FeatureValue {
                        value: serde_json::json!(10.0 + (i as f64) * 0.1),
                        timestamp: old_ts + i as u64,
                        version: 1,
                        source: "test".to_string(),
                    },
                );
            }
            // Current: values around 50.0 (significant drift)
            for i in 0..20 {
                let entity = data.entry(format!("current-{}", i)).or_default();
                let timeline = entity.entry("metric".to_string()).or_default();
                timeline.insert(
                    now - 10 + i as u64,
                    FeatureValue {
                        value: serde_json::json!(50.0 + (i as f64) * 0.1),
                        timestamp: now - 10 + i as u64,
                        version: 1,
                        source: "test".to_string(),
                    },
                );
            }
        }

        let report = store.detect_drift("metric");
        assert!(report.is_drifted);
        assert!(report.drift_score > 2.0);
        assert_eq!(report.samples, 40);
    }

    #[test]
    fn test_entity_features() {
        let store = default_store();
        store.define_feature(sample_def("a")).expect("define a");
        store.define_feature(sample_def("b")).expect("define b");
        store
            .set_feature("e1", "a", serde_json::json!(1.0))
            .expect("set a");
        store
            .set_feature("e1", "b", serde_json::json!(2.0))
            .expect("set b");

        let features = store.entity_features("e1");
        assert_eq!(features.len(), 2);
    }

    #[test]
    fn test_stats() {
        let store = default_store();
        store.define_feature(sample_def("s")).expect("define");
        store
            .set_feature("e1", "s", serde_json::json!(1.0))
            .expect("set");

        let stats = store.stats();
        assert_eq!(stats.features_defined, 1);
        assert_eq!(stats.entities, 1);
        assert_eq!(stats.total_values, 1);
    }

    #[test]
    fn test_max_features_limit() {
        let store = FeatureStore::new(FeatureStoreConfig {
            max_features: 2,
            ..Default::default()
        });
        store.define_feature(sample_def("f1")).expect("f1");
        store.define_feature(sample_def("f2")).expect("f2");
        let err = store.define_feature(sample_def("f3")).unwrap_err();
        assert!(matches!(err, FeatureStoreError::MaxFeatures(2)));
    }
}
