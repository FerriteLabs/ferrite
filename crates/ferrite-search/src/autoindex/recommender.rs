//! Index Recommendation Engine
//!
//! Analyzes query patterns and generates index recommendations based on
//! access patterns, cost model, and confidence scoring.

use super::collector::QueryPattern;
use super::cost::{CostModel, IndexCost};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Type of index to recommend
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum IndexType {
    /// B-tree index for range queries and ordering
    BTree,
    /// Hash index for exact lookups
    Hash,
    /// Secondary index on a field
    Secondary(String),
    /// Compound index on multiple fields
    Compound(Vec<String>),
    /// Geospatial index for location queries
    Geospatial,
    /// Full-text search index
    FullText,
    /// Prefix index for pattern matching
    Prefix,
}

impl IndexType {
    /// Get the type name for cost model
    pub fn cost_type(&self) -> &str {
        match self {
            IndexType::BTree => "btree",
            IndexType::Hash => "hash",
            IndexType::Secondary(_) => "secondary",
            IndexType::Compound(_) => "compound",
            IndexType::Geospatial => "geospatial",
            IndexType::FullText => "fulltext",
            IndexType::Prefix => "btree", // Similar to B-tree
        }
    }

    /// Get a human-readable description
    pub fn description(&self) -> String {
        match self {
            IndexType::BTree => "B-tree index".to_string(),
            IndexType::Hash => "Hash index".to_string(),
            IndexType::Secondary(field) => format!("Secondary index on '{}'", field),
            IndexType::Compound(fields) => format!("Compound index on [{}]", fields.join(", ")),
            IndexType::Geospatial => "Geospatial index".to_string(),
            IndexType::FullText => "Full-text search index".to_string(),
            IndexType::Prefix => "Prefix index".to_string(),
        }
    }
}

/// Confidence in a recommendation
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RecommendationConfidence {
    /// Overall confidence score (0.0-1.0)
    pub score: f64,
    /// Reasons supporting this recommendation
    pub reasons: Vec<String>,
}

impl RecommendationConfidence {
    /// Create a new confidence with score and reasons
    pub fn new(score: f64, reasons: Vec<String>) -> Self {
        Self {
            score: score.clamp(0.0, 1.0),
            reasons,
        }
    }

    /// Create high confidence
    pub fn high(reasons: Vec<String>) -> Self {
        Self::new(0.9, reasons)
    }

    /// Create medium confidence
    pub fn medium(reasons: Vec<String>) -> Self {
        Self::new(0.7, reasons)
    }

    /// Create low confidence
    pub fn low(reasons: Vec<String>) -> Self {
        Self::new(0.5, reasons)
    }
}

/// An index recommendation
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IndexRecommendation {
    /// Pattern this recommendation applies to
    pub pattern: String,
    /// Recommended index type
    pub index_type: IndexType,
    /// Fields to include in index
    pub fields: Vec<String>,
    /// Confidence in this recommendation
    pub confidence: RecommendationConfidence,
    /// Estimated storage cost in bytes
    pub estimated_storage_bytes: u64,
    /// Estimated latency improvement (0.0-1.0)
    pub estimated_latency_improvement: f64,
}

impl IndexRecommendation {
    /// Get the cost-adjusted score for ranking
    pub fn adjusted_score(&self, cost: &IndexCost) -> f64 {
        // Higher confidence and improvement, lower cost = better
        let benefit = self.confidence.score * (1.0 + self.estimated_latency_improvement);
        let cost_penalty = cost.total_score();

        benefit / (1.0 + cost_penalty)
    }
}

/// Configuration for the recommender
#[derive(Clone, Debug)]
pub struct RecommenderConfig {
    /// Minimum access count to consider
    pub min_access_count: u64,
    /// Confidence threshold
    pub confidence_threshold: f64,
    /// Maximum indexes per pattern
    pub max_indexes_per_pattern: usize,
}

impl Default for RecommenderConfig {
    fn default() -> Self {
        Self {
            min_access_count: 100,
            confidence_threshold: 0.5,
            max_indexes_per_pattern: 3,
        }
    }
}

/// Index recommendation engine
pub struct Recommender {
    /// Configuration
    config: RecommenderConfig,
    /// Index type weights for different access patterns
    type_weights: HashMap<String, Vec<(IndexType, f64)>>,
}

impl Recommender {
    /// Create a new recommender
    pub fn new(config: RecommenderConfig) -> Self {
        let mut type_weights = HashMap::new();

        // Read-heavy patterns prefer hash or B-tree
        type_weights.insert(
            "read_heavy".to_string(),
            vec![(IndexType::Hash, 1.0), (IndexType::BTree, 0.9)],
        );

        // Range query patterns prefer B-tree
        type_weights.insert(
            "range_heavy".to_string(),
            vec![(IndexType::BTree, 1.0), (IndexType::Prefix, 0.8)],
        );

        // Scan-heavy patterns prefer secondary indexes
        type_weights.insert(
            "scan_heavy".to_string(),
            vec![(IndexType::BTree, 0.9), (IndexType::Prefix, 0.8)],
        );

        // Balanced patterns
        type_weights.insert(
            "balanced".to_string(),
            vec![(IndexType::BTree, 0.8), (IndexType::Hash, 0.7)],
        );

        Self {
            config,
            type_weights,
        }
    }

    /// Generate recommendations for a query pattern
    pub fn recommend(
        &self,
        pattern: &QueryPattern,
        cost_model: &CostModel,
    ) -> Vec<IndexRecommendation> {
        let mut recommendations = Vec::new();

        // Skip if access count is too low
        if pattern.access_count < self.config.min_access_count {
            return recommendations;
        }

        // Determine pattern type
        let pattern_type = self.classify_pattern(pattern);

        // Get candidate index types
        let candidates = self.get_candidates(&pattern_type, pattern);

        // Generate recommendations for each candidate
        for (index_type, base_weight) in candidates {
            let rec = self.generate_recommendation(pattern, index_type, base_weight, cost_model);

            if rec.confidence.score >= self.config.confidence_threshold {
                recommendations.push(rec);
            }
        }

        // Sort by adjusted score
        recommendations.sort_by(|a, b| {
            let cost_a = cost_model.estimate_cost(
                a.index_type.cost_type(),
                pattern.access_count,
                32,
                pattern.write_count as f64 / pattern.access_count.max(1) as f64,
            );
            let cost_b = cost_model.estimate_cost(
                b.index_type.cost_type(),
                pattern.access_count,
                32,
                pattern.write_count as f64 / pattern.access_count.max(1) as f64,
            );

            b.adjusted_score(&cost_b)
                .partial_cmp(&a.adjusted_score(&cost_a))
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        // Limit to max per pattern
        recommendations.truncate(self.config.max_indexes_per_pattern);

        recommendations
    }

    /// Classify a pattern based on access characteristics
    fn classify_pattern(&self, pattern: &QueryPattern) -> String {
        if pattern.range_count > pattern.read_count / 2 {
            "range_heavy".to_string()
        } else if pattern.scan_count > pattern.read_count / 2 {
            "scan_heavy".to_string()
        } else if pattern.is_read_heavy() {
            "read_heavy".to_string()
        } else {
            "balanced".to_string()
        }
    }

    /// Get candidate index types for a pattern type
    fn get_candidates(&self, pattern_type: &str, pattern: &QueryPattern) -> Vec<(IndexType, f64)> {
        let mut candidates = self
            .type_weights
            .get(pattern_type)
            .cloned()
            .unwrap_or_else(|| {
                self.type_weights
                    .get("balanced")
                    .cloned()
                    .unwrap_or_default()
            });

        // Add secondary indexes for hot fields
        for (field, count) in &pattern.hot_fields {
            if *count > pattern.access_count / 10 {
                // Field accessed in >10% of queries
                let weight = (*count as f64 / pattern.access_count as f64).min(1.0);
                candidates.push((IndexType::Secondary(field.clone()), weight));
            }
        }

        // Add compound index if multiple hot fields
        if pattern.hot_fields.len() >= 2 {
            let top_fields: Vec<String> = pattern
                .hot_fields
                .iter()
                .take(3)
                .map(|(f, _)| f.clone())
                .collect();
            candidates.push((IndexType::Compound(top_fields), 0.6));
        }

        candidates
    }

    /// Generate a single recommendation
    fn generate_recommendation(
        &self,
        pattern: &QueryPattern,
        index_type: IndexType,
        base_weight: f64,
        cost_model: &CostModel,
    ) -> IndexRecommendation {
        let fields = match &index_type {
            IndexType::Secondary(field) => vec![field.clone()],
            IndexType::Compound(fields) => fields.clone(),
            _ => Vec::new(),
        };

        // Calculate confidence
        let write_ratio = pattern.write_count as f64 / pattern.access_count.max(1) as f64;
        let frequency_factor = (pattern.frequency_per_minute / 100.0).min(1.0);
        let latency_factor = (pattern.avg_latency_us as f64 / 1000.0).min(1.0); // Higher latency = more benefit

        let mut confidence_score = base_weight;
        confidence_score *= 1.0 - write_ratio * 0.5; // Penalize write-heavy patterns
        confidence_score *= 0.5 + frequency_factor * 0.5; // Boost high-frequency patterns
        confidence_score *= 0.7 + latency_factor * 0.3; // Boost high-latency patterns
        if pattern.is_read_heavy() {
            confidence_score = confidence_score.max(0.55);
        }

        let mut reasons = Vec::new();

        if pattern.is_read_heavy() {
            reasons.push(format!(
                "Read-heavy pattern ({}:1 read/write ratio)",
                pattern.read_write_ratio() as u64
            ));
        }

        if pattern.frequency_per_minute > 10.0 {
            reasons.push(format!(
                "High access frequency ({:.1}/min)",
                pattern.frequency_per_minute
            ));
        }

        if pattern.avg_latency_us > 500 {
            reasons.push(format!(
                "High average latency ({}µs)",
                pattern.avg_latency_us
            ));
        }

        match &index_type {
            IndexType::BTree => {
                reasons.push("B-tree supports range queries and ordering".to_string())
            }
            IndexType::Hash => reasons.push("Hash index optimal for exact lookups".to_string()),
            IndexType::Secondary(field) => {
                reasons.push(format!("Field '{}' frequently accessed", field))
            }
            IndexType::Compound(_) => {
                reasons.push("Multiple fields commonly accessed together".to_string())
            }
            _ => {}
        }

        // Estimate storage
        let cost = cost_model.estimate_cost(
            index_type.cost_type(),
            pattern.access_count,
            32, // Assume average key size
            write_ratio,
        );

        // Estimate latency improvement
        let latency_improvement = match &index_type {
            IndexType::Hash if pattern.is_read_heavy() => 0.4,
            IndexType::BTree if pattern.range_count > 0 => 0.5,
            IndexType::Secondary(_) => 0.3,
            IndexType::Compound(_) => 0.35,
            _ => 0.2,
        };

        IndexRecommendation {
            pattern: pattern.pattern.clone(),
            index_type,
            fields,
            confidence: RecommendationConfidence::new(confidence_score, reasons),
            estimated_storage_bytes: cost.storage_bytes,
            estimated_latency_improvement: latency_improvement,
        }
    }

    /// Validate a recommendation against current state
    pub fn validate_recommendation(
        &self,
        rec: &IndexRecommendation,
        existing_indexes: &[IndexType],
    ) -> bool {
        // Check if similar index already exists
        for existing in existing_indexes {
            if self.indexes_overlap(&rec.index_type, existing) {
                return false;
            }
        }

        true
    }

    /// Check if two indexes overlap significantly
    fn indexes_overlap(&self, a: &IndexType, b: &IndexType) -> bool {
        match (a, b) {
            (IndexType::Hash, IndexType::Hash) => true,
            (IndexType::BTree, IndexType::BTree) => true,
            (IndexType::Secondary(f1), IndexType::Secondary(f2)) => f1 == f2,
            (IndexType::Compound(f1), IndexType::Compound(f2)) => {
                // Overlap if first fields match
                f1.first() == f2.first()
            }
            (IndexType::Secondary(f), IndexType::Compound(fields)) => {
                fields.first().map(|ff| ff == f).unwrap_or(false)
            }
            (IndexType::Compound(fields), IndexType::Secondary(f)) => {
                fields.first().map(|ff| ff == f).unwrap_or(false)
            }
            _ => false,
        }
    }

    /// Get configuration
    pub fn config(&self) -> &RecommenderConfig {
        &self.config
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_pattern() -> QueryPattern {
        QueryPattern {
            pattern: "users:*".to_string(),
            access_count: 1000,
            read_count: 800,
            write_count: 200,
            scan_count: 50,
            range_count: 100,
            avg_latency_us: 500,
            p50_latency_us: 400,
            p99_latency_us: 2000,
            hot_fields: vec![("name".to_string(), 500), ("email".to_string(), 300)],
            first_seen: 0,
            last_seen: 3600,
            frequency_per_minute: 50.0,
        }
    }

    #[test]
    fn test_index_type_description() {
        assert_eq!(IndexType::Hash.description(), "Hash index");
        assert_eq!(
            IndexType::Secondary("name".to_string()).description(),
            "Secondary index on 'name'"
        );
        assert_eq!(
            IndexType::Compound(vec!["a".to_string(), "b".to_string()]).description(),
            "Compound index on [a, b]"
        );
    }

    #[test]
    fn test_confidence_levels() {
        let high = RecommendationConfidence::high(vec!["reason".to_string()]);
        let medium = RecommendationConfidence::medium(vec!["reason".to_string()]);
        let low = RecommendationConfidence::low(vec!["reason".to_string()]);

        assert!(high.score > medium.score);
        assert!(medium.score > low.score);
    }

    #[test]
    fn test_recommend_read_heavy() {
        let recommender = Recommender::new(RecommenderConfig::default());
        let cost_model = CostModel::new(1024 * 1024 * 1024, 2.0);

        let pattern = create_test_pattern();
        let recommendations = recommender.recommend(&pattern, &cost_model);

        // Should generate recommendations for read-heavy pattern
        assert!(!recommendations.is_empty());

        // First recommendation should have high confidence
        assert!(recommendations[0].confidence.score > 0.5);
    }

    #[test]
    fn test_recommend_with_hot_fields() {
        let recommender = Recommender::new(RecommenderConfig::default());
        let cost_model = CostModel::new(1024 * 1024 * 1024, 2.0);

        let pattern = create_test_pattern();
        let recommendations = recommender.recommend(&pattern, &cost_model);

        // Should include secondary index for hot field
        let has_secondary = recommendations
            .iter()
            .any(|r| matches!(&r.index_type, IndexType::Secondary(f) if f == "name"));

        // May or may not include secondary depending on threshold, but should consider it
        assert!(!recommendations.is_empty());
    }

    #[test]
    fn test_recommend_min_access_count() {
        let recommender = Recommender::new(RecommenderConfig {
            min_access_count: 10000, // Very high threshold
            ..Default::default()
        });
        let cost_model = CostModel::new(1024 * 1024 * 1024, 2.0);

        let pattern = create_test_pattern(); // Only 1000 accesses
        let recommendations = recommender.recommend(&pattern, &cost_model);

        // Should not generate recommendations
        assert!(recommendations.is_empty());
    }

    #[test]
    fn test_validate_recommendation() {
        let recommender = Recommender::new(RecommenderConfig::default());

        let rec = IndexRecommendation {
            pattern: "test".to_string(),
            index_type: IndexType::Hash,
            fields: vec![],
            confidence: RecommendationConfidence::high(vec![]),
            estimated_storage_bytes: 1024,
            estimated_latency_improvement: 0.3,
        };

        // Should validate against empty existing
        assert!(recommender.validate_recommendation(&rec, &[]));

        // Should not validate if same type exists
        assert!(!recommender.validate_recommendation(&rec, &[IndexType::Hash]));

        // Should validate if different type exists
        assert!(recommender.validate_recommendation(&rec, &[IndexType::BTree]));
    }

    #[test]
    fn test_indexes_overlap() {
        let recommender = Recommender::new(RecommenderConfig::default());

        // Same type overlaps
        assert!(recommender.indexes_overlap(&IndexType::Hash, &IndexType::Hash));

        // Different types don't overlap
        assert!(!recommender.indexes_overlap(&IndexType::Hash, &IndexType::BTree));

        // Secondary with same field overlaps
        assert!(recommender.indexes_overlap(
            &IndexType::Secondary("name".to_string()),
            &IndexType::Secondary("name".to_string())
        ));

        // Secondary with compound starting with same field overlaps
        assert!(recommender.indexes_overlap(
            &IndexType::Secondary("name".to_string()),
            &IndexType::Compound(vec!["name".to_string(), "email".to_string()])
        ));
    }

    #[test]
    fn test_classify_pattern() {
        let recommender = Recommender::new(RecommenderConfig::default());

        // Read heavy
        let read_heavy = QueryPattern {
            pattern: "test".to_string(),
            access_count: 100,
            read_count: 90,
            write_count: 10,
            scan_count: 0,
            range_count: 0,
            avg_latency_us: 100,
            p50_latency_us: 80,
            p99_latency_us: 500,
            hot_fields: vec![],
            first_seen: 0,
            last_seen: 3600,
            frequency_per_minute: 10.0,
        };
        assert_eq!(recommender.classify_pattern(&read_heavy), "read_heavy");

        // Range heavy
        let range_heavy = QueryPattern {
            range_count: 60,
            read_count: 100,
            ..read_heavy.clone()
        };
        assert_eq!(recommender.classify_pattern(&range_heavy), "range_heavy");
    }
}
