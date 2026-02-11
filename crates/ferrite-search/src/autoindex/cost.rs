//! Cost Model for Auto-Indexing
//!
//! Provides cost estimation for index creation and maintenance, including
//! storage overhead, write amplification, and memory usage.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Index cost estimation
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IndexCost {
    /// Estimated storage in bytes
    pub storage_bytes: u64,
    /// Write amplification factor (1.0 = no amplification)
    pub write_amplification: f64,
    /// Memory overhead in bytes
    pub memory_bytes: u64,
    /// Build time estimate in seconds
    pub build_time_secs: f64,
    /// Maintenance cost score (0.0-1.0)
    pub maintenance_score: f64,
}

impl IndexCost {
    /// Calculate total cost score (lower is better)
    pub fn total_score(&self) -> f64 {
        let storage_factor = (self.storage_bytes as f64 / (1024.0 * 1024.0)).ln_1p(); // MB
        let write_factor = self.write_amplification - 1.0; // Amplification above 1.0
        let memory_factor = (self.memory_bytes as f64 / (1024.0 * 1024.0)).ln_1p();
        let maintenance_factor = self.maintenance_score;

        // Weighted sum
        storage_factor * 0.3 + write_factor * 0.3 + memory_factor * 0.2 + maintenance_factor * 0.2
    }

    /// Check if cost is within acceptable limits
    pub fn is_acceptable(&self, max_storage: u64, max_write_amp: f64) -> bool {
        self.storage_bytes <= max_storage && self.write_amplification <= max_write_amp
    }
}

/// Storage cost breakdown
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StorageCost {
    /// Index data storage
    pub index_data_bytes: u64,
    /// Index metadata storage
    pub metadata_bytes: u64,
    /// Bloom filter storage (if applicable)
    pub bloom_filter_bytes: u64,
    /// Total storage
    pub total_bytes: u64,
}

impl StorageCost {
    /// Create a new storage cost
    pub fn new(index_data: u64, metadata: u64, bloom_filter: u64) -> Self {
        Self {
            index_data_bytes: index_data,
            metadata_bytes: metadata,
            bloom_filter_bytes: bloom_filter,
            total_bytes: index_data + metadata + bloom_filter,
        }
    }

    /// Estimate storage for a given number of keys
    pub fn estimate_for_keys(key_count: u64, avg_key_size: usize, index_type: &str) -> Self {
        let key_data = key_count * avg_key_size as u64;

        match index_type {
            "btree" => {
                // B-tree: ~1.5x key data for tree structure
                let index_data = (key_data as f64 * 1.5) as u64;
                let metadata = key_count * 16; // 16 bytes per node pointer
                let bloom_filter = 0;
                Self::new(index_data, metadata, bloom_filter)
            }
            "hash" => {
                // Hash: ~1.2x for hash table overhead
                let index_data = (key_data as f64 * 1.2) as u64;
                let metadata = key_count * 8; // 8 bytes per bucket
                let bloom_filter = 0;
                Self::new(index_data, metadata, bloom_filter)
            }
            "secondary" => {
                // Secondary index: key data + value references
                let index_data = key_data + key_count * 8; // 8 bytes for value ref
                let metadata = key_count * 4;
                let bloom_filter = (key_count as f64 * 1.44 * 10.0 / 8.0) as u64; // 10 bits per key
                Self::new(index_data, metadata, bloom_filter)
            }
            "compound" => {
                // Compound index: larger due to multiple fields
                let index_data = (key_data as f64 * 2.0) as u64;
                let metadata = key_count * 24;
                let bloom_filter = (key_count as f64 * 1.44 * 10.0 / 8.0) as u64;
                Self::new(index_data, metadata, bloom_filter)
            }
            "geospatial" => {
                // Geo index: R-tree or similar
                let index_data = key_count * 32; // lat/lng + bounding box
                let metadata = key_count * 16;
                let bloom_filter = 0;
                Self::new(index_data, metadata, bloom_filter)
            }
            "fulltext" => {
                // Full-text: inverted index is typically larger
                let index_data = (key_data as f64 * 3.0) as u64; // Inverted index expansion
                let metadata = key_count * 20;
                let bloom_filter = (key_count as f64 * 1.44 * 10.0 / 8.0) as u64;
                Self::new(index_data, metadata, bloom_filter)
            }
            _ => {
                // Default: assume similar to B-tree
                let index_data = (key_data as f64 * 1.5) as u64;
                let metadata = key_count * 16;
                let bloom_filter = 0;
                Self::new(index_data, metadata, bloom_filter)
            }
        }
    }
}

/// Cost model for evaluating index creation decisions
pub struct CostModel {
    /// Storage budget in bytes
    storage_budget: u64,
    /// Maximum write amplification allowed
    max_write_amplification: f64,
    /// Index type cost multipliers
    type_multipliers: HashMap<String, f64>,
    /// Current storage usage
    current_storage: u64,
}

impl CostModel {
    /// Create a new cost model
    pub fn new(storage_budget: u64, max_write_amplification: f64) -> Self {
        let mut type_multipliers = HashMap::new();
        type_multipliers.insert("btree".to_string(), 1.0);
        type_multipliers.insert("hash".to_string(), 0.8);
        type_multipliers.insert("secondary".to_string(), 1.2);
        type_multipliers.insert("compound".to_string(), 1.5);
        type_multipliers.insert("geospatial".to_string(), 1.3);
        type_multipliers.insert("fulltext".to_string(), 2.0);

        Self {
            storage_budget,
            max_write_amplification,
            type_multipliers,
            current_storage: 0,
        }
    }

    /// Estimate cost for creating an index
    pub fn estimate_cost(
        &self,
        index_type: &str,
        key_count: u64,
        avg_key_size: usize,
        write_rate: f64,
    ) -> IndexCost {
        let storage = StorageCost::estimate_for_keys(key_count, avg_key_size, index_type);
        let multiplier = self
            .type_multipliers
            .get(index_type)
            .copied()
            .unwrap_or(1.0);

        // Write amplification based on index type and write rate
        let base_amplification = match index_type {
            "btree" => 1.2,
            "hash" => 1.1,
            "secondary" => 1.3,
            "compound" => 1.4,
            "geospatial" => 1.2,
            "fulltext" => 1.5,
            _ => 1.2,
        };
        let write_amplification = base_amplification * (1.0 + write_rate * 0.1);

        // Memory overhead (rough estimate)
        let memory_bytes = (storage.total_bytes as f64 * 0.1) as u64; // 10% for in-memory structures

        // Build time estimate based on key count
        let build_time_secs = (key_count as f64 / 100000.0) * multiplier;

        // Maintenance score based on write rate and index complexity
        let maintenance_score = (write_rate * multiplier).min(1.0);

        IndexCost {
            storage_bytes: storage.total_bytes,
            write_amplification,
            memory_bytes,
            build_time_secs,
            maintenance_score,
        }
    }

    /// Check if an index can be created within budget
    pub fn can_afford(&self, cost: &IndexCost) -> bool {
        let new_storage = self.current_storage + cost.storage_bytes;
        new_storage <= self.storage_budget
            && cost.write_amplification <= self.max_write_amplification
    }

    /// Calculate remaining storage budget
    pub fn remaining_budget(&self) -> u64 {
        self.storage_budget.saturating_sub(self.current_storage)
    }

    /// Update current storage usage
    pub fn update_storage(&mut self, bytes: u64) {
        self.current_storage = bytes;
    }

    /// Add storage usage
    pub fn add_storage(&mut self, bytes: u64) {
        self.current_storage += bytes;
    }

    /// Remove storage usage
    pub fn remove_storage(&mut self, bytes: u64) {
        self.current_storage = self.current_storage.saturating_sub(bytes);
    }

    /// Get storage budget
    pub fn storage_budget(&self) -> u64 {
        self.storage_budget
    }

    /// Get max write amplification
    pub fn max_write_amplification(&self) -> f64 {
        self.max_write_amplification
    }

    /// Calculate cost-benefit score
    ///
    /// Returns a score where higher is better.
    /// Takes into account the estimated performance improvement vs cost.
    pub fn cost_benefit_score(
        &self,
        cost: &IndexCost,
        estimated_latency_improvement: f64,
        access_frequency: f64,
    ) -> f64 {
        if cost.total_score() == 0.0 {
            return 0.0;
        }

        // Benefit: latency improvement * frequency
        let benefit = estimated_latency_improvement * access_frequency.ln_1p();

        // Cost: total cost score
        let cost_score = cost.total_score();

        // Benefit / Cost ratio
        benefit / (cost_score + 0.1) // Add small constant to avoid division by zero
    }

    /// Compare two index options and return the better one
    pub fn compare_options(
        &self,
        option_a: (&IndexCost, f64, f64), // (cost, latency_improvement, frequency)
        option_b: (&IndexCost, f64, f64),
    ) -> std::cmp::Ordering {
        let score_a = self.cost_benefit_score(option_a.0, option_a.1, option_a.2);
        let score_b = self.cost_benefit_score(option_b.0, option_b.1, option_b.2);

        score_b
            .partial_cmp(&score_a)
            .unwrap_or(std::cmp::Ordering::Equal)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_storage_cost_btree() {
        let cost = StorageCost::estimate_for_keys(10000, 32, "btree");
        assert!(cost.total_bytes > 10000 * 32);
        assert_eq!(cost.bloom_filter_bytes, 0); // B-tree doesn't use bloom filter
    }

    #[test]
    fn test_storage_cost_secondary() {
        let cost = StorageCost::estimate_for_keys(10000, 32, "secondary");
        assert!(cost.bloom_filter_bytes > 0); // Secondary index uses bloom filter
    }

    #[test]
    fn test_index_cost_score() {
        let low_cost = IndexCost {
            storage_bytes: 1024,
            write_amplification: 1.1,
            memory_bytes: 100,
            build_time_secs: 0.1,
            maintenance_score: 0.1,
        };

        let high_cost = IndexCost {
            storage_bytes: 1024 * 1024,
            write_amplification: 2.0,
            memory_bytes: 10000,
            build_time_secs: 10.0,
            maintenance_score: 0.8,
        };

        assert!(low_cost.total_score() < high_cost.total_score());
    }

    #[test]
    fn test_cost_model_estimate() {
        let model = CostModel::new(1024 * 1024 * 1024, 2.0);

        let btree_cost = model.estimate_cost("btree", 10000, 32, 0.1);
        let hash_cost = model.estimate_cost("hash", 10000, 32, 0.1);

        // Hash should have lower write amplification
        assert!(hash_cost.write_amplification < btree_cost.write_amplification);
    }

    #[test]
    fn test_cost_model_budget() {
        let model = CostModel::new(1024, 2.0); // Very small budget

        let cost = IndexCost {
            storage_bytes: 2048,
            write_amplification: 1.5,
            memory_bytes: 100,
            build_time_secs: 0.1,
            maintenance_score: 0.1,
        };

        assert!(!model.can_afford(&cost)); // Exceeds storage budget
    }

    #[test]
    fn test_cost_benefit_score() {
        let model = CostModel::new(1024 * 1024 * 1024, 2.0);

        let low_cost = IndexCost {
            storage_bytes: 1024,
            write_amplification: 1.1,
            memory_bytes: 100,
            build_time_secs: 0.1,
            maintenance_score: 0.1,
        };

        // High improvement with low cost should have high score
        let high_benefit = model.cost_benefit_score(&low_cost, 0.5, 1000.0);
        // Low improvement with same cost should have lower score
        let low_benefit = model.cost_benefit_score(&low_cost, 0.1, 100.0);

        assert!(high_benefit > low_benefit);
    }

    #[test]
    fn test_remaining_budget() {
        let mut model = CostModel::new(1000, 2.0);
        assert_eq!(model.remaining_budget(), 1000);

        model.add_storage(300);
        assert_eq!(model.remaining_budget(), 700);

        model.remove_storage(100);
        assert_eq!(model.remaining_budget(), 800);
    }
}
