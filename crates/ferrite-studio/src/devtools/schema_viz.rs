#![forbid(unsafe_code)]
//! Schema visualization — analyze key patterns and type distribution.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Analyzes database schemas from key metadata.
#[derive(Debug, Clone, Default)]
pub struct SchemaVisualizer;

/// A point-in-time snapshot of the schema across all databases.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaSnapshot {
    pub databases: Vec<DatabaseSchema>,
    pub total_keys: u64,
    pub total_memory_bytes: u64,
    pub snapshot_at: DateTime<Utc>,
}

/// Schema information for a single logical database.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseSchema {
    pub db_index: u8,
    pub key_count: u64,
    pub type_distribution: HashMap<String, u64>,
    pub key_patterns: Vec<KeyPattern>,
    pub memory_bytes: u64,
}

/// A detected key naming pattern with statistics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeyPattern {
    pub pattern: String,
    pub count: u64,
    pub avg_size_bytes: u64,
    pub data_types: Vec<String>,
    pub sample_keys: Vec<String>,
}

impl SchemaVisualizer {
    /// Build a schema snapshot from (key, type, size) tuples.
    #[allow(dead_code)]
    pub fn analyze_schema(keys: &[(String, String, u64)]) -> SchemaSnapshot {
        let mut type_dist: HashMap<String, u64> = HashMap::new();
        let mut total_memory: u64 = 0;
        let key_names: Vec<String> = keys.iter().map(|(k, _, _)| k.clone()).collect();

        for (_, dtype, size) in keys {
            *type_dist.entry(dtype.clone()).or_insert(0) += 1;
            total_memory += size;
        }

        let mut patterns = Self::detect_patterns(&key_names);
        // Enrich patterns with type and size info
        for pat in &mut patterns {
            let mut sizes: Vec<u64> = Vec::new();
            let mut types: HashMap<String, bool> = HashMap::new();
            for (k, t, s) in keys {
                if Self::key_matches_pattern(k, &pat.pattern) {
                    sizes.push(*s);
                    types.insert(t.clone(), true);
                    if pat.sample_keys.len() < 3 {
                        pat.sample_keys.push(k.clone());
                    }
                }
            }
            pat.count = sizes.len() as u64;
            pat.avg_size_bytes = if sizes.is_empty() {
                0
            } else {
                sizes.iter().sum::<u64>() / sizes.len() as u64
            };
            pat.data_types = types.into_keys().collect();
        }

        let db = DatabaseSchema {
            db_index: 0,
            key_count: keys.len() as u64,
            type_distribution: type_dist,
            key_patterns: patterns,
            memory_bytes: total_memory,
        };

        SchemaSnapshot {
            total_keys: keys.len() as u64,
            total_memory_bytes: total_memory,
            databases: vec![db],
            snapshot_at: Utc::now(),
        }
    }

    /// Detect common key patterns by splitting on `:` and grouping by prefix.
    #[allow(dead_code)]
    pub fn detect_patterns(keys: &[String]) -> Vec<KeyPattern> {
        let mut prefix_counts: HashMap<String, Vec<String>> = HashMap::new();

        for key in keys {
            let prefix = if let Some(idx) = key.find(':') {
                &key[..idx]
            } else {
                key.as_str()
            };
            prefix_counts
                .entry(prefix.to_string())
                .or_default()
                .push(key.clone());
        }

        let mut patterns: Vec<KeyPattern> = prefix_counts
            .into_iter()
            .map(|(prefix, matched)| {
                let sample: Vec<String> = matched.iter().take(3).cloned().collect();
                let has_suffix = matched.iter().any(|k| k.contains(':'));
                let pattern = if has_suffix {
                    format!("{prefix}:*")
                } else {
                    prefix.clone()
                };
                KeyPattern {
                    pattern,
                    count: matched.len() as u64,
                    avg_size_bytes: 0,
                    data_types: Vec::new(),
                    sample_keys: sample,
                }
            })
            .collect();

        patterns.sort_by(|a, b| b.count.cmp(&a.count));
        patterns
    }

    /// Render an ASCII tree representation of a schema snapshot.
    #[allow(dead_code)]
    pub fn format_tree(snapshot: &SchemaSnapshot) -> String {
        let mut out = String::new();
        out.push_str(&format!(
            "Schema Snapshot ({} keys, {} bytes)\n",
            snapshot.total_keys, snapshot.total_memory_bytes
        ));
        for db in &snapshot.databases {
            out.push_str(&format!(
                "├── db{} ({} keys, {} bytes)\n",
                db.db_index, db.key_count, db.memory_bytes
            ));
            // Type distribution
            out.push_str("│   ├── types\n");
            let mut types: Vec<_> = db.type_distribution.iter().collect();
            types.sort_by(|a, b| b.1.cmp(a.1));
            for (i, (t, count)) in types.iter().enumerate() {
                let connector = if i + 1 == types.len() { "└" } else { "├" };
                out.push_str(&format!("│   │   {connector}── {t}: {count}\n"));
            }
            // Patterns
            out.push_str("│   └── patterns\n");
            for (i, pat) in db.key_patterns.iter().enumerate() {
                let connector = if i + 1 == db.key_patterns.len() {
                    "└"
                } else {
                    "├"
                };
                out.push_str(&format!(
                    "│       {connector}── {} ({} keys, avg {} bytes)\n",
                    pat.pattern, pat.count, pat.avg_size_bytes
                ));
            }
        }
        out
    }

    fn key_matches_pattern(key: &str, pattern: &str) -> bool {
        if let Some(prefix) = pattern.strip_suffix(":*") {
            key.starts_with(prefix)
                && key.len() > prefix.len()
                && key.as_bytes()[prefix.len()] == b':'
        } else {
            key == pattern
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_detect_patterns() {
        let keys = vec![
            "users:1".to_string(),
            "users:2".to_string(),
            "users:3".to_string(),
            "session:abc".to_string(),
            "session:def".to_string(),
            "config".to_string(),
        ];
        let patterns = SchemaVisualizer::detect_patterns(&keys);
        assert!(!patterns.is_empty());
        assert_eq!(patterns[0].pattern, "users:*");
        assert_eq!(patterns[0].count, 3);
        assert_eq!(patterns[1].pattern, "session:*");
        assert_eq!(patterns[1].count, 2);
    }

    #[test]
    fn test_analyze_schema() {
        let keys = vec![
            ("users:1".to_string(), "string".to_string(), 128),
            ("users:2".to_string(), "string".to_string(), 256),
            ("session:abc".to_string(), "hash".to_string(), 512),
        ];
        let snapshot = SchemaVisualizer::analyze_schema(&keys);
        assert_eq!(snapshot.total_keys, 3);
        assert_eq!(snapshot.total_memory_bytes, 896);
        assert_eq!(snapshot.databases.len(), 1);
        assert_eq!(snapshot.databases[0].key_count, 3);
    }

    #[test]
    fn test_format_tree() {
        let keys = vec![
            ("users:1".to_string(), "string".to_string(), 100),
            ("session:a".to_string(), "hash".to_string(), 200),
        ];
        let snapshot = SchemaVisualizer::analyze_schema(&keys);
        let tree = SchemaVisualizer::format_tree(&snapshot);
        assert!(tree.contains("db0"));
        assert!(tree.contains("types"));
        assert!(tree.contains("patterns"));
    }

    #[test]
    fn test_key_matches_pattern() {
        assert!(SchemaVisualizer::key_matches_pattern("users:1", "users:*"));
        assert!(!SchemaVisualizer::key_matches_pattern("users", "users:*"));
        assert!(SchemaVisualizer::key_matches_pattern("config", "config"));
        assert!(!SchemaVisualizer::key_matches_pattern("config2", "config"));
    }
}
