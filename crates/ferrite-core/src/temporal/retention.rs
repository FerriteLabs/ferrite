//! Retention Policy for Temporal Data
//!
//! Manages how long historical versions are kept and cleanup policies.

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Retention policy for temporal data
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RetentionPolicy {
    /// Maximum age for historical data
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "option_duration_serde"
    )]
    pub max_age: Option<Duration>,

    /// Maximum versions to keep per key
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_versions: Option<usize>,

    /// Minimum versions to always keep
    #[serde(default = "default_min_versions")]
    pub min_versions: usize,

    /// Per-pattern overrides
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub overrides: Vec<PatternOverride>,
}

fn default_min_versions() -> usize {
    1
}

impl RetentionPolicy {
    /// Create a new retention policy with defaults
    pub fn new() -> Self {
        Self::default()
    }

    /// Set maximum age
    pub fn with_max_age(mut self, age: Duration) -> Self {
        self.max_age = Some(age);
        self
    }

    /// Set maximum versions
    pub fn with_max_versions(mut self, versions: usize) -> Self {
        self.max_versions = Some(versions);
        self
    }

    /// Set minimum versions
    pub fn with_min_versions(mut self, versions: usize) -> Self {
        self.min_versions = versions;
        self
    }

    /// Add a pattern override
    pub fn with_override(mut self, pattern: &str, override_policy: RetentionOverride) -> Self {
        self.overrides.push(PatternOverride {
            pattern: pattern.to_string(),
            policy: override_policy,
        });
        self
    }

    /// Get the effective policy for a specific key
    pub fn effective_for_key(&self, key: &Bytes) -> RetentionPolicy {
        let key_str = String::from_utf8_lossy(key);

        // Check overrides in order
        for override_entry in &self.overrides {
            if glob_match(&override_entry.pattern, &key_str) {
                return self.merge_override(&override_entry.policy);
            }
        }

        // No override matched, use base policy
        self.clone()
    }

    /// Merge an override into the base policy
    fn merge_override(&self, override_policy: &RetentionOverride) -> RetentionPolicy {
        RetentionPolicy {
            max_age: override_policy.max_age.or(self.max_age),
            max_versions: override_policy.max_versions.or(self.max_versions),
            min_versions: override_policy.min_versions.unwrap_or(self.min_versions),
            overrides: vec![], // Don't inherit overrides
        }
    }
}

impl Default for RetentionPolicy {
    fn default() -> Self {
        Self {
            max_age: Some(Duration::from_secs(7 * 24 * 3600)), // 7 days
            max_versions: Some(1000),
            min_versions: 1,
            overrides: vec![],
        }
    }
}

/// Override retention for keys matching a pattern
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PatternOverride {
    /// Glob pattern to match keys
    pub pattern: String,
    /// Override policy
    #[serde(flatten)]
    pub policy: RetentionOverride,
}

/// Retention override values
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct RetentionOverride {
    /// Override max age
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "option_duration_serde"
    )]
    pub max_age: Option<Duration>,

    /// Override max versions
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_versions: Option<usize>,

    /// Override min versions
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_versions: Option<usize>,
}

impl RetentionOverride {
    /// Create a new override with max age
    pub fn with_max_age(age: Duration) -> Self {
        Self {
            max_age: Some(age),
            ..Default::default()
        }
    }

    /// Create a new override with max versions
    pub fn with_max_versions(versions: usize) -> Self {
        Self {
            max_versions: Some(versions),
            ..Default::default()
        }
    }
}

/// Statistics about retention cleanup
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct RetentionStats {
    /// Total keys processed
    pub keys_processed: u64,
    /// Total versions removed
    pub versions_removed: u64,
    /// Total keys fully removed
    pub keys_removed: u64,
    /// Bytes reclaimed
    pub bytes_reclaimed: u64,
    /// Duration of cleanup
    pub duration_ms: u64,
}

/// Simple glob pattern matching
fn glob_match(pattern: &str, text: &str) -> bool {
    let mut pattern_chars = pattern.chars().peekable();
    let mut text_chars = text.chars().peekable();

    while let Some(p) = pattern_chars.next() {
        match p {
            '*' => {
                // Match any sequence
                if pattern_chars.peek().is_none() {
                    return true; // * at end matches everything
                }

                // Try matching from each position
                let remaining_pattern: String = pattern_chars.collect();
                let mut remaining_text = String::new();

                while let Some(t) = text_chars.next() {
                    remaining_text.push(t);
                    let _full_remaining: String =
                        remaining_text.clone() + &text_chars.clone().collect::<String>();
                    if glob_match(&remaining_pattern, &text_chars.clone().collect::<String>()) {
                        return true;
                    }
                }

                return glob_match(&remaining_pattern, "");
            }
            '?' => {
                // Match any single character
                if text_chars.next().is_none() {
                    return false;
                }
            }
            c => {
                // Match literal character
                match text_chars.next() {
                    Some(t) if t == c => continue,
                    _ => return false,
                }
            }
        }
    }

    // Pattern exhausted, text should also be exhausted
    text_chars.next().is_none()
}

/// Serde module for optional Duration
mod option_duration_serde {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::time::Duration;

    pub fn serialize<S>(duration: &Option<Duration>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match duration {
            Some(d) => {
                let secs = d.as_secs();
                if secs >= 31536000 && secs % 31536000 == 0 {
                    format!("{}y", secs / 31536000).serialize(serializer)
                } else if secs >= 86400 && secs % 86400 == 0 {
                    format!("{}d", secs / 86400).serialize(serializer)
                } else if secs >= 3600 && secs % 3600 == 0 {
                    format!("{}h", secs / 3600).serialize(serializer)
                } else if secs >= 60 && secs % 60 == 0 {
                    format!("{}m", secs / 60).serialize(serializer)
                } else {
                    format!("{}s", secs).serialize(serializer)
                }
            }
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<Duration>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let opt: Option<String> = Option::deserialize(deserializer)?;
        match opt {
            Some(s) => parse_duration(&s)
                .map(Some)
                .ok_or_else(|| serde::de::Error::custom(format!("invalid duration: {}", s))),
            None => Ok(None),
        }
    }

    fn parse_duration(s: &str) -> Option<Duration> {
        let s = s.trim();
        if s.is_empty() {
            return None;
        }

        let (num, unit) = s.split_at(s.len() - 1);
        let value: u64 = num.parse().ok()?;

        let secs = match unit {
            "s" => value,
            "m" => value * 60,
            "h" => value * 3600,
            "d" => value * 86400,
            "w" => value * 604800,
            "y" => value * 31536000,
            _ => return None,
        };

        Some(Duration::from_secs(secs))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_retention_policy_default() {
        let policy = RetentionPolicy::default();
        assert_eq!(policy.max_age, Some(Duration::from_secs(7 * 86400)));
        assert_eq!(policy.max_versions, Some(1000));
        assert_eq!(policy.min_versions, 1);
    }

    #[test]
    fn test_retention_policy_builder() {
        let policy = RetentionPolicy::new()
            .with_max_age(Duration::from_secs(86400))
            .with_max_versions(100)
            .with_min_versions(5);

        assert_eq!(policy.max_age, Some(Duration::from_secs(86400)));
        assert_eq!(policy.max_versions, Some(100));
        assert_eq!(policy.min_versions, 5);
    }

    #[test]
    fn test_retention_policy_override() {
        let policy = RetentionPolicy::new()
            .with_max_age(Duration::from_secs(86400))
            .with_override(
                "audit:*",
                RetentionOverride::with_max_age(Duration::from_secs(7 * 365 * 86400)),
            )
            .with_override("cache:*", RetentionOverride::with_max_versions(10));

        // Regular key uses base policy
        let effective = policy.effective_for_key(&Bytes::from("user:123"));
        assert_eq!(effective.max_age, Some(Duration::from_secs(86400)));

        // Audit key uses override
        let effective = policy.effective_for_key(&Bytes::from("audit:login"));
        assert_eq!(
            effective.max_age,
            Some(Duration::from_secs(7 * 365 * 86400))
        );

        // Cache key uses override
        let effective = policy.effective_for_key(&Bytes::from("cache:page:1"));
        assert_eq!(effective.max_versions, Some(10));
    }

    #[test]
    fn test_glob_match_star() {
        assert!(glob_match("user:*", "user:123"));
        assert!(glob_match("user:*", "user:"));
        assert!(glob_match("user:*", "user:abc:xyz"));
        assert!(!glob_match("user:*", "admin:123"));
    }

    #[test]
    fn test_glob_match_question() {
        assert!(glob_match("user:?", "user:1"));
        assert!(!glob_match("user:?", "user:12"));
        assert!(!glob_match("user:?", "user:"));
    }

    #[test]
    fn test_glob_match_exact() {
        assert!(glob_match("exact", "exact"));
        assert!(!glob_match("exact", "exacty"));
        assert!(!glob_match("exact", "exac"));
    }

    #[test]
    fn test_glob_match_complex() {
        assert!(glob_match("user:*:profile", "user:123:profile"));
        assert!(glob_match("*:*:*", "a:b:c"));
        assert!(glob_match("prefix*suffix", "prefixmiddlesuffix"));
    }

    #[test]
    fn test_retention_override_creation() {
        let override1 = RetentionOverride::with_max_age(Duration::from_secs(3600));
        assert_eq!(override1.max_age, Some(Duration::from_secs(3600)));
        assert!(override1.max_versions.is_none());

        let override2 = RetentionOverride::with_max_versions(50);
        assert_eq!(override2.max_versions, Some(50));
        assert!(override2.max_age.is_none());
    }

    #[test]
    fn test_retention_stats_default() {
        let stats = RetentionStats::default();
        assert_eq!(stats.keys_processed, 0);
        assert_eq!(stats.versions_removed, 0);
    }
}
