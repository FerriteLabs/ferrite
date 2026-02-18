//! Configuration for the embedded Ferrite API.
//!
//! Provides [`EmbeddedConfig`] and [`EvictionPolicy`] for tuning the
//! embedded database's memory limits, persistence, and eviction behaviour.

use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Eviction policy applied when the database reaches its memory limit.
///
/// Mirrors the Redis `maxmemory-policy` semantics so users migrating from
/// Redis see familiar behaviour.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum EvictionPolicy {
    /// Return errors when memory limit is reached (no eviction).
    NoEviction,
    /// Evict least-recently-used keys from the entire keyspace.
    AllKeysLru,
    /// Evict least-frequently-used keys from the entire keyspace.
    AllKeysLfu,
    /// Evict random keys from the entire keyspace.
    AllKeysRandom,
    /// Evict least-recently-used keys that have an expiry set.
    VolatileLru,
    /// Evict least-frequently-used keys that have an expiry set.
    VolatileLfu,
    /// Evict keys with the nearest TTL first (volatile keys only).
    VolatileTtl,
    /// Evict random keys that have an expiry set.
    VolatileRandom,
}

impl Default for EvictionPolicy {
    fn default() -> Self {
        Self::NoEviction
    }
}

impl EvictionPolicy {
    /// Return the Redis-compatible configuration string for this policy.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::NoEviction => "noeviction",
            Self::AllKeysLru => "allkeys-lru",
            Self::AllKeysLfu => "allkeys-lfu",
            Self::AllKeysRandom => "allkeys-random",
            Self::VolatileLru => "volatile-lru",
            Self::VolatileLfu => "volatile-lfu",
            Self::VolatileTtl => "volatile-ttl",
            Self::VolatileRandom => "volatile-random",
        }
    }
}

/// Configuration for the high-level embedded Ferrite API ([`super::Ferrite`]).
///
/// All fields have sensible defaults; the builder API (`Ferrite::builder()`)
/// is the preferred way to construct this struct.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EmbeddedConfig {
    /// Maximum memory budget in bytes. When this limit is reached the
    /// configured [`EvictionPolicy`] is applied.
    ///
    /// Default: 256 MiB.
    pub max_memory: usize,

    /// Number of logical databases (Redis SELECT index 0..N-1).
    ///
    /// Default: 16.
    pub databases: u8,

    /// Whether data should be persisted to disk.
    ///
    /// When `false`, the database is purely in-memory and all data is lost
    /// when the process exits or the [`super::Ferrite`] instance is dropped.
    ///
    /// Default: `false`.
    pub persistence: bool,

    /// Directory for on-disk data files when persistence is enabled.
    ///
    /// Ignored when `persistence` is `false`.
    pub data_dir: Option<PathBuf>,

    /// Policy applied when `max_memory` is reached.
    ///
    /// Default: [`EvictionPolicy::NoEviction`].
    pub eviction_policy: EvictionPolicy,

    /// Enable transparent LZ4 compression for values larger than the
    /// inline threshold. Saves memory at the cost of a small CPU overhead
    /// on reads.
    ///
    /// Default: `false`.
    pub compression: bool,
}

impl Default for EmbeddedConfig {
    fn default() -> Self {
        Self {
            max_memory: 256 * 1024 * 1024, // 256 MiB
            databases: 16,
            persistence: false,
            data_dir: None,
            eviction_policy: EvictionPolicy::default(),
            compression: false,
        }
    }
}

/// Parse a human-readable memory string into bytes.
///
/// Accepts sizes like `"128mb"`, `"1gb"`, `"512kb"`, `"1024"` (bytes).
/// Case-insensitive.
pub(crate) fn parse_memory_string(s: &str) -> Option<usize> {
    let s = s.trim().to_lowercase();

    if let Ok(bytes) = s.parse::<usize>() {
        return Some(bytes);
    }

    let (num_str, multiplier) = if s.ends_with("gb") {
        (&s[..s.len() - 2], 1024 * 1024 * 1024)
    } else if s.ends_with("mb") {
        (&s[..s.len() - 2], 1024 * 1024)
    } else if s.ends_with("kb") {
        (&s[..s.len() - 2], 1024)
    } else if s.ends_with('b') {
        (&s[..s.len() - 1], 1)
    } else {
        return None;
    };

    let num: usize = num_str.trim().parse().ok()?;
    num.checked_mul(multiplier)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let cfg = EmbeddedConfig::default();
        assert_eq!(cfg.max_memory, 256 * 1024 * 1024);
        assert_eq!(cfg.databases, 16);
        assert!(!cfg.persistence);
        assert!(cfg.data_dir.is_none());
        assert_eq!(cfg.eviction_policy, EvictionPolicy::NoEviction);
        assert!(!cfg.compression);
    }

    #[test]
    fn test_eviction_policy_as_str() {
        assert_eq!(EvictionPolicy::NoEviction.as_str(), "noeviction");
        assert_eq!(EvictionPolicy::AllKeysLru.as_str(), "allkeys-lru");
        assert_eq!(EvictionPolicy::VolatileTtl.as_str(), "volatile-ttl");
    }

    #[test]
    fn test_parse_memory_string() {
        assert_eq!(parse_memory_string("256mb"), Some(256 * 1024 * 1024));
        assert_eq!(parse_memory_string("1gb"), Some(1024 * 1024 * 1024));
        assert_eq!(parse_memory_string("512kb"), Some(512 * 1024));
        assert_eq!(parse_memory_string("1024"), Some(1024));
        assert_eq!(parse_memory_string("100b"), Some(100));
        assert_eq!(parse_memory_string("  128MB  "), Some(128 * 1024 * 1024));
        assert!(parse_memory_string("invalid").is_none());
        assert!(parse_memory_string("").is_none());
    }

    #[test]
    fn test_eviction_policy_default() {
        let policy = EvictionPolicy::default();
        assert_eq!(policy, EvictionPolicy::NoEviction);
    }
}
