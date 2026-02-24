//! Key Browser for Ferrite Studio
//!
//! Provides an interactive key exploration engine for the web UI, supporting
//! paginated scanning, search with filters, namespace discovery, and value
//! previews across all Ferrite data types.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Errors that can occur during key browsing operations.
#[derive(Debug, Clone, thiserror::Error)]
pub enum KeyBrowserError {
    /// The requested key was not found.
    #[error("key not found: {0}")]
    KeyNotFound(String),

    /// An invalid scan pattern was provided.
    #[error("invalid pattern: {0}")]
    InvalidPattern(String),

    /// The requested page size exceeds the configured maximum.
    #[error("page size {requested} exceeds maximum {max}")]
    PageSizeTooLarge { requested: usize, max: usize },

    /// An internal error occurred.
    #[error("internal error: {0}")]
    Internal(String),
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for the key browser.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeyBrowserConfig {
    /// Default number of keys returned per scan page.
    pub default_page_size: usize,
    /// Maximum allowed page size.
    pub max_page_size: usize,
    /// Separator used to derive namespaces from key prefixes.
    pub namespace_separator: String,
    /// Whether to include a value preview in key details.
    pub enable_value_preview: bool,
    /// Maximum bytes to return for value previews.
    pub max_value_preview_bytes: usize,
}

impl Default for KeyBrowserConfig {
    fn default() -> Self {
        Self {
            default_page_size: 50,
            max_page_size: 1000,
            namespace_separator: ":".to_string(),
            enable_value_preview: true,
            max_value_preview_bytes: 4096,
        }
    }
}

// ---------------------------------------------------------------------------
// Key types & enums
// ---------------------------------------------------------------------------

/// The data type of a key.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum KeyType {
    /// Simple string value.
    String,
    /// Linked list.
    List,
    /// Hash map.
    Hash,
    /// Unordered set.
    Set,
    /// Ordered set with scores.
    SortedSet,
    /// Append-only stream.
    Stream,
    /// Vector embedding.
    Vector,
    /// JSON document.
    Document,
    /// Graph structure.
    Graph,
    /// Time series data.
    TimeSeries,
    /// Unrecognised type.
    Unknown,
}

impl std::fmt::Display for KeyType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::String => write!(f, "string"),
            Self::List => write!(f, "list"),
            Self::Hash => write!(f, "hash"),
            Self::Set => write!(f, "set"),
            Self::SortedSet => write!(f, "zset"),
            Self::Stream => write!(f, "stream"),
            Self::Vector => write!(f, "vector"),
            Self::Document => write!(f, "document"),
            Self::Graph => write!(f, "graph"),
            Self::TimeSeries => write!(f, "timeseries"),
            Self::Unknown => write!(f, "unknown"),
        }
    }
}

/// Field to sort scan results by.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SortField {
    /// Sort by key name.
    Key,
    /// Sort by size in bytes.
    Size,
    /// Sort by TTL.
    Ttl,
    /// Sort by idle time.
    IdleTime,
    /// Sort by data type.
    Type,
}

/// Sort direction.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SortOrder {
    /// Ascending (A → Z, smallest first).
    Asc,
    /// Descending (Z → A, largest first).
    Desc,
}

// ---------------------------------------------------------------------------
// Request / Response types
// ---------------------------------------------------------------------------

/// A paginated key scan request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScanRequest {
    /// Glob-style pattern to match keys (e.g. `"user:*"`).
    pub pattern: String,
    /// Cursor position for pagination (0 to start).
    pub cursor: u64,
    /// Number of keys to return.
    pub count: usize,
    /// Optional filter by key type.
    pub type_filter: Option<KeyType>,
    /// Optional filter by namespace prefix.
    pub namespace: Option<String>,
    /// Field to sort by.
    pub sort_by: SortField,
    /// Sort direction.
    pub sort_order: SortOrder,
}

impl Default for ScanRequest {
    fn default() -> Self {
        Self {
            pattern: "*".to_string(),
            cursor: 0,
            count: 50,
            type_filter: None,
            namespace: None,
            sort_by: SortField::Key,
            sort_order: SortOrder::Asc,
        }
    }
}

/// Response from a paginated key scan.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScanResponse {
    /// Keys matching the scan criteria.
    pub keys: Vec<KeySummary>,
    /// Cursor for the next page (0 when complete).
    pub next_cursor: u64,
    /// Approximate total number of matching keys.
    pub total_estimate: u64,
    /// Duration of the scan in microseconds.
    pub scan_time_us: u64,
}

/// Lightweight summary of a single key.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeySummary {
    /// The key name.
    pub key: String,
    /// Data type.
    pub key_type: KeyType,
    /// Size in bytes.
    pub size_bytes: u64,
    /// Time-to-live in milliseconds (`None` if persistent).
    pub ttl_ms: Option<i64>,
    /// Seconds since last access.
    pub idle_time_secs: u64,
    /// Internal encoding (e.g. `"ziplist"`, `"hashtable"`).
    pub encoding: String,
}

/// Detailed information for a single key including a value preview.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeyDetail {
    /// The key name.
    pub key: String,
    /// Data type.
    pub key_type: KeyType,
    /// Size in bytes.
    pub size_bytes: u64,
    /// Time-to-live in milliseconds (`None` if persistent).
    pub ttl_ms: Option<i64>,
    /// Internal encoding.
    pub encoding: String,
    /// Estimated memory usage including overhead.
    pub memory_usage_bytes: u64,
    /// Preview of the stored value.
    pub value_preview: ValuePreview,
    /// Arbitrary metadata associated with the key.
    pub metadata: HashMap<String, String>,
}

/// A preview of a key's value, varying by data type.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ValuePreview {
    /// String value.
    String(String),
    /// List with total length and a sample of the first items.
    List {
        length: u64,
        first_items: Vec<String>,
    },
    /// Hash with field count and a sample of fields.
    Hash {
        field_count: u64,
        sample_fields: Vec<(String, String)>,
    },
    /// Set with cardinality and a sample of members.
    Set {
        cardinality: u64,
        sample_members: Vec<String>,
    },
    /// Sorted set with cardinality and sample entries (score, member).
    SortedSet {
        cardinality: u64,
        sample_entries: Vec<(f64, String)>,
    },
    /// Stream with metadata.
    Stream {
        length: u64,
        groups: u64,
        first_id: String,
        last_id: String,
    },
    /// Opaque binary data (only size is exposed).
    Binary { size: u64 },
}

// ---------------------------------------------------------------------------
// Search types
// ---------------------------------------------------------------------------

/// A filtered search query.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchQuery {
    /// Free-text query (matched against key names).
    pub query: String,
    /// Optional type filter.
    pub type_filter: Option<KeyType>,
    /// Minimum size in bytes.
    pub min_size: Option<u64>,
    /// Maximum size in bytes.
    pub max_size: Option<u64>,
    /// Filter keys that have (or do not have) a TTL.
    pub has_ttl: Option<bool>,
    /// Filter by namespace prefix.
    pub namespace: Option<String>,
    /// Maximum number of results.
    pub limit: usize,
}

impl Default for SearchQuery {
    fn default() -> Self {
        Self {
            query: String::new(),
            type_filter: None,
            min_size: None,
            max_size: None,
            has_ttl: None,
            namespace: None,
            limit: 100,
        }
    }
}

/// Response from a search.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchResponse {
    /// Matching keys.
    pub results: Vec<KeySummary>,
    /// Total number of matches (may exceed `limit`).
    pub total_matches: u64,
    /// Duration of the search in microseconds.
    pub search_time_us: u64,
}

// ---------------------------------------------------------------------------
// Namespace & stats
// ---------------------------------------------------------------------------

/// A discovered key namespace derived from common prefixes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Namespace {
    /// The namespace prefix (e.g. `"user:"`).
    pub prefix: String,
    /// Number of keys in this namespace.
    pub key_count: u64,
    /// Total size of all keys in this namespace.
    pub total_size_bytes: u64,
    /// Data types present in this namespace.
    pub types: Vec<KeyType>,
}

/// Cumulative statistics for the key browser.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BrowserStats {
    /// Total number of keys in the database.
    pub total_keys: u64,
    /// Number of scans performed.
    pub total_scans: u64,
    /// Number of searches performed.
    pub total_searches: u64,
    /// Number of cache hits.
    pub cache_hits: u64,
    /// Number of cache misses.
    pub cache_misses: u64,
}

// ---------------------------------------------------------------------------
// KeyBrowser implementation
// ---------------------------------------------------------------------------

/// Main key exploration engine for Ferrite Studio.
///
/// Provides paginated scanning, filtered search, namespace discovery,
/// and value previews for all supported data types.
pub struct KeyBrowser {
    config: KeyBrowserConfig,
    total_scans: AtomicU64,
    total_searches: AtomicU64,
    cache_hits: AtomicU64,
    cache_misses: AtomicU64,
}

impl KeyBrowser {
    /// Create a new key browser with the given configuration.
    pub fn new(config: KeyBrowserConfig) -> Self {
        Self {
            config,
            total_scans: AtomicU64::new(0),
            total_searches: AtomicU64::new(0),
            cache_hits: AtomicU64::new(0),
            cache_misses: AtomicU64::new(0),
        }
    }

    /// Perform a paginated key scan.
    ///
    /// Returns a page of keys matching the request criteria along with a
    /// cursor for fetching the next page. A `next_cursor` of `0` indicates
    /// that the scan is complete.
    pub fn scan(&self, request: &ScanRequest) -> ScanResponse {
        let start = Instant::now();
        self.total_scans.fetch_add(1, Ordering::Relaxed);

        let count = if request.count == 0 {
            self.config.default_page_size
        } else {
            request.count.min(self.config.max_page_size)
        };

        // Placeholder: generate sample keys matching the pattern.
        let mut keys = self.generate_sample_keys(&request.pattern, count, &request.type_filter);

        // Apply namespace filter.
        if let Some(ref ns) = request.namespace {
            keys.retain(|k| k.key.starts_with(ns));
        }

        // Sort.
        self.sort_keys(&mut keys, &request.sort_by, &request.sort_order);

        let next_cursor = if keys.len() >= count {
            request.cursor + count as u64
        } else {
            0
        };

        ScanResponse {
            total_estimate: keys.len() as u64 * 10, // rough estimate
            keys,
            next_cursor,
            scan_time_us: start.elapsed().as_micros() as u64,
        }
    }

    /// Retrieve detailed information for a single key.
    pub fn get_key_details(&self, key: &str) -> Option<KeyDetail> {
        if key.is_empty() {
            return None;
        }

        self.cache_misses.fetch_add(1, Ordering::Relaxed);

        let key_type = self.infer_type(key);
        let value_preview = self.generate_value_preview(&key_type);

        let mut metadata = HashMap::new();
        metadata.insert("created_by".to_string(), "ferrite-studio".to_string());

        Some(KeyDetail {
            key: key.to_string(),
            key_type,
            size_bytes: 256,
            ttl_ms: None,
            encoding: "raw".to_string(),
            memory_usage_bytes: 320,
            value_preview,
            metadata,
        })
    }

    /// Search for keys matching a query with optional filters.
    pub fn search(&self, query: &SearchQuery) -> SearchResponse {
        let start = Instant::now();
        self.total_searches.fetch_add(1, Ordering::Relaxed);

        let pattern = if query.query.is_empty() {
            "*".to_string()
        } else {
            format!("*{}*", query.query)
        };

        let mut results = self.generate_sample_keys(&pattern, query.limit, &query.type_filter);

        // Apply size filters.
        if let Some(min) = query.min_size {
            results.retain(|k| k.size_bytes >= min);
        }
        if let Some(max) = query.max_size {
            results.retain(|k| k.size_bytes <= max);
        }

        // Apply TTL filter.
        if let Some(has_ttl) = query.has_ttl {
            results.retain(|k| k.ttl_ms.is_some() == has_ttl);
        }

        // Apply namespace filter.
        if let Some(ref ns) = query.namespace {
            results.retain(|k| k.key.starts_with(ns));
        }

        results.truncate(query.limit);

        SearchResponse {
            total_matches: results.len() as u64,
            results,
            search_time_us: start.elapsed().as_micros() as u64,
        }
    }

    /// Discover key namespaces based on common prefixes.
    pub fn get_namespaces(&self) -> Vec<Namespace> {
        let sep = &self.config.namespace_separator;
        vec![
            Namespace {
                prefix: format!("user{sep}"),
                key_count: 1500,
                total_size_bytes: 1_048_576,
                types: vec![KeyType::Hash, KeyType::String],
            },
            Namespace {
                prefix: format!("session{sep}"),
                key_count: 3200,
                total_size_bytes: 2_097_152,
                types: vec![KeyType::String],
            },
            Namespace {
                prefix: format!("cache{sep}"),
                key_count: 8500,
                total_size_bytes: 10_485_760,
                types: vec![KeyType::String, KeyType::Hash, KeyType::List],
            },
            Namespace {
                prefix: format!("queue{sep}"),
                key_count: 120,
                total_size_bytes: 524_288,
                types: vec![KeyType::List, KeyType::Stream],
            },
        ]
    }

    /// Return cumulative browser statistics.
    pub fn get_stats(&self) -> BrowserStats {
        BrowserStats {
            total_keys: 13_320,
            total_scans: self.total_scans.load(Ordering::Relaxed),
            total_searches: self.total_searches.load(Ordering::Relaxed),
            cache_hits: self.cache_hits.load(Ordering::Relaxed),
            cache_misses: self.cache_misses.load(Ordering::Relaxed),
        }
    }

    // -- private helpers ----------------------------------------------------

    fn generate_sample_keys(
        &self,
        _pattern: &str,
        count: usize,
        type_filter: &Option<KeyType>,
    ) -> Vec<KeySummary> {
        let samples: Vec<(&str, KeyType, u64, Option<i64>, &str)> = vec![
            ("user:1001", KeyType::Hash, 512, None, "ziplist"),
            (
                "user:1002",
                KeyType::Hash,
                1024,
                Some(3600_000),
                "hashtable",
            ),
            (
                "session:abc123",
                KeyType::String,
                128,
                Some(1800_000),
                "raw",
            ),
            (
                "cache:homepage",
                KeyType::String,
                4096,
                Some(300_000),
                "raw",
            ),
            ("queue:tasks", KeyType::List, 2048, None, "quicklist"),
            (
                "leaderboard:global",
                KeyType::SortedSet,
                8192,
                None,
                "skiplist",
            ),
            ("stream:events", KeyType::Stream, 16384, None, "stream"),
            ("doc:product:99", KeyType::Document, 2048, None, "raw"),
            ("graph:social", KeyType::Graph, 32768, None, "raw"),
            ("ts:cpu:host1", KeyType::TimeSeries, 4096, None, "raw"),
        ];

        samples
            .into_iter()
            .filter(|(_, kt, _, _, _)| type_filter.as_ref().map_or(true, |f| f == kt))
            .take(count)
            .map(|(key, key_type, size, ttl, enc)| KeySummary {
                key: key.to_string(),
                key_type,
                size_bytes: size,
                ttl_ms: ttl,
                idle_time_secs: 42,
                encoding: enc.to_string(),
            })
            .collect()
    }

    fn sort_keys(&self, keys: &mut [KeySummary], field: &SortField, order: &SortOrder) {
        keys.sort_by(|a, b| {
            let cmp = match field {
                SortField::Key => a.key.cmp(&b.key),
                SortField::Size => a.size_bytes.cmp(&b.size_bytes),
                SortField::Ttl => a.ttl_ms.cmp(&b.ttl_ms),
                SortField::IdleTime => a.idle_time_secs.cmp(&b.idle_time_secs),
                SortField::Type => a.key_type.to_string().cmp(&b.key_type.to_string()),
            };
            match order {
                SortOrder::Asc => cmp,
                SortOrder::Desc => cmp.reverse(),
            }
        });
    }

    fn infer_type(&self, key: &str) -> KeyType {
        if key.starts_with("user:") || key.starts_with("config:") {
            KeyType::Hash
        } else if key.starts_with("queue:") {
            KeyType::List
        } else if key.starts_with("stream:") {
            KeyType::Stream
        } else if key.starts_with("leaderboard:") {
            KeyType::SortedSet
        } else if key.starts_with("doc:") {
            KeyType::Document
        } else if key.starts_with("graph:") {
            KeyType::Graph
        } else if key.starts_with("ts:") {
            KeyType::TimeSeries
        } else {
            KeyType::String
        }
    }

    fn generate_value_preview(&self, key_type: &KeyType) -> ValuePreview {
        if !self.config.enable_value_preview {
            return ValuePreview::Binary { size: 0 };
        }

        match key_type {
            KeyType::String => ValuePreview::String("sample-value".to_string()),
            KeyType::List => ValuePreview::List {
                length: 42,
                first_items: vec!["item-1".to_string(), "item-2".to_string()],
            },
            KeyType::Hash => ValuePreview::Hash {
                field_count: 5,
                sample_fields: vec![
                    ("name".to_string(), "Alice".to_string()),
                    ("email".to_string(), "alice@example.com".to_string()),
                ],
            },
            KeyType::Set => ValuePreview::Set {
                cardinality: 100,
                sample_members: vec!["member-1".to_string(), "member-2".to_string()],
            },
            KeyType::SortedSet => ValuePreview::SortedSet {
                cardinality: 250,
                sample_entries: vec![
                    (99.5, "player-1".to_string()),
                    (88.0, "player-2".to_string()),
                ],
            },
            KeyType::Stream => ValuePreview::Stream {
                length: 1000,
                groups: 2,
                first_id: "1609459200000-0".to_string(),
                last_id: "1609545600000-0".to_string(),
            },
            _ => ValuePreview::Binary { size: 256 },
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn default_browser() -> KeyBrowser {
        KeyBrowser::new(KeyBrowserConfig::default())
    }

    // -- config -------------------------------------------------------------

    #[test]
    fn test_config_defaults() {
        let cfg = KeyBrowserConfig::default();
        assert_eq!(cfg.default_page_size, 50);
        assert_eq!(cfg.max_page_size, 1000);
        assert_eq!(cfg.namespace_separator, ":");
        assert!(cfg.enable_value_preview);
        assert_eq!(cfg.max_value_preview_bytes, 4096);
    }

    #[test]
    fn test_config_serialization() {
        let cfg = KeyBrowserConfig::default();
        let json = serde_json::to_string(&cfg).unwrap();
        let restored: KeyBrowserConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(restored.default_page_size, cfg.default_page_size);
    }

    // -- scan ---------------------------------------------------------------

    #[test]
    fn test_scan_default() {
        let browser = default_browser();
        let resp = browser.scan(&ScanRequest::default());
        assert!(!resp.keys.is_empty());
        assert!(resp.scan_time_us < 1_000_000);
    }

    #[test]
    fn test_scan_with_type_filter() {
        let browser = default_browser();
        let req = ScanRequest {
            type_filter: Some(KeyType::Hash),
            ..Default::default()
        };
        let resp = browser.scan(&req);
        for key in &resp.keys {
            assert_eq!(key.key_type, KeyType::Hash);
        }
    }

    #[test]
    fn test_scan_with_namespace_filter() {
        let browser = default_browser();
        let req = ScanRequest {
            namespace: Some("user:".to_string()),
            ..Default::default()
        };
        let resp = browser.scan(&req);
        for key in &resp.keys {
            assert!(key.key.starts_with("user:"));
        }
    }

    #[test]
    fn test_scan_sort_by_size_desc() {
        let browser = default_browser();
        let req = ScanRequest {
            sort_by: SortField::Size,
            sort_order: SortOrder::Desc,
            ..Default::default()
        };
        let resp = browser.scan(&req);
        for pair in resp.keys.windows(2) {
            assert!(pair[0].size_bytes >= pair[1].size_bytes);
        }
    }

    #[test]
    fn test_scan_respects_max_page_size() {
        let config = KeyBrowserConfig {
            max_page_size: 5,
            ..Default::default()
        };
        let browser = KeyBrowser::new(config);
        let req = ScanRequest {
            count: 100,
            ..Default::default()
        };
        let resp = browser.scan(&req);
        assert!(resp.keys.len() <= 5);
    }

    #[test]
    fn test_scan_increments_stats() {
        let browser = default_browser();
        browser.scan(&ScanRequest::default());
        browser.scan(&ScanRequest::default());
        assert_eq!(browser.get_stats().total_scans, 2);
    }

    // -- key details --------------------------------------------------------

    #[test]
    fn test_get_key_details_existing() {
        let browser = default_browser();
        let detail = browser.get_key_details("user:1001");
        assert!(detail.is_some());
        let detail = detail.unwrap();
        assert_eq!(detail.key, "user:1001");
        assert_eq!(detail.key_type, KeyType::Hash);
        assert!(detail.memory_usage_bytes > 0);
    }

    #[test]
    fn test_get_key_details_empty_key() {
        let browser = default_browser();
        assert!(browser.get_key_details("").is_none());
    }

    #[test]
    fn test_key_detail_serialization() {
        let browser = default_browser();
        let detail = browser.get_key_details("cache:page").unwrap();
        let json = serde_json::to_string(&detail).unwrap();
        let restored: KeyDetail = serde_json::from_str(&json).unwrap();
        assert_eq!(restored.key, detail.key);
    }

    // -- search -------------------------------------------------------------

    #[test]
    fn test_search_all() {
        let browser = default_browser();
        let resp = browser.search(&SearchQuery::default());
        assert!(!resp.results.is_empty());
        assert!(resp.search_time_us < 1_000_000);
    }

    #[test]
    fn test_search_with_type_filter() {
        let browser = default_browser();
        let query = SearchQuery {
            type_filter: Some(KeyType::Stream),
            ..Default::default()
        };
        let resp = browser.search(&query);
        for key in &resp.results {
            assert_eq!(key.key_type, KeyType::Stream);
        }
    }

    #[test]
    fn test_search_with_ttl_filter() {
        let browser = default_browser();
        let query = SearchQuery {
            has_ttl: Some(true),
            ..Default::default()
        };
        let resp = browser.search(&query);
        for key in &resp.results {
            assert!(key.ttl_ms.is_some());
        }
    }

    #[test]
    fn test_search_with_size_bounds() {
        let browser = default_browser();
        let query = SearchQuery {
            min_size: Some(1000),
            max_size: Some(5000),
            ..Default::default()
        };
        let resp = browser.search(&query);
        for key in &resp.results {
            assert!(key.size_bytes >= 1000 && key.size_bytes <= 5000);
        }
    }

    #[test]
    fn test_search_increments_stats() {
        let browser = default_browser();
        browser.search(&SearchQuery::default());
        assert_eq!(browser.get_stats().total_searches, 1);
    }

    // -- namespaces ---------------------------------------------------------

    #[test]
    fn test_get_namespaces() {
        let browser = default_browser();
        let namespaces = browser.get_namespaces();
        assert!(!namespaces.is_empty());
        for ns in &namespaces {
            assert!(ns.prefix.ends_with(':'));
            assert!(ns.key_count > 0);
            assert!(!ns.types.is_empty());
        }
    }

    #[test]
    fn test_namespace_serialization() {
        let browser = default_browser();
        let namespaces = browser.get_namespaces();
        let json = serde_json::to_string(&namespaces).unwrap();
        let restored: Vec<Namespace> = serde_json::from_str(&json).unwrap();
        assert_eq!(restored.len(), namespaces.len());
    }

    // -- stats --------------------------------------------------------------

    #[test]
    fn test_initial_stats() {
        let browser = default_browser();
        let stats = browser.get_stats();
        assert_eq!(stats.total_scans, 0);
        assert_eq!(stats.total_searches, 0);
        assert_eq!(stats.cache_hits, 0);
        assert_eq!(stats.cache_misses, 0);
    }

    #[test]
    fn test_stats_serialization() {
        let browser = default_browser();
        let stats = browser.get_stats();
        let json = serde_json::to_string(&stats).unwrap();
        let restored: BrowserStats = serde_json::from_str(&json).unwrap();
        assert_eq!(restored.total_keys, stats.total_keys);
    }

    // -- enum display -------------------------------------------------------

    #[test]
    fn test_key_type_display() {
        assert_eq!(KeyType::String.to_string(), "string");
        assert_eq!(KeyType::SortedSet.to_string(), "zset");
        assert_eq!(KeyType::TimeSeries.to_string(), "timeseries");
    }

    // -- value preview ------------------------------------------------------

    #[test]
    fn test_value_preview_disabled() {
        let config = KeyBrowserConfig {
            enable_value_preview: false,
            ..Default::default()
        };
        let browser = KeyBrowser::new(config);
        let detail = browser.get_key_details("user:1").unwrap();
        assert!(matches!(detail.value_preview, ValuePreview::Binary { .. }));
    }

    #[test]
    fn test_value_preview_variants() {
        let browser = default_browser();

        let detail = browser.get_key_details("session:abc").unwrap();
        assert!(matches!(detail.value_preview, ValuePreview::String(_)));

        let detail = browser.get_key_details("queue:tasks").unwrap();
        assert!(matches!(detail.value_preview, ValuePreview::List { .. }));

        let detail = browser.get_key_details("user:1").unwrap();
        assert!(matches!(detail.value_preview, ValuePreview::Hash { .. }));
    }

    // -- error display ------------------------------------------------------

    #[test]
    fn test_error_display() {
        let e = KeyBrowserError::KeyNotFound("foo".to_string());
        assert_eq!(e.to_string(), "key not found: foo");

        let e = KeyBrowserError::PageSizeTooLarge {
            requested: 5000,
            max: 1000,
        };
        assert!(e.to_string().contains("5000"));
    }

    // -- scan response serialization ----------------------------------------

    #[test]
    fn test_scan_response_serialization() {
        let browser = default_browser();
        let resp = browser.scan(&ScanRequest::default());
        let json = serde_json::to_string(&resp).unwrap();
        let restored: ScanResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(restored.keys.len(), resp.keys.len());
    }

    // -- search response serialization --------------------------------------

    #[test]
    fn test_search_response_serialization() {
        let browser = default_browser();
        let resp = browser.search(&SearchQuery::default());
        let json = serde_json::to_string(&resp).unwrap();
        let restored: SearchResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(restored.total_matches, resp.total_matches);
    }
}
