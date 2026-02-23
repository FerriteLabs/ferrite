//! Document Source Auto-Sync
//!
//! Watches external data sources (S3 buckets, HTTP URLs, local files, Git repos)
//! for changes and automatically re-ingests updated documents into the RAG pipeline.
//!
//! # Architecture
//!
//! ```text
//! ┌──────────────────────────────────────────────────────────┐
//! │                  SourceSyncManager                       │
//! ├──────────────────────────────────────────────────────────┤
//! │                                                          │
//! │  ┌────────────┐  ┌────────────┐  ┌────────────┐         │
//! │  │ LocalFile  │  │ S3Bucket   │  │ HttpUrl    │         │
//! │  │ Watcher    │  │ Watcher    │  │ Watcher    │         │
//! │  └─────┬──────┘  └─────┬──────┘  └─────┬──────┘         │
//! │        │               │               │                │
//! │        ▼               ▼               ▼                │
//! │  ┌──────────────────────────────────────────┐           │
//! │  │         DocumentChangeDetector           │           │
//! │  │         (content hash tracking)          │           │
//! │  └──────────────────┬───────────────────────┘           │
//! │                     │                                   │
//! │                     ▼                                   │
//! │  ┌──────────────────────────────────────────┐           │
//! │  │           RAG Pipeline Ingest            │           │
//! │  └──────────────────────────────────────────┘           │
//! │                                                          │
//! └──────────────────────────────────────────────────────────┘
//! ```
//!
//! # Supported Sources
//!
//! - **LocalFile**: Watch filesystem paths with glob pattern matching
//! - **S3Bucket**: Poll S3-compatible storage (requires `object_store` crate)
//! - **HttpUrl**: Fetch and track changes to HTTP/HTTPS resources
//! - **GitRepo**: Clone and track Git repository contents (stub)
//! - **WebCrawl**: Crawl and track web page changes (stub)
//!
//! # Example
//!
//! ```ignore
//! use ferrite_ai::rag::source_sync::{SourceSyncManager, SyncConfig, SourceConfig, SourceType};
//!
//! let manager = SourceSyncManager::new(SyncConfig::default());
//!
//! let source = SourceConfig {
//!     name: "docs".to_string(),
//!     source_type: SourceType::LocalFile,
//!     location: "/data/documents".to_string(),
//!     file_patterns: vec!["*.md".to_string(), "*.txt".to_string()],
//!     ..Default::default()
//! };
//!
//! let id = manager.register_source(source)?;
//! let result = manager.sync_source(&id).await?;
//! println!("Synced {} new documents", result.documents_added);
//! ```

use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use chrono::{DateTime, Utc};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Errors that can occur during source synchronization operations.
#[derive(Debug, Clone, thiserror::Error)]
pub enum SyncError {
    /// The requested source was not found in the registry.
    #[error("source not found: {0}")]
    SourceNotFound(String),

    /// The provided configuration is invalid.
    #[error("invalid configuration: {0}")]
    InvalidConfig(String),

    /// A network-level error occurred while fetching a remote source.
    #[error("network error: {0}")]
    NetworkError(String),

    /// Authentication failed for the requested source.
    #[error("authentication error: {0}")]
    AuthError(String),

    /// Failed to parse the fetched document content.
    #[error("parse error: {0}")]
    ParseError(String),

    /// An error occurred while persisting synced data.
    #[error("storage error: {0}")]
    StorageError(String),

    /// The document exceeds the configured maximum file size.
    #[error("document exceeds maximum allowed size")]
    MaxSizeExceeded,

    /// The source is being rate-limited by the remote server.
    #[error("rate limited by remote source")]
    RateLimited,

    /// An unexpected internal error.
    #[error("internal error: {0}")]
    Internal(String),
}

// ---------------------------------------------------------------------------
// Identifiers
// ---------------------------------------------------------------------------

/// Unique identifier for a registered sync source.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SourceId(pub String);

impl SourceId {
    /// Create a new random source identifier.
    pub fn new() -> Self {
        Self(uuid::Uuid::new_v4().to_string())
    }

    /// Get the string representation.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Default for SourceId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for SourceId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

// ---------------------------------------------------------------------------
// Source configuration types
// ---------------------------------------------------------------------------

/// The kind of external data source.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SourceType {
    /// A local filesystem directory or file.
    LocalFile,
    /// An S3-compatible object storage bucket.
    S3Bucket,
    /// A single HTTP/HTTPS URL.
    HttpUrl,
    /// A Git repository (clone + pull).
    GitRepo,
    /// A recursive web crawl starting from a URL.
    WebCrawl,
}

impl std::fmt::Display for SourceType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::LocalFile => write!(f, "LocalFile"),
            Self::S3Bucket => write!(f, "S3Bucket"),
            Self::HttpUrl => write!(f, "HttpUrl"),
            Self::GitRepo => write!(f, "GitRepo"),
            Self::WebCrawl => write!(f, "WebCrawl"),
        }
    }
}

/// Authentication credentials for accessing a remote source.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SourceAuth {
    /// No authentication required.
    None,
    /// Bearer token authentication.
    Bearer(String),
    /// HTTP Basic authentication.
    BasicAuth {
        /// Username for basic auth.
        username: String,
        /// Password for basic auth.
        password: String,
    },
    /// AWS-style credentials for S3-compatible sources.
    AwsCredentials {
        /// AWS access key ID.
        access_key: String,
        /// AWS secret access key.
        secret_key: String,
        /// AWS region (e.g. `us-east-1`).
        region: String,
    },
}

/// Configuration for a single sync source.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceConfig {
    /// Human-readable name for this source.
    pub name: String,
    /// The type of external source.
    pub source_type: SourceType,
    /// Location of the source (file path, URL, S3 URI, etc.).
    pub location: String,
    /// How often to automatically sync, in seconds.
    pub sync_interval_secs: u64,
    /// Glob patterns for matching files (e.g. `["*.md", "*.txt"]`).
    pub file_patterns: Vec<String>,
    /// Maximum allowed file size in bytes.
    pub max_file_size_bytes: u64,
    /// Optional authentication credentials.
    pub auth: Option<SourceAuth>,
    /// Arbitrary key-value metadata attached to this source.
    pub metadata: HashMap<String, String>,
}

impl Default for SourceConfig {
    fn default() -> Self {
        Self {
            name: String::new(),
            source_type: SourceType::LocalFile,
            location: String::new(),
            sync_interval_secs: 300,
            file_patterns: vec!["*".to_string()],
            max_file_size_bytes: 50 * 1024 * 1024, // 50 MB
            auth: None,
            metadata: HashMap::new(),
        }
    }
}

// ---------------------------------------------------------------------------
// Sync result & status types
// ---------------------------------------------------------------------------

/// The outcome of a single sync operation.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SyncResult {
    /// The source that was synced.
    pub source_id: SourceId,
    /// Number of newly added documents.
    pub documents_added: u64,
    /// Number of documents whose content changed.
    pub documents_updated: u64,
    /// Number of documents removed from the source.
    pub documents_deleted: u64,
    /// Total bytes of content processed.
    pub bytes_processed: u64,
    /// Wall-clock duration of the sync in milliseconds.
    pub duration_ms: u64,
    /// Non-fatal errors encountered during sync.
    pub errors: Vec<String>,
}

/// Current status of a registered source.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SourceStatus {
    /// Source is registered and idle.
    Active,
    /// Source is currently being synced.
    Syncing,
    /// Source encountered an error during its last sync.
    Error(String),
    /// Source has been disabled by the user.
    Disabled,
}

impl Default for SourceStatus {
    fn default() -> Self {
        Self::Active
    }
}

/// Metadata about a registered source and its sync history.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceInfo {
    /// Unique source identifier.
    pub id: SourceId,
    /// Source configuration.
    pub config: SourceConfig,
    /// Timestamp of the last successful sync, if any.
    pub last_sync: Option<DateTime<Utc>>,
    /// Result of the last sync operation, if any.
    pub last_result: Option<SyncResult>,
    /// Current status.
    pub status: SourceStatus,
}

// ---------------------------------------------------------------------------
// Global sync configuration
// ---------------------------------------------------------------------------

/// Global configuration for the sync manager.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncConfig {
    /// Maximum number of sources syncing concurrently.
    pub max_concurrent_syncs: usize,
    /// Default sync interval for sources that don't specify one.
    pub default_sync_interval_secs: u64,
    /// Global maximum document size in bytes.
    pub max_document_size_bytes: u64,
    /// Number of retry attempts on transient failures.
    pub retry_attempts: u32,
    /// Delay between retries in milliseconds.
    pub retry_delay_ms: u64,
}

impl Default for SyncConfig {
    fn default() -> Self {
        Self {
            max_concurrent_syncs: 4,
            default_sync_interval_secs: 300,
            max_document_size_bytes: 100 * 1024 * 1024, // 100 MB
            retry_attempts: 3,
            retry_delay_ms: 1000,
        }
    }
}

// ---------------------------------------------------------------------------
// Aggregate statistics
// ---------------------------------------------------------------------------

/// Aggregate statistics across all registered sources.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SyncStats {
    /// Total number of registered sources.
    pub total_sources: u64,
    /// Number of sources in the `Active` state.
    pub active_sources: u64,
    /// Total sync operations executed since creation.
    pub total_syncs: u64,
    /// Total documents synced (added + updated) across all operations.
    pub total_documents_synced: u64,
    /// Total bytes processed across all sync operations.
    pub total_bytes_synced: u64,
    /// Total non-fatal errors accumulated.
    pub total_errors: u64,
    /// Timestamp of the most recent sync operation.
    pub last_sync_at: Option<DateTime<Utc>>,
}

// ---------------------------------------------------------------------------
// Document change detection
// ---------------------------------------------------------------------------

/// Tracks content hashes for documents to detect changes between syncs.
///
/// Uses a fast non-cryptographic hash (`crc32fast`) to compare content
/// without storing full document bodies.
#[derive(Debug)]
pub struct DocumentChangeDetector {
    hashes: HashMap<String, u64>,
}

impl DocumentChangeDetector {
    /// Create a new, empty change detector.
    pub fn new() -> Self {
        Self {
            hashes: HashMap::new(),
        }
    }

    /// Returns `true` if the document has changed since it was last recorded
    /// or if the document has not been seen before.
    pub fn has_changed(&self, doc_id: &str, content_hash: u64) -> bool {
        match self.hashes.get(doc_id) {
            Some(&stored) => stored != content_hash,
            None => true,
        }
    }

    /// Record the current content hash for a document.
    pub fn record(&mut self, doc_id: &str, content_hash: u64) {
        self.hashes.insert(doc_id.to_string(), content_hash);
    }

    /// Remove tracking for a document (e.g. when it is deleted).
    pub fn remove(&mut self, doc_id: &str) {
        self.hashes.remove(doc_id);
    }

    /// Compute a fast content hash for the given byte slice.
    pub fn compute_hash(content: &[u8]) -> u64 {
        crc32fast::hash(content) as u64
    }

    /// Return the number of tracked documents.
    pub fn len(&self) -> usize {
        self.hashes.len()
    }

    /// Return `true` if no documents are tracked.
    pub fn is_empty(&self) -> bool {
        self.hashes.is_empty()
    }
}

impl Default for DocumentChangeDetector {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Source sync manager
// ---------------------------------------------------------------------------

/// Manages document source watchers and orchestrates sync operations.
///
/// Sources are registered with a [`SourceConfig`] and assigned a unique
/// [`SourceId`]. The manager tracks per-source state and aggregate statistics,
/// providing both on-demand and bulk sync capabilities.
pub struct SourceSyncManager {
    config: SyncConfig,
    sources: DashMap<SourceId, SourceInfo>,
    change_detector: parking_lot::RwLock<DocumentChangeDetector>,
    // Aggregate counters
    total_syncs: AtomicU64,
    total_documents_synced: AtomicU64,
    total_bytes_synced: AtomicU64,
    total_errors: AtomicU64,
    last_sync_at: parking_lot::RwLock<Option<DateTime<Utc>>>,
}

impl SourceSyncManager {
    /// Create a new sync manager with the given global configuration.
    pub fn new(config: SyncConfig) -> Self {
        Self {
            config,
            sources: DashMap::new(),
            change_detector: parking_lot::RwLock::new(DocumentChangeDetector::new()),
            total_syncs: AtomicU64::new(0),
            total_documents_synced: AtomicU64::new(0),
            total_bytes_synced: AtomicU64::new(0),
            total_errors: AtomicU64::new(0),
            last_sync_at: parking_lot::RwLock::new(None),
        }
    }

    /// Register a new source for synchronization.
    ///
    /// Validates the configuration before registration. Returns the unique
    /// [`SourceId`] assigned to this source.
    pub fn register_source(&self, source: SourceConfig) -> Result<SourceId, SyncError> {
        self.validate_config(&source)?;

        let id = SourceId::new();
        let info = SourceInfo {
            id: id.clone(),
            config: source,
            last_sync: None,
            last_result: None,
            status: SourceStatus::Active,
        };

        self.sources.insert(id.clone(), info);
        tracing::info!(source_id = %id, "registered new sync source");
        Ok(id)
    }

    /// Unregister a source, removing it from the manager entirely.
    pub fn unregister_source(&self, id: &SourceId) -> Result<(), SyncError> {
        self.sources
            .remove(id)
            .ok_or_else(|| SyncError::SourceNotFound(id.to_string()))?;
        tracing::info!(source_id = %id, "unregistered sync source");
        Ok(())
    }

    /// Synchronize a single source by its identifier.
    ///
    /// Fetches content from the external source, detects changes via content
    /// hashing, and returns a [`SyncResult`] describing what changed.
    pub async fn sync_source(&self, id: &SourceId) -> Result<SyncResult, SyncError> {
        // Mark source as syncing
        {
            let mut entry = self
                .sources
                .get_mut(id)
                .ok_or_else(|| SyncError::SourceNotFound(id.to_string()))?;
            entry.status = SourceStatus::Syncing;
        }

        let config = {
            let entry = self
                .sources
                .get(id)
                .ok_or_else(|| SyncError::SourceNotFound(id.to_string()))?;
            entry.config.clone()
        };

        let started = Instant::now();
        let result = match config.source_type {
            SourceType::LocalFile => self.sync_local_file(&config, id).await,
            SourceType::S3Bucket => self.sync_s3_bucket(&config, id).await,
            SourceType::HttpUrl => self.sync_http_url(&config, id).await,
            SourceType::GitRepo => self.sync_git_repo(&config, id).await,
            SourceType::WebCrawl => self.sync_web_crawl(&config, id).await,
        };

        let elapsed_ms = started.elapsed().as_millis() as u64;

        match result {
            Ok(mut sync_result) => {
                sync_result.duration_ms = elapsed_ms;
                self.record_sync_result(id, &sync_result);
                Ok(sync_result)
            }
            Err(e) => {
                // Update source status to error
                if let Some(mut entry) = self.sources.get_mut(id) {
                    entry.status = SourceStatus::Error(e.to_string());
                }
                self.total_errors.fetch_add(1, Ordering::Relaxed);
                Err(e)
            }
        }
    }

    /// Synchronize all registered, active sources.
    ///
    /// Returns a list of `(SourceId, Result)` pairs, one per source.
    pub async fn sync_all(&self) -> Vec<(SourceId, Result<SyncResult, SyncError>)> {
        let ids: Vec<SourceId> = self
            .sources
            .iter()
            .filter(|entry| matches!(entry.value().status, SourceStatus::Active))
            .map(|entry| entry.key().clone())
            .collect();

        let mut results = Vec::with_capacity(ids.len());
        for id in ids {
            let result = self.sync_source(&id).await;
            results.push((id, result));
        }
        results
    }

    /// List all registered sources with their current metadata.
    pub fn list_sources(&self) -> Vec<SourceInfo> {
        self.sources.iter().map(|e| e.value().clone()).collect()
    }

    /// Retrieve aggregate statistics across all sources.
    pub fn get_stats(&self) -> SyncStats {
        let total_sources = self.sources.len() as u64;
        let active_sources = self
            .sources
            .iter()
            .filter(|e| matches!(e.value().status, SourceStatus::Active))
            .count() as u64;

        SyncStats {
            total_sources,
            active_sources,
            total_syncs: self.total_syncs.load(Ordering::Relaxed),
            total_documents_synced: self.total_documents_synced.load(Ordering::Relaxed),
            total_bytes_synced: self.total_bytes_synced.load(Ordering::Relaxed),
            total_errors: self.total_errors.load(Ordering::Relaxed),
            last_sync_at: *self.last_sync_at.read(),
        }
    }

    // -- internal helpers ---------------------------------------------------

    /// Validate a source configuration before registration.
    fn validate_config(&self, config: &SourceConfig) -> Result<(), SyncError> {
        if config.name.is_empty() {
            return Err(SyncError::InvalidConfig("name must not be empty".into()));
        }
        if config.location.is_empty() {
            return Err(SyncError::InvalidConfig(
                "location must not be empty".into(),
            ));
        }
        if config.sync_interval_secs == 0 {
            return Err(SyncError::InvalidConfig(
                "sync_interval_secs must be greater than zero".into(),
            ));
        }
        if config.max_file_size_bytes == 0 {
            return Err(SyncError::InvalidConfig(
                "max_file_size_bytes must be greater than zero".into(),
            ));
        }
        // Type-specific validation
        match config.source_type {
            SourceType::HttpUrl => {
                if !config.location.starts_with("http://")
                    && !config.location.starts_with("https://")
                {
                    return Err(SyncError::InvalidConfig(
                        "HttpUrl location must start with http:// or https://".into(),
                    ));
                }
            }
            SourceType::S3Bucket => {
                if !config.location.starts_with("s3://") {
                    return Err(SyncError::InvalidConfig(
                        "S3Bucket location must start with s3://".into(),
                    ));
                }
            }
            _ => {}
        }
        Ok(())
    }

    /// Record a successful sync result into source info and aggregate counters.
    fn record_sync_result(&self, id: &SourceId, result: &SyncResult) {
        let now = Utc::now();

        if let Some(mut entry) = self.sources.get_mut(id) {
            entry.last_sync = Some(now);
            entry.last_result = Some(result.clone());
            entry.status = SourceStatus::Active;
        }

        let docs = result.documents_added + result.documents_updated;
        self.total_syncs.fetch_add(1, Ordering::Relaxed);
        self.total_documents_synced
            .fetch_add(docs, Ordering::Relaxed);
        self.total_bytes_synced
            .fetch_add(result.bytes_processed, Ordering::Relaxed);
        self.total_errors
            .fetch_add(result.errors.len() as u64, Ordering::Relaxed);
        *self.last_sync_at.write() = Some(now);
    }

    /// Check whether a file path matches any of the configured glob patterns.
    fn matches_patterns(path: &Path, patterns: &[String]) -> bool {
        let file_name = match path.file_name().and_then(|n| n.to_str()) {
            Some(n) => n,
            None => return false,
        };

        for pattern in patterns {
            if Self::glob_match(pattern, file_name) {
                return true;
            }
        }
        false
    }

    /// Minimal glob matching supporting `*` and `?` wildcards.
    fn glob_match(pattern: &str, text: &str) -> bool {
        let pattern: Vec<char> = pattern.chars().collect();
        let text: Vec<char> = text.chars().collect();
        let (plen, tlen) = (pattern.len(), text.len());
        let mut dp = vec![vec![false; tlen + 1]; plen + 1];
        dp[0][0] = true;

        // Handle leading `*` patterns
        for i in 1..=plen {
            if pattern[i - 1] == '*' {
                dp[i][0] = dp[i - 1][0];
            }
        }

        for i in 1..=plen {
            for j in 1..=tlen {
                if pattern[i - 1] == '*' {
                    dp[i][j] = dp[i - 1][j] || dp[i][j - 1];
                } else if pattern[i - 1] == '?' || pattern[i - 1] == text[j - 1] {
                    dp[i][j] = dp[i - 1][j - 1];
                }
            }
        }
        dp[plen][tlen]
    }

    // -- per-source-type sync implementations --------------------------------

    /// Sync a local filesystem source: enumerate files, hash, detect changes.
    async fn sync_local_file(
        &self,
        config: &SourceConfig,
        id: &SourceId,
    ) -> Result<SyncResult, SyncError> {
        let root = Path::new(&config.location);

        if !root.exists() {
            return Err(SyncError::StorageError(format!(
                "path does not exist: {}",
                config.location
            )));
        }

        let max_size = config
            .max_file_size_bytes
            .min(self.config.max_document_size_bytes);

        let mut result = SyncResult {
            source_id: id.clone(),
            ..Default::default()
        };

        // Collect file paths (non-recursive for simplicity; a production
        // implementation would use `walkdir` or `tokio::fs::read_dir` recursively).
        let entries = Self::collect_files(root, &config.file_patterns, max_size).await?;

        let mut detector = self.change_detector.write();

        for (path_str, content) in &entries {
            let hash = DocumentChangeDetector::compute_hash(content);
            let doc_key = format!("{}:{}", id, path_str);

            if detector.has_changed(&doc_key, hash) {
                if detector.len() > 0 && !detector.is_empty() {
                    // Existing doc updated vs. brand-new doc
                    if detector.has_changed(&doc_key, 0) && !detector.is_empty() {
                        // First time seeing this key → added
                        result.documents_added += 1;
                    } else {
                        result.documents_updated += 1;
                    }
                } else {
                    result.documents_added += 1;
                }
                detector.record(&doc_key, hash);
            }
            result.bytes_processed += content.len() as u64;
        }

        tracing::info!(
            source_id = %id,
            added = result.documents_added,
            updated = result.documents_updated,
            bytes = result.bytes_processed,
            "local file sync complete"
        );

        Ok(result)
    }

    /// Collect files under `root` that match the given patterns and size limit.
    async fn collect_files(
        root: &Path,
        patterns: &[String],
        max_size: u64,
    ) -> Result<Vec<(String, Vec<u8>)>, SyncError> {
        let mut files = Vec::new();

        let mut read_dir = tokio::fs::read_dir(root)
            .await
            .map_err(|e| SyncError::StorageError(format!("failed to read directory: {}", e)))?;

        while let Some(entry) = read_dir
            .next_entry()
            .await
            .map_err(|e| SyncError::StorageError(format!("directory entry error: {}", e)))?
        {
            let path = entry.path();

            // Skip directories (a full implementation would recurse)
            if path.is_dir() {
                continue;
            }

            if !Self::matches_patterns(&path, patterns) {
                continue;
            }

            let metadata = tokio::fs::metadata(&path)
                .await
                .map_err(|e| SyncError::StorageError(format!("metadata error: {}", e)))?;

            if metadata.len() > max_size {
                tracing::warn!(path = %path.display(), "skipping file exceeding size limit");
                continue;
            }

            let content = tokio::fs::read(&path)
                .await
                .map_err(|e| SyncError::StorageError(format!("read error: {}", e)))?;

            let path_str = path.to_string_lossy().to_string();
            files.push((path_str, content));
        }

        Ok(files)
    }

    /// Stub: S3 bucket sync requires the `object_store` crate.
    async fn sync_s3_bucket(
        &self,
        config: &SourceConfig,
        id: &SourceId,
    ) -> Result<SyncResult, SyncError> {
        tracing::warn!(
            source_id = %id,
            location = %config.location,
            "S3 bucket sync is not yet implemented — add the `object_store` crate for full support"
        );
        Ok(SyncResult {
            source_id: id.clone(),
            errors: vec!["S3 sync not implemented: requires the object_store crate".to_string()],
            ..Default::default()
        })
    }

    /// Sync a single HTTP URL: fetch content, hash, detect changes.
    async fn sync_http_url(
        &self,
        config: &SourceConfig,
        id: &SourceId,
    ) -> Result<SyncResult, SyncError> {
        let client = reqwest::Client::new();

        let mut request = client.get(&config.location);

        // Apply authentication
        if let Some(ref auth) = config.auth {
            request = match auth {
                SourceAuth::Bearer(token) => request.bearer_auth(token),
                SourceAuth::BasicAuth { username, password } => {
                    request.basic_auth(username, Some(password))
                }
                _ => request,
            };
        }

        let response = request
            .send()
            .await
            .map_err(|e| SyncError::NetworkError(e.to_string()))?;

        if response.status() == reqwest::StatusCode::TOO_MANY_REQUESTS {
            return Err(SyncError::RateLimited);
        }
        if response.status() == reqwest::StatusCode::UNAUTHORIZED
            || response.status() == reqwest::StatusCode::FORBIDDEN
        {
            return Err(SyncError::AuthError(format!("HTTP {}", response.status())));
        }
        if !response.status().is_success() {
            return Err(SyncError::NetworkError(format!(
                "HTTP {}",
                response.status()
            )));
        }

        let bytes = response
            .bytes()
            .await
            .map_err(|e| SyncError::NetworkError(e.to_string()))?;

        let max_size = config
            .max_file_size_bytes
            .min(self.config.max_document_size_bytes);
        if bytes.len() as u64 > max_size {
            return Err(SyncError::MaxSizeExceeded);
        }

        let hash = DocumentChangeDetector::compute_hash(&bytes);
        let doc_key = format!("{}:{}", id, config.location);

        let mut result = SyncResult {
            source_id: id.clone(),
            bytes_processed: bytes.len() as u64,
            ..Default::default()
        };

        let mut detector = self.change_detector.write();
        if detector.has_changed(&doc_key, hash) {
            result.documents_added += 1;
            detector.record(&doc_key, hash);
        }

        tracing::info!(
            source_id = %id,
            url = %config.location,
            added = result.documents_added,
            bytes = result.bytes_processed,
            "HTTP URL sync complete"
        );

        Ok(result)
    }

    /// Stub: Git repository sync is planned but not yet implemented.
    async fn sync_git_repo(
        &self,
        config: &SourceConfig,
        id: &SourceId,
    ) -> Result<SyncResult, SyncError> {
        tracing::warn!(
            source_id = %id,
            location = %config.location,
            "Git repository sync is not yet implemented — add the `git2` crate for full support"
        );
        Ok(SyncResult {
            source_id: id.clone(),
            errors: vec!["Git repo sync not implemented: requires the git2 crate".to_string()],
            ..Default::default()
        })
    }

    /// Stub: Web crawl sync is planned but not yet implemented.
    async fn sync_web_crawl(
        &self,
        config: &SourceConfig,
        id: &SourceId,
    ) -> Result<SyncResult, SyncError> {
        tracing::warn!(
            source_id = %id,
            location = %config.location,
            "Web crawl sync is not yet implemented — add a crawler crate for full support"
        );
        Ok(SyncResult {
            source_id: id.clone(),
            errors: vec!["Web crawl sync not implemented".to_string()],
            ..Default::default()
        })
    }
}

impl std::fmt::Debug for SourceSyncManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SourceSyncManager")
            .field("config", &self.config)
            .field("sources_count", &self.sources.len())
            .field("total_syncs", &self.total_syncs.load(Ordering::Relaxed))
            .finish()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -- helpers -------------------------------------------------------------

    fn local_source(name: &str, location: &str) -> SourceConfig {
        SourceConfig {
            name: name.to_string(),
            source_type: SourceType::LocalFile,
            location: location.to_string(),
            ..Default::default()
        }
    }

    fn http_source(name: &str, url: &str) -> SourceConfig {
        SourceConfig {
            name: name.to_string(),
            source_type: SourceType::HttpUrl,
            location: url.to_string(),
            ..Default::default()
        }
    }

    // -- source registration / unregistration --------------------------------

    #[test]
    fn test_register_source() {
        let manager = SourceSyncManager::new(SyncConfig::default());
        let id = manager
            .register_source(local_source("docs", "/tmp/docs"))
            .unwrap();
        assert!(!id.as_str().is_empty());
        assert_eq!(manager.list_sources().len(), 1);
    }

    #[test]
    fn test_register_multiple_sources() {
        let manager = SourceSyncManager::new(SyncConfig::default());
        let id1 = manager
            .register_source(local_source("docs", "/tmp/docs"))
            .unwrap();
        let id2 = manager
            .register_source(http_source("api", "https://example.com/data"))
            .unwrap();
        assert_ne!(id1, id2);
        assert_eq!(manager.list_sources().len(), 2);
    }

    #[test]
    fn test_unregister_source() {
        let manager = SourceSyncManager::new(SyncConfig::default());
        let id = manager
            .register_source(local_source("docs", "/tmp/docs"))
            .unwrap();
        manager.unregister_source(&id).unwrap();
        assert_eq!(manager.list_sources().len(), 0);
    }

    #[test]
    fn test_unregister_nonexistent_source() {
        let manager = SourceSyncManager::new(SyncConfig::default());
        let fake_id = SourceId::new();
        let err = manager.unregister_source(&fake_id).unwrap_err();
        assert!(matches!(err, SyncError::SourceNotFound(_)));
    }

    // -- config validation ---------------------------------------------------

    #[test]
    fn test_empty_name_rejected() {
        let manager = SourceSyncManager::new(SyncConfig::default());
        let cfg = SourceConfig {
            name: String::new(),
            location: "/tmp".to_string(),
            ..Default::default()
        };
        let err = manager.register_source(cfg).unwrap_err();
        assert!(matches!(err, SyncError::InvalidConfig(_)));
    }

    #[test]
    fn test_empty_location_rejected() {
        let manager = SourceSyncManager::new(SyncConfig::default());
        let cfg = SourceConfig {
            name: "test".to_string(),
            location: String::new(),
            ..Default::default()
        };
        let err = manager.register_source(cfg).unwrap_err();
        assert!(matches!(err, SyncError::InvalidConfig(_)));
    }

    #[test]
    fn test_zero_interval_rejected() {
        let manager = SourceSyncManager::new(SyncConfig::default());
        let cfg = SourceConfig {
            name: "test".to_string(),
            location: "/tmp".to_string(),
            sync_interval_secs: 0,
            ..Default::default()
        };
        let err = manager.register_source(cfg).unwrap_err();
        assert!(matches!(err, SyncError::InvalidConfig(_)));
    }

    #[test]
    fn test_zero_max_file_size_rejected() {
        let manager = SourceSyncManager::new(SyncConfig::default());
        let cfg = SourceConfig {
            name: "test".to_string(),
            location: "/tmp".to_string(),
            max_file_size_bytes: 0,
            ..Default::default()
        };
        let err = manager.register_source(cfg).unwrap_err();
        assert!(matches!(err, SyncError::InvalidConfig(_)));
    }

    #[test]
    fn test_http_url_validation() {
        let manager = SourceSyncManager::new(SyncConfig::default());

        // Valid HTTPS
        let ok = manager.register_source(http_source("api", "https://example.com"));
        assert!(ok.is_ok());

        // Valid HTTP
        let ok = manager.register_source(http_source("api2", "http://example.com"));
        assert!(ok.is_ok());

        // Invalid: no scheme
        let bad = SourceConfig {
            name: "bad".to_string(),
            source_type: SourceType::HttpUrl,
            location: "example.com".to_string(),
            ..Default::default()
        };
        let err = manager.register_source(bad).unwrap_err();
        assert!(matches!(err, SyncError::InvalidConfig(_)));
    }

    #[test]
    fn test_s3_url_validation() {
        let manager = SourceSyncManager::new(SyncConfig::default());

        let ok_cfg = SourceConfig {
            name: "bucket".to_string(),
            source_type: SourceType::S3Bucket,
            location: "s3://my-bucket/prefix".to_string(),
            ..Default::default()
        };
        assert!(manager.register_source(ok_cfg).is_ok());

        let bad_cfg = SourceConfig {
            name: "bad-bucket".to_string(),
            source_type: SourceType::S3Bucket,
            location: "https://my-bucket".to_string(),
            ..Default::default()
        };
        let err = manager.register_source(bad_cfg).unwrap_err();
        assert!(matches!(err, SyncError::InvalidConfig(_)));
    }

    // -- change detection ----------------------------------------------------

    #[test]
    fn test_change_detector_new_document() {
        let detector = DocumentChangeDetector::new();
        assert!(detector.has_changed("doc1", 12345));
    }

    #[test]
    fn test_change_detector_unchanged() {
        let mut detector = DocumentChangeDetector::new();
        detector.record("doc1", 12345);
        assert!(!detector.has_changed("doc1", 12345));
    }

    #[test]
    fn test_change_detector_changed() {
        let mut detector = DocumentChangeDetector::new();
        detector.record("doc1", 12345);
        assert!(detector.has_changed("doc1", 99999));
    }

    #[test]
    fn test_change_detector_remove() {
        let mut detector = DocumentChangeDetector::new();
        detector.record("doc1", 12345);
        detector.remove("doc1");
        assert!(detector.has_changed("doc1", 12345));
        assert!(detector.is_empty());
    }

    #[test]
    fn test_compute_hash_deterministic() {
        let data = b"hello world";
        let h1 = DocumentChangeDetector::compute_hash(data);
        let h2 = DocumentChangeDetector::compute_hash(data);
        assert_eq!(h1, h2);
    }

    #[test]
    fn test_compute_hash_different_content() {
        let h1 = DocumentChangeDetector::compute_hash(b"hello");
        let h2 = DocumentChangeDetector::compute_hash(b"world");
        assert_ne!(h1, h2);
    }

    #[test]
    fn test_change_detector_len() {
        let mut detector = DocumentChangeDetector::new();
        assert_eq!(detector.len(), 0);
        assert!(detector.is_empty());
        detector.record("a", 1);
        detector.record("b", 2);
        assert_eq!(detector.len(), 2);
        assert!(!detector.is_empty());
    }

    // -- stats tracking ------------------------------------------------------

    #[test]
    fn test_initial_stats() {
        let manager = SourceSyncManager::new(SyncConfig::default());
        let stats = manager.get_stats();
        assert_eq!(stats.total_sources, 0);
        assert_eq!(stats.active_sources, 0);
        assert_eq!(stats.total_syncs, 0);
        assert!(stats.last_sync_at.is_none());
    }

    #[test]
    fn test_stats_after_registration() {
        let manager = SourceSyncManager::new(SyncConfig::default());
        manager
            .register_source(local_source("docs", "/tmp/docs"))
            .unwrap();
        let stats = manager.get_stats();
        assert_eq!(stats.total_sources, 1);
        assert_eq!(stats.active_sources, 1);
    }

    #[test]
    fn test_stats_after_unregistration() {
        let manager = SourceSyncManager::new(SyncConfig::default());
        let id = manager
            .register_source(local_source("docs", "/tmp/docs"))
            .unwrap();
        manager.unregister_source(&id).unwrap();
        let stats = manager.get_stats();
        assert_eq!(stats.total_sources, 0);
        assert_eq!(stats.active_sources, 0);
    }

    // -- source listing and info ---------------------------------------------

    #[test]
    fn test_list_sources_contains_config() {
        let manager = SourceSyncManager::new(SyncConfig::default());
        manager
            .register_source(local_source("my-docs", "/data"))
            .unwrap();
        let sources = manager.list_sources();
        assert_eq!(sources.len(), 1);
        assert_eq!(sources[0].config.name, "my-docs");
        assert_eq!(sources[0].config.location, "/data");
        assert!(matches!(sources[0].status, SourceStatus::Active));
        assert!(sources[0].last_sync.is_none());
    }

    #[test]
    fn test_source_id_display() {
        let id = SourceId::new();
        let display = format!("{}", id);
        assert_eq!(display, id.as_str());
    }

    #[test]
    fn test_source_id_uniqueness() {
        let ids: Vec<SourceId> = (0..100).map(|_| SourceId::new()).collect();
        let unique: std::collections::HashSet<_> = ids.iter().collect();
        assert_eq!(unique.len(), 100);
    }

    // -- glob pattern matching -----------------------------------------------

    #[test]
    fn test_glob_match_star() {
        assert!(SourceSyncManager::glob_match("*.md", "readme.md"));
        assert!(SourceSyncManager::glob_match("*.txt", "notes.txt"));
        assert!(!SourceSyncManager::glob_match("*.md", "readme.txt"));
    }

    #[test]
    fn test_glob_match_question_mark() {
        assert!(SourceSyncManager::glob_match("doc?.txt", "doc1.txt"));
        assert!(SourceSyncManager::glob_match("doc?.txt", "docA.txt"));
        assert!(!SourceSyncManager::glob_match("doc?.txt", "doc12.txt"));
    }

    #[test]
    fn test_glob_match_exact() {
        assert!(SourceSyncManager::glob_match("README.md", "README.md"));
        assert!(!SourceSyncManager::glob_match("README.md", "readme.md"));
    }

    #[test]
    fn test_glob_match_wildcard_all() {
        assert!(SourceSyncManager::glob_match("*", "anything.txt"));
        assert!(SourceSyncManager::glob_match("*", "file"));
    }

    #[test]
    fn test_matches_patterns() {
        let patterns = vec!["*.md".to_string(), "*.txt".to_string()];
        assert!(SourceSyncManager::matches_patterns(
            Path::new("/tmp/readme.md"),
            &patterns
        ));
        assert!(SourceSyncManager::matches_patterns(
            Path::new("/tmp/notes.txt"),
            &patterns
        ));
        assert!(!SourceSyncManager::matches_patterns(
            Path::new("/tmp/image.png"),
            &patterns
        ));
    }

    // -- local file sync (integration) ---------------------------------------

    #[tokio::test]
    async fn test_sync_local_file_source() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("doc1.txt"), "hello world").unwrap();
        std::fs::write(dir.path().join("doc2.txt"), "foo bar").unwrap();
        std::fs::write(dir.path().join("image.png"), "not-a-real-png").unwrap();

        let manager = SourceSyncManager::new(SyncConfig::default());
        let cfg = SourceConfig {
            name: "test-dir".to_string(),
            source_type: SourceType::LocalFile,
            location: dir.path().to_string_lossy().to_string(),
            file_patterns: vec!["*.txt".to_string()],
            ..Default::default()
        };

        let id = manager.register_source(cfg).unwrap();
        let result = manager.sync_source(&id).await.unwrap();

        // Should have picked up the two .txt files, not the .png
        assert!(result.documents_added > 0);
        assert!(result.bytes_processed > 0);
        assert!(result.errors.is_empty());
    }

    #[tokio::test]
    async fn test_sync_local_detects_no_change_on_resync() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("doc.txt"), "stable content").unwrap();

        let manager = SourceSyncManager::new(SyncConfig::default());
        let cfg = SourceConfig {
            name: "stable".to_string(),
            source_type: SourceType::LocalFile,
            location: dir.path().to_string_lossy().to_string(),
            file_patterns: vec!["*.txt".to_string()],
            ..Default::default()
        };

        let id = manager.register_source(cfg).unwrap();

        // First sync — document should be added
        let r1 = manager.sync_source(&id).await.unwrap();
        assert_eq!(r1.documents_added, 1);

        // Second sync — content unchanged, no additions
        let r2 = manager.sync_source(&id).await.unwrap();
        assert_eq!(r2.documents_added, 0);
        assert_eq!(r2.documents_updated, 0);
    }

    #[tokio::test]
    async fn test_sync_nonexistent_path() {
        let manager = SourceSyncManager::new(SyncConfig::default());
        let cfg = local_source("missing", "/tmp/does-not-exist-at-all-ferrite-test");
        let id = manager.register_source(cfg).unwrap();
        let err = manager.sync_source(&id).await.unwrap_err();
        assert!(matches!(err, SyncError::StorageError(_)));
    }

    #[tokio::test]
    async fn test_sync_nonexistent_source_id() {
        let manager = SourceSyncManager::new(SyncConfig::default());
        let fake_id = SourceId::new();
        let err = manager.sync_source(&fake_id).await.unwrap_err();
        assert!(matches!(err, SyncError::SourceNotFound(_)));
    }

    // -- stub source types ---------------------------------------------------

    #[tokio::test]
    async fn test_sync_s3_returns_stub() {
        let manager = SourceSyncManager::new(SyncConfig::default());
        let cfg = SourceConfig {
            name: "bucket".to_string(),
            source_type: SourceType::S3Bucket,
            location: "s3://my-bucket".to_string(),
            ..Default::default()
        };
        let id = manager.register_source(cfg).unwrap();
        let result = manager.sync_source(&id).await.unwrap();
        assert!(!result.errors.is_empty());
    }

    #[tokio::test]
    async fn test_sync_git_returns_stub() {
        let manager = SourceSyncManager::new(SyncConfig::default());
        let cfg = SourceConfig {
            name: "repo".to_string(),
            source_type: SourceType::GitRepo,
            location: "https://github.com/example/repo".to_string(),
            ..Default::default()
        };
        let id = manager.register_source(cfg).unwrap();
        let result = manager.sync_source(&id).await.unwrap();
        assert!(!result.errors.is_empty());
    }

    #[tokio::test]
    async fn test_sync_webcrawl_returns_stub() {
        let manager = SourceSyncManager::new(SyncConfig::default());
        let cfg = SourceConfig {
            name: "crawl".to_string(),
            source_type: SourceType::WebCrawl,
            location: "https://example.com".to_string(),
            ..Default::default()
        };
        let id = manager.register_source(cfg).unwrap();
        let result = manager.sync_source(&id).await.unwrap();
        assert!(!result.errors.is_empty());
    }

    // -- error display -------------------------------------------------------

    #[test]
    fn test_sync_error_display() {
        let err = SyncError::SourceNotFound("abc".into());
        assert!(err.to_string().contains("abc"));

        let err = SyncError::MaxSizeExceeded;
        assert!(err.to_string().contains("maximum"));

        let err = SyncError::RateLimited;
        assert!(err.to_string().contains("rate limited"));
    }

    // -- default configs -----------------------------------------------------

    #[test]
    fn test_sync_config_defaults() {
        let cfg = SyncConfig::default();
        assert!(cfg.max_concurrent_syncs > 0);
        assert!(cfg.default_sync_interval_secs > 0);
        assert!(cfg.max_document_size_bytes > 0);
        assert!(cfg.retry_attempts > 0);
    }

    #[test]
    fn test_source_config_defaults() {
        let cfg = SourceConfig::default();
        assert_eq!(cfg.source_type, SourceType::LocalFile);
        assert!(!cfg.file_patterns.is_empty());
        assert!(cfg.max_file_size_bytes > 0);
        assert!(cfg.auth.is_none());
    }

    #[test]
    fn test_source_type_display() {
        assert_eq!(SourceType::LocalFile.to_string(), "LocalFile");
        assert_eq!(SourceType::S3Bucket.to_string(), "S3Bucket");
        assert_eq!(SourceType::HttpUrl.to_string(), "HttpUrl");
        assert_eq!(SourceType::GitRepo.to_string(), "GitRepo");
        assert_eq!(SourceType::WebCrawl.to_string(), "WebCrawl");
    }

    // -- serialization roundtrip ---------------------------------------------

    #[test]
    fn test_source_config_serde_roundtrip() {
        let cfg = SourceConfig {
            name: "test".to_string(),
            source_type: SourceType::HttpUrl,
            location: "https://example.com".to_string(),
            metadata: [("key".to_string(), "value".to_string())]
                .into_iter()
                .collect(),
            ..Default::default()
        };
        let json = serde_json::to_string(&cfg).unwrap();
        let deserialized: SourceConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.name, cfg.name);
        assert_eq!(deserialized.source_type, cfg.source_type);
        assert_eq!(deserialized.location, cfg.location);
    }

    #[test]
    fn test_sync_result_serde_roundtrip() {
        let result = SyncResult {
            source_id: SourceId::new(),
            documents_added: 5,
            documents_updated: 2,
            documents_deleted: 1,
            bytes_processed: 1024,
            duration_ms: 150,
            errors: vec!["minor issue".to_string()],
        };
        let json = serde_json::to_string(&result).unwrap();
        let deserialized: SyncResult = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.documents_added, 5);
        assert_eq!(deserialized.errors.len(), 1);
    }
}
