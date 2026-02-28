//! Cloud Object Storage Backend for HybridLog Tiering
//!
//! This module implements a cloud object storage backend (S3/GCS/Azure) as the
//! 4th HybridLog tier with real-time cost tracking and a cost optimization
//! dashboard.
//!
//! ## Architecture
//!
//! The [`CloudTierBackend`] manages an in-process object index backed by a
//! pluggable cloud provider. All metadata is tracked locally so that cost
//! reports can be computed without querying the remote service.
//!
//! ## Usage
//!
//! ```rust,ignore
//! use ferrite_core::tiering::cloud_tier::{CloudTierBackend, CloudTierConfig, CloudProvider};
//!
//! let config = CloudTierConfig {
//!     provider: CloudProvider::LocalFilesystem,
//!     bucket: "test-bucket".into(),
//!     ..Default::default()
//! };
//! let backend = CloudTierBackend::new(config);
//! ```

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Return seconds since the Unix epoch.
fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// Bytes per gigabyte (as `f64`).
const GB: f64 = 1_073_741_824.0;

/// Assumed cost of keeping 1 GB in RAM for one month (USD).
const RAM_COST_PER_GB_MONTH: f64 = 10.0;

// ---------------------------------------------------------------------------
// CloudProvider
// ---------------------------------------------------------------------------

/// Supported cloud object-storage providers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum CloudProvider {
    /// Amazon S3
    S3,
    /// Google Cloud Storage
    GCS,
    /// Azure Blob Storage
    Azure,
    /// Local filesystem (for testing)
    LocalFilesystem,
}

impl Default for CloudProvider {
    fn default() -> Self {
        Self::S3
    }
}

// ---------------------------------------------------------------------------
// StorageClass
// ---------------------------------------------------------------------------

/// Object storage class controlling cost and retrieval characteristics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum StorageClass {
    /// Standard (hot) access tier.
    Standard,
    /// Infrequent-access tier — cheaper storage, higher retrieval cost.
    InfrequentAccess,
    /// Glacier / cold archive tier — very cheap, minutes-to-hours retrieval.
    Glacier,
    /// Deep archive — cheapest, hours-to-days retrieval.
    Archive,
}

impl Default for StorageClass {
    fn default() -> Self {
        Self::Standard
    }
}

// ---------------------------------------------------------------------------
// CloudTierConfig
// ---------------------------------------------------------------------------

/// Configuration for the cloud tier backend.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CloudTierConfig {
    /// Cloud provider to use.
    pub provider: CloudProvider,
    /// Target bucket / container name.
    pub bucket: String,
    /// Key prefix prepended to every stored object.
    pub prefix: String,
    /// Cloud region (e.g. `"us-east-1"`).
    pub region: String,
    /// Access key / service-account ID.
    pub access_key: Option<String>,
    /// Secret key / service-account secret.
    pub secret_key: Option<String>,
    /// Custom endpoint for S3-compatible services (e.g. MinIO).
    pub endpoint: Option<String>,
    /// Maximum allowed object size in bytes (default 64 MiB).
    pub max_object_size: usize,
    /// Maximum concurrent uploads.
    pub upload_concurrency: usize,
    /// Maximum concurrent downloads.
    pub download_concurrency: usize,
    /// Storage cost per GB per month (USD).
    pub cost_per_gb_month: f64,
    /// Cost per 1 000 PUT requests (USD).
    pub cost_per_1000_puts: f64,
    /// Cost per 1 000 GET requests (USD).
    pub cost_per_1000_gets: f64,
    /// Data-transfer / egress cost per GB (USD).
    pub cost_per_gb_transfer: f64,
}

impl Default for CloudTierConfig {
    fn default() -> Self {
        Self {
            provider: CloudProvider::default(),
            bucket: String::new(),
            prefix: "ferrite/data/".into(),
            region: "us-east-1".into(),
            access_key: None,
            secret_key: None,
            endpoint: None,
            max_object_size: 64 * 1024 * 1024, // 64 MiB
            upload_concurrency: 4,
            download_concurrency: 8,
            cost_per_gb_month: 0.023,
            cost_per_1000_puts: 0.005,
            cost_per_1000_gets: 0.0004,
            cost_per_gb_transfer: 0.09,
        }
    }
}

// ---------------------------------------------------------------------------
// ObjectMetadata
// ---------------------------------------------------------------------------

/// Metadata tracked for each object stored in the cloud tier.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectMetadata {
    /// Logical key identifying this object.
    pub key: String,
    /// Size of the stored payload in bytes.
    pub size_bytes: u64,
    /// Timestamp (seconds since epoch) when the object was first stored.
    pub stored_at: u64,
    /// Timestamp (seconds since epoch) of the most recent access.
    pub last_accessed: u64,
    /// Total number of times the object has been accessed.
    pub access_count: u64,
    /// Storage class used for this object.
    pub storage_class: StorageClass,
    /// ETag (content hash) returned by the provider, if available.
    pub etag: Option<String>,
}

// ---------------------------------------------------------------------------
// ObjectRef
// ---------------------------------------------------------------------------

/// A reference to a successfully stored cloud object.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectRef {
    /// Logical key.
    pub key: String,
    /// Bucket the object was stored in.
    pub bucket: String,
    /// Object size in bytes.
    pub size_bytes: u64,
    /// ETag / content hash.
    pub etag: String,
}

// ---------------------------------------------------------------------------
// ObjectOptions
// ---------------------------------------------------------------------------

/// Options controlling how an object is stored.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectOptions {
    /// Desired storage class.
    pub storage_class: StorageClass,
    /// Whether to compress the payload before upload.
    pub compression: bool,
    /// Whether to encrypt the payload before upload.
    pub encryption: bool,
    /// Optional time-to-live in seconds.
    pub ttl_secs: Option<u64>,
}

impl Default for ObjectOptions {
    fn default() -> Self {
        Self {
            storage_class: StorageClass::default(),
            compression: true,
            encryption: false,
            ttl_secs: None,
        }
    }
}

// ---------------------------------------------------------------------------
// UploadRequest / DownloadRequest
// ---------------------------------------------------------------------------

/// A pending upload request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UploadRequest {
    /// Logical key to upload.
    pub key: String,
    /// Priority (0 = highest).
    pub priority: u8,
    /// Timestamp when the request was created.
    pub requested_at: u64,
}

/// A pending download request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DownloadRequest {
    /// Logical key to download.
    pub key: String,
    /// Priority (0 = highest).
    pub priority: u8,
    /// Timestamp when the request was created.
    pub requested_at: u64,
}

// ---------------------------------------------------------------------------
// CloudCostReport
// ---------------------------------------------------------------------------

/// Real-time cost report for the cloud tier.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CloudCostReport {
    /// Reporting period label (e.g. `"monthly"`).
    pub period: String,
    /// Estimated monthly storage cost (USD).
    pub storage_cost: f64,
    /// Estimated PUT-request cost (USD).
    pub put_request_cost: f64,
    /// Estimated GET-request cost (USD).
    pub get_request_cost: f64,
    /// Estimated data-transfer / egress cost (USD).
    pub transfer_cost: f64,
    /// Sum of all cost components.
    pub total_cost: f64,
    /// Effective cost per GB stored.
    pub cost_per_gb: f64,
    /// Total data stored in GB.
    pub total_stored_gb: f64,
    /// Total number of objects.
    pub total_objects: u64,
    /// Projected annual cost (total_cost × 12).
    pub projected_annual_cost: f64,
    /// Absolute dollar savings compared to keeping all data in RAM.
    pub savings_vs_memory: f64,
    /// Savings as a percentage.
    pub savings_pct: f64,
    /// Per-class breakdown: (class, cost, bytes).
    pub cost_by_class: Vec<(StorageClass, f64, u64)>,
}

// ---------------------------------------------------------------------------
// BatchEvictResult
// ---------------------------------------------------------------------------

/// Result of a batch-evict operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchEvictResult {
    /// Number of entries that succeeded.
    pub succeeded: usize,
    /// Number of entries that failed.
    pub failed: usize,
    /// Total bytes successfully uploaded.
    pub total_bytes: u64,
    /// Per-key error messages for failures.
    pub errors: Vec<(String, String)>,
}

// ---------------------------------------------------------------------------
// CloudTierStats (atomic counters)
// ---------------------------------------------------------------------------

/// Atomic counters for cloud tier operations.
#[derive(Debug)]
pub struct CloudTierStats {
    /// Total upload operations.
    pub uploads: AtomicU64,
    /// Total download operations.
    pub downloads: AtomicU64,
    /// Total delete operations.
    pub deletes: AtomicU64,
    /// Total bytes uploaded.
    pub upload_bytes: AtomicU64,
    /// Total bytes downloaded.
    pub download_bytes: AtomicU64,
    /// Total upload errors.
    pub upload_errors: AtomicU64,
    /// Total download errors.
    pub download_errors: AtomicU64,
    /// Cache hits (object found locally).
    pub cache_hits: AtomicU64,
    /// Cache misses (object required cloud fetch).
    pub cache_misses: AtomicU64,
}

impl CloudTierStats {
    /// Create zeroed stats.
    fn new() -> Self {
        Self {
            uploads: AtomicU64::new(0),
            downloads: AtomicU64::new(0),
            deletes: AtomicU64::new(0),
            upload_bytes: AtomicU64::new(0),
            download_bytes: AtomicU64::new(0),
            upload_errors: AtomicU64::new(0),
            download_errors: AtomicU64::new(0),
            cache_hits: AtomicU64::new(0),
            cache_misses: AtomicU64::new(0),
        }
    }

    /// Take a point-in-time snapshot.
    fn snapshot(&self) -> CloudTierStatsSnapshot {
        CloudTierStatsSnapshot {
            uploads: self.uploads.load(Ordering::Relaxed),
            downloads: self.downloads.load(Ordering::Relaxed),
            deletes: self.deletes.load(Ordering::Relaxed),
            upload_bytes: self.upload_bytes.load(Ordering::Relaxed),
            download_bytes: self.download_bytes.load(Ordering::Relaxed),
            upload_errors: self.upload_errors.load(Ordering::Relaxed),
            download_errors: self.download_errors.load(Ordering::Relaxed),
            cache_hits: self.cache_hits.load(Ordering::Relaxed),
            cache_misses: self.cache_misses.load(Ordering::Relaxed),
        }
    }
}

// ---------------------------------------------------------------------------
// CloudTierStatsSnapshot
// ---------------------------------------------------------------------------

/// Serializable snapshot of cloud tier statistics.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CloudTierStatsSnapshot {
    /// Total upload operations.
    pub uploads: u64,
    /// Total download operations.
    pub downloads: u64,
    /// Total delete operations.
    pub deletes: u64,
    /// Total bytes uploaded.
    pub upload_bytes: u64,
    /// Total bytes downloaded.
    pub download_bytes: u64,
    /// Total upload errors.
    pub upload_errors: u64,
    /// Total download errors.
    pub download_errors: u64,
    /// Cache hits.
    pub cache_hits: u64,
    /// Cache misses.
    pub cache_misses: u64,
}

// ---------------------------------------------------------------------------
// CloudTierError
// ---------------------------------------------------------------------------

/// Errors that can occur during cloud tier operations.
#[derive(Debug, thiserror::Error)]
pub enum CloudTierError {
    /// Failed to connect to the cloud provider.
    #[error("connection failed: {0}")]
    ConnectionFailed(String),

    /// The object exceeds the configured maximum size.
    #[error("object too large: {0} bytes")]
    ObjectTooLarge(usize),

    /// The requested object was not found.
    #[error("not found: {0}")]
    NotFound(String),

    /// An upload operation failed.
    #[error("upload failed: {0}")]
    UploadFailed(String),

    /// A download operation failed.
    #[error("download failed: {0}")]
    DownloadFailed(String),

    /// Authentication with the provider failed.
    #[error("authentication failed")]
    AuthenticationFailed,

    /// The target bucket does not exist.
    #[error("bucket not found: {0}")]
    BucketNotFound(String),

    /// Storage quota has been exceeded.
    #[error("quota exceeded")]
    QuotaExceeded,

    /// The cloud tier is disabled.
    #[error("cloud tier disabled")]
    Disabled,
}

// ---------------------------------------------------------------------------
// CloudTierBackend
// ---------------------------------------------------------------------------

/// Cloud object-storage backend for the 4th HybridLog tier.
///
/// Manages an in-memory object index together with atomic operation counters
/// and queues for pending uploads/downloads. Data is stored in a simulated
/// object store (in-memory `HashMap`) that can be swapped for real S3/GCS/Azure
/// transport via the provider abstraction.
#[derive(Debug)]
pub struct CloudTierBackend {
    /// Backend configuration.
    config: CloudTierConfig,
    /// Atomic operation counters.
    stats: CloudTierStats,
    /// Object metadata index keyed by logical key.
    object_index: RwLock<HashMap<String, ObjectMetadata>>,
    /// Simulated object data store.
    object_data: RwLock<HashMap<String, Vec<u8>>>,
    /// Queue of pending upload requests.
    pending_uploads: RwLock<VecDeque<UploadRequest>>,
    /// Queue of pending download requests.
    pending_downloads: RwLock<VecDeque<DownloadRequest>>,
}

impl CloudTierBackend {
    /// Create a new cloud tier backend with the given configuration.
    pub fn new(config: CloudTierConfig) -> Self {
        Self {
            config,
            stats: CloudTierStats::new(),
            object_index: RwLock::new(HashMap::new()),
            object_data: RwLock::new(HashMap::new()),
            pending_uploads: RwLock::new(VecDeque::new()),
            pending_downloads: RwLock::new(VecDeque::new()),
        }
    }

    /// Store an object in the cloud tier.
    ///
    /// Returns an [`ObjectRef`] on success, or a [`CloudTierError`] if the
    /// object exceeds `max_object_size` or the upload fails.
    pub fn store_object(
        &self,
        key: &str,
        value: Vec<u8>,
        metadata: ObjectOptions,
    ) -> Result<ObjectRef, CloudTierError> {
        if value.len() > self.config.max_object_size {
            return Err(CloudTierError::ObjectTooLarge(value.len()));
        }

        let size = value.len() as u64;
        let now = now_secs();
        let etag = format!("{:016x}", crc32fast::hash(&value));

        let obj_meta = ObjectMetadata {
            key: key.to_string(),
            size_bytes: size,
            stored_at: now,
            last_accessed: now,
            access_count: 0,
            storage_class: metadata.storage_class,
            etag: Some(etag.clone()),
        };

        self.object_data.write().insert(key.to_string(), value);
        self.object_index.write().insert(key.to_string(), obj_meta);

        self.stats.uploads.fetch_add(1, Ordering::Relaxed);
        self.stats.upload_bytes.fetch_add(size, Ordering::Relaxed);

        Ok(ObjectRef {
            key: key.to_string(),
            bucket: self.config.bucket.clone(),
            size_bytes: size,
            etag,
        })
    }

    /// Retrieve an object from the cloud tier.
    ///
    /// Returns `Ok(None)` if the key does not exist.
    pub fn retrieve_object(&self, key: &str) -> Result<Option<Vec<u8>>, CloudTierError> {
        let data = self.object_data.read().get(key).cloned();
        if data.is_some() {
            // Update metadata.
            if let Some(meta) = self.object_index.write().get_mut(key) {
                meta.last_accessed = now_secs();
                meta.access_count = meta.access_count.saturating_add(1);
            }
            let size = data.as_ref().map_or(0, |d| d.len() as u64);
            self.stats.downloads.fetch_add(1, Ordering::Relaxed);
            self.stats
                .download_bytes
                .fetch_add(size, Ordering::Relaxed);
            self.stats.cache_hits.fetch_add(1, Ordering::Relaxed);
        } else {
            self.stats.cache_misses.fetch_add(1, Ordering::Relaxed);
        }
        Ok(data)
    }

    /// Delete an object from the cloud tier.
    ///
    /// Returns `true` if the object existed and was removed, `false` otherwise.
    pub fn delete_object(&self, key: &str) -> Result<bool, CloudTierError> {
        let removed_data = self.object_data.write().remove(key).is_some();
        let removed_meta = self.object_index.write().remove(key).is_some();
        if removed_data || removed_meta {
            self.stats.deletes.fetch_add(1, Ordering::Relaxed);
        }
        Ok(removed_data)
    }

    /// Check whether an object exists in the cloud tier.
    pub fn object_exists(&self, key: &str) -> bool {
        self.object_index.read().contains_key(key)
    }

    /// Return the total number of objects stored.
    pub fn object_count(&self) -> usize {
        self.object_index.read().len()
    }

    /// Return the total bytes stored across all objects.
    pub fn total_stored_bytes(&self) -> u64 {
        self.object_index
            .read()
            .values()
            .map(|m| m.size_bytes)
            .sum()
    }

    /// List objects whose key starts with `prefix`, up to `limit` results.
    pub fn list_objects(&self, prefix: &str, limit: usize) -> Vec<ObjectMetadata> {
        self.object_index
            .read()
            .values()
            .filter(|m| m.key.starts_with(prefix))
            .take(limit)
            .cloned()
            .collect()
    }

    /// Generate a real-time cost report for the cloud tier.
    ///
    /// Costs are calculated from atomic counters and the object index using
    /// the pricing parameters in [`CloudTierConfig`].
    pub fn cost_report(&self) -> CloudCostReport {
        let snap = self.stats.snapshot();
        let total_bytes = self.total_stored_bytes();
        let total_gb = total_bytes as f64 / GB;
        let total_objects = self.object_count() as u64;

        let storage_cost = total_gb * self.config.cost_per_gb_month;
        let put_request_cost = (snap.uploads as f64 / 1000.0) * self.config.cost_per_1000_puts;
        let get_request_cost = (snap.downloads as f64 / 1000.0) * self.config.cost_per_1000_gets;
        let transfer_cost =
            (snap.download_bytes as f64 / GB) * self.config.cost_per_gb_transfer;

        let total_cost = storage_cost + put_request_cost + get_request_cost + transfer_cost;
        let cost_per_gb = if total_gb > 0.0 {
            total_cost / total_gb
        } else {
            0.0
        };

        let ram_cost = total_gb * RAM_COST_PER_GB_MONTH;
        let savings_vs_memory = ram_cost - storage_cost;
        let savings_pct = if ram_cost > 0.0 {
            (savings_vs_memory / ram_cost) * 100.0
        } else {
            0.0
        };

        // Per-class breakdown.
        let mut class_map: HashMap<StorageClass, (f64, u64)> = HashMap::new();
        for meta in self.object_index.read().values() {
            let entry = class_map.entry(meta.storage_class).or_insert((0.0, 0));
            let obj_gb = meta.size_bytes as f64 / GB;
            entry.0 += obj_gb * self.config.cost_per_gb_month;
            entry.1 += meta.size_bytes;
        }
        let cost_by_class: Vec<(StorageClass, f64, u64)> = class_map
            .into_iter()
            .map(|(class, (cost, bytes))| (class, cost, bytes))
            .collect();

        CloudCostReport {
            period: "monthly".into(),
            storage_cost,
            put_request_cost,
            get_request_cost,
            transfer_cost,
            total_cost,
            cost_per_gb,
            total_stored_gb: total_gb,
            total_objects,
            projected_annual_cost: total_cost * 12.0,
            savings_vs_memory,
            savings_pct,
            cost_by_class,
        }
    }

    /// Take a point-in-time snapshot of operation statistics.
    pub fn stats(&self) -> CloudTierStatsSnapshot {
        self.stats.snapshot()
    }

    /// Evict a key from a hotter tier into the cloud tier.
    ///
    /// The `access_score` is stored in the metadata via the access count field
    /// for later promotion decisions.
    pub fn evict_to_cloud(
        &self,
        key: &str,
        value: Vec<u8>,
        access_score: f64,
    ) -> Result<ObjectRef, CloudTierError> {
        let opts = ObjectOptions::default();
        let obj_ref = self.store_object(key, value, opts)?;

        // Persist the access score as the initial access count.
        if let Some(meta) = self.object_index.write().get_mut(key) {
            meta.access_count = access_score as u64;
        }

        Ok(obj_ref)
    }

    /// Promote a key from the cloud tier back to a hotter tier.
    ///
    /// The object is retrieved and then deleted from the cloud tier so that it
    /// exists only in the caller's (hotter) tier.
    pub fn promote_from_cloud(&self, key: &str) -> Result<Option<Vec<u8>>, CloudTierError> {
        let data = self.retrieve_object(key)?;
        if data.is_some() {
            self.delete_object(key)?;
        }
        Ok(data)
    }

    /// Evict multiple entries in a single batch.
    pub fn batch_evict(&self, entries: Vec<(String, Vec<u8>, f64)>) -> BatchEvictResult {
        let mut succeeded: usize = 0;
        let mut failed: usize = 0;
        let mut total_bytes: u64 = 0;
        let mut errors: Vec<(String, String)> = Vec::new();

        for (key, value, score) in entries {
            let size = value.len() as u64;
            match self.evict_to_cloud(&key, value, score) {
                Ok(_) => {
                    succeeded += 1;
                    total_bytes += size;
                }
                Err(e) => {
                    failed += 1;
                    errors.push((key, e.to_string()));
                }
            }
        }

        BatchEvictResult {
            succeeded,
            failed,
            total_bytes,
            errors,
        }
    }

    /// Return a reference to the backend configuration.
    pub fn config(&self) -> &CloudTierConfig {
        &self.config
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper: build a default backend with the `LocalFilesystem` provider.
    fn test_backend() -> CloudTierBackend {
        let config = CloudTierConfig {
            provider: CloudProvider::LocalFilesystem,
            bucket: "test-bucket".into(),
            ..Default::default()
        };
        CloudTierBackend::new(config)
    }

    // -- store / retrieve / delete ------------------------------------------

    #[test]
    fn test_store_retrieve_delete() {
        let backend = test_backend();

        let obj_ref = backend
            .store_object("key1", b"hello".to_vec(), ObjectOptions::default())
            .expect("store should succeed");
        assert_eq!(obj_ref.key, "key1");
        assert_eq!(obj_ref.size_bytes, 5);
        assert_eq!(obj_ref.bucket, "test-bucket");
        assert!(!obj_ref.etag.is_empty());

        let data = backend
            .retrieve_object("key1")
            .expect("retrieve should succeed");
        assert_eq!(data, Some(b"hello".to_vec()));

        let deleted = backend
            .delete_object("key1")
            .expect("delete should succeed");
        assert!(deleted);

        let data = backend
            .retrieve_object("key1")
            .expect("retrieve should succeed");
        assert_eq!(data, None);
    }

    // -- cost report --------------------------------------------------------

    #[test]
    fn test_cost_report_calculation() {
        let backend = test_backend();

        // Store 1 GiB of data (simulated as one object).
        let one_gib = vec![0u8; 1024];
        for i in 0..1024 {
            backend
                .store_object(
                    &format!("obj-{i}"),
                    one_gib.clone(),
                    ObjectOptions::default(),
                )
                .expect("store should succeed");
        }
        // Total stored: 1024 * 1024 = 1 MiB.
        // Retrieve a few objects to generate GET / transfer costs.
        for i in 0..10 {
            let _ = backend.retrieve_object(&format!("obj-{i}"));
        }

        let report = backend.cost_report();
        assert_eq!(report.period, "monthly");
        assert_eq!(report.total_objects, 1024);
        assert!(report.storage_cost > 0.0, "storage cost should be > 0");
        assert!(
            report.put_request_cost > 0.0,
            "put request cost should be > 0"
        );
        assert!(
            report.get_request_cost > 0.0,
            "get request cost should be > 0"
        );
        assert!(report.transfer_cost > 0.0, "transfer cost should be > 0");
        let sum = report.storage_cost
            + report.put_request_cost
            + report.get_request_cost
            + report.transfer_cost;
        assert!(
            (report.total_cost - sum).abs() < 1e-10,
            "total_cost should equal sum of components"
        );
        assert!(
            (report.projected_annual_cost - report.total_cost * 12.0).abs() < 1e-10,
            "projected annual cost should be 12× monthly"
        );
        assert!(
            report.savings_vs_memory > 0.0,
            "cloud should be cheaper than RAM"
        );
        assert!(report.savings_pct > 0.0);
        assert!(!report.cost_by_class.is_empty());
    }

    // -- batch evict --------------------------------------------------------

    #[test]
    fn test_batch_evict() {
        let backend = test_backend();

        let entries = vec![
            ("a".into(), b"data-a".to_vec(), 1.0),
            ("b".into(), b"data-b".to_vec(), 2.0),
            ("c".into(), b"data-c".to_vec(), 3.0),
        ];

        let result = backend.batch_evict(entries);
        assert_eq!(result.succeeded, 3);
        assert_eq!(result.failed, 0);
        assert_eq!(result.total_bytes, 18); // 6 * 3
        assert!(result.errors.is_empty());

        assert_eq!(backend.object_count(), 3);
    }

    // -- batch evict with oversized entry -----------------------------------

    #[test]
    fn test_batch_evict_with_error() {
        let config = CloudTierConfig {
            provider: CloudProvider::LocalFilesystem,
            bucket: "test-bucket".into(),
            max_object_size: 4, // very small limit
            ..Default::default()
        };
        let backend = CloudTierBackend::new(config);

        let entries = vec![
            ("ok".into(), b"hi".to_vec(), 1.0),
            ("too-big".into(), b"this is way too large".to_vec(), 2.0),
        ];

        let result = backend.batch_evict(entries);
        assert_eq!(result.succeeded, 1);
        assert_eq!(result.failed, 1);
        assert_eq!(result.errors.len(), 1);
        assert_eq!(result.errors[0].0, "too-big");
    }

    // -- stats tracking -----------------------------------------------------

    #[test]
    fn test_stats_tracking() {
        let backend = test_backend();

        backend
            .store_object("x", b"value".to_vec(), ObjectOptions::default())
            .expect("store should succeed");
        let _ = backend.retrieve_object("x");
        let _ = backend.delete_object("x");

        let snap = backend.stats();
        assert_eq!(snap.uploads, 1);
        assert_eq!(snap.downloads, 1);
        assert_eq!(snap.deletes, 1);
        assert_eq!(snap.upload_bytes, 5);
        assert_eq!(snap.download_bytes, 5);
        assert_eq!(snap.cache_hits, 1);
        assert_eq!(snap.cache_misses, 0);
    }

    // -- evict and promote round-trip ---------------------------------------

    #[test]
    fn test_evict_and_promote_roundtrip() {
        let backend = test_backend();

        backend
            .evict_to_cloud("rt-key", b"round-trip".to_vec(), 42.0)
            .expect("evict should succeed");
        assert!(backend.object_exists("rt-key"));

        // Verify access score was persisted.
        {
            let idx = backend.object_index.read();
            let meta = idx.get("rt-key").expect("metadata should exist");
            assert_eq!(meta.access_count, 42);
        }

        let data = backend
            .promote_from_cloud("rt-key")
            .expect("promote should succeed");
        assert_eq!(data, Some(b"round-trip".to_vec()));

        // Object should no longer exist in cloud.
        assert!(!backend.object_exists("rt-key"));
    }

    // -- object listing -----------------------------------------------------

    #[test]
    fn test_list_objects() {
        let backend = test_backend();

        backend
            .store_object("user:1", b"a".to_vec(), ObjectOptions::default())
            .expect("store should succeed");
        backend
            .store_object("user:2", b"b".to_vec(), ObjectOptions::default())
            .expect("store should succeed");
        backend
            .store_object("session:1", b"c".to_vec(), ObjectOptions::default())
            .expect("store should succeed");

        let users = backend.list_objects("user:", 100);
        assert_eq!(users.len(), 2);
        for m in &users {
            assert!(m.key.starts_with("user:"));
        }

        // Limit should be respected.
        let limited = backend.list_objects("user:", 1);
        assert_eq!(limited.len(), 1);
    }

    // -- config defaults ----------------------------------------------------

    #[test]
    fn test_config_defaults() {
        let cfg = CloudTierConfig::default();
        assert_eq!(cfg.provider, CloudProvider::S3);
        assert_eq!(cfg.prefix, "ferrite/data/");
        assert_eq!(cfg.region, "us-east-1");
        assert_eq!(cfg.max_object_size, 64 * 1024 * 1024);
        assert_eq!(cfg.upload_concurrency, 4);
        assert_eq!(cfg.download_concurrency, 8);
        assert!((cfg.cost_per_gb_month - 0.023).abs() < f64::EPSILON);
        assert!((cfg.cost_per_1000_puts - 0.005).abs() < f64::EPSILON);
        assert!((cfg.cost_per_1000_gets - 0.0004).abs() < f64::EPSILON);
        assert!((cfg.cost_per_gb_transfer - 0.09).abs() < f64::EPSILON);
    }

    // -- error handling -----------------------------------------------------

    #[test]
    fn test_error_object_too_large() {
        let config = CloudTierConfig {
            provider: CloudProvider::LocalFilesystem,
            bucket: "b".into(),
            max_object_size: 8,
            ..Default::default()
        };
        let backend = CloudTierBackend::new(config);

        let result = backend.store_object("big", vec![0u8; 16], ObjectOptions::default());
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(err, CloudTierError::ObjectTooLarge(16)),
            "expected ObjectTooLarge, got {err:?}"
        );
    }

    #[test]
    fn test_retrieve_missing_key() {
        let backend = test_backend();
        let data = backend
            .retrieve_object("nonexistent")
            .expect("retrieve should succeed");
        assert_eq!(data, None);
    }

    #[test]
    fn test_delete_missing_key() {
        let backend = test_backend();
        let deleted = backend
            .delete_object("nonexistent")
            .expect("delete should succeed");
        assert!(!deleted);
    }

    #[test]
    fn test_error_display() {
        let err = CloudTierError::ConnectionFailed("timeout".into());
        assert_eq!(format!("{err}"), "connection failed: timeout");

        let err = CloudTierError::ObjectTooLarge(999);
        assert_eq!(format!("{err}"), "object too large: 999 bytes");

        let err = CloudTierError::AuthenticationFailed;
        assert_eq!(format!("{err}"), "authentication failed");

        let err = CloudTierError::Disabled;
        assert_eq!(format!("{err}"), "cloud tier disabled");
    }

    // -- promote from empty -------------------------------------------------

    #[test]
    fn test_promote_missing_returns_none() {
        let backend = test_backend();
        let data = backend
            .promote_from_cloud("ghost")
            .expect("promote should succeed");
        assert_eq!(data, None);
    }

    // -- total_stored_bytes / object_count ----------------------------------

    #[test]
    fn test_total_stored_bytes_and_count() {
        let backend = test_backend();
        assert_eq!(backend.total_stored_bytes(), 0);
        assert_eq!(backend.object_count(), 0);

        backend
            .store_object("a", vec![1; 100], ObjectOptions::default())
            .expect("store should succeed");
        backend
            .store_object("b", vec![2; 200], ObjectOptions::default())
            .expect("store should succeed");

        assert_eq!(backend.object_count(), 2);
        assert_eq!(backend.total_stored_bytes(), 300);
    }
}
