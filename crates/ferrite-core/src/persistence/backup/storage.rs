//! Backup storage backends
//!
//! This module provides storage backends for backups including local
//! filesystem and cloud storage (S3, GCS, Azure).

use std::io::{Read, Write};
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use parking_lot::Mutex;
use thiserror::Error;
use tokio::fs;
use tracing::{debug, info};

#[cfg(feature = "cloud")]
use crate::storage::cloud::{CloudBackend, CloudConfig};

/// Backup storage error
#[derive(Debug, Error)]
pub enum BackupStorageError {
    /// I/O error
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Cloud storage error
    #[error("Cloud storage error: {0}")]
    Cloud(String),

    /// Backup not found
    #[error("Backup not found: {0}")]
    NotFound(String),

    /// Compression error
    #[error("Compression error: {0}")]
    Compression(String),

    /// Invalid backup format
    #[error("Invalid backup format: {0}")]
    InvalidFormat(String),
}

/// Backup storage trait
#[async_trait]
pub trait BackupStorage: Send + Sync {
    /// Store a backup
    async fn store(&self, name: &str, data: &[u8]) -> Result<(), BackupStorageError>;

    /// Retrieve a backup
    async fn retrieve(&self, name: &str) -> Result<Bytes, BackupStorageError>;

    /// List available backups
    async fn list(&self) -> Result<Vec<String>, BackupStorageError>;

    /// Delete a backup
    async fn delete(&self, name: &str) -> Result<(), BackupStorageError>;

    /// Check if a backup exists
    async fn exists(&self, name: &str) -> Result<bool, BackupStorageError>;

    /// Get backup size in bytes
    async fn size(&self, name: &str) -> Result<u64, BackupStorageError>;
}

/// Local filesystem backup storage
pub struct LocalBackupStorage {
    /// Base directory for backups
    base_dir: PathBuf,
    /// Whether to compress backups
    compress: bool,
    /// Maximum number of backups to retain
    max_backups: usize,
}

impl LocalBackupStorage {
    /// Create a new local backup storage
    pub fn new(base_dir: PathBuf) -> Self {
        Self {
            base_dir,
            compress: true,
            max_backups: 10,
        }
    }

    /// Set compression enabled/disabled
    pub fn with_compression(mut self, compress: bool) -> Self {
        self.compress = compress;
        self
    }

    /// Set maximum number of backups to retain
    pub fn with_max_backups(mut self, max: usize) -> Self {
        self.max_backups = max;
        self
    }

    /// Get the full path for a backup
    fn backup_path(&self, name: &str) -> PathBuf {
        let filename = if self.compress && !name.ends_with(".gz") {
            format!("{}.ferrite.backup.gz", name)
        } else {
            format!("{}.ferrite.backup", name)
        };
        self.base_dir.join(filename)
    }

    /// Compress data using gzip
    fn compress_data(&self, data: &[u8]) -> Result<Vec<u8>, BackupStorageError> {
        use flate2::write::GzEncoder;
        use flate2::Compression;

        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder
            .write_all(data)
            .map_err(|e| BackupStorageError::Compression(e.to_string()))?;
        encoder
            .finish()
            .map_err(|e| BackupStorageError::Compression(e.to_string()))
    }

    /// Decompress gzip data
    fn decompress_data(&self, data: &[u8]) -> Result<Vec<u8>, BackupStorageError> {
        use flate2::read::GzDecoder;

        let mut decoder = GzDecoder::new(data);
        let mut decompressed = Vec::new();
        decoder
            .read_to_end(&mut decompressed)
            .map_err(|e| BackupStorageError::Compression(e.to_string()))?;
        Ok(decompressed)
    }

    /// Cleanup old backups to respect max_backups limit
    async fn cleanup_old_backups(&self) -> Result<(), BackupStorageError> {
        let mut backups = self.list().await?;
        if backups.len() <= self.max_backups {
            return Ok(());
        }

        // Sort by name (which includes timestamp, so oldest first)
        backups.sort();

        // Remove oldest backups
        let to_remove = backups.len() - self.max_backups;
        for name in backups.iter().take(to_remove) {
            info!("Removing old backup: {}", name);
            self.delete(name).await?;
        }

        Ok(())
    }
}

#[async_trait]
impl BackupStorage for LocalBackupStorage {
    async fn store(&self, name: &str, data: &[u8]) -> Result<(), BackupStorageError> {
        // Ensure directory exists
        fs::create_dir_all(&self.base_dir).await?;

        let path = self.backup_path(name);
        debug!("Storing backup to: {:?}", path);

        let data_to_write = if self.compress {
            self.compress_data(data)?
        } else {
            data.to_vec()
        };

        fs::write(&path, &data_to_write).await?;
        info!(
            "Backup stored: {:?} ({} bytes, compressed: {})",
            path,
            data_to_write.len(),
            self.compress
        );

        // Cleanup old backups
        self.cleanup_old_backups().await?;

        Ok(())
    }

    async fn retrieve(&self, name: &str) -> Result<Bytes, BackupStorageError> {
        let path = self.backup_path(name);

        if !path.exists() {
            return Err(BackupStorageError::NotFound(name.to_string()));
        }

        debug!("Retrieving backup from: {:?}", path);
        let data = fs::read(&path).await?;

        let result = if self.compress || path.extension().is_some_and(|ext| ext == "gz") {
            Bytes::from(self.decompress_data(&data)?)
        } else {
            Bytes::from(data)
        };

        info!("Backup retrieved: {:?} ({} bytes)", path, result.len());
        Ok(result)
    }

    async fn list(&self) -> Result<Vec<String>, BackupStorageError> {
        if !self.base_dir.exists() {
            return Ok(Vec::new());
        }

        let mut backups = Vec::new();
        let mut entries = fs::read_dir(&self.base_dir).await?;

        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                if name.contains(".ferrite.backup") {
                    // Extract the backup name (timestamp part)
                    let backup_name = name
                        .replace(".ferrite.backup.gz", "")
                        .replace(".ferrite.backup", "");
                    backups.push(backup_name);
                }
            }
        }

        Ok(backups)
    }

    async fn delete(&self, name: &str) -> Result<(), BackupStorageError> {
        let path = self.backup_path(name);

        if !path.exists() {
            return Err(BackupStorageError::NotFound(name.to_string()));
        }

        fs::remove_file(&path).await?;
        info!("Backup deleted: {:?}", path);
        Ok(())
    }

    async fn exists(&self, name: &str) -> Result<bool, BackupStorageError> {
        let path = self.backup_path(name);
        Ok(path.exists())
    }

    async fn size(&self, name: &str) -> Result<u64, BackupStorageError> {
        let path = self.backup_path(name);

        if !path.exists() {
            return Err(BackupStorageError::NotFound(name.to_string()));
        }

        let metadata = fs::metadata(&path).await?;
        Ok(metadata.len())
    }
}

/// Cloud backup storage (S3/GCS/Azure)
#[cfg(feature = "cloud")]
pub struct CloudBackupStorage {
    /// Cloud storage backend from the cloud module
    backend: Box<dyn crate::storage::cloud::CloudStorage>,
    /// Prefix for backup objects
    prefix: String,
    /// Whether to compress backups
    compress: bool,
}

#[cfg(feature = "cloud")]
impl CloudBackupStorage {
    /// Create a new cloud backup storage
    pub fn new(backend: Box<dyn crate::storage::cloud::CloudStorage>, prefix: String) -> Self {
        Self {
            backend,
            prefix,
            compress: true,
        }
    }

    /// Set compression enabled/disabled
    pub fn with_compression(mut self, compress: bool) -> Self {
        self.compress = compress;
        self
    }

    /// Get the cloud key for a backup
    fn backup_key(&self, name: &str) -> String {
        let filename = if self.compress {
            format!("{}.ferrite.backup.gz", name)
        } else {
            format!("{}.ferrite.backup", name)
        };
        format!("{}/{}", self.prefix, filename)
    }

    /// Compress data using gzip
    fn compress_data(&self, data: &[u8]) -> Result<Vec<u8>, BackupStorageError> {
        use flate2::write::GzEncoder;
        use flate2::Compression;

        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder
            .write_all(data)
            .map_err(|e| BackupStorageError::Compression(e.to_string()))?;
        encoder
            .finish()
            .map_err(|e| BackupStorageError::Compression(e.to_string()))
    }

    /// Decompress gzip data
    fn decompress_data(&self, data: &[u8]) -> Result<Vec<u8>, BackupStorageError> {
        use flate2::read::GzDecoder;

        let mut decoder = GzDecoder::new(data);
        let mut decompressed = Vec::new();
        decoder
            .read_to_end(&mut decompressed)
            .map_err(|e| BackupStorageError::Compression(e.to_string()))?;
        Ok(decompressed)
    }
}

#[cfg(feature = "cloud")]
#[async_trait]
impl BackupStorage for CloudBackupStorage {
    async fn store(&self, name: &str, data: &[u8]) -> Result<(), BackupStorageError> {
        let key = self.backup_key(name);
        debug!("Storing backup to cloud: {}", key);

        let data_to_write = if self.compress {
            Bytes::from(self.compress_data(data)?)
        } else {
            Bytes::copy_from_slice(data)
        };

        self.backend
            .put(&key, data_to_write.clone())
            .await
            .map_err(|e| BackupStorageError::Cloud(e.to_string()))?;

        info!(
            "Backup stored to cloud: {} ({} bytes)",
            key,
            data_to_write.len()
        );
        Ok(())
    }

    async fn retrieve(&self, name: &str) -> Result<Bytes, BackupStorageError> {
        let key = self.backup_key(name);
        debug!("Retrieving backup from cloud: {}", key);

        let data = self
            .backend
            .get(&key)
            .await
            .map_err(|e| BackupStorageError::Cloud(e.to_string()))?
            .ok_or_else(|| BackupStorageError::NotFound(name.to_string()))?;

        let result = if self.compress {
            Bytes::from(self.decompress_data(&data)?)
        } else {
            data
        };

        info!(
            "Backup retrieved from cloud: {} ({} bytes)",
            key,
            result.len()
        );
        Ok(result)
    }

    async fn list(&self) -> Result<Vec<String>, BackupStorageError> {
        let keys = self
            .backend
            .list(&self.prefix)
            .await
            .map_err(|e| BackupStorageError::Cloud(e.to_string()))?;

        let backups: Vec<String> = keys
            .into_iter()
            .filter_map(|k| {
                if k.contains(".ferrite.backup") {
                    let name = k
                        .replace(&format!("{}/", self.prefix), "")
                        .replace(".ferrite.backup.gz", "")
                        .replace(".ferrite.backup", "");
                    Some(name)
                } else {
                    None
                }
            })
            .collect();

        Ok(backups)
    }

    async fn delete(&self, name: &str) -> Result<(), BackupStorageError> {
        let key = self.backup_key(name);

        self.backend
            .delete(&key)
            .await
            .map_err(|e| BackupStorageError::Cloud(e.to_string()))?;

        info!("Backup deleted from cloud: {}", key);
        Ok(())
    }

    async fn exists(&self, name: &str) -> Result<bool, BackupStorageError> {
        let key = self.backup_key(name);

        self.backend
            .exists(&key)
            .await
            .map_err(|e| BackupStorageError::Cloud(e.to_string()))
    }

    async fn size(&self, name: &str) -> Result<u64, BackupStorageError> {
        let key = self.backup_key(name);

        self.backend
            .size(&key)
            .await
            .map_err(|e| BackupStorageError::Cloud(e.to_string()))?
            .ok_or_else(|| BackupStorageError::NotFound(name.to_string()))
    }
}

/// Progress callback for tracking upload/download progress
pub trait ProgressCallback: Send + Sync {
    /// Called when progress is made
    /// - `bytes_transferred`: Number of bytes transferred so far
    /// - `total_bytes`: Total bytes to transfer (if known)
    fn on_progress(&self, bytes_transferred: u64, total_bytes: Option<u64>);

    /// Called when the operation completes
    fn on_complete(&self);

    /// Called when an error occurs
    fn on_error(&self, error: &str);
}

/// Simple progress callback that logs progress
pub struct LoggingProgressCallback {
    operation: String,
    last_logged_pct: Mutex<u8>,
}

impl LoggingProgressCallback {
    /// Create a new logging progress callback for the given operation
    pub fn new(operation: &str) -> Self {
        Self {
            operation: operation.to_string(),
            last_logged_pct: Mutex::new(0),
        }
    }
}

impl ProgressCallback for LoggingProgressCallback {
    fn on_progress(&self, bytes_transferred: u64, total_bytes: Option<u64>) {
        if let Some(total) = total_bytes {
            let pct = ((bytes_transferred as f64 / total as f64) * 100.0) as u8;
            let mut last_pct = self.last_logged_pct.lock();
            // Only log every 10%
            if pct >= *last_pct + 10 {
                info!(
                    "{}: {}% complete ({}/{} bytes)",
                    self.operation, pct, bytes_transferred, total
                );
                *last_pct = pct;
            }
        } else {
            debug!(
                "{}: {} bytes transferred",
                self.operation, bytes_transferred
            );
        }
    }

    fn on_complete(&self) {
        info!("{}: complete", self.operation);
    }

    fn on_error(&self, error: &str) {
        tracing::error!("{}: error - {}", self.operation, error);
    }
}

/// Progress-aware cloud backup storage with callbacks
#[cfg(feature = "cloud")]
pub struct ProgressCloudBackupStorage {
    /// Inner cloud backup storage
    inner: CloudBackupStorage,
    /// Progress callback
    callback: Arc<dyn ProgressCallback>,
}

#[cfg(feature = "cloud")]
impl ProgressCloudBackupStorage {
    /// Create a new progress-aware cloud backup storage
    pub fn new(inner: CloudBackupStorage, callback: Arc<dyn ProgressCallback>) -> Self {
        Self { inner, callback }
    }

    /// Store with progress tracking
    pub async fn store_with_progress(
        &self,
        name: &str,
        data: &[u8],
    ) -> Result<(), BackupStorageError> {
        let total_size = data.len() as u64;

        // Report initial progress
        self.callback.on_progress(0, Some(total_size));

        // For small files, just upload directly
        // Note: For large files, we could implement multipart upload with progress
        let result = self.inner.store(name, data).await;

        match &result {
            Ok(_) => {
                self.callback.on_progress(total_size, Some(total_size));
                self.callback.on_complete();
            }
            Err(e) => {
                self.callback.on_error(&e.to_string());
            }
        }

        result
    }

    /// Retrieve with progress tracking
    pub async fn retrieve_with_progress(&self, name: &str) -> Result<Bytes, BackupStorageError> {
        // Get size first if possible
        let size = self.inner.size(name).await.ok();

        self.callback.on_progress(0, size);

        let result = self.inner.retrieve(name).await;

        match &result {
            Ok(data) => {
                self.callback.on_progress(data.len() as u64, size);
                self.callback.on_complete();
            }
            Err(e) => {
                self.callback.on_error(&e.to_string());
            }
        }

        result
    }
}

#[cfg(feature = "cloud")]
#[async_trait]
impl BackupStorage for ProgressCloudBackupStorage {
    async fn store(&self, name: &str, data: &[u8]) -> Result<(), BackupStorageError> {
        self.store_with_progress(name, data).await
    }

    async fn retrieve(&self, name: &str) -> Result<Bytes, BackupStorageError> {
        self.retrieve_with_progress(name).await
    }

    async fn list(&self) -> Result<Vec<String>, BackupStorageError> {
        self.inner.list().await
    }

    async fn delete(&self, name: &str) -> Result<(), BackupStorageError> {
        self.inner.delete(name).await
    }

    async fn exists(&self, name: &str) -> Result<bool, BackupStorageError> {
        self.inner.exists(name).await
    }

    async fn size(&self, name: &str) -> Result<u64, BackupStorageError> {
        self.inner.size(name).await
    }
}

/// Create a cloud backup storage from cloud configuration
///
/// This is a convenience function that creates a CloudBackupStorage
/// from a CloudConfig, handling the creation of the underlying CloudBackend.
///
/// # Arguments
/// * `config` - Cloud storage configuration (S3, GCS, Azure, or Local)
/// * `prefix` - Prefix for backup objects (e.g., "backups/")
/// * `compress` - Whether to compress backups
///
/// # Example
/// ```ignore
/// use ferrite::storage::cloud::{CloudConfig, CloudProvider};
/// use ferrite::persistence::backup::create_cloud_backup_storage;
///
/// let config = CloudConfig {
///     provider: CloudProvider::S3,
///     bucket: "my-bucket".to_string(),
///     region: Some("us-east-1".to_string()),
///     access_key_id: Some("AKIAIOSFODNN7EXAMPLE".to_string()),
///     secret_access_key: Some("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string()),
///     ..Default::default()
/// };
///
/// let storage = create_cloud_backup_storage(&config, "backups/", true)?;
/// ```
#[cfg(feature = "cloud")]
pub fn create_cloud_backup_storage(
    config: &CloudConfig,
    prefix: &str,
    compress: bool,
) -> Result<CloudBackupStorage, BackupStorageError> {
    let backend =
        CloudBackend::new(config).map_err(|e| BackupStorageError::Cloud(e.to_string()))?;

    Ok(CloudBackupStorage::new(backend, prefix.to_string()).with_compression(compress))
}

/// Create a progress-aware cloud backup storage from cloud configuration
///
/// Similar to `create_cloud_backup_storage` but with progress tracking.
#[cfg(feature = "cloud")]
pub fn create_cloud_backup_storage_with_progress(
    config: &CloudConfig,
    prefix: &str,
    compress: bool,
    callback: Arc<dyn ProgressCallback>,
) -> Result<ProgressCloudBackupStorage, BackupStorageError> {
    let inner = create_cloud_backup_storage(config, prefix, compress)?;
    Ok(ProgressCloudBackupStorage::new(inner, callback))
}

/// S3-specific configuration helper
#[cfg(feature = "cloud")]
#[derive(Debug, Clone)]
pub struct S3Config {
    /// S3 bucket name
    pub bucket: String,
    /// AWS region
    pub region: String,
    /// Access key ID
    pub access_key_id: String,
    /// Secret access key
    pub secret_access_key: String,
    /// Custom endpoint (for S3-compatible services like MinIO)
    pub endpoint: Option<String>,
    /// Object prefix
    pub prefix: Option<String>,
}

#[cfg(feature = "cloud")]
impl S3Config {
    /// Create a new S3 configuration
    pub fn new(
        bucket: impl Into<String>,
        region: impl Into<String>,
        access_key_id: impl Into<String>,
        secret_access_key: impl Into<String>,
    ) -> Self {
        Self {
            bucket: bucket.into(),
            region: region.into(),
            access_key_id: access_key_id.into(),
            secret_access_key: secret_access_key.into(),
            endpoint: None,
            prefix: None,
        }
    }

    /// Set custom endpoint (for S3-compatible services)
    pub fn with_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoint = Some(endpoint.into());
        self
    }

    /// Set object prefix
    pub fn with_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = Some(prefix.into());
        self
    }

    /// Convert to CloudConfig
    pub fn into_cloud_config(self) -> CloudConfig {
        CloudConfig {
            provider: crate::storage::cloud::CloudProvider::S3,
            bucket: self.bucket,
            prefix: self.prefix,
            region: Some(self.region),
            endpoint: self.endpoint,
            access_key_id: Some(self.access_key_id),
            secret_access_key: Some(self.secret_access_key),
            compression_enabled: true,
            compression_level: 6,
        }
    }
}

/// Create an S3 backup storage from S3-specific configuration
///
/// This is a convenience function for creating S3 backup storage.
///
/// # Example
/// ```ignore
/// use ferrite::persistence::backup::{create_s3_backup_storage, S3Config};
///
/// let s3_config = S3Config::new(
///     "my-bucket",
///     "us-east-1",
///     "AKIAIOSFODNN7EXAMPLE",
///     "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
/// ).with_prefix("backups/");
///
/// let storage = create_s3_backup_storage(s3_config, true)?;
/// ```
#[cfg(feature = "cloud")]
pub fn create_s3_backup_storage(
    config: S3Config,
    compress: bool,
) -> Result<CloudBackupStorage, BackupStorageError> {
    let prefix = config.prefix.clone().unwrap_or_default();
    let cloud_config = config.into_cloud_config();
    create_cloud_backup_storage(&cloud_config, &prefix, compress)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_local_storage_roundtrip() {
        let temp_dir = TempDir::new().unwrap();
        let storage = LocalBackupStorage::new(temp_dir.path().to_path_buf());

        let name = "test-backup";
        let data = b"Hello, backup world!";

        // Store
        storage.store(name, data).await.unwrap();

        // Check exists
        assert!(storage.exists(name).await.unwrap());

        // Retrieve
        let retrieved = storage.retrieve(name).await.unwrap();
        assert_eq!(&retrieved[..], &data[..]);

        // List
        let backups = storage.list().await.unwrap();
        assert!(backups.contains(&name.to_string()));

        // Delete
        storage.delete(name).await.unwrap();
        assert!(!storage.exists(name).await.unwrap());
    }

    #[tokio::test]
    async fn test_local_storage_no_compression() {
        let temp_dir = TempDir::new().unwrap();
        let storage =
            LocalBackupStorage::new(temp_dir.path().to_path_buf()).with_compression(false);

        let name = "test-uncompressed";
        let data = b"Uncompressed backup data";

        storage.store(name, data).await.unwrap();
        let retrieved = storage.retrieve(name).await.unwrap();
        assert_eq!(&retrieved[..], &data[..]);
    }

    #[tokio::test]
    async fn test_local_storage_max_backups() {
        let temp_dir = TempDir::new().unwrap();
        let storage = LocalBackupStorage::new(temp_dir.path().to_path_buf()).with_max_backups(3);

        // Create 5 backups
        for i in 0..5 {
            let name = format!("backup-{:03}", i);
            storage.store(&name, b"test data").await.unwrap();
        }

        // Should only have 3 backups (oldest removed)
        let backups = storage.list().await.unwrap();
        assert_eq!(backups.len(), 3);

        // Should have the newest 3
        assert!(backups.contains(&"backup-002".to_string()));
        assert!(backups.contains(&"backup-003".to_string()));
        assert!(backups.contains(&"backup-004".to_string()));
    }

    #[tokio::test]
    async fn test_local_storage_not_found() {
        let temp_dir = TempDir::new().unwrap();
        let storage = LocalBackupStorage::new(temp_dir.path().to_path_buf());

        let result = storage.retrieve("nonexistent").await;
        assert!(matches!(result, Err(BackupStorageError::NotFound(_))));
    }

    #[test]
    fn test_compression() {
        let storage = LocalBackupStorage::new(PathBuf::from("/tmp"));

        let original = b"Hello, this is test data that should compress well! ".repeat(100);

        let compressed = storage.compress_data(&original).unwrap();
        assert!(compressed.len() < original.len());

        let decompressed = storage.decompress_data(&compressed).unwrap();
        assert_eq!(decompressed, original);
    }

    #[cfg(feature = "cloud")]
    #[test]
    fn test_s3_config() {
        let config = S3Config::new("my-bucket", "us-east-1", "access-key", "secret-key")
            .with_prefix("backups/")
            .with_endpoint("http://localhost:9000");

        assert_eq!(config.bucket, "my-bucket");
        assert_eq!(config.region, "us-east-1");
        assert_eq!(config.access_key_id, "access-key");
        assert_eq!(config.secret_access_key, "secret-key");
        assert_eq!(config.prefix, Some("backups/".to_string()));
        assert_eq!(config.endpoint, Some("http://localhost:9000".to_string()));

        let cloud_config = config.into_cloud_config();
        assert_eq!(
            cloud_config.provider,
            crate::storage::cloud::CloudProvider::S3
        );
        assert_eq!(cloud_config.bucket, "my-bucket");
        assert_eq!(cloud_config.region, Some("us-east-1".to_string()));
    }

    #[test]
    fn test_logging_progress_callback() {
        let callback = LoggingProgressCallback::new("test-upload");

        // Test progress tracking
        callback.on_progress(0, Some(100));
        callback.on_progress(50, Some(100));
        callback.on_progress(100, Some(100));
        callback.on_complete();

        // Test without total (unknown size)
        callback.on_progress(1024, None);

        // Test error
        callback.on_error("test error");
    }

    /// Mock progress callback for testing
    struct MockProgressCallback {
        progress_calls: Mutex<Vec<(u64, Option<u64>)>>,
        completed: Mutex<bool>,
        errors: Mutex<Vec<String>>,
    }

    impl MockProgressCallback {
        fn new() -> Self {
            Self {
                progress_calls: Mutex::new(Vec::new()),
                completed: Mutex::new(false),
                errors: Mutex::new(Vec::new()),
            }
        }
    }

    impl ProgressCallback for MockProgressCallback {
        fn on_progress(&self, bytes_transferred: u64, total_bytes: Option<u64>) {
            self.progress_calls
                .lock()
                .push((bytes_transferred, total_bytes));
        }

        fn on_complete(&self) {
            *self.completed.lock() = true;
        }

        fn on_error(&self, error: &str) {
            self.errors.lock().push(error.to_string());
        }
    }

    #[cfg(feature = "cloud")]
    #[tokio::test]
    async fn test_progress_cloud_backup_storage() {
        use crate::storage::cloud::{CloudConfig, CloudProvider};

        let temp_dir = TempDir::new().unwrap();
        let config = CloudConfig {
            provider: CloudProvider::Local,
            bucket: temp_dir.path().to_string_lossy().to_string(),
            prefix: None,
            compression_enabled: false,
            ..Default::default()
        };

        let inner = create_cloud_backup_storage(&config, "backups", false).unwrap();
        let callback = Arc::new(MockProgressCallback::new());
        let storage = ProgressCloudBackupStorage::new(inner, callback.clone());

        // Store with progress
        let data = b"test backup data for progress tracking";
        storage.store("progress-test", data).await.unwrap();

        // Check progress was called
        let progress = callback.progress_calls.lock();
        assert!(!progress.is_empty());
        // Should have called with 0 (start) and total size (complete)
        assert!(progress
            .iter()
            .any(|(b, t)| *b == 0 && *t == Some(data.len() as u64)));
        assert!(progress
            .iter()
            .any(|(b, t)| *b == data.len() as u64 && *t == Some(data.len() as u64)));
        drop(progress);

        assert!(*callback.completed.lock());

        // Retrieve with progress - reset callback
        let callback2 = Arc::new(MockProgressCallback::new());
        let inner2 = create_cloud_backup_storage(&config, "backups", false).unwrap();
        let storage2 = ProgressCloudBackupStorage::new(inner2, callback2.clone());

        let retrieved = storage2.retrieve("progress-test").await.unwrap();
        assert_eq!(&retrieved[..], &data[..]);
        assert!(*callback2.completed.lock());
    }

    #[cfg(feature = "cloud")]
    #[tokio::test]
    async fn test_create_cloud_backup_storage_local() {
        use crate::storage::cloud::{CloudConfig, CloudProvider};

        let temp_dir = TempDir::new().unwrap();
        let config = CloudConfig {
            provider: CloudProvider::Local,
            bucket: temp_dir.path().to_string_lossy().to_string(),
            prefix: None,
            compression_enabled: false,
            ..Default::default()
        };

        let storage = create_cloud_backup_storage(&config, "test", true).unwrap();

        // Test basic operations
        storage.store("test-key", b"test-value").await.unwrap();
        assert!(storage.exists("test-key").await.unwrap());

        let data = storage.retrieve("test-key").await.unwrap();
        assert_eq!(&data[..], b"test-value");

        storage.delete("test-key").await.unwrap();
        assert!(!storage.exists("test-key").await.unwrap());
    }
}
