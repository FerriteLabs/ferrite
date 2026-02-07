//! Cloud storage backends
//!
//! This module implements cloud storage backends for S3, GCS, Azure, and local filesystem.

use async_trait::async_trait;
use bytes::Bytes;
use flate2::read::{GzDecoder, GzEncoder};
use flate2::Compression;
use object_store::aws::AmazonS3Builder;
use object_store::azure::MicrosoftAzureBuilder;
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectPath;
use object_store::ObjectStore;
use std::io::Read;
use std::path::PathBuf;
use std::sync::Arc;

use super::CloudStorage;

/// Cloud storage errors
#[derive(Debug, thiserror::Error)]
pub enum CloudError {
    /// Configuration error
    #[error("Configuration error: {0}")]
    Config(String),

    /// Object store error
    #[error("Object store error: {0}")]
    ObjectStore(#[from] object_store::Error),

    /// IO error
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// Compression error
    #[error("Compression error: {0}")]
    Compression(String),

    /// Invalid path
    #[error("Invalid path: {0}")]
    InvalidPath(String),
}

/// Cloud storage provider
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CloudProvider {
    /// Amazon S3
    S3,
    /// Google Cloud Storage
    Gcs,
    /// Azure Blob Storage
    Azure,
    /// Local filesystem (for testing/development)
    Local,
}

impl Default for CloudProvider {
    fn default() -> Self {
        Self::Local
    }
}

/// Cloud storage configuration
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CloudConfig {
    /// Cloud provider
    pub provider: CloudProvider,
    /// Bucket/container name
    pub bucket: String,
    /// Key prefix for all objects
    #[serde(default)]
    pub prefix: Option<String>,
    /// Region (for S3 and Azure)
    #[serde(default)]
    pub region: Option<String>,
    /// Custom endpoint URL (for S3-compatible stores)
    #[serde(default)]
    pub endpoint: Option<String>,
    /// Access key ID (for S3)
    #[serde(default)]
    pub access_key_id: Option<String>,
    /// Secret access key (for S3)
    #[serde(default)]
    pub secret_access_key: Option<String>,
    /// Enable compression for uploads
    #[serde(default = "default_compression_enabled")]
    pub compression_enabled: bool,
    /// Compression level (1-9, default 6)
    #[serde(default = "default_compression_level")]
    pub compression_level: u32,
}

fn default_compression_enabled() -> bool {
    true
}

fn default_compression_level() -> u32 {
    6
}

impl Default for CloudConfig {
    fn default() -> Self {
        Self {
            provider: CloudProvider::Local,
            bucket: "ferrite-data".to_string(),
            prefix: Some("cold/".to_string()),
            region: None,
            endpoint: None,
            access_key_id: None,
            secret_access_key: None,
            compression_enabled: default_compression_enabled(),
            compression_level: default_compression_level(),
        }
    }
}

/// Cloud storage backend using object_store crate
pub struct CloudBackend {
    /// The underlying object store
    store: Arc<dyn ObjectStore>,
    /// Key prefix
    prefix: String,
    /// Compression settings
    compression_enabled: bool,
    compression_level: u32,
}

impl CloudBackend {
    /// Create a new cloud backend from configuration
    #[allow(clippy::new_ret_no_self)]
    pub fn new(config: &CloudConfig) -> Result<Box<dyn CloudStorage>, CloudError> {
        let store: Arc<dyn ObjectStore> = match config.provider {
            CloudProvider::S3 => {
                let mut builder = AmazonS3Builder::new().with_bucket_name(&config.bucket);

                if let Some(ref region) = config.region {
                    builder = builder.with_region(region);
                }
                if let Some(ref endpoint) = config.endpoint {
                    builder = builder.with_endpoint(endpoint);
                }
                if let Some(ref access_key) = config.access_key_id {
                    builder = builder.with_access_key_id(access_key);
                }
                if let Some(ref secret_key) = config.secret_access_key {
                    builder = builder.with_secret_access_key(secret_key);
                }

                Arc::new(builder.build()?)
            }
            CloudProvider::Gcs => {
                let builder = GoogleCloudStorageBuilder::new().with_bucket_name(&config.bucket);

                Arc::new(builder.build()?)
            }
            CloudProvider::Azure => {
                let mut builder = MicrosoftAzureBuilder::new().with_container_name(&config.bucket);

                if let Some(ref account) = config.access_key_id {
                    builder = builder.with_account(account);
                }
                if let Some(ref access_key) = config.secret_access_key {
                    builder = builder.with_access_key(access_key);
                }

                Arc::new(builder.build()?)
            }
            CloudProvider::Local => {
                let path = PathBuf::from(&config.bucket);
                std::fs::create_dir_all(&path)?;
                Arc::new(LocalFileSystem::new_with_prefix(path)?)
            }
        };

        let prefix = config.prefix.clone().unwrap_or_default();

        Ok(Box::new(Self {
            store,
            prefix,
            compression_enabled: config.compression_enabled,
            compression_level: config.compression_level,
        }))
    }

    /// Build the full path for a key
    fn build_path(&self, key: &str) -> Result<ObjectPath, CloudError> {
        let full_key = if self.prefix.is_empty() {
            key.to_string()
        } else {
            format!("{}{}", self.prefix, key)
        };
        ObjectPath::parse(&full_key).map_err(|e| CloudError::InvalidPath(e.to_string()))
    }

    /// Compress data if compression is enabled
    fn compress(&self, data: &[u8]) -> Result<Bytes, CloudError> {
        if !self.compression_enabled {
            return Ok(Bytes::copy_from_slice(data));
        }

        let level = Compression::new(self.compression_level);
        let mut encoder = GzEncoder::new(data, level);
        let mut compressed = Vec::new();
        encoder
            .read_to_end(&mut compressed)
            .map_err(|e| CloudError::Compression(e.to_string()))?;

        Ok(Bytes::from(compressed))
    }

    /// Decompress data if compression is enabled
    fn decompress(&self, data: &[u8]) -> Result<Bytes, CloudError> {
        if !self.compression_enabled {
            return Ok(Bytes::copy_from_slice(data));
        }

        // Check if data looks compressed (gzip magic bytes)
        if data.len() < 2 || data[0] != 0x1f || data[1] != 0x8b {
            // Not compressed, return as-is
            return Ok(Bytes::copy_from_slice(data));
        }

        let mut decoder = GzDecoder::new(data);
        let mut decompressed = Vec::new();
        decoder
            .read_to_end(&mut decompressed)
            .map_err(|e| CloudError::Compression(e.to_string()))?;

        Ok(Bytes::from(decompressed))
    }
}

#[async_trait]
impl CloudStorage for CloudBackend {
    async fn put(&self, key: &str, data: Bytes) -> Result<(), CloudError> {
        let path = self.build_path(key)?;
        let compressed = self.compress(&data)?;
        self.store.put(&path, compressed.into()).await?;
        Ok(())
    }

    async fn get(&self, key: &str) -> Result<Option<Bytes>, CloudError> {
        let path = self.build_path(key)?;

        match self.store.get(&path).await {
            Ok(result) => {
                let data = result.bytes().await?;
                let decompressed = self.decompress(&data)?;
                Ok(Some(decompressed))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    async fn delete(&self, key: &str) -> Result<bool, CloudError> {
        let path = self.build_path(key)?;

        match self.store.delete(&path).await {
            Ok(()) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    async fn exists(&self, key: &str) -> Result<bool, CloudError> {
        let path = self.build_path(key)?;

        match self.store.head(&path).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, CloudError> {
        use futures::StreamExt;

        let full_prefix = if self.prefix.is_empty() {
            prefix.to_string()
        } else {
            format!("{}{}", self.prefix, prefix)
        };

        let prefix_path =
            ObjectPath::parse(&full_prefix).map_err(|e| CloudError::InvalidPath(e.to_string()))?;

        let mut keys = Vec::new();
        let mut stream = self.store.list(Some(&prefix_path));

        while let Some(result) = stream.next().await {
            let meta = result?;
            let key = meta.location.to_string();
            // Remove the prefix to return relative keys
            let relative_key = if !self.prefix.is_empty() && key.starts_with(&self.prefix) {
                key[self.prefix.len()..].to_string()
            } else {
                key
            };
            keys.push(relative_key);
        }

        Ok(keys)
    }

    async fn size(&self, key: &str) -> Result<Option<u64>, CloudError> {
        let path = self.build_path(key)?;

        match self.store.head(&path).await {
            Ok(meta) => Ok(Some(meta.size as u64)),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    async fn create_local_backend() -> (Box<dyn CloudStorage>, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let config = CloudConfig {
            provider: CloudProvider::Local,
            bucket: temp_dir.path().to_string_lossy().to_string(),
            prefix: Some("test/".to_string()),
            compression_enabled: true,
            ..Default::default()
        };
        let backend = CloudBackend::new(&config).unwrap();
        (backend, temp_dir)
    }

    #[tokio::test]
    async fn test_put_get() {
        let (backend, _temp_dir) = create_local_backend().await;

        let key = "test-key";
        let data = Bytes::from("hello world");

        // Put
        backend.put(key, data.clone()).await.unwrap();

        // Get
        let retrieved = backend.get(key).await.unwrap();
        assert_eq!(retrieved, Some(data));
    }

    #[tokio::test]
    async fn test_delete() {
        let (backend, _temp_dir) = create_local_backend().await;

        let key = "delete-test";
        let data = Bytes::from("to be deleted");

        backend.put(key, data).await.unwrap();
        assert!(backend.exists(key).await.unwrap());

        let deleted = backend.delete(key).await.unwrap();
        assert!(deleted);
        assert!(!backend.exists(key).await.unwrap());

        // Delete non-existent should return false
        let deleted = backend.delete(key).await.unwrap();
        assert!(!deleted);
    }

    #[tokio::test]
    async fn test_exists() {
        let (backend, _temp_dir) = create_local_backend().await;

        let key = "exists-test";
        assert!(!backend.exists(key).await.unwrap());

        backend.put(key, Bytes::from("data")).await.unwrap();
        assert!(backend.exists(key).await.unwrap());
    }

    #[tokio::test]
    async fn test_list() {
        let (backend, _temp_dir) = create_local_backend().await;

        // Put several keys
        backend.put("prefix1/a", Bytes::from("a")).await.unwrap();
        backend.put("prefix1/b", Bytes::from("b")).await.unwrap();
        backend.put("prefix2/c", Bytes::from("c")).await.unwrap();

        // List with prefix
        let keys = backend.list("prefix1/").await.unwrap();
        assert_eq!(keys.len(), 2);
        assert!(keys.iter().any(|k| k.contains("prefix1/a")));
        assert!(keys.iter().any(|k| k.contains("prefix1/b")));
    }

    #[tokio::test]
    async fn test_compression() {
        let (backend, _temp_dir) = create_local_backend().await;

        let key = "compressed-test";
        // Create some data that compresses well
        let data = Bytes::from("a".repeat(1000));

        backend.put(key, data.clone()).await.unwrap();

        let retrieved = backend.get(key).await.unwrap();
        assert_eq!(retrieved, Some(data));
    }

    #[tokio::test]
    async fn test_size() {
        let (backend, _temp_dir) = create_local_backend().await;

        let key = "size-test";
        assert!(backend.size(key).await.unwrap().is_none());

        backend.put(key, Bytes::from("hello")).await.unwrap();
        let size = backend.size(key).await.unwrap();
        // Size might be smaller due to compression
        assert!(size.is_some());
    }
}
