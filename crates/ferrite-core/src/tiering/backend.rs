//! Pluggable Cold Storage Backends
//!
//! This module provides a trait-based abstraction for cold storage tiers,
//! allowing different backend implementations (local filesystem, S3/GCS/Azure,
//! in-memory for testing) to be plugged in at runtime.
//!
//! ## Architecture
//!
//! The [`ColdStorageBackend`] trait defines the interface that all backends must
//! implement. The [`BackendRegistry`] manages available backends and provides
//! lookup by name.
//!
//! ## Usage
//!
//! ```rust,ignore
//! use ferrite::tiering::backend::{BackendRegistry, InMemoryBackend, ColdStorageBackend};
//! use std::sync::Arc;
//!
//! let mut registry = BackendRegistry::new();
//! let backend = Arc::new(InMemoryBackend::new());
//! registry.register("memory", backend);
//!
//! if let Some(backend) = registry.get("memory") {
//!     // use backend
//! }
//! ```

use super::stats::StorageTier;
use async_trait::async_trait;
use dashmap::DashMap;
#[cfg(feature = "cloud")]
use object_store::path::Path as ObjectPath;
#[cfg(feature = "cloud")]
use object_store::ObjectStore;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Error types
// ---------------------------------------------------------------------------

/// Errors that can occur during cold storage operations.
#[derive(Debug, thiserror::Error)]
pub enum ColdStorageError {
    /// An I/O error occurred during a storage operation.
    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),

    /// The requested key was not found in the backend.
    #[error("key not found")]
    NotFound,

    /// The caller does not have permission to perform the operation.
    #[error("permission denied: {0}")]
    PermissionDenied(String),

    /// Failed to establish a connection to the backend.
    #[error("connection failed: {0}")]
    ConnectionFailed(String),

    /// The operation timed out.
    #[error("operation timed out: {0}")]
    Timeout(String),

    /// A backend-specific error that does not map to other variants.
    #[error("backend error: {0}")]
    BackendSpecific(String),
}

// ---------------------------------------------------------------------------
// Trait
// ---------------------------------------------------------------------------

/// Pluggable interface for cold-tier storage backends.
///
/// Implementors provide asynchronous key/value access that the tiering engine
/// uses to read and write data in the cold (SSD, cloud, archive) tiers.
#[async_trait]
pub trait ColdStorageBackend: Send + Sync + std::fmt::Debug {
    /// Retrieve the value associated with `key`, or `None` if absent.
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, ColdStorageError>;

    /// Store `value` under `key`, overwriting any previous value.
    async fn put(&self, key: &[u8], value: &[u8]) -> Result<(), ColdStorageError>;

    /// Delete the value associated with `key`.
    ///
    /// Returns `Ok(())` even if the key did not exist.
    async fn delete(&self, key: &[u8]) -> Result<(), ColdStorageError>;

    /// Check whether `key` exists in this backend.
    async fn exists(&self, key: &[u8]) -> Result<bool, ColdStorageError>;

    /// List all keys that start with the given `prefix`.
    async fn list_keys(&self, prefix: &[u8]) -> Result<Vec<Vec<u8>>, ColdStorageError>;

    /// Human-readable name of this backend (e.g. `"local-fs"`, `"s3"`).
    fn backend_name(&self) -> &str;

    /// The storage tier this backend corresponds to.
    fn tier(&self) -> StorageTier;
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for the [`LocalFilesystemBackend`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalFilesystemConfig {
    /// Base directory for stored files.
    pub base_dir: PathBuf,
}

/// Configuration for the [`S3Backend`].
#[cfg(feature = "cloud")]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3BackendConfig {
    /// Prefix prepended to every object path.
    pub prefix: String,
}

// ---------------------------------------------------------------------------
// LocalFilesystemBackend
// ---------------------------------------------------------------------------

/// Stores cold data on a local filesystem using async file I/O.
///
/// Each key is hex-encoded and used as the filename under `base_dir`.
#[derive(Debug)]
pub struct LocalFilesystemBackend {
    base_dir: PathBuf,
}

impl LocalFilesystemBackend {
    /// Create a new backend rooted at `base_dir`.
    ///
    /// The directory is created if it does not already exist.
    pub async fn new(base_dir: PathBuf) -> Result<Self, ColdStorageError> {
        tokio::fs::create_dir_all(&base_dir).await?;
        Ok(Self { base_dir })
    }

    /// Convert a raw key to its hex-encoded filename.
    fn key_to_path(&self, key: &[u8]) -> PathBuf {
        let hex = hex_encode(key);
        self.base_dir.join(hex)
    }
}

#[async_trait]
impl ColdStorageBackend for LocalFilesystemBackend {
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, ColdStorageError> {
        let path = self.key_to_path(key);
        match tokio::fs::read(&path).await {
            Ok(data) => Ok(Some(data)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(ColdStorageError::IoError(e)),
        }
    }

    async fn put(&self, key: &[u8], value: &[u8]) -> Result<(), ColdStorageError> {
        let path = self.key_to_path(key);
        tokio::fs::write(&path, value).await?;
        Ok(())
    }

    async fn delete(&self, key: &[u8]) -> Result<(), ColdStorageError> {
        let path = self.key_to_path(key);
        match tokio::fs::remove_file(&path).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(ColdStorageError::IoError(e)),
        }
    }

    async fn exists(&self, key: &[u8]) -> Result<bool, ColdStorageError> {
        let path = self.key_to_path(key);
        Ok(path.exists())
    }

    async fn list_keys(&self, prefix: &[u8]) -> Result<Vec<Vec<u8>>, ColdStorageError> {
        let hex_prefix = hex_encode(prefix);
        let mut keys = Vec::new();
        let mut entries = tokio::fs::read_dir(&self.base_dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            if let Some(name) = entry.file_name().to_str() {
                if name.starts_with(&hex_prefix) {
                    if let Some(decoded) = hex_decode(name) {
                        keys.push(decoded);
                    }
                }
            }
        }
        Ok(keys)
    }

    fn backend_name(&self) -> &str {
        "local-fs"
    }

    fn tier(&self) -> StorageTier {
        StorageTier::Ssd
    }
}

// ---------------------------------------------------------------------------
// S3Backend (via object_store) — requires the `cloud` feature
// ---------------------------------------------------------------------------

/// Backend that delegates to an [`ObjectStore`] implementation (S3, GCS, Azure, etc.).
///
/// Keys are hex-encoded and stored under an optional path prefix.
#[cfg(feature = "cloud")]
#[derive(Debug)]
pub struct S3Backend {
    store: Arc<dyn ObjectStore>,
    prefix: String,
    tier: StorageTier,
}

#[cfg(feature = "cloud")]
impl S3Backend {
    /// Create a new backend wrapping the given [`ObjectStore`].
    ///
    /// * `store` – any `ObjectStore` implementation (S3, GCS, Azure, local, in-memory).
    /// * `prefix` – path prefix prepended to every object key.
    /// * `tier` – the storage tier this backend represents (typically `Cloud` or `Archive`).
    pub fn new(store: Arc<dyn ObjectStore>, prefix: impl Into<String>, tier: StorageTier) -> Self {
        Self {
            store,
            prefix: prefix.into(),
            tier,
        }
    }

    /// Build the [`ObjectPath`] for a given raw key.
    fn object_path(&self, key: &[u8]) -> ObjectPath {
        let hex = hex_encode(key);
        if self.prefix.is_empty() {
            ObjectPath::from(hex)
        } else {
            ObjectPath::from(format!("{}/{}", self.prefix, hex))
        }
    }
}

#[cfg(feature = "cloud")]
#[async_trait]
impl ColdStorageBackend for S3Backend {
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, ColdStorageError> {
        let path = self.object_path(key);
        match self.store.get(&path).await {
            Ok(result) => {
                let bytes = result.bytes().await.map_err(|e| {
                    ColdStorageError::BackendSpecific(format!("failed to read bytes: {e}"))
                })?;
                Ok(Some(bytes.to_vec()))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(ColdStorageError::BackendSpecific(e.to_string())),
        }
    }

    async fn put(&self, key: &[u8], value: &[u8]) -> Result<(), ColdStorageError> {
        let path = self.object_path(key);
        self.store
            .put(&path, bytes::Bytes::copy_from_slice(value).into())
            .await
            .map_err(|e| ColdStorageError::BackendSpecific(e.to_string()))?;
        Ok(())
    }

    async fn delete(&self, key: &[u8]) -> Result<(), ColdStorageError> {
        let path = self.object_path(key);
        match self.store.delete(&path).await {
            Ok(()) => Ok(()),
            Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(e) => Err(ColdStorageError::BackendSpecific(e.to_string())),
        }
    }

    async fn exists(&self, key: &[u8]) -> Result<bool, ColdStorageError> {
        let path = self.object_path(key);
        match self.store.head(&path).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(e) => Err(ColdStorageError::BackendSpecific(e.to_string())),
        }
    }

    async fn list_keys(&self, prefix: &[u8]) -> Result<Vec<Vec<u8>>, ColdStorageError> {
        use futures::TryStreamExt;

        let hex_prefix = hex_encode(prefix);
        let list_dir = if self.prefix.is_empty() {
            None
        } else {
            Some(ObjectPath::from(format!("{}/", self.prefix)))
        };

        let mut keys = Vec::new();
        let mut stream = self.store.list(list_dir.as_ref());
        while let Some(meta) = stream
            .try_next()
            .await
            .map_err(|e| ColdStorageError::BackendSpecific(e.to_string()))?
        {
            let filename = meta.location.filename().unwrap_or_default();
            if filename.starts_with(&hex_prefix) {
                if let Some(decoded) = hex_decode(filename) {
                    keys.push(decoded);
                }
            }
        }
        Ok(keys)
    }

    fn backend_name(&self) -> &str {
        "s3"
    }

    fn tier(&self) -> StorageTier {
        self.tier
    }
}

// ---------------------------------------------------------------------------
// InMemoryBackend (testing)
// ---------------------------------------------------------------------------

/// In-memory cold storage backend intended for testing.
///
/// Uses [`DashMap`] for thread-safe concurrent access.
#[derive(Debug)]
pub struct InMemoryBackend {
    data: DashMap<Vec<u8>, Vec<u8>>,
}

impl InMemoryBackend {
    /// Create a new, empty in-memory backend.
    pub fn new() -> Self {
        Self {
            data: DashMap::new(),
        }
    }
}

impl Default for InMemoryBackend {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ColdStorageBackend for InMemoryBackend {
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, ColdStorageError> {
        Ok(self.data.get(key).map(|v| v.value().clone()))
    }

    async fn put(&self, key: &[u8], value: &[u8]) -> Result<(), ColdStorageError> {
        self.data.insert(key.to_vec(), value.to_vec());
        Ok(())
    }

    async fn delete(&self, key: &[u8]) -> Result<(), ColdStorageError> {
        self.data.remove(key);
        Ok(())
    }

    async fn exists(&self, key: &[u8]) -> Result<bool, ColdStorageError> {
        Ok(self.data.contains_key(key))
    }

    async fn list_keys(&self, prefix: &[u8]) -> Result<Vec<Vec<u8>>, ColdStorageError> {
        Ok(self
            .data
            .iter()
            .filter(|entry| entry.key().starts_with(prefix))
            .map(|entry| entry.key().clone())
            .collect())
    }

    fn backend_name(&self) -> &str {
        "in-memory"
    }

    fn tier(&self) -> StorageTier {
        StorageTier::Memory
    }
}

// ---------------------------------------------------------------------------
// BackendRegistry
// ---------------------------------------------------------------------------

/// Registry that manages named cold storage backends.
///
/// The first backend registered is treated as the default.
#[derive(Debug)]
pub struct BackendRegistry {
    backends: RwLock<HashMap<String, Arc<dyn ColdStorageBackend>>>,
    default_name: RwLock<Option<String>>,
}

impl BackendRegistry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self {
            backends: RwLock::new(HashMap::new()),
            default_name: RwLock::new(None),
        }
    }

    /// Register a backend under the given `name`.
    ///
    /// The first registered backend automatically becomes the default.
    pub fn register(&self, name: &str, backend: Arc<dyn ColdStorageBackend>) {
        let mut backends = self.backends.write();
        let is_first = backends.is_empty();
        backends.insert(name.to_string(), backend);
        if is_first {
            *self.default_name.write() = Some(name.to_string());
        }
    }

    /// Look up a backend by name.
    pub fn get(&self, name: &str) -> Option<Arc<dyn ColdStorageBackend>> {
        self.backends.read().get(name).cloned()
    }

    /// List the names of all registered backends.
    pub fn list_backends(&self) -> Vec<String> {
        self.backends.read().keys().cloned().collect()
    }

    /// Return the default backend, if any.
    pub fn default_backend(&self) -> Option<Arc<dyn ColdStorageBackend>> {
        let name = self.default_name.read().clone()?;
        self.get(&name)
    }

    /// Explicitly set the default backend by name.
    ///
    /// Returns `true` if the name exists in the registry.
    pub fn set_default(&self, name: &str) -> bool {
        let backends = self.backends.read();
        if backends.contains_key(name) {
            *self.default_name.write() = Some(name.to_string());
            true
        } else {
            false
        }
    }
}

impl Default for BackendRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Hex helpers
// ---------------------------------------------------------------------------

/// Hex-encode a byte slice into a lowercase hex string.
fn hex_encode(data: &[u8]) -> String {
    data.iter().map(|b| format!("{b:02x}")).collect()
}

/// Decode a hex string back into bytes. Returns `None` on invalid input.
fn hex_decode(hex: &str) -> Option<Vec<u8>> {
    if hex.len() % 2 != 0 {
        return None;
    }
    (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).ok())
        .collect()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -- hex helpers --------------------------------------------------------

    #[test]
    fn test_hex_encode_decode_roundtrip() {
        let data = b"hello world";
        let encoded = hex_encode(data);
        assert_eq!(encoded, "68656c6c6f20776f726c64");
        let decoded = hex_decode(&encoded).unwrap();
        assert_eq!(decoded, data);
    }

    #[test]
    fn test_hex_encode_empty() {
        assert_eq!(hex_encode(b""), "");
        assert_eq!(hex_decode(""), Some(vec![]));
    }

    #[test]
    fn test_hex_decode_invalid() {
        assert!(hex_decode("zz").is_none());
        assert!(hex_decode("abc").is_none()); // odd length
    }

    // -- InMemoryBackend ----------------------------------------------------

    #[tokio::test]
    async fn test_in_memory_put_get() {
        let backend = InMemoryBackend::new();
        backend.put(b"key1", b"value1").await.unwrap();

        let val = backend.get(b"key1").await.unwrap();
        assert_eq!(val, Some(b"value1".to_vec()));
    }

    #[tokio::test]
    async fn test_in_memory_get_missing() {
        let backend = InMemoryBackend::new();
        let val = backend.get(b"missing").await.unwrap();
        assert_eq!(val, None);
    }

    #[tokio::test]
    async fn test_in_memory_delete() {
        let backend = InMemoryBackend::new();
        backend.put(b"key1", b"value1").await.unwrap();
        backend.delete(b"key1").await.unwrap();

        let val = backend.get(b"key1").await.unwrap();
        assert_eq!(val, None);
    }

    #[tokio::test]
    async fn test_in_memory_delete_missing() {
        let backend = InMemoryBackend::new();
        // Deleting a nonexistent key should not error.
        backend.delete(b"missing").await.unwrap();
    }

    #[tokio::test]
    async fn test_in_memory_exists() {
        let backend = InMemoryBackend::new();
        assert!(!backend.exists(b"key1").await.unwrap());

        backend.put(b"key1", b"value1").await.unwrap();
        assert!(backend.exists(b"key1").await.unwrap());
    }

    #[tokio::test]
    async fn test_in_memory_list_keys() {
        let backend = InMemoryBackend::new();
        backend.put(b"user:1", b"alice").await.unwrap();
        backend.put(b"user:2", b"bob").await.unwrap();
        backend.put(b"session:1", b"data").await.unwrap();

        let mut keys = backend.list_keys(b"user:").await.unwrap();
        keys.sort();
        assert_eq!(keys, vec![b"user:1".to_vec(), b"user:2".to_vec()]);
    }

    #[tokio::test]
    async fn test_in_memory_overwrite() {
        let backend = InMemoryBackend::new();
        backend.put(b"key", b"v1").await.unwrap();
        backend.put(b"key", b"v2").await.unwrap();

        let val = backend.get(b"key").await.unwrap();
        assert_eq!(val, Some(b"v2".to_vec()));
    }

    #[test]
    fn test_in_memory_backend_name_and_tier() {
        let backend = InMemoryBackend::new();
        assert_eq!(backend.backend_name(), "in-memory");
        assert_eq!(backend.tier(), StorageTier::Memory);
    }

    // -- LocalFilesystemBackend ---------------------------------------------

    #[tokio::test]
    async fn test_local_fs_put_get() {
        let dir = tempfile::tempdir().unwrap();
        let backend = LocalFilesystemBackend::new(dir.path().to_path_buf())
            .await
            .unwrap();

        backend.put(b"key1", b"value1").await.unwrap();
        let val = backend.get(b"key1").await.unwrap();
        assert_eq!(val, Some(b"value1".to_vec()));
    }

    #[tokio::test]
    async fn test_local_fs_get_missing() {
        let dir = tempfile::tempdir().unwrap();
        let backend = LocalFilesystemBackend::new(dir.path().to_path_buf())
            .await
            .unwrap();

        let val = backend.get(b"missing").await.unwrap();
        assert_eq!(val, None);
    }

    #[tokio::test]
    async fn test_local_fs_delete() {
        let dir = tempfile::tempdir().unwrap();
        let backend = LocalFilesystemBackend::new(dir.path().to_path_buf())
            .await
            .unwrap();

        backend.put(b"key1", b"value1").await.unwrap();
        backend.delete(b"key1").await.unwrap();
        let val = backend.get(b"key1").await.unwrap();
        assert_eq!(val, None);
    }

    #[tokio::test]
    async fn test_local_fs_delete_missing() {
        let dir = tempfile::tempdir().unwrap();
        let backend = LocalFilesystemBackend::new(dir.path().to_path_buf())
            .await
            .unwrap();

        backend.delete(b"missing").await.unwrap();
    }

    #[tokio::test]
    async fn test_local_fs_exists() {
        let dir = tempfile::tempdir().unwrap();
        let backend = LocalFilesystemBackend::new(dir.path().to_path_buf())
            .await
            .unwrap();

        assert!(!backend.exists(b"key1").await.unwrap());
        backend.put(b"key1", b"v").await.unwrap();
        assert!(backend.exists(b"key1").await.unwrap());
    }

    #[tokio::test]
    async fn test_local_fs_list_keys() {
        let dir = tempfile::tempdir().unwrap();
        let backend = LocalFilesystemBackend::new(dir.path().to_path_buf())
            .await
            .unwrap();

        backend.put(b"ab:1", b"data").await.unwrap();
        backend.put(b"ab:2", b"data").await.unwrap();
        backend.put(b"cd:1", b"data").await.unwrap();

        let mut keys = backend.list_keys(b"ab:").await.unwrap();
        keys.sort();
        assert_eq!(keys, vec![b"ab:1".to_vec(), b"ab:2".to_vec()]);
    }

    #[test]
    fn test_local_fs_backend_name_and_tier() {
        // Construct directly to avoid async in sync test.
        let backend = LocalFilesystemBackend {
            base_dir: PathBuf::from("/tmp/test"),
        };
        assert_eq!(backend.backend_name(), "local-fs");
        assert_eq!(backend.tier(), StorageTier::Ssd);
    }

    // -- S3Backend (using object_store InMemory) ----------------------------

    #[cfg(feature = "cloud")]
    #[tokio::test]
    async fn test_s3_backend_put_get() {
        let store = Arc::new(object_store::memory::InMemory::new());
        let backend = S3Backend::new(store, "test", StorageTier::Cloud);

        backend.put(b"key1", b"value1").await.unwrap();
        let val = backend.get(b"key1").await.unwrap();
        assert_eq!(val, Some(b"value1".to_vec()));
    }

    #[cfg(feature = "cloud")]
    #[tokio::test]
    async fn test_s3_backend_get_missing() {
        let store = Arc::new(object_store::memory::InMemory::new());
        let backend = S3Backend::new(store, "test", StorageTier::Cloud);

        let val = backend.get(b"missing").await.unwrap();
        assert_eq!(val, None);
    }

    #[cfg(feature = "cloud")]
    #[tokio::test]
    async fn test_s3_backend_delete() {
        let store = Arc::new(object_store::memory::InMemory::new());
        let backend = S3Backend::new(store, "test", StorageTier::Cloud);

        backend.put(b"key1", b"value1").await.unwrap();
        backend.delete(b"key1").await.unwrap();
        let val = backend.get(b"key1").await.unwrap();
        assert_eq!(val, None);
    }

    #[cfg(feature = "cloud")]
    #[tokio::test]
    async fn test_s3_backend_exists() {
        let store = Arc::new(object_store::memory::InMemory::new());
        let backend = S3Backend::new(store, "test", StorageTier::Cloud);

        assert!(!backend.exists(b"key1").await.unwrap());
        backend.put(b"key1", b"v").await.unwrap();
        assert!(backend.exists(b"key1").await.unwrap());
    }

    #[cfg(feature = "cloud")]
    #[tokio::test]
    async fn test_s3_backend_list_keys() {
        let store = Arc::new(object_store::memory::InMemory::new());
        let backend = S3Backend::new(store, "pfx", StorageTier::Cloud);

        backend.put(b"ab:1", b"data").await.unwrap();
        backend.put(b"ab:2", b"data").await.unwrap();
        backend.put(b"cd:1", b"data").await.unwrap();

        let mut keys = backend.list_keys(b"ab:").await.unwrap();
        keys.sort();
        assert_eq!(keys, vec![b"ab:1".to_vec(), b"ab:2".to_vec()]);
    }

    #[cfg(feature = "cloud")]
    #[tokio::test]
    async fn test_s3_backend_empty_prefix() {
        let store = Arc::new(object_store::memory::InMemory::new());
        let backend = S3Backend::new(store, "", StorageTier::Cloud);

        backend.put(b"key1", b"value1").await.unwrap();
        let val = backend.get(b"key1").await.unwrap();
        assert_eq!(val, Some(b"value1".to_vec()));
    }

    #[cfg(feature = "cloud")]
    #[test]
    fn test_s3_backend_name_and_tier() {
        let store = Arc::new(object_store::memory::InMemory::new());
        let backend = S3Backend::new(store, "", StorageTier::Archive);
        assert_eq!(backend.backend_name(), "s3");
        assert_eq!(backend.tier(), StorageTier::Archive);
    }

    // -- BackendRegistry ----------------------------------------------------

    #[test]
    fn test_registry_register_and_get() {
        let registry = BackendRegistry::new();
        let backend = Arc::new(InMemoryBackend::new());
        registry.register("mem", backend.clone());

        let got = registry.get("mem");
        assert!(got.is_some());
        assert_eq!(got.unwrap().backend_name(), "in-memory");
    }

    #[test]
    fn test_registry_get_missing() {
        let registry = BackendRegistry::new();
        assert!(registry.get("nonexistent").is_none());
    }

    #[test]
    fn test_registry_list_backends() {
        let registry = BackendRegistry::new();
        registry.register("a", Arc::new(InMemoryBackend::new()));
        registry.register("b", Arc::new(InMemoryBackend::new()));

        let mut names = registry.list_backends();
        names.sort();
        assert_eq!(names, vec!["a", "b"]);
    }

    #[test]
    fn test_registry_default_backend() {
        let registry = BackendRegistry::new();
        assert!(registry.default_backend().is_none());

        registry.register("first", Arc::new(InMemoryBackend::new()));
        registry.register("second", Arc::new(InMemoryBackend::new()));

        // First registered is the default.
        let default = registry.default_backend().unwrap();
        assert_eq!(default.backend_name(), "in-memory");
    }

    #[test]
    fn test_registry_set_default() {
        let registry = BackendRegistry::new();
        registry.register("a", Arc::new(InMemoryBackend::new()));
        registry.register("b", Arc::new(InMemoryBackend::new()));

        assert!(registry.set_default("b"));
        assert!(!registry.set_default("nonexistent"));
    }

    // -- ColdStorageError ---------------------------------------------------

    #[test]
    fn test_error_display() {
        let err = ColdStorageError::NotFound;
        assert_eq!(format!("{err}"), "key not found");

        let err = ColdStorageError::Timeout("5s elapsed".into());
        assert!(format!("{err}").contains("5s elapsed"));
    }

    #[test]
    fn test_error_from_io() {
        let io_err = std::io::Error::new(std::io::ErrorKind::Other, "disk full");
        let err: ColdStorageError = io_err.into();
        assert!(matches!(err, ColdStorageError::IoError(_)));
    }
}
