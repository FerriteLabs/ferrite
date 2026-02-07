//! Cloud tiering policy and migration
//!
//! This module implements the tiering policy that decides when to migrate data
//! to cloud storage and handles the actual migration process.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use parking_lot::RwLock;
use tokio::sync::Semaphore;
use tracing::{debug, info, warn};

use super::backend::{CloudConfig, CloudError};
use super::{create_backend, CloudStorage};

/// Tiering configuration
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TieringConfig {
    /// Cloud storage configuration
    pub cloud: CloudConfig,
    /// Minimum age (in seconds) before data can be tiered to cloud
    #[serde(default = "default_min_age_seconds")]
    pub min_age_seconds: u64,
    /// Maximum number of concurrent uploads
    #[serde(default = "default_max_concurrent_uploads")]
    pub max_concurrent_uploads: usize,
    /// Size threshold (in bytes) - only tier items larger than this
    #[serde(default = "default_size_threshold")]
    pub size_threshold: usize,
    /// Maximum cloud storage usage (in bytes, 0 = unlimited)
    #[serde(default)]
    pub max_cloud_size: u64,
    /// Enable automatic tiering
    #[serde(default = "default_auto_tier_enabled")]
    pub auto_tier_enabled: bool,
    /// Interval for automatic tiering check (in seconds)
    #[serde(default = "default_tier_check_interval")]
    pub tier_check_interval: u64,
}

fn default_min_age_seconds() -> u64 {
    3600 // 1 hour
}

fn default_max_concurrent_uploads() -> usize {
    4
}

fn default_size_threshold() -> usize {
    1024 // 1KB minimum
}

fn default_auto_tier_enabled() -> bool {
    false
}

fn default_tier_check_interval() -> u64 {
    300 // 5 minutes
}

impl Default for TieringConfig {
    fn default() -> Self {
        Self {
            cloud: CloudConfig::default(),
            min_age_seconds: default_min_age_seconds(),
            max_concurrent_uploads: default_max_concurrent_uploads(),
            size_threshold: default_size_threshold(),
            max_cloud_size: 0,
            auto_tier_enabled: default_auto_tier_enabled(),
            tier_check_interval: default_tier_check_interval(),
        }
    }
}

/// Tiering policy for deciding when to migrate data
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TieringPolicy {
    /// Never tier to cloud
    Never,
    /// Tier based on age (items older than threshold)
    AgeBased,
    /// Tier based on access frequency (least recently used)
    LruBased,
    /// Tier based on size (largest items first)
    SizeBased,
    /// Custom policy combining multiple factors
    Combined,
}

impl Default for TieringPolicy {
    fn default() -> Self {
        Self::AgeBased
    }
}

/// Metadata for tracking tiered items
#[derive(Debug, Clone)]
pub struct TieredItemMeta {
    /// Cloud storage key
    pub cloud_key: String,
    /// Original size (before compression)
    pub original_size: u64,
    /// Compressed size in cloud
    pub cloud_size: u64,
    /// When the item was tiered
    pub tiered_at: Instant,
    /// Last access time
    pub last_access: Instant,
    /// Number of times fetched from cloud
    pub fetch_count: u64,
}

/// Cloud tier manager
pub struct CloudTier {
    /// Configuration
    config: TieringConfig,
    /// Cloud storage backend
    backend: Box<dyn CloudStorage>,
    /// Semaphore for limiting concurrent operations
    upload_semaphore: Semaphore,
    /// Metadata for tiered items (key -> metadata)
    tiered_items: RwLock<HashMap<String, TieredItemMeta>>,
    /// Total size of data in cloud storage
    cloud_size: AtomicU64,
    /// Statistics: total items tiered
    stats_items_tiered: AtomicU64,
    /// Statistics: total bytes tiered
    stats_bytes_tiered: AtomicU64,
    /// Statistics: total items fetched
    stats_items_fetched: AtomicU64,
    /// Statistics: total bytes fetched
    stats_bytes_fetched: AtomicU64,
}

impl CloudTier {
    /// Create a new cloud tier manager
    pub fn new(config: TieringConfig) -> Result<Self, CloudError> {
        let backend = create_backend(&config.cloud)?;
        let upload_semaphore = Semaphore::new(config.max_concurrent_uploads);

        Ok(Self {
            config,
            backend,
            upload_semaphore,
            tiered_items: RwLock::new(HashMap::new()),
            cloud_size: AtomicU64::new(0),
            stats_items_tiered: AtomicU64::new(0),
            stats_bytes_tiered: AtomicU64::new(0),
            stats_items_fetched: AtomicU64::new(0),
            stats_bytes_fetched: AtomicU64::new(0),
        })
    }

    /// Tier data to cloud storage
    pub async fn tier(&self, key: &str, data: Bytes) -> Result<(), CloudError> {
        let data_size = data.len() as u64;

        // Check size threshold
        if data.len() < self.config.size_threshold {
            debug!(
                "Skipping tier for key {}: size {} below threshold {}",
                key,
                data.len(),
                self.config.size_threshold
            );
            return Ok(());
        }

        // Check cloud size limit
        if self.config.max_cloud_size > 0 {
            let current_size = self.cloud_size.load(Ordering::Relaxed);
            if current_size + data_size > self.config.max_cloud_size {
                warn!("Cloud storage limit reached, skipping tier for key {}", key);
                return Ok(());
            }
        }

        // Acquire semaphore permit for concurrent upload limiting
        let _permit = self
            .upload_semaphore
            .acquire()
            .await
            .map_err(|e| CloudError::Config(format!("Semaphore error: {}", e)))?;

        // Build cloud key (hash-based path to avoid collisions)
        let cloud_key = self.build_cloud_key(key);

        // Upload to cloud
        info!("Tiering key {} to cloud as {}", key, cloud_key);
        self.backend.put(&cloud_key, data.clone()).await?;

        // Get actual size in cloud (may be compressed)
        let cloud_size = self.backend.size(&cloud_key).await?.unwrap_or(data_size);

        // Update metadata
        let meta = TieredItemMeta {
            cloud_key: cloud_key.clone(),
            original_size: data_size,
            cloud_size,
            tiered_at: Instant::now(),
            last_access: Instant::now(),
            fetch_count: 0,
        };

        self.tiered_items.write().insert(key.to_string(), meta);
        self.cloud_size.fetch_add(cloud_size, Ordering::Relaxed);
        self.stats_items_tiered.fetch_add(1, Ordering::Relaxed);
        self.stats_bytes_tiered
            .fetch_add(data_size, Ordering::Relaxed);

        Ok(())
    }

    /// Fetch data from cloud storage
    pub async fn fetch(&self, key: &str) -> Result<Option<Bytes>, CloudError> {
        let cloud_key = {
            let items = self.tiered_items.read();
            match items.get(key) {
                Some(meta) => meta.cloud_key.clone(),
                None => return Ok(None),
            }
        };

        debug!("Fetching key {} from cloud ({})", key, cloud_key);
        let data = self.backend.get(&cloud_key).await?;

        if let Some(ref data) = data {
            // Update access metadata
            if let Some(meta) = self.tiered_items.write().get_mut(key) {
                meta.last_access = Instant::now();
                meta.fetch_count += 1;
            }

            self.stats_items_fetched.fetch_add(1, Ordering::Relaxed);
            self.stats_bytes_fetched
                .fetch_add(data.len() as u64, Ordering::Relaxed);
        }

        Ok(data)
    }

    /// Remove data from cloud storage
    pub async fn remove(&self, key: &str) -> Result<bool, CloudError> {
        let cloud_key = {
            let mut items = self.tiered_items.write();
            match items.remove(key) {
                Some(meta) => {
                    self.cloud_size
                        .fetch_sub(meta.cloud_size, Ordering::Relaxed);
                    meta.cloud_key
                }
                None => return Ok(false),
            }
        };

        info!("Removing key {} from cloud ({})", key, cloud_key);
        self.backend.delete(&cloud_key).await
    }

    /// Check if key exists in cloud tier
    pub fn is_tiered(&self, key: &str) -> bool {
        self.tiered_items.read().contains_key(key)
    }

    /// Get metadata for a tiered item
    pub fn get_meta(&self, key: &str) -> Option<TieredItemMeta> {
        self.tiered_items.read().get(key).cloned()
    }

    /// Promote data back to hot tier (fetch and remove from cloud)
    pub async fn promote(&self, key: &str) -> Result<Option<Bytes>, CloudError> {
        let data = self.fetch(key).await?;
        if data.is_some() {
            self.remove(key).await?;
        }
        Ok(data)
    }

    /// Get statistics
    pub fn stats(&self) -> CloudTierStats {
        CloudTierStats {
            items_tiered: self.stats_items_tiered.load(Ordering::Relaxed),
            bytes_tiered: self.stats_bytes_tiered.load(Ordering::Relaxed),
            items_fetched: self.stats_items_fetched.load(Ordering::Relaxed),
            bytes_fetched: self.stats_bytes_fetched.load(Ordering::Relaxed),
            current_cloud_size: self.cloud_size.load(Ordering::Relaxed),
            tiered_item_count: self.tiered_items.read().len() as u64,
        }
    }

    /// Build cloud key from original key
    fn build_cloud_key(&self, key: &str) -> String {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        // Create a hash-based path to distribute keys
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let hash = hasher.finish();

        // Use first 4 hex chars as directory prefix for better distribution
        let prefix = format!("{:04x}", (hash >> 48) as u16);
        format!("{}/{}", prefix, key.replace('/', "_"))
    }

    /// Check if an item should be tiered based on policy
    pub fn should_tier(&self, key: &str, size: usize, age: Duration) -> bool {
        // Check size threshold
        if size < self.config.size_threshold {
            return false;
        }

        // Check age threshold
        if age.as_secs() < self.config.min_age_seconds {
            return false;
        }

        // Check cloud size limit
        if self.config.max_cloud_size > 0 {
            let current_size = self.cloud_size.load(Ordering::Relaxed);
            if current_size + size as u64 > self.config.max_cloud_size {
                return false;
            }
        }

        // Already tiered?
        if self.is_tiered(key) {
            return false;
        }

        true
    }

    /// List all tiered keys
    pub fn list_tiered_keys(&self) -> Vec<String> {
        self.tiered_items.read().keys().cloned().collect()
    }
}

/// Cloud tier statistics
#[derive(Debug, Clone)]
pub struct CloudTierStats {
    /// Total items tiered
    pub items_tiered: u64,
    /// Total bytes tiered
    pub bytes_tiered: u64,
    /// Total items fetched
    pub items_fetched: u64,
    /// Total bytes fetched
    pub bytes_fetched: u64,
    /// Current cloud storage size
    pub current_cloud_size: u64,
    /// Current number of tiered items
    pub tiered_item_count: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn create_test_config(temp_dir: &TempDir) -> TieringConfig {
        TieringConfig {
            cloud: CloudConfig {
                provider: super::super::CloudProvider::Local,
                bucket: temp_dir.path().to_string_lossy().to_string(),
                prefix: Some("tier/".to_string()),
                compression_enabled: true,
                ..Default::default()
            },
            min_age_seconds: 0,
            size_threshold: 10,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn test_tier_and_fetch() {
        let temp_dir = TempDir::new().unwrap();
        let config = create_test_config(&temp_dir);
        let tier = CloudTier::new(config).unwrap();

        let key = "test-key";
        let data = Bytes::from("hello world - this is test data");

        // Tier
        tier.tier(key, data.clone()).await.unwrap();
        assert!(tier.is_tiered(key));

        // Fetch
        let fetched = tier.fetch(key).await.unwrap();
        assert_eq!(fetched, Some(data));

        // Check stats
        let stats = tier.stats();
        assert_eq!(stats.items_tiered, 1);
        assert_eq!(stats.items_fetched, 1);
    }

    #[tokio::test]
    async fn test_promote() {
        let temp_dir = TempDir::new().unwrap();
        let config = create_test_config(&temp_dir);
        let tier = CloudTier::new(config).unwrap();

        let key = "promote-key";
        let data = Bytes::from("data to promote back");

        tier.tier(key, data.clone()).await.unwrap();
        assert!(tier.is_tiered(key));

        // Promote
        let promoted = tier.promote(key).await.unwrap();
        assert_eq!(promoted, Some(data));
        assert!(!tier.is_tiered(key));
    }

    #[tokio::test]
    async fn test_remove() {
        let temp_dir = TempDir::new().unwrap();
        let config = create_test_config(&temp_dir);
        let tier = CloudTier::new(config).unwrap();

        let key = "remove-key";
        let data = Bytes::from("data to remove");

        tier.tier(key, data).await.unwrap();
        assert!(tier.is_tiered(key));

        // Remove
        let removed = tier.remove(key).await.unwrap();
        assert!(removed);
        assert!(!tier.is_tiered(key));

        // Fetch should return None
        let fetched = tier.fetch(key).await.unwrap();
        assert!(fetched.is_none());
    }

    #[tokio::test]
    async fn test_size_threshold() {
        let temp_dir = TempDir::new().unwrap();
        let mut config = create_test_config(&temp_dir);
        config.size_threshold = 100; // Require at least 100 bytes

        let tier = CloudTier::new(config).unwrap();

        // Small data should not be tiered
        let key = "small-key";
        let data = Bytes::from("small");

        tier.tier(key, data).await.unwrap();
        assert!(!tier.is_tiered(key)); // Should not be tiered due to size

        // Large data should be tiered
        let key2 = "large-key";
        let data2 = Bytes::from("a".repeat(200));

        tier.tier(key2, data2).await.unwrap();
        assert!(tier.is_tiered(key2));
    }

    #[test]
    fn test_should_tier() {
        let temp_dir = TempDir::new().unwrap();
        let config = create_test_config(&temp_dir);
        let tier = CloudTier::new(config).unwrap();

        // Size below threshold
        assert!(!tier.should_tier("key1", 5, Duration::from_secs(3600)));

        // Age below threshold (if we had one set > 0)
        // Our test config has min_age_seconds = 0

        // Should tier
        assert!(tier.should_tier("key2", 1000, Duration::from_secs(3600)));
    }

    #[test]
    fn test_build_cloud_key() {
        let temp_dir = TempDir::new().unwrap();
        let config = create_test_config(&temp_dir);
        let tier = CloudTier::new(config).unwrap();

        let key1 = tier.build_cloud_key("user:123");
        let key2 = tier.build_cloud_key("user:456");

        // Keys should be different
        assert_ne!(key1, key2);

        // Keys should contain hash prefix
        assert!(key1.contains('/'));
    }
}
