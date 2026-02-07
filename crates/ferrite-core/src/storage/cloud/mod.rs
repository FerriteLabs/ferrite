//! Cloud Storage Tiering
//!
//! This module provides cloud storage tiering capabilities for cold data.
//! It supports multiple cloud providers (S3, GCS, Azure) through a unified interface.

mod backend;
mod tiering;

pub use backend::{CloudBackend, CloudConfig, CloudError, CloudProvider};
pub use tiering::{CloudTier, TieringConfig, TieringPolicy};

use async_trait::async_trait;
use bytes::Bytes;

/// Cloud storage operations trait
#[async_trait]
pub trait CloudStorage: Send + Sync {
    /// Store data to cloud storage
    async fn put(&self, key: &str, data: Bytes) -> Result<(), CloudError>;

    /// Retrieve data from cloud storage
    async fn get(&self, key: &str) -> Result<Option<Bytes>, CloudError>;

    /// Delete data from cloud storage
    async fn delete(&self, key: &str) -> Result<bool, CloudError>;

    /// Check if data exists in cloud storage
    async fn exists(&self, key: &str) -> Result<bool, CloudError>;

    /// List keys with a given prefix
    async fn list(&self, prefix: &str) -> Result<Vec<String>, CloudError>;

    /// Get the size of an object (in bytes)
    async fn size(&self, key: &str) -> Result<Option<u64>, CloudError>;
}

/// Create a cloud backend from configuration
pub fn create_backend(config: &CloudConfig) -> Result<Box<dyn CloudStorage>, CloudError> {
    CloudBackend::new(config)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_cloud_config_parse() {
        let config = CloudConfig {
            provider: CloudProvider::Local,
            bucket: "test-bucket".to_string(),
            prefix: Some("ferrite/".to_string()),
            region: None,
            endpoint: None,
            access_key_id: None,
            secret_access_key: None,
            compression_enabled: true,
            compression_level: 6,
        };

        assert_eq!(config.provider, CloudProvider::Local);
        assert_eq!(config.bucket, "test-bucket");
        assert!(config.compression_enabled);
    }
}
