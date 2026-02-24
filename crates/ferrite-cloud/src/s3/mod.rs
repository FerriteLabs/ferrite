//! Native S3-Compatible API
//!
//! Provides S3-compatible object storage interface for Ferrite.
//! Enables direct S3 protocol access to stored data.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                   S3 Compatible API                          │
//! ├─────────────────────────────────────────────────────────────┤
//! │  ┌──────────┐   ┌──────────┐   ┌──────────┐   ┌──────────┐ │
//! │  │  HTTP    │   │  Auth    │   │  Object  │   │ Storage  │ │
//! │  │ Router   │──▶│  (SigV4) │──▶│  Handler │──▶│  Backend │ │
//! │  └──────────┘   └──────────┘   └──────────┘   └──────────┘ │
//! │       │              │              │              │        │
//! │       ▼              ▼              ▼              ▼        │
//! │  Path Parsing   Credentials    CRUD Ops      Ferrite KV    │
//! └─────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Features
//!
//! - **Bucket Operations**: CreateBucket, DeleteBucket, ListBuckets
//! - **Object Operations**: GetObject, PutObject, DeleteObject, ListObjects
//! - **Multipart Upload**: InitiateMultipartUpload, UploadPart, CompleteMultipartUpload
//! - **Metadata**: HeadObject, GetObjectMetadata
//! - **Authentication**: AWS Signature V4
//!
//! # Example
//!
//! ```ignore
//! use ferrite::s3::{S3Server, S3Config};
//!
//! let config = S3Config::default()
//!     .with_access_key("AKIAEXAMPLE")
//!     .with_secret_key("secret");
//!
//! let server = S3Server::new(config);
//! server.start("0.0.0.0:9000").await?;
//! ```

#![allow(dead_code)]
mod auth;
mod bucket;
mod object;
mod request;
mod response;

pub use auth::{Credentials, S3Auth, SignatureV4};
pub use bucket::{Bucket, BucketConfig, BucketInfo};
pub use object::{MultipartUpload, ObjectMetadata, S3Object, UploadPart};
pub use request::{S3Operation, S3Request};
pub use response::{S3Error, S3ErrorCode, S3Response};

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

/// S3 server configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3Config {
    /// Server bind address
    pub bind_address: String,
    /// Access key ID
    pub access_key_id: String,
    /// Secret access key
    pub secret_access_key: String,
    /// Region name
    pub region: String,
    /// Maximum object size (bytes)
    pub max_object_size: u64,
    /// Maximum number of buckets
    pub max_buckets: usize,
    /// Enable virtual-hosted style (bucket.domain.com)
    pub virtual_hosted_style: bool,
    /// Enable path-style (domain.com/bucket)
    pub path_style: bool,
}

impl Default for S3Config {
    fn default() -> Self {
        Self {
            bind_address: "0.0.0.0:9000".to_string(),
            access_key_id: "minioadmin".to_string(),
            secret_access_key: "minioadmin".to_string(),
            region: "us-east-1".to_string(),
            max_object_size: 5 * 1024 * 1024 * 1024, // 5GB
            max_buckets: 100,
            virtual_hosted_style: true,
            path_style: true,
        }
    }
}

impl S3Config {
    /// Create a new config
    pub fn new() -> Self {
        Self::default()
    }

    /// Set access key
    pub fn with_access_key(mut self, key: impl Into<String>) -> Self {
        self.access_key_id = key.into();
        self
    }

    /// Set secret key
    pub fn with_secret_key(mut self, key: impl Into<String>) -> Self {
        self.secret_access_key = key.into();
        self
    }

    /// Set region
    pub fn with_region(mut self, region: impl Into<String>) -> Self {
        self.region = region.into();
        self
    }

    /// Set bind address
    pub fn with_bind_address(mut self, addr: impl Into<String>) -> Self {
        self.bind_address = addr.into();
        self
    }
}

/// S3 compatible server
pub struct S3Server {
    config: S3Config,
    buckets: Arc<RwLock<HashMap<String, Bucket>>>,
    auth: S3Auth,
}

impl S3Server {
    /// Create a new S3 server
    pub fn new(config: S3Config) -> Self {
        let credentials = Credentials::new(&config.access_key_id, &config.secret_access_key);
        let auth = S3Auth::new(credentials, &config.region);

        Self {
            config,
            buckets: Arc::new(RwLock::new(HashMap::new())),
            auth,
        }
    }

    /// Create with default config
    pub fn with_defaults() -> Self {
        Self::new(S3Config::default())
    }

    /// Handle an S3 request
    pub fn handle_request(&self, request: &S3Request) -> S3Response {
        // Verify authentication
        if let Err(e) = self.auth.verify(request) {
            return S3Response::error(S3ErrorCode::AccessDenied, &e.to_string());
        }

        match request.operation {
            // Bucket operations
            S3Operation::ListBuckets => self.list_buckets(),
            S3Operation::CreateBucket => self.create_bucket(&request.bucket),
            S3Operation::DeleteBucket => self.delete_bucket(&request.bucket),
            S3Operation::HeadBucket => self.head_bucket(&request.bucket),

            // Object operations
            S3Operation::GetObject => self.get_object(&request.bucket, &request.key),
            S3Operation::PutObject => self.put_object(
                &request.bucket,
                &request.key,
                request.body.clone(),
                request.metadata.clone(),
            ),
            S3Operation::DeleteObject => self.delete_object(&request.bucket, &request.key),
            S3Operation::HeadObject => self.head_object(&request.bucket, &request.key),
            S3Operation::ListObjects => {
                self.list_objects(&request.bucket, request.prefix.as_deref(), request.max_keys)
            }

            // Multipart operations
            S3Operation::InitiateMultipartUpload => {
                self.initiate_multipart(&request.bucket, &request.key)
            }
            S3Operation::UploadPart => self.upload_part(
                &request.bucket,
                &request.key,
                &request.upload_id,
                request.part_number,
                request.body.clone(),
            ),
            S3Operation::CompleteMultipartUpload => self.complete_multipart(
                &request.bucket,
                &request.key,
                &request.upload_id,
                &request.parts,
            ),
            S3Operation::AbortMultipartUpload => {
                self.abort_multipart(&request.bucket, &request.key, &request.upload_id)
            }
        }
    }

    // Bucket operations

    /// List all buckets
    pub fn list_buckets(&self) -> S3Response {
        let buckets = self.buckets.read();
        let bucket_list: Vec<BucketInfo> = buckets.values().map(|b| b.info()).collect();
        S3Response::list_buckets(bucket_list)
    }

    /// Create a bucket
    pub fn create_bucket(&self, name: &str) -> S3Response {
        if name.is_empty() {
            return S3Response::error(S3ErrorCode::InvalidBucketName, "Bucket name is empty");
        }

        let mut buckets = self.buckets.write();

        if buckets.len() >= self.config.max_buckets {
            return S3Response::error(S3ErrorCode::TooManyBuckets, "Maximum bucket limit reached");
        }

        if buckets.contains_key(name) {
            return S3Response::error(S3ErrorCode::BucketAlreadyExists, "Bucket already exists");
        }

        let bucket = Bucket::new(name);
        buckets.insert(name.to_string(), bucket);

        S3Response::ok()
    }

    /// Delete a bucket
    pub fn delete_bucket(&self, name: &str) -> S3Response {
        let mut buckets = self.buckets.write();

        if let Some(bucket) = buckets.get(name) {
            if !bucket.is_empty() {
                return S3Response::error(S3ErrorCode::BucketNotEmpty, "Bucket is not empty");
            }
            buckets.remove(name);
            S3Response::ok()
        } else {
            S3Response::error(S3ErrorCode::NoSuchBucket, "Bucket not found")
        }
    }

    /// Check if bucket exists
    pub fn head_bucket(&self, name: &str) -> S3Response {
        let buckets = self.buckets.read();
        if buckets.contains_key(name) {
            S3Response::ok()
        } else {
            S3Response::error(S3ErrorCode::NoSuchBucket, "Bucket not found")
        }
    }

    // Object operations

    /// Get an object
    pub fn get_object(&self, bucket: &str, key: &str) -> S3Response {
        let buckets = self.buckets.read();
        match buckets.get(bucket) {
            Some(b) => match b.get_object(key) {
                Some(obj) => S3Response::object(obj.data.clone(), obj.metadata.clone()),
                None => S3Response::error(S3ErrorCode::NoSuchKey, "Object not found"),
            },
            None => S3Response::error(S3ErrorCode::NoSuchBucket, "Bucket not found"),
        }
    }

    /// Put an object
    pub fn put_object(
        &self,
        bucket: &str,
        key: &str,
        data: Vec<u8>,
        metadata: HashMap<String, String>,
    ) -> S3Response {
        if data.len() as u64 > self.config.max_object_size {
            return S3Response::error(S3ErrorCode::EntityTooLarge, "Object too large");
        }

        let mut buckets = self.buckets.write();
        match buckets.get_mut(bucket) {
            Some(b) => {
                let etag = b.put_object(key, data, metadata);
                S3Response::put_object(&etag)
            }
            None => S3Response::error(S3ErrorCode::NoSuchBucket, "Bucket not found"),
        }
    }

    /// Delete an object
    pub fn delete_object(&self, bucket: &str, key: &str) -> S3Response {
        let mut buckets = self.buckets.write();
        match buckets.get_mut(bucket) {
            Some(b) => {
                b.delete_object(key);
                S3Response::ok()
            }
            None => S3Response::error(S3ErrorCode::NoSuchBucket, "Bucket not found"),
        }
    }

    /// Head object (get metadata)
    pub fn head_object(&self, bucket: &str, key: &str) -> S3Response {
        let buckets = self.buckets.read();
        match buckets.get(bucket) {
            Some(b) => match b.get_object(key) {
                Some(obj) => S3Response::head_object(&obj.metadata),
                None => S3Response::error(S3ErrorCode::NoSuchKey, "Object not found"),
            },
            None => S3Response::error(S3ErrorCode::NoSuchBucket, "Bucket not found"),
        }
    }

    /// List objects in bucket
    pub fn list_objects(
        &self,
        bucket: &str,
        prefix: Option<&str>,
        max_keys: Option<usize>,
    ) -> S3Response {
        let buckets = self.buckets.read();
        match buckets.get(bucket) {
            Some(b) => {
                let objects = b.list_objects(prefix, max_keys.unwrap_or(1000));
                S3Response::list_objects(objects)
            }
            None => S3Response::error(S3ErrorCode::NoSuchBucket, "Bucket not found"),
        }
    }

    // Multipart operations

    fn initiate_multipart(&self, bucket: &str, key: &str) -> S3Response {
        let mut buckets = self.buckets.write();
        match buckets.get_mut(bucket) {
            Some(b) => {
                let upload_id = b.initiate_multipart(key);
                S3Response::initiate_multipart(&upload_id)
            }
            None => S3Response::error(S3ErrorCode::NoSuchBucket, "Bucket not found"),
        }
    }

    fn upload_part(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        part_number: u32,
        data: Vec<u8>,
    ) -> S3Response {
        let mut buckets = self.buckets.write();
        match buckets.get_mut(bucket) {
            Some(b) => match b.upload_part(key, upload_id, part_number, data) {
                Ok(etag) => S3Response::upload_part(&etag),
                Err(e) => S3Response::error(S3ErrorCode::NoSuchUpload, &e),
            },
            None => S3Response::error(S3ErrorCode::NoSuchBucket, "Bucket not found"),
        }
    }

    fn complete_multipart(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        parts: &[(u32, String)],
    ) -> S3Response {
        let mut buckets = self.buckets.write();
        match buckets.get_mut(bucket) {
            Some(b) => match b.complete_multipart(key, upload_id, parts) {
                Ok(etag) => S3Response::complete_multipart(&etag),
                Err(e) => S3Response::error(S3ErrorCode::NoSuchUpload, &e),
            },
            None => S3Response::error(S3ErrorCode::NoSuchBucket, "Bucket not found"),
        }
    }

    fn abort_multipart(&self, bucket: &str, key: &str, upload_id: &str) -> S3Response {
        let mut buckets = self.buckets.write();
        match buckets.get_mut(bucket) {
            Some(b) => {
                b.abort_multipart(key, upload_id);
                S3Response::ok()
            }
            None => S3Response::error(S3ErrorCode::NoSuchBucket, "Bucket not found"),
        }
    }

    /// Get server config
    pub fn config(&self) -> &S3Config {
        &self.config
    }

    /// Get statistics
    pub fn stats(&self) -> S3Stats {
        let buckets = self.buckets.read();
        let mut total_objects = 0u64;
        let mut total_size = 0u64;

        for bucket in buckets.values() {
            let (count, size) = bucket.stats();
            total_objects += count;
            total_size += size;
        }

        S3Stats {
            bucket_count: buckets.len() as u64,
            total_objects,
            total_size,
        }
    }
}

impl Default for S3Server {
    fn default() -> Self {
        Self::with_defaults()
    }
}

/// S3 server statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3Stats {
    /// Number of buckets
    pub bucket_count: u64,
    /// Total objects
    pub total_objects: u64,
    /// Total size in bytes
    pub total_size: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_server() {
        let server = S3Server::with_defaults();
        assert_eq!(server.config().region, "us-east-1");
    }

    #[test]
    fn test_bucket_operations() {
        let server = S3Server::with_defaults();

        // Create a bucket
        let req = S3Request {
            operation: S3Operation::CreateBucket,
            bucket: "test-bucket".to_string(),
            key: String::new(),
            body: vec![],
            metadata: HashMap::new(),
            prefix: None,
            max_keys: None,
            upload_id: String::new(),
            part_number: 0,
            parts: vec![],
            auth_header: String::new(),
        };

        // Need to skip auth for testing
        let result = server.create_bucket("test-bucket");
        assert!(result.is_success());

        // List buckets
        let list_result = server.list_buckets();
        assert!(list_result.is_success());

        // Delete bucket
        let delete_result = server.delete_bucket("test-bucket");
        assert!(delete_result.is_success());
    }
}
