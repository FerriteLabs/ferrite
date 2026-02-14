//! S3 Bucket implementation

use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use super::object::{MultipartUpload, ObjectMetadata, S3Object};

/// Bucket configuration
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BucketConfig {
    /// Versioning enabled
    pub versioning: bool,
    /// Encryption enabled
    pub encryption: bool,
    /// Lifecycle rules
    pub lifecycle_enabled: bool,
}

/// Bucket info for listing
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BucketInfo {
    /// Bucket name
    pub name: String,
    /// Creation date
    pub creation_date: u64,
}

/// An S3 bucket
pub struct Bucket {
    /// Bucket name
    pub name: String,
    /// Creation timestamp
    pub created_at: u64,
    /// Configuration
    pub config: BucketConfig,
    /// Objects in the bucket
    objects: RwLock<HashMap<String, S3Object>>,
    /// Active multipart uploads
    multipart_uploads: RwLock<HashMap<String, MultipartUpload>>,
}

impl Bucket {
    /// Create a new bucket
    pub fn new(name: impl Into<String>) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        Self {
            name: name.into(),
            created_at: now,
            config: BucketConfig::default(),
            objects: RwLock::new(HashMap::new()),
            multipart_uploads: RwLock::new(HashMap::new()),
        }
    }

    /// Get bucket info
    pub fn info(&self) -> BucketInfo {
        BucketInfo {
            name: self.name.clone(),
            creation_date: self.created_at,
        }
    }

    /// Check if bucket is empty
    pub fn is_empty(&self) -> bool {
        self.objects.read().is_empty()
    }

    /// Get an object
    pub fn get_object(&self, key: &str) -> Option<S3Object> {
        self.objects.read().get(key).cloned()
    }

    /// Put an object
    pub fn put_object(
        &mut self,
        key: &str,
        data: Vec<u8>,
        mut metadata: HashMap<String, String>,
    ) -> String {
        let etag = compute_etag(&data);
        let size = data.len() as u64;

        metadata.insert("content-length".to_string(), size.to_string());
        metadata.insert("etag".to_string(), etag.clone());

        let obj_metadata = ObjectMetadata {
            key: key.to_string(),
            size,
            etag: etag.clone(),
            content_type: metadata
                .get("content-type")
                .cloned()
                .unwrap_or_else(|| "application/octet-stream".to_string()),
            last_modified: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            custom: metadata,
        };

        let obj = S3Object {
            key: key.to_string(),
            data,
            metadata: obj_metadata,
        };

        self.objects.write().insert(key.to_string(), obj);
        etag
    }

    /// Delete an object
    pub fn delete_object(&mut self, key: &str) -> bool {
        self.objects.write().remove(key).is_some()
    }

    /// List objects with prefix
    pub fn list_objects(&self, prefix: Option<&str>, max_keys: usize) -> Vec<ObjectMetadata> {
        let objects = self.objects.read();

        objects
            .values()
            .filter(|obj| {
                if let Some(p) = prefix {
                    obj.key.starts_with(p)
                } else {
                    true
                }
            })
            .take(max_keys)
            .map(|obj| obj.metadata.clone())
            .collect()
    }

    /// Initiate multipart upload
    pub fn initiate_multipart(&mut self, key: &str) -> String {
        let upload_id = generate_upload_id();
        let upload = MultipartUpload::new(key, &upload_id);

        let multipart_key = format!("{}:{}", key, upload_id);
        self.multipart_uploads.write().insert(multipart_key, upload);

        upload_id
    }

    /// Upload a part
    pub fn upload_part(
        &mut self,
        key: &str,
        upload_id: &str,
        part_number: u32,
        data: Vec<u8>,
    ) -> Result<String, String> {
        let multipart_key = format!("{}:{}", key, upload_id);
        let mut uploads = self.multipart_uploads.write();

        match uploads.get_mut(&multipart_key) {
            Some(upload) => {
                let etag = compute_etag(&data);
                upload.add_part(part_number, data, &etag);
                Ok(etag)
            }
            None => Err("No such upload".to_string()),
        }
    }

    /// Complete multipart upload
    pub fn complete_multipart(
        &mut self,
        key: &str,
        upload_id: &str,
        parts: &[(u32, String)],
    ) -> Result<String, String> {
        let multipart_key = format!("{}:{}", key, upload_id);
        let mut uploads = self.multipart_uploads.write();

        match uploads.remove(&multipart_key) {
            Some(upload) => {
                // Verify all parts
                for (part_num, etag) in parts {
                    if !upload.has_part(*part_num, etag) {
                        return Err(format!("Invalid part {} or etag mismatch", part_num));
                    }
                }

                // Assemble object
                let data = upload.assemble(parts);
                drop(uploads); // Release lock before put_object

                let final_etag = self.put_object(key, data, HashMap::new());
                Ok(final_etag)
            }
            None => Err("No such upload".to_string()),
        }
    }

    /// Abort multipart upload
    pub fn abort_multipart(&mut self, key: &str, upload_id: &str) {
        let multipart_key = format!("{}:{}", key, upload_id);
        self.multipart_uploads.write().remove(&multipart_key);
    }

    /// Get bucket statistics
    pub fn stats(&self) -> (u64, u64) {
        let objects = self.objects.read();
        let count = objects.len() as u64;
        let size: u64 = objects.values().map(|o| o.data.len() as u64).sum();
        (count, size)
    }
}

/// Compute ETag (MD5 hash) for data
fn compute_etag(data: &[u8]) -> String {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let mut hasher = DefaultHasher::new();
    data.hash(&mut hasher);
    let hash = hasher.finish();

    format!("\"{:016x}\"", hash)
}

/// Generate a unique upload ID
fn generate_upload_id() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};

    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();

    let random: u64 = rand::random();
    format!("{:x}{:x}", timestamp, random)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bucket_creation() {
        let bucket = Bucket::new("test-bucket");
        assert_eq!(bucket.name, "test-bucket");
        assert!(bucket.is_empty());
    }

    #[test]
    fn test_put_get_object() {
        let mut bucket = Bucket::new("test");
        let data = b"Hello, World!".to_vec();

        bucket.put_object("hello.txt", data.clone(), HashMap::new());

        let obj = bucket.get_object("hello.txt").unwrap();
        assert_eq!(obj.data, data);
    }

    #[test]
    fn test_delete_object() {
        let mut bucket = Bucket::new("test");
        bucket.put_object("hello.txt", b"data".to_vec(), HashMap::new());

        assert!(bucket.delete_object("hello.txt"));
        assert!(bucket.get_object("hello.txt").is_none());
    }

    #[test]
    fn test_list_objects() {
        let mut bucket = Bucket::new("test");
        bucket.put_object("folder/a.txt", b"a".to_vec(), HashMap::new());
        bucket.put_object("folder/b.txt", b"b".to_vec(), HashMap::new());
        bucket.put_object("other.txt", b"c".to_vec(), HashMap::new());

        let with_prefix = bucket.list_objects(Some("folder/"), 100);
        assert_eq!(with_prefix.len(), 2);

        let all = bucket.list_objects(None, 100);
        assert_eq!(all.len(), 3);
    }

    #[test]
    fn test_multipart_upload() {
        let mut bucket = Bucket::new("test");

        let upload_id = bucket.initiate_multipart("large.bin");

        bucket
            .upload_part("large.bin", &upload_id, 1, b"part1".to_vec())
            .unwrap();
        bucket
            .upload_part("large.bin", &upload_id, 2, b"part2".to_vec())
            .unwrap();

        // Get the ETags
        let etag1 = compute_etag(b"part1");
        let etag2 = compute_etag(b"part2");

        let parts = vec![(1, etag1), (2, etag2)];
        bucket
            .complete_multipart("large.bin", &upload_id, &parts)
            .unwrap();

        let obj = bucket.get_object("large.bin").unwrap();
        assert_eq!(obj.data, b"part1part2");
    }
}
