//! S3 Object types

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// Object metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectMetadata {
    /// Object key
    pub key: String,
    /// Size in bytes
    pub size: u64,
    /// ETag (content hash)
    pub etag: String,
    /// Content type
    pub content_type: String,
    /// Last modified timestamp
    pub last_modified: u64,
    /// Custom metadata
    pub custom: HashMap<String, String>,
}

impl ObjectMetadata {
    /// Create new metadata
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            size: 0,
            etag: String::new(),
            content_type: "application/octet-stream".to_string(),
            last_modified: 0,
            custom: HashMap::new(),
        }
    }
}

/// An S3 object
#[derive(Debug, Clone)]
pub struct S3Object {
    /// Object key
    pub key: String,
    /// Object data
    pub data: Vec<u8>,
    /// Object metadata
    pub metadata: ObjectMetadata,
}

impl S3Object {
    /// Create a new object
    pub fn new(key: impl Into<String>, data: Vec<u8>) -> Self {
        let key = key.into();
        let size = data.len() as u64;

        Self {
            key: key.clone(),
            data,
            metadata: ObjectMetadata {
                key,
                size,
                etag: String::new(),
                content_type: "application/octet-stream".to_string(),
                last_modified: 0,
                custom: HashMap::new(),
            },
        }
    }
}

/// A part in a multipart upload
#[derive(Debug, Clone)]
pub struct UploadPart {
    /// Part number (1-10000)
    pub part_number: u32,
    /// Part data
    pub data: Vec<u8>,
    /// Part ETag
    pub etag: String,
}

impl UploadPart {
    /// Create a new part
    pub fn new(part_number: u32, data: Vec<u8>, etag: impl Into<String>) -> Self {
        Self {
            part_number,
            data,
            etag: etag.into(),
        }
    }
}

/// A multipart upload session
#[derive(Debug, Clone)]
pub struct MultipartUpload {
    /// Target object key
    pub key: String,
    /// Upload ID
    pub upload_id: String,
    /// Uploaded parts
    pub parts: HashMap<u32, UploadPart>,
    /// Initiated timestamp
    pub initiated: u64,
}

impl MultipartUpload {
    /// Create a new multipart upload
    pub fn new(key: impl Into<String>, upload_id: impl Into<String>) -> Self {
        use std::time::{SystemTime, UNIX_EPOCH};

        Self {
            key: key.into(),
            upload_id: upload_id.into(),
            parts: HashMap::new(),
            initiated: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        }
    }

    /// Add a part
    pub fn add_part(&mut self, part_number: u32, data: Vec<u8>, etag: &str) {
        let part = UploadPart::new(part_number, data, etag);
        self.parts.insert(part_number, part);
    }

    /// Check if a part exists with matching etag
    pub fn has_part(&self, part_number: u32, etag: &str) -> bool {
        self.parts
            .get(&part_number)
            .map(|p| p.etag == etag)
            .unwrap_or(false)
    }

    /// Assemble parts into final object data
    pub fn assemble(&self, part_order: &[(u32, String)]) -> Vec<u8> {
        let mut result = Vec::new();

        for (part_num, _etag) in part_order {
            if let Some(part) = self.parts.get(part_num) {
                result.extend_from_slice(&part.data);
            }
        }

        result
    }

    /// Get total size of all parts
    pub fn total_size(&self) -> u64 {
        self.parts.values().map(|p| p.data.len() as u64).sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_object_creation() {
        let obj = S3Object::new("test.txt", b"Hello".to_vec());
        assert_eq!(obj.key, "test.txt");
        assert_eq!(obj.data, b"Hello");
        assert_eq!(obj.metadata.size, 5);
    }

    #[test]
    fn test_multipart_upload() {
        let mut upload = MultipartUpload::new("large.bin", "upload-123");

        upload.add_part(1, b"part1".to_vec(), "etag1");
        upload.add_part(2, b"part2".to_vec(), "etag2");
        upload.add_part(3, b"part3".to_vec(), "etag3");

        assert!(upload.has_part(1, "etag1"));
        assert!(upload.has_part(2, "etag2"));
        assert!(!upload.has_part(1, "wrong"));
        assert!(!upload.has_part(99, "etag"));

        let assembled = upload.assemble(&[(1, "etag1".to_string()), (2, "etag2".to_string())]);
        assert_eq!(assembled, b"part1part2");
    }
}
