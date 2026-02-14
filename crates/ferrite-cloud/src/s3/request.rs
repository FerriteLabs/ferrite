//! S3 Request types

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// S3 operation types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum S3Operation {
    // Bucket operations
    /// List all buckets
    ListBuckets,
    /// Create a bucket
    CreateBucket,
    /// Delete a bucket
    DeleteBucket,
    /// Check if bucket exists
    HeadBucket,

    // Object operations
    /// Get an object
    GetObject,
    /// Put an object
    PutObject,
    /// Delete an object
    DeleteObject,
    /// Get object metadata
    HeadObject,
    /// List objects in bucket
    ListObjects,

    // Multipart operations
    /// Start multipart upload
    InitiateMultipartUpload,
    /// Upload a part
    UploadPart,
    /// Complete multipart upload
    CompleteMultipartUpload,
    /// Abort multipart upload
    AbortMultipartUpload,
}

impl S3Operation {
    /// Parse operation from HTTP method and path
    pub fn parse(method: &str, path: &str, query: &HashMap<String, String>) -> Option<Self> {
        let parts: Vec<&str> = path.trim_start_matches('/').split('/').collect();

        match (method, parts.as_slice()) {
            // Service level
            ("GET", [""]) | ("GET", []) => Some(S3Operation::ListBuckets),

            // Bucket level
            ("PUT", [_bucket]) | ("PUT", [_bucket, ""]) if query.is_empty() => {
                Some(S3Operation::CreateBucket)
            }
            ("DELETE", [_bucket]) | ("DELETE", [_bucket, ""]) if query.is_empty() => {
                Some(S3Operation::DeleteBucket)
            }
            ("HEAD", [_bucket]) | ("HEAD", [_bucket, ""]) => Some(S3Operation::HeadBucket),
            ("GET", [_bucket]) | ("GET", [_bucket, ""]) => Some(S3Operation::ListObjects),

            // Object level
            ("GET", [_bucket, key, ..]) if !key.is_empty() => Some(S3Operation::GetObject),
            ("PUT", [_bucket, key, ..]) if !key.is_empty() => {
                if query.contains_key("uploadId") && query.contains_key("partNumber") {
                    Some(S3Operation::UploadPart)
                } else {
                    Some(S3Operation::PutObject)
                }
            }
            ("DELETE", [_bucket, key, ..]) if !key.is_empty() => {
                if query.contains_key("uploadId") {
                    Some(S3Operation::AbortMultipartUpload)
                } else {
                    Some(S3Operation::DeleteObject)
                }
            }
            ("HEAD", [_bucket, key, ..]) if !key.is_empty() => Some(S3Operation::HeadObject),

            // Multipart
            ("POST", [_bucket, key, ..]) if !key.is_empty() => {
                if query.contains_key("uploads") {
                    Some(S3Operation::InitiateMultipartUpload)
                } else if query.contains_key("uploadId") {
                    Some(S3Operation::CompleteMultipartUpload)
                } else {
                    None
                }
            }

            _ => None,
        }
    }

    /// Get HTTP method for operation
    pub fn method(&self) -> &'static str {
        match self {
            S3Operation::ListBuckets => "GET",
            S3Operation::CreateBucket => "PUT",
            S3Operation::DeleteBucket => "DELETE",
            S3Operation::HeadBucket => "HEAD",
            S3Operation::GetObject => "GET",
            S3Operation::PutObject => "PUT",
            S3Operation::DeleteObject => "DELETE",
            S3Operation::HeadObject => "HEAD",
            S3Operation::ListObjects => "GET",
            S3Operation::InitiateMultipartUpload => "POST",
            S3Operation::UploadPart => "PUT",
            S3Operation::CompleteMultipartUpload => "POST",
            S3Operation::AbortMultipartUpload => "DELETE",
        }
    }
}

/// An S3 request
#[derive(Debug, Clone)]
pub struct S3Request {
    /// Operation type
    pub operation: S3Operation,
    /// Target bucket
    pub bucket: String,
    /// Object key
    pub key: String,
    /// Request body
    pub body: Vec<u8>,
    /// Custom metadata headers
    pub metadata: HashMap<String, String>,
    /// List prefix filter
    pub prefix: Option<String>,
    /// Max keys for listing
    pub max_keys: Option<usize>,
    /// Multipart upload ID
    pub upload_id: String,
    /// Part number for multipart
    pub part_number: u32,
    /// Parts for complete multipart
    pub parts: Vec<(u32, String)>,
    /// Authorization header
    pub auth_header: String,
}

impl S3Request {
    /// Create a new request
    pub fn new(operation: S3Operation) -> Self {
        Self {
            operation,
            bucket: String::new(),
            key: String::new(),
            body: Vec::new(),
            metadata: HashMap::new(),
            prefix: None,
            max_keys: None,
            upload_id: String::new(),
            part_number: 0,
            parts: Vec::new(),
            auth_header: String::new(),
        }
    }

    /// Set bucket
    pub fn with_bucket(mut self, bucket: impl Into<String>) -> Self {
        self.bucket = bucket.into();
        self
    }

    /// Set key
    pub fn with_key(mut self, key: impl Into<String>) -> Self {
        self.key = key.into();
        self
    }

    /// Set body
    pub fn with_body(mut self, body: Vec<u8>) -> Self {
        self.body = body;
        self
    }

    /// Add metadata
    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }

    /// Set auth header
    pub fn with_auth(mut self, auth: impl Into<String>) -> Self {
        self.auth_header = auth.into();
        self
    }

    /// Parse from HTTP request components
    pub fn parse(
        method: &str,
        path: &str,
        query: &HashMap<String, String>,
        headers: &HashMap<String, String>,
        body: Vec<u8>,
    ) -> Option<Self> {
        let operation = S3Operation::parse(method, path, query)?;

        let parts: Vec<&str> = path.trim_start_matches('/').split('/').collect();

        let bucket = parts.first().map(|s| s.to_string()).unwrap_or_default();
        let key = if parts.len() > 1 {
            parts[1..].join("/")
        } else {
            String::new()
        };

        // Extract metadata from x-amz-meta-* headers
        let metadata: HashMap<String, String> = headers
            .iter()
            .filter(|(k, _)| k.starts_with("x-amz-meta-"))
            .map(|(k, v)| (k.trim_start_matches("x-amz-meta-").to_string(), v.clone()))
            .collect();

        let mut request = Self {
            operation,
            bucket,
            key,
            body,
            metadata,
            prefix: query.get("prefix").cloned(),
            max_keys: query.get("max-keys").and_then(|s| s.parse().ok()),
            upload_id: query.get("uploadId").cloned().unwrap_or_default(),
            part_number: query
                .get("partNumber")
                .and_then(|s| s.parse().ok())
                .unwrap_or(0),
            parts: Vec::new(),
            auth_header: headers.get("authorization").cloned().unwrap_or_default(),
        };

        // Parse content-type
        if let Some(ct) = headers.get("content-type") {
            request
                .metadata
                .insert("content-type".to_string(), ct.clone());
        }

        Some(request)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_list_buckets() {
        let op = S3Operation::parse("GET", "/", &HashMap::new());
        assert_eq!(op, Some(S3Operation::ListBuckets));
    }

    #[test]
    fn test_parse_create_bucket() {
        let op = S3Operation::parse("PUT", "/my-bucket", &HashMap::new());
        assert_eq!(op, Some(S3Operation::CreateBucket));
    }

    #[test]
    fn test_parse_get_object() {
        let op = S3Operation::parse("GET", "/bucket/key.txt", &HashMap::new());
        assert_eq!(op, Some(S3Operation::GetObject));
    }

    #[test]
    fn test_parse_list_objects() {
        let op = S3Operation::parse("GET", "/bucket", &HashMap::new());
        assert_eq!(op, Some(S3Operation::ListObjects));
    }

    #[test]
    fn test_parse_multipart() {
        let mut query = HashMap::new();
        query.insert("uploads".to_string(), "".to_string());
        let op = S3Operation::parse("POST", "/bucket/key", &query);
        assert_eq!(op, Some(S3Operation::InitiateMultipartUpload));
    }
}
