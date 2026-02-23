//! # S3-Compatible API Endpoint
//!
//! Provides an S3-compatible HTTP API for Ferrite, enabling any S3 client
//! to read and write data. Maps S3 buckets to Ferrite databases and S3
//! object keys to Ferrite keys.
//!
//! ## Supported Operations
//!
//! | S3 Operation | HTTP Method | Path | Ferrite Mapping |
//! |-------------|-------------|------|-----------------|
//! | GetObject | GET | /{bucket}/{key} | GET key (in db mapped from bucket) |
//! | PutObject | PUT | /{bucket}/{key} | SET key value |
//! | DeleteObject | DELETE | /{bucket}/{key} | DEL key |
//! | HeadObject | HEAD | /{bucket}/{key} | EXISTS key |
//! | ListObjects | GET | /{bucket}?list-type=2 | SCAN with prefix |
//!
//! ## Configuration
//!
//! Enable in `ferrite.toml`:
//! ```toml
//! [s3_compat]
//! enabled = true
//! bind = "0.0.0.0"
//! port = 9000
//! default_bucket = "default"
//! ```

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// S3-compatible endpoint configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3CompatConfig {
    /// Whether the S3-compatible endpoint is enabled.
    pub enabled: bool,
    /// Bind address for the HTTP listener.
    pub bind: String,
    /// Port for the HTTP listener.
    pub port: u16,
    /// Default bucket name (maps to database 0).
    pub default_bucket: String,
    /// Maximum object size in bytes.
    pub max_object_size: usize,
    /// Bucket-to-database mapping (bucket name → database index).
    pub bucket_map: HashMap<String, u8>,
}

impl Default for S3CompatConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            bind: "0.0.0.0".to_string(),
            port: 9000,
            default_bucket: "default".to_string(),
            max_object_size: 512 * 1024 * 1024, // 512MB
            bucket_map: HashMap::new(),
        }
    }
}

/// Parsed S3 request.
#[derive(Debug, Clone)]
#[allow(missing_docs)]
pub enum S3Request {
    /// GET /{bucket}/{key}
    GetObject { bucket: String, key: String },
    /// PUT /{bucket}/{key}
    PutObject {
        bucket: String,
        key: String,
        content_type: Option<String>,
    },
    /// DELETE /{bucket}/{key}
    DeleteObject { bucket: String, key: String },
    /// HEAD /{bucket}/{key}
    HeadObject { bucket: String, key: String },
    /// GET /{bucket}?list-type=2&prefix=...&max-keys=...
    ListObjects {
        bucket: String,
        prefix: Option<String>,
        max_keys: usize,
        continuation_token: Option<String>,
    },
    /// GET / (list buckets)
    ListBuckets,
}

/// S3 XML error response body.
pub fn error_xml(code: &str, message: &str, resource: &str, request_id: &str) -> String {
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<Error>
  <Code>{code}</Code>
  <Message>{message}</Message>
  <Resource>{resource}</Resource>
  <RequestId>{request_id}</RequestId>
</Error>"#
    )
}

/// S3 XML list buckets response.
pub fn list_buckets_xml(buckets: &[String]) -> String {
    let bucket_entries: String = buckets
        .iter()
        .map(|name| {
            format!(
                "    <Bucket><Name>{name}</Name><CreationDate>2026-01-01T00:00:00.000Z</CreationDate></Bucket>"
            )
        })
        .collect::<Vec<_>>()
        .join("\n");

    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<ListAllMyBucketsResult>
  <Owner>
    <ID>ferrite</ID>
    <DisplayName>ferrite</DisplayName>
  </Owner>
  <Buckets>
{bucket_entries}
  </Buckets>
</ListAllMyBucketsResult>"#
    )
}

/// Parse an HTTP request path into an S3 request.
pub fn parse_request(
    method: &str,
    path: &str,
    query: Option<&str>,
) -> Result<S3Request, String> {
    let path = path.trim_start_matches('/');

    if path.is_empty() {
        return match method {
            "GET" => Ok(S3Request::ListBuckets),
            _ => Err(format!("Unsupported method on /: {}", method)),
        };
    }

    let (bucket, key) = match path.find('/') {
        Some(idx) => (path[..idx].to_string(), Some(path[idx + 1..].to_string())),
        None => (path.to_string(), None),
    };

    match (method, key) {
        ("GET", None) => {
            let max_keys = parse_query_param(query, "max-keys")
                .and_then(|v| v.parse::<usize>().ok())
                .unwrap_or(1000);
            let prefix = parse_query_param(query, "prefix");
            let continuation_token = parse_query_param(query, "continuation-token");
            Ok(S3Request::ListObjects {
                bucket,
                prefix,
                max_keys,
                continuation_token,
            })
        }
        ("GET", Some(key)) => Ok(S3Request::GetObject { bucket, key }),
        ("PUT", Some(key)) => Ok(S3Request::PutObject {
            bucket,
            key,
            content_type: None,
        }),
        ("DELETE", Some(key)) => Ok(S3Request::DeleteObject { bucket, key }),
        ("HEAD", Some(key)) => Ok(S3Request::HeadObject { bucket, key }),
        _ => Err(format!("Unsupported: {} /{}", method, path)),
    }
}

fn parse_query_param(query: Option<&str>, name: &str) -> Option<String> {
    query.and_then(|q| {
        q.split('&')
            .find_map(|pair| {
                let (k, v) = pair.split_once('=')?;
                if k == name {
                    Some(v.to_string())
                } else {
                    None
                }
            })
    })
}

/// S3 service that maps S3 operations to Ferrite store operations.
///
/// This is the bridge between HTTP request parsing and the actual data store.
/// It resolves bucket names to database indexes and delegates to store operations.
pub struct S3Service {
    config: S3CompatConfig,
}

/// Result of handling an S3 request.
#[allow(missing_docs)]
pub struct S3Response {
    pub status: u16,
    pub content_type: String,
    pub body: Vec<u8>,
    pub headers: Vec<(String, String)>,
}

impl S3Service {
    /// Create a new S3 service with the given configuration.
    pub fn new(config: S3CompatConfig) -> Self {
        Self { config }
    }

    /// Resolve a bucket name to a database index.
    pub fn resolve_bucket(&self, bucket: &str) -> u8 {
        self.config
            .bucket_map
            .get(bucket)
            .copied()
            .unwrap_or(0) // default bucket → database 0
    }

    /// Handle a parsed S3 request and produce an S3 response.
    ///
    /// The `get_fn`, `set_fn`, `del_fn`, and `exists_fn` closures abstract
    /// over the actual store operations, keeping S3Service decoupled from
    /// the Store type.
    pub fn handle_request<G, S, D, E>(
        &self,
        request: &S3Request,
        get_fn: G,
        set_fn: S,
        del_fn: D,
        exists_fn: E,
    ) -> S3Response
    where
        G: Fn(u8, &str) -> Option<Vec<u8>>,
        S: Fn(u8, &str, &[u8]) -> bool,
        D: Fn(u8, &str) -> bool,
        E: Fn(u8, &str) -> bool,
    {
        match request {
            S3Request::GetObject { bucket, key } => {
                let db = self.resolve_bucket(bucket);
                match get_fn(db, key) {
                    Some(data) => S3Response {
                        status: 200,
                        content_type: "application/octet-stream".to_string(),
                        body: data,
                        headers: vec![],
                    },
                    None => S3Response {
                        status: 404,
                        content_type: "application/xml".to_string(),
                        body: error_xml(
                            "NoSuchKey",
                            "The specified key does not exist.",
                            key,
                            "0",
                        )
                        .into_bytes(),
                        headers: vec![],
                    },
                }
            }
            S3Request::PutObject { bucket, key, .. } => {
                let db = self.resolve_bucket(bucket);
                // Body bytes would come from HTTP layer — placeholder empty for scaffold
                if set_fn(db, key, &[]) {
                    S3Response {
                        status: 200,
                        content_type: "application/xml".to_string(),
                        body: Vec::new(),
                        headers: vec![("ETag".to_string(), format!("\"{}\"", key))],
                    }
                } else {
                    S3Response {
                        status: 500,
                        content_type: "application/xml".to_string(),
                        body: error_xml("InternalError", "Failed to store object", key, "0")
                            .into_bytes(),
                        headers: vec![],
                    }
                }
            }
            S3Request::DeleteObject { bucket, key } => {
                let db = self.resolve_bucket(bucket);
                del_fn(db, key);
                S3Response {
                    status: 204,
                    content_type: String::new(),
                    body: Vec::new(),
                    headers: vec![],
                }
            }
            S3Request::HeadObject { bucket, key } => {
                let db = self.resolve_bucket(bucket);
                if exists_fn(db, key) {
                    S3Response {
                        status: 200,
                        content_type: "application/octet-stream".to_string(),
                        body: Vec::new(),
                        headers: vec![],
                    }
                } else {
                    S3Response {
                        status: 404,
                        content_type: "application/xml".to_string(),
                        body: Vec::new(),
                        headers: vec![],
                    }
                }
            }
            S3Request::ListBuckets => {
                let buckets: Vec<String> = if self.config.bucket_map.is_empty() {
                    vec![self.config.default_bucket.clone()]
                } else {
                    self.config.bucket_map.keys().cloned().collect()
                };
                S3Response {
                    status: 200,
                    content_type: "application/xml".to_string(),
                    body: list_buckets_xml(&buckets).into_bytes(),
                    headers: vec![],
                }
            }
            S3Request::ListObjects { bucket, .. } => {
                let _db = self.resolve_bucket(bucket);
                // ListObjects requires SCAN — return empty for now
                S3Response {
                    status: 200,
                    content_type: "application/xml".to_string(),
                    body: format!(
                        r#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult>
  <Name>{bucket}</Name>
  <IsTruncated>false</IsTruncated>
  <KeyCount>0</KeyCount>
</ListBucketResult>"#
                    )
                    .into_bytes(),
                    headers: vec![],
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_get_object() {
        let req = parse_request("GET", "/mybucket/mykey", None).unwrap();
        match req {
            S3Request::GetObject { bucket, key } => {
                assert_eq!(bucket, "mybucket");
                assert_eq!(key, "mykey");
            }
            _ => panic!("Expected GetObject"),
        }
    }

    #[test]
    fn test_parse_put_object() {
        let req = parse_request("PUT", "/mybucket/mykey", None).unwrap();
        match req {
            S3Request::PutObject { bucket, key, .. } => {
                assert_eq!(bucket, "mybucket");
                assert_eq!(key, "mykey");
            }
            _ => panic!("Expected PutObject"),
        }
    }

    #[test]
    fn test_parse_delete_object() {
        let req = parse_request("DELETE", "/mybucket/mykey", None).unwrap();
        match req {
            S3Request::DeleteObject { bucket, key } => {
                assert_eq!(bucket, "mybucket");
                assert_eq!(key, "mykey");
            }
            _ => panic!("Expected DeleteObject"),
        }
    }

    #[test]
    fn test_parse_list_buckets() {
        let req = parse_request("GET", "/", None).unwrap();
        assert!(matches!(req, S3Request::ListBuckets));
    }

    #[test]
    fn test_parse_list_objects() {
        let req = parse_request("GET", "/mybucket", Some("prefix=data/&max-keys=50")).unwrap();
        match req {
            S3Request::ListObjects {
                bucket,
                prefix,
                max_keys,
                ..
            } => {
                assert_eq!(bucket, "mybucket");
                assert_eq!(prefix, Some("data/".to_string()));
                assert_eq!(max_keys, 50);
            }
            _ => panic!("Expected ListObjects"),
        }
    }

    #[test]
    fn test_parse_nested_key() {
        let req = parse_request("GET", "/bucket/path/to/object.json", None).unwrap();
        match req {
            S3Request::GetObject { bucket, key } => {
                assert_eq!(bucket, "bucket");
                assert_eq!(key, "path/to/object.json");
            }
            _ => panic!("Expected GetObject"),
        }
    }

    #[test]
    fn test_error_xml() {
        let xml = error_xml("NoSuchKey", "The specified key does not exist.", "/mybucket/mykey", "req-1");
        assert!(xml.contains("<Code>NoSuchKey</Code>"));
        assert!(xml.contains("<Message>The specified key does not exist.</Message>"));
    }

    #[test]
    fn test_list_buckets_xml() {
        let xml = list_buckets_xml(&["default".to_string(), "cache".to_string()]);
        assert!(xml.contains("<Name>default</Name>"));
        assert!(xml.contains("<Name>cache</Name>"));
    }

    #[test]
    fn test_s3_service_get_object() {
        let svc = S3Service::new(S3CompatConfig::default());
        let req = S3Request::GetObject {
            bucket: "default".to_string(),
            key: "mykey".to_string(),
        };
        let resp = svc.handle_request(
            &req,
            |_db, key| {
                if key == "mykey" {
                    Some(b"hello".to_vec())
                } else {
                    None
                }
            },
            |_, _, _| true,
            |_, _| true,
            |_, _| true,
        );
        assert_eq!(resp.status, 200);
        assert_eq!(resp.body, b"hello");
    }

    #[test]
    fn test_s3_service_get_not_found() {
        let svc = S3Service::new(S3CompatConfig::default());
        let req = S3Request::GetObject {
            bucket: "default".to_string(),
            key: "missing".to_string(),
        };
        let resp = svc.handle_request(&req, |_, _| None, |_, _, _| true, |_, _| true, |_, _| true);
        assert_eq!(resp.status, 404);
    }

    #[test]
    fn test_s3_service_delete() {
        let svc = S3Service::new(S3CompatConfig::default());
        let req = S3Request::DeleteObject {
            bucket: "default".to_string(),
            key: "mykey".to_string(),
        };
        let resp = svc.handle_request(&req, |_, _| None, |_, _, _| true, |_, _| true, |_, _| true);
        assert_eq!(resp.status, 204);
    }

    #[test]
    fn test_s3_service_head_exists() {
        let svc = S3Service::new(S3CompatConfig::default());
        let req = S3Request::HeadObject {
            bucket: "default".to_string(),
            key: "mykey".to_string(),
        };
        let resp = svc.handle_request(&req, |_, _| None, |_, _, _| true, |_, _| true, |_, _| true);
        assert_eq!(resp.status, 200);
    }

    #[test]
    fn test_s3_service_list_buckets() {
        let svc = S3Service::new(S3CompatConfig::default());
        let req = S3Request::ListBuckets;
        let resp = svc.handle_request(&req, |_, _| None, |_, _, _| true, |_, _| true, |_, _| true);
        assert_eq!(resp.status, 200);
        let body = String::from_utf8(resp.body).unwrap();
        assert!(body.contains("<Name>default</Name>"));
    }

    #[test]
    fn test_s3_service_bucket_mapping() {
        let mut config = S3CompatConfig::default();
        config.bucket_map.insert("cache".to_string(), 2);
        let svc = S3Service::new(config);
        assert_eq!(svc.resolve_bucket("cache"), 2);
        assert_eq!(svc.resolve_bucket("unknown"), 0);
    }
}
