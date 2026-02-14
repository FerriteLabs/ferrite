//! S3 Response types

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use super::bucket::BucketInfo;
use super::object::ObjectMetadata;

/// S3 error codes
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum S3ErrorCode {
    /// Access denied
    AccessDenied,
    /// Bucket already exists
    BucketAlreadyExists,
    /// Bucket not empty
    BucketNotEmpty,
    /// Entity too large
    EntityTooLarge,
    /// Internal error
    InternalError,
    /// Invalid argument
    InvalidArgument,
    /// Invalid bucket name
    InvalidBucketName,
    /// Invalid part
    InvalidPart,
    /// Invalid part order
    InvalidPartOrder,
    /// Malformed XML
    MalformedXML,
    /// No such bucket
    NoSuchBucket,
    /// No such key
    NoSuchKey,
    /// No such upload
    NoSuchUpload,
    /// Too many buckets
    TooManyBuckets,
}

impl S3ErrorCode {
    /// Get HTTP status code
    pub fn status_code(&self) -> u16 {
        match self {
            S3ErrorCode::AccessDenied => 403,
            S3ErrorCode::BucketAlreadyExists => 409,
            S3ErrorCode::BucketNotEmpty => 409,
            S3ErrorCode::EntityTooLarge => 400,
            S3ErrorCode::InternalError => 500,
            S3ErrorCode::InvalidArgument => 400,
            S3ErrorCode::InvalidBucketName => 400,
            S3ErrorCode::InvalidPart => 400,
            S3ErrorCode::InvalidPartOrder => 400,
            S3ErrorCode::MalformedXML => 400,
            S3ErrorCode::NoSuchBucket => 404,
            S3ErrorCode::NoSuchKey => 404,
            S3ErrorCode::NoSuchUpload => 404,
            S3ErrorCode::TooManyBuckets => 400,
        }
    }

    /// Get error code string
    pub fn as_str(&self) -> &'static str {
        match self {
            S3ErrorCode::AccessDenied => "AccessDenied",
            S3ErrorCode::BucketAlreadyExists => "BucketAlreadyExists",
            S3ErrorCode::BucketNotEmpty => "BucketNotEmpty",
            S3ErrorCode::EntityTooLarge => "EntityTooLarge",
            S3ErrorCode::InternalError => "InternalError",
            S3ErrorCode::InvalidArgument => "InvalidArgument",
            S3ErrorCode::InvalidBucketName => "InvalidBucketName",
            S3ErrorCode::InvalidPart => "InvalidPart",
            S3ErrorCode::InvalidPartOrder => "InvalidPartOrder",
            S3ErrorCode::MalformedXML => "MalformedXML",
            S3ErrorCode::NoSuchBucket => "NoSuchBucket",
            S3ErrorCode::NoSuchKey => "NoSuchKey",
            S3ErrorCode::NoSuchUpload => "NoSuchUpload",
            S3ErrorCode::TooManyBuckets => "TooManyBuckets",
        }
    }
}

/// S3 error
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3Error {
    /// Error code
    pub code: S3ErrorCode,
    /// Error message
    pub message: String,
    /// Resource that caused the error
    pub resource: Option<String>,
    /// Request ID
    pub request_id: String,
}

impl S3Error {
    /// Create a new error
    pub fn new(code: S3ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            resource: None,
            request_id: generate_request_id(),
        }
    }

    /// Set resource
    pub fn with_resource(mut self, resource: impl Into<String>) -> Self {
        self.resource = Some(resource.into());
        self
    }

    /// Convert to XML
    pub fn to_xml(&self) -> String {
        let mut xml = String::from("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
        xml.push_str("<Error>\n");
        xml.push_str(&format!("  <Code>{}</Code>\n", self.code.as_str()));
        xml.push_str(&format!(
            "  <Message>{}</Message>\n",
            escape_xml(&self.message)
        ));
        if let Some(ref resource) = self.resource {
            xml.push_str(&format!(
                "  <Resource>{}</Resource>\n",
                escape_xml(resource)
            ));
        }
        xml.push_str(&format!("  <RequestId>{}</RequestId>\n", self.request_id));
        xml.push_str("</Error>");
        xml
    }
}

/// S3 response
#[derive(Debug, Clone)]
pub struct S3Response {
    /// HTTP status code
    pub status: u16,
    /// Response headers
    pub headers: HashMap<String, String>,
    /// Response body
    pub body: Vec<u8>,
    /// Error (if any)
    pub error: Option<S3Error>,
}

impl S3Response {
    /// Create a success response
    pub fn ok() -> Self {
        Self {
            status: 200,
            headers: HashMap::new(),
            body: Vec::new(),
            error: None,
        }
    }

    /// Create an error response
    pub fn error(code: S3ErrorCode, message: &str) -> Self {
        let error = S3Error::new(code, message);
        Self {
            status: code.status_code(),
            headers: {
                let mut h = HashMap::new();
                h.insert("Content-Type".to_string(), "application/xml".to_string());
                h
            },
            body: error.to_xml().into_bytes(),
            error: Some(error),
        }
    }

    /// Check if response is successful
    pub fn is_success(&self) -> bool {
        self.status >= 200 && self.status < 300
    }

    /// Create list buckets response
    pub fn list_buckets(buckets: Vec<BucketInfo>) -> Self {
        let mut xml = String::from("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
        xml.push_str(
            "<ListAllMyBucketsResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n",
        );
        xml.push_str("  <Owner>\n");
        xml.push_str("    <ID>owner</ID>\n");
        xml.push_str("    <DisplayName>owner</DisplayName>\n");
        xml.push_str("  </Owner>\n");
        xml.push_str("  <Buckets>\n");

        for bucket in buckets {
            xml.push_str("    <Bucket>\n");
            xml.push_str(&format!(
                "      <Name>{}</Name>\n",
                escape_xml(&bucket.name)
            ));
            xml.push_str(&format!(
                "      <CreationDate>{}</CreationDate>\n",
                format_timestamp(bucket.creation_date)
            ));
            xml.push_str("    </Bucket>\n");
        }

        xml.push_str("  </Buckets>\n");
        xml.push_str("</ListAllMyBucketsResult>");

        Self {
            status: 200,
            headers: {
                let mut h = HashMap::new();
                h.insert("Content-Type".to_string(), "application/xml".to_string());
                h
            },
            body: xml.into_bytes(),
            error: None,
        }
    }

    /// Create get object response
    pub fn object(data: Vec<u8>, metadata: ObjectMetadata) -> Self {
        let mut headers = HashMap::new();
        headers.insert("Content-Type".to_string(), metadata.content_type);
        headers.insert("Content-Length".to_string(), data.len().to_string());
        headers.insert("ETag".to_string(), metadata.etag);
        headers.insert(
            "Last-Modified".to_string(),
            format_timestamp(metadata.last_modified),
        );

        for (key, value) in metadata.custom {
            headers.insert(format!("x-amz-meta-{}", key), value);
        }

        Self {
            status: 200,
            headers,
            body: data,
            error: None,
        }
    }

    /// Create put object response
    pub fn put_object(etag: &str) -> Self {
        let mut headers = HashMap::new();
        headers.insert("ETag".to_string(), etag.to_string());

        Self {
            status: 200,
            headers,
            body: Vec::new(),
            error: None,
        }
    }

    /// Create head object response
    pub fn head_object(metadata: &ObjectMetadata) -> Self {
        let mut headers = HashMap::new();
        headers.insert("Content-Type".to_string(), metadata.content_type.clone());
        headers.insert("Content-Length".to_string(), metadata.size.to_string());
        headers.insert("ETag".to_string(), metadata.etag.clone());
        headers.insert(
            "Last-Modified".to_string(),
            format_timestamp(metadata.last_modified),
        );

        Self {
            status: 200,
            headers,
            body: Vec::new(),
            error: None,
        }
    }

    /// Create list objects response
    pub fn list_objects(objects: Vec<ObjectMetadata>) -> Self {
        let mut xml = String::from("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
        xml.push_str("<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n");
        xml.push_str("  <IsTruncated>false</IsTruncated>\n");

        for obj in objects {
            xml.push_str("  <Contents>\n");
            xml.push_str(&format!("    <Key>{}</Key>\n", escape_xml(&obj.key)));
            xml.push_str(&format!(
                "    <LastModified>{}</LastModified>\n",
                format_timestamp(obj.last_modified)
            ));
            xml.push_str(&format!("    <ETag>{}</ETag>\n", escape_xml(&obj.etag)));
            xml.push_str(&format!("    <Size>{}</Size>\n", obj.size));
            xml.push_str("    <StorageClass>STANDARD</StorageClass>\n");
            xml.push_str("  </Contents>\n");
        }

        xml.push_str("</ListBucketResult>");

        Self {
            status: 200,
            headers: {
                let mut h = HashMap::new();
                h.insert("Content-Type".to_string(), "application/xml".to_string());
                h
            },
            body: xml.into_bytes(),
            error: None,
        }
    }

    /// Create initiate multipart response
    pub fn initiate_multipart(upload_id: &str) -> Self {
        let xml = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
             <InitiateMultipartUploadResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n\
             <UploadId>{}</UploadId>\n\
             </InitiateMultipartUploadResult>",
            upload_id
        );

        Self {
            status: 200,
            headers: {
                let mut h = HashMap::new();
                h.insert("Content-Type".to_string(), "application/xml".to_string());
                h
            },
            body: xml.into_bytes(),
            error: None,
        }
    }

    /// Create upload part response
    pub fn upload_part(etag: &str) -> Self {
        let mut headers = HashMap::new();
        headers.insert("ETag".to_string(), etag.to_string());

        Self {
            status: 200,
            headers,
            body: Vec::new(),
            error: None,
        }
    }

    /// Create complete multipart response
    pub fn complete_multipart(etag: &str) -> Self {
        let xml = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
             <CompleteMultipartUploadResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n\
             <ETag>{}</ETag>\n\
             </CompleteMultipartUploadResult>",
            etag
        );

        Self {
            status: 200,
            headers: {
                let mut h = HashMap::new();
                h.insert("Content-Type".to_string(), "application/xml".to_string());
                h
            },
            body: xml.into_bytes(),
            error: None,
        }
    }
}

/// Generate a unique request ID
fn generate_request_id() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};

    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();

    format!("{:x}", timestamp)
}

/// Format timestamp as ISO 8601
fn format_timestamp(timestamp: u64) -> String {
    // Simple ISO 8601 formatting without chrono dependency
    let days_since_epoch = timestamp / 86400;
    let secs_in_day = timestamp % 86400;

    let hours = secs_in_day / 3600;
    let mins = (secs_in_day % 3600) / 60;
    let secs = secs_in_day % 60;

    let mut year = 1970u64;
    let mut days_remaining = days_since_epoch;

    loop {
        let days_in_year = if is_leap_year(year) { 366 } else { 365 };
        if days_remaining < days_in_year {
            break;
        }
        days_remaining -= days_in_year;
        year += 1;
    }

    let month_days = if is_leap_year(year) {
        [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    } else {
        [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    };

    let mut month = 1u64;
    for &days in &month_days {
        if days_remaining < days {
            break;
        }
        days_remaining -= days;
        month += 1;
    }

    let day = days_remaining + 1;

    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        year, month, day, hours, mins, secs
    )
}

fn is_leap_year(year: u64) -> bool {
    (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0)
}

/// Escape special XML characters
fn escape_xml(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_response() {
        let resp = S3Response::error(S3ErrorCode::NoSuchBucket, "Bucket not found");
        assert_eq!(resp.status, 404);
        assert!(resp.error.is_some());

        let body = String::from_utf8(resp.body).unwrap();
        assert!(body.contains("NoSuchBucket"));
    }

    #[test]
    fn test_success_response() {
        let resp = S3Response::ok();
        assert!(resp.is_success());
        assert_eq!(resp.status, 200);
    }

    #[test]
    fn test_format_timestamp() {
        let ts = format_timestamp(0);
        assert_eq!(ts, "1970-01-01T00:00:00Z");
    }

    #[test]
    fn test_escape_xml() {
        assert_eq!(escape_xml("<test>"), "&lt;test&gt;");
        assert_eq!(escape_xml("a & b"), "a &amp; b");
    }
}
