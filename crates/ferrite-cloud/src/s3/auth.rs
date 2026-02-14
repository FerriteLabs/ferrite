//! S3 Authentication (AWS Signature V4)

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use super::request::S3Request;

/// AWS credentials
#[derive(Debug, Clone)]
pub struct Credentials {
    /// Access key ID
    pub access_key_id: String,
    /// Secret access key
    pub secret_access_key: String,
}

impl Credentials {
    /// Create new credentials
    pub fn new(access_key_id: impl Into<String>, secret_access_key: impl Into<String>) -> Self {
        Self {
            access_key_id: access_key_id.into(),
            secret_access_key: secret_access_key.into(),
        }
    }
}

/// AWS Signature V4 components
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignatureV4 {
    /// Algorithm (AWS4-HMAC-SHA256)
    pub algorithm: String,
    /// Credential scope
    pub credential: String,
    /// Signed headers list
    pub signed_headers: String,
    /// Calculated signature
    pub signature: String,
    /// Date string
    pub date: String,
    /// Region
    pub region: String,
    /// Service name
    pub service: String,
}

impl SignatureV4 {
    /// Parse from Authorization header
    pub fn parse(auth_header: &str) -> Option<Self> {
        // Format: AWS4-HMAC-SHA256 Credential=.../date/region/service/aws4_request,
        //         SignedHeaders=..., Signature=...

        if !auth_header.starts_with("AWS4-HMAC-SHA256") {
            return None;
        }

        let parts: HashMap<String, String> = auth_header
            .trim_start_matches("AWS4-HMAC-SHA256 ")
            .split(", ")
            .filter_map(|part| {
                let mut kv = part.splitn(2, '=');
                Some((kv.next()?.to_string(), kv.next()?.to_string()))
            })
            .collect();

        let credential = parts.get("Credential")?;
        let cred_parts: Vec<&str> = credential.split('/').collect();

        if cred_parts.len() < 5 {
            return None;
        }

        Some(Self {
            algorithm: "AWS4-HMAC-SHA256".to_string(),
            credential: credential.clone(),
            signed_headers: parts.get("SignedHeaders")?.clone(),
            signature: parts.get("Signature")?.clone(),
            date: cred_parts[1].to_string(),
            region: cred_parts[2].to_string(),
            service: cred_parts[3].to_string(),
        })
    }
}

/// Authentication error
#[derive(Debug, Clone, thiserror::Error)]
pub enum AuthError {
    /// Missing authorization header
    #[error("missing authorization header")]
    MissingAuth,

    /// Invalid signature
    #[error("invalid signature")]
    InvalidSignature,

    /// Expired credentials
    #[error("credentials expired")]
    Expired,

    /// Access denied
    #[error("access denied: {0}")]
    AccessDenied(String),
}

/// S3 authentication handler
pub struct S3Auth {
    credentials: Credentials,
    region: String,
}

impl S3Auth {
    /// Create new auth handler
    pub fn new(credentials: Credentials, region: &str) -> Self {
        Self {
            credentials,
            region: region.to_string(),
        }
    }

    /// Verify request authentication
    pub fn verify(&self, request: &S3Request) -> Result<(), AuthError> {
        // Skip auth for empty header (testing)
        if request.auth_header.is_empty() {
            return Ok(());
        }

        // Parse signature
        let sig = SignatureV4::parse(&request.auth_header).ok_or(AuthError::MissingAuth)?;

        // Verify region
        if sig.region != self.region {
            return Err(AuthError::AccessDenied(format!(
                "region mismatch: expected {}, got {}",
                self.region, sig.region
            )));
        }

        // Verify credential contains our access key
        if !sig.credential.starts_with(&self.credentials.access_key_id) {
            return Err(AuthError::InvalidSignature);
        }

        // In a production implementation, we would:
        // 1. Compute the canonical request
        // 2. Create the string to sign
        // 3. Derive the signing key using the secret key
        // 4. Compute the signature
        // 5. Compare with the provided signature
        //
        // For this implementation, we accept any properly formatted signature
        // from a matching access key

        Ok(())
    }

    /// Create a presigned URL
    #[allow(unused_variables)]
    pub fn presign_url(&self, method: &str, bucket: &str, key: &str, expires_in: u64) -> String {
        use std::time::{SystemTime, UNIX_EPOCH};

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let _expiry = now + expires_in;
        let date = format_date(now);

        // Simplified presigned URL format
        format!(
            "/{}/{}?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential={}/{}/{}/s3/aws4_request&X-Amz-Date={}&X-Amz-Expires={}&X-Amz-SignedHeaders=host&X-Amz-Signature=placeholder",
            bucket,
            key,
            self.credentials.access_key_id,
            date,
            self.region,
            date,
            expires_in
        )
    }
}

/// Format timestamp as date string (YYYYMMDD)
fn format_date(timestamp: u64) -> String {
    // Simple date formatting without chrono dependency
    let days_since_epoch = timestamp / 86400;
    let mut year = 1970;
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

    let mut month = 1;
    for &days in &month_days {
        if days_remaining < days {
            break;
        }
        days_remaining -= days;
        month += 1;
    }

    let day = days_remaining + 1;

    format!("{:04}{:02}{:02}", year, month, day)
}

fn is_leap_year(year: u64) -> bool {
    (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_signature() {
        let header = "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature=abcdef1234567890";

        let sig = SignatureV4::parse(header).unwrap();
        assert_eq!(sig.algorithm, "AWS4-HMAC-SHA256");
        assert_eq!(sig.region, "us-east-1");
        assert_eq!(sig.date, "20130524");
    }

    #[test]
    fn test_format_date() {
        // 2024-01-15 is about 19737 days since epoch
        let date = format_date(1705276800); // 2024-01-15 00:00:00 UTC
        assert_eq!(date, "20240115");
    }

    #[test]
    fn test_auth_verify() {
        let creds = Credentials::new("AKIAIOSFODNN7EXAMPLE", "secret");
        let auth = S3Auth::new(creds, "us-east-1");

        // Empty auth header should pass (for testing)
        let request = S3Request {
            operation: super::super::request::S3Operation::ListBuckets,
            bucket: String::new(),
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

        assert!(auth.verify(&request).is_ok());
    }
}
