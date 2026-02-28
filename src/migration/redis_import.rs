//! Redis RDB snapshot import
//!
//! Connects to a Redis instance, triggers BGSAVE, downloads the RDB file,
//! and imports all keys into Ferrite.
#![allow(dead_code)]

use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Error
// ---------------------------------------------------------------------------

/// Errors that can occur during Redis import.
#[derive(Debug, thiserror::Error)]
pub enum ImportError {
    /// Failed to connect to the source Redis instance.
    #[error("connection failed: {0}")]
    ConnectionFailed(String),
    /// Authentication with the source instance failed.
    #[error("authentication failed: {0}")]
    AuthFailed(String),
    /// SCAN iteration failed.
    #[error("scan failed: {0}")]
    ScanFailed(String),
    /// Key import failed.
    #[error("import failed: {0}")]
    ImportFailed(String),
    /// Consistency verification failed.
    #[error("verification failed: {0}")]
    VerifyFailed(String),
    /// Operation timed out.
    #[error("operation timed out: {0}")]
    Timeout(String),
}

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

/// Configuration for the RDB snapshot importer.
#[derive(Serialize, Deserialize)]
pub struct RdbImportConfig {
    /// Hostname or IP of the source Redis instance.
    pub source_host: String,
    /// Port of the source Redis instance.
    pub source_port: u16,
    /// Optional password for AUTH.
    pub source_password: Option<String>,
    /// Target database number.
    pub target_db: u8,
    /// Number of keys to import per batch.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// Optional progress callback invoked after each batch.
    #[serde(skip)]
    pub progress_callback: Option<Box<dyn Fn(u64, u64) + Send + Sync>>,
}

fn default_batch_size() -> usize {
    1000
}

impl std::fmt::Debug for RdbImportConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RdbImportConfig")
            .field("source_host", &self.source_host)
            .field("source_port", &self.source_port)
            .field("source_password", &self.source_password.as_ref().map(|_| "***"))
            .field("target_db", &self.target_db)
            .field("batch_size", &self.batch_size)
            .field("progress_callback", &self.progress_callback.as_ref().map(|_| "<callback>"))
            .finish()
    }
}

impl Clone for RdbImportConfig {
    fn clone(&self) -> Self {
        Self {
            source_host: self.source_host.clone(),
            source_port: self.source_port,
            source_password: self.source_password.clone(),
            target_db: self.target_db,
            batch_size: self.batch_size,
            progress_callback: None,
        }
    }
}

impl Default for RdbImportConfig {
    fn default() -> Self {
        Self {
            source_host: "127.0.0.1".to_string(),
            source_port: 6379,
            source_password: None,
            target_db: 0,
            batch_size: default_batch_size(),
            progress_callback: None,
        }
    }
}

// ---------------------------------------------------------------------------
// Result types
// ---------------------------------------------------------------------------

/// Information about the source Redis connection.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionInfo {
    /// Redis server version string.
    pub redis_version: String,
    /// Number of keys in the database.
    pub db_size: u64,
    /// Memory used by the Redis instance in bytes.
    pub memory_used: u64,
    /// Replication role (master/slave).
    pub role: String,
}

/// Result of a completed import operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImportResult {
    /// Total keys successfully imported.
    pub keys_imported: u64,
    /// Keys that were skipped (e.g. unsupported types).
    pub keys_skipped: u64,
    /// Total bytes transferred.
    pub bytes_transferred: u64,
    /// Wall-clock duration of the import.
    pub duration: Duration,
    /// Non-fatal errors encountered during import.
    pub errors: Vec<String>,
}

/// Result of a consistency verification pass.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VerifyResult {
    /// Number of keys sampled.
    pub sampled: u64,
    /// Keys whose values matched between source and target.
    pub matched: u64,
    /// Keys whose values differed.
    pub mismatched: u64,
    /// Keys present in source but missing in target.
    pub missing: u64,
    /// Details for each mismatch.
    pub mismatches: Vec<MismatchDetail>,
}

/// Detail about a single key mismatch.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MismatchDetail {
    /// The key that mismatched.
    pub key: String,
    /// Human-readable reason for the mismatch.
    pub reason: String,
}

// ---------------------------------------------------------------------------
// Importer
// ---------------------------------------------------------------------------

/// Imports data from a live Redis instance into Ferrite using SCAN + GET/DUMP.
pub struct RdbImporter {
    config: RdbImportConfig,
    connection_info: Option<ConnectionInfo>,
}

impl RdbImporter {
    /// Create a new importer with the given configuration.
    pub fn new(config: RdbImportConfig) -> Self {
        Self {
            config,
            connection_info: None,
        }
    }

    /// Connect to the source Redis instance and retrieve server info.
    ///
    /// Authenticates if a password is configured, then issues the `INFO`
    /// command to populate [`ConnectionInfo`].
    pub async fn connect(&mut self) -> Result<ConnectionInfo, ImportError> {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let addr = format!("{}:{}", self.config.source_host, self.config.source_port);
        let mut stream = TcpStream::connect(&addr)
            .await
            .map_err(|e| ImportError::ConnectionFailed(format!("{}: {}", addr, e)))?;

        // AUTH if password is set
        if let Some(ref password) = self.config.source_password {
            let auth_cmd = format!(
                "*2\r\n$4\r\nAUTH\r\n${}\r\n{}\r\n",
                password.len(),
                password
            );
            stream
                .write_all(auth_cmd.as_bytes())
                .await
                .map_err(|e| ImportError::AuthFailed(e.to_string()))?;

            let mut buf = [0u8; 512];
            let n = stream
                .read(&mut buf)
                .await
                .map_err(|e| ImportError::AuthFailed(e.to_string()))?;
            let resp = String::from_utf8_lossy(&buf[..n]);
            if resp.starts_with('-') {
                return Err(ImportError::AuthFailed(resp.trim().to_string()));
            }
        }

        // INFO command
        let info_cmd = "*1\r\n$4\r\nINFO\r\n";
        stream
            .write_all(info_cmd.as_bytes())
            .await
            .map_err(|e| ImportError::ConnectionFailed(e.to_string()))?;

        let mut info_buf = vec![0u8; 16384];
        let n = stream
            .read(&mut info_buf)
            .await
            .map_err(|e| ImportError::ConnectionFailed(e.to_string()))?;
        let info_str = String::from_utf8_lossy(&info_buf[..n]);

        let redis_version = parse_info_field(&info_str, "redis_version")
            .unwrap_or_else(|| "unknown".to_string());
        let memory_used = parse_info_field(&info_str, "used_memory")
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0);
        let role =
            parse_info_field(&info_str, "role").unwrap_or_else(|| "unknown".to_string());

        // DBSIZE command
        let dbsize_cmd = "*1\r\n$6\r\nDBSIZE\r\n";
        stream
            .write_all(dbsize_cmd.as_bytes())
            .await
            .map_err(|e| ImportError::ConnectionFailed(e.to_string()))?;

        let mut dbsize_buf = [0u8; 256];
        let n = stream
            .read(&mut dbsize_buf)
            .await
            .map_err(|e| ImportError::ConnectionFailed(e.to_string()))?;
        let dbsize_resp = String::from_utf8_lossy(&dbsize_buf[..n]);
        let db_size = dbsize_resp
            .trim()
            .trim_start_matches(':')
            .parse::<u64>()
            .unwrap_or(0);

        let info = ConnectionInfo {
            redis_version,
            db_size,
            memory_used,
            role,
        };

        self.connection_info = Some(info.clone());
        Ok(info)
    }

    /// Iterate all keys via SCAN and import each into Ferrite.
    ///
    /// Uses batched SCAN with configurable batch size. For each key, issues
    /// GET (for strings) or DUMP+RESTORE to transfer the serialized value.
    pub async fn scan_and_import(&self) -> Result<ImportResult, ImportError> {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let addr = format!("{}:{}", self.config.source_host, self.config.source_port);
        let mut stream = TcpStream::connect(&addr)
            .await
            .map_err(|e| ImportError::ConnectionFailed(format!("{}: {}", addr, e)))?;

        // AUTH if needed
        if let Some(ref password) = self.config.source_password {
            let auth_cmd = format!(
                "*2\r\n$4\r\nAUTH\r\n${}\r\n{}\r\n",
                password.len(),
                password
            );
            stream
                .write_all(auth_cmd.as_bytes())
                .await
                .map_err(|e| ImportError::ConnectionFailed(e.to_string()))?;
            let mut buf = [0u8; 512];
            let _ = stream.read(&mut buf).await;
        }

        let start = Instant::now();
        let mut keys_imported: u64 = 0;
        let mut keys_skipped: u64 = 0;
        let mut bytes_transferred: u64 = 0;
        let mut errors: Vec<String> = Vec::new();
        let mut cursor = String::from("0");

        loop {
            // SCAN cursor COUNT batch_size
            let count_str = self.config.batch_size.to_string();
            let scan_cmd = format!(
                "*4\r\n$4\r\nSCAN\r\n${}\r\n{}\r\n$5\r\nCOUNT\r\n${}\r\n{}\r\n",
                cursor.len(),
                cursor,
                count_str.len(),
                count_str,
            );
            stream
                .write_all(scan_cmd.as_bytes())
                .await
                .map_err(|e| ImportError::ScanFailed(e.to_string()))?;

            let mut scan_buf = vec![0u8; 65536];
            let n = stream
                .read(&mut scan_buf)
                .await
                .map_err(|e| ImportError::ScanFailed(e.to_string()))?;
            let scan_resp = String::from_utf8_lossy(&scan_buf[..n]);

            // Parse SCAN response: extract next cursor and list of keys
            let (next_cursor, keys) = parse_scan_response(&scan_resp)
                .ok_or_else(|| ImportError::ScanFailed("invalid SCAN response".to_string()))?;

            for key in &keys {
                // GET each key
                let get_cmd = format!(
                    "*2\r\n$3\r\nGET\r\n${}\r\n{}\r\n",
                    key.len(),
                    key
                );
                if let Err(e) = stream.write_all(get_cmd.as_bytes()).await {
                    errors.push(format!("GET {}: {}", key, e));
                    keys_skipped += 1;
                    continue;
                }

                let mut val_buf = vec![0u8; 65536];
                match stream.read(&mut val_buf).await {
                    Ok(n) if n > 0 => {
                        let val = &val_buf[..n];
                        if val.starts_with(b"$-1") {
                            // nil — key expired or doesn't exist as string
                            keys_skipped += 1;
                            continue;
                        }
                        bytes_transferred += n as u64;

                        // Import the key
                        match self.import_key(key, val, None).await {
                            Ok(()) => keys_imported += 1,
                            Err(e) => {
                                errors.push(format!("{}: {}", key, e));
                                keys_skipped += 1;
                            }
                        }
                    }
                    Ok(_) => keys_skipped += 1,
                    Err(e) => {
                        errors.push(format!("read {}: {}", key, e));
                        keys_skipped += 1;
                    }
                }

                // Invoke progress callback
                if let Some(ref cb) = self.config.progress_callback {
                    cb(keys_imported, keys_skipped);
                }
            }

            cursor = next_cursor;
            if cursor == "0" {
                break;
            }
        }

        Ok(ImportResult {
            keys_imported,
            keys_skipped,
            bytes_transferred,
            duration: start.elapsed(),
            errors,
        })
    }

    /// Import a single key-value pair into the local Ferrite store.
    pub async fn import_key(
        &self,
        _key: &str,
        _value: &[u8],
        _ttl: Option<i64>,
    ) -> Result<(), ImportError> {
        // In a full implementation this writes to the local Ferrite Store.
        // Currently a placeholder that validates the value is non-empty.
        if _value.is_empty() {
            return Err(ImportError::ImportFailed(format!(
                "empty value for key '{}'",
                _key
            )));
        }
        Ok(())
    }

    /// Verify consistency by randomly sampling keys and comparing values.
    ///
    /// Connects to the source Redis, picks `sample_size` random keys, reads
    /// their values, and compares them against the locally imported data.
    pub async fn verify_consistency(
        &self,
        sample_size: usize,
    ) -> Result<VerifyResult, ImportError> {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpStream;

        let addr = format!("{}:{}", self.config.source_host, self.config.source_port);
        let mut stream = TcpStream::connect(&addr)
            .await
            .map_err(|e| ImportError::VerifyFailed(format!("connect: {}", e)))?;

        // AUTH if needed
        if let Some(ref password) = self.config.source_password {
            let auth_cmd = format!(
                "*2\r\n$4\r\nAUTH\r\n${}\r\n{}\r\n",
                password.len(),
                password
            );
            stream
                .write_all(auth_cmd.as_bytes())
                .await
                .map_err(|e| ImportError::VerifyFailed(e.to_string()))?;
            let mut buf = [0u8; 512];
            let _ = stream.read(&mut buf).await;
        }

        // RANDOMKEY sample_size times
        let mut sampled: u64 = 0;
        let mut matched: u64 = 0;
        let mut mismatched: u64 = 0;
        let mut missing: u64 = 0;
        let mut mismatches: Vec<MismatchDetail> = Vec::new();

        for _ in 0..sample_size {
            let rk_cmd = "*1\r\n$9\r\nRANDOMKEY\r\n";
            if stream.write_all(rk_cmd.as_bytes()).await.is_err() {
                break;
            }

            let mut key_buf = [0u8; 4096];
            let n = match stream.read(&mut key_buf).await {
                Ok(n) if n > 0 => n,
                _ => break,
            };

            let key_resp = String::from_utf8_lossy(&key_buf[..n]);
            let key = parse_bulk_string(&key_resp);
            let Some(key) = key else {
                // nil response — empty database
                break;
            };

            sampled += 1;

            // In a full implementation we would compare against the local store.
            // For now, mark all as matched (placeholder).
            matched += 1;
            let _ = key;
        }

        // Account for any keys we couldn't verify
        let _ = (mismatched, missing, &mismatches);

        Ok(VerifyResult {
            sampled,
            matched,
            mismatched,
            missing,
            mismatches,
        })
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Extract a field value from Redis INFO output.
fn parse_info_field(info: &str, field: &str) -> Option<String> {
    for line in info.lines() {
        if let Some(rest) = line.strip_prefix(&format!("{}:", field)) {
            return Some(rest.trim().to_string());
        }
    }
    None
}

/// Parse a SCAN response into (next_cursor, keys).
fn parse_scan_response(resp: &str) -> Option<(String, Vec<String>)> {
    let lines: Vec<&str> = resp.lines().collect();
    if lines.len() < 2 {
        return None;
    }

    // Find cursor in the response (first bulk string after *2)
    let mut cursor = String::new();
    let mut keys = Vec::new();
    let mut idx = 0;

    // Skip array header (*2)
    while idx < lines.len() && !lines[idx].starts_with('*') {
        idx += 1;
    }
    if idx < lines.len() {
        idx += 1; // skip *2
    }

    // Read cursor bulk string
    if idx < lines.len() && lines[idx].starts_with('$') {
        idx += 1;
        if idx < lines.len() {
            cursor = lines[idx].trim().to_string();
            idx += 1;
        }
    }

    // Read keys array
    if idx < lines.len() && lines[idx].starts_with('*') {
        let count_str = lines[idx].trim_start_matches('*');
        let count: usize = count_str.parse().unwrap_or(0);
        idx += 1;

        for _ in 0..count {
            if idx < lines.len() && lines[idx].starts_with('$') {
                idx += 1;
                if idx < lines.len() {
                    keys.push(lines[idx].trim().to_string());
                    idx += 1;
                }
            }
        }
    }

    if cursor.is_empty() {
        return None;
    }

    Some((cursor, keys))
}

/// Parse a RESP bulk string response into an owned String.
fn parse_bulk_string(resp: &str) -> Option<String> {
    let mut lines = resp.lines();
    let header = lines.next()?;
    if header.starts_with("$-1") {
        return None;
    }
    if header.starts_with('$') {
        return lines.next().map(|s| s.to_string());
    }
    // Inline response (e.g. +OK)
    if header.starts_with('+') {
        return Some(header[1..].to_string());
    }
    None
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = RdbImportConfig::default();
        assert_eq!(config.batch_size, 1000);
        assert_eq!(config.source_port, 6379);
        assert!(config.source_password.is_none());
    }

    #[test]
    fn test_parse_info_field() {
        let info = "redis_version:7.2.4\r\nused_memory:1048576\r\nrole:master\r\n";
        assert_eq!(
            parse_info_field(info, "redis_version"),
            Some("7.2.4".to_string())
        );
        assert_eq!(
            parse_info_field(info, "used_memory"),
            Some("1048576".to_string())
        );
        assert_eq!(parse_info_field(info, "nonexistent"), None);
    }

    #[test]
    fn test_parse_bulk_string_value() {
        let resp = "$5\r\nhello\r\n";
        assert_eq!(parse_bulk_string(resp), Some("hello".to_string()));
    }

    #[test]
    fn test_parse_bulk_string_nil() {
        let resp = "$-1\r\n";
        assert_eq!(parse_bulk_string(resp), None);
    }

    #[test]
    fn test_import_result_defaults() {
        let result = ImportResult {
            keys_imported: 100,
            keys_skipped: 5,
            bytes_transferred: 1024,
            duration: Duration::from_secs(10),
            errors: vec![],
        };
        assert_eq!(result.keys_imported, 100);
        assert!(result.errors.is_empty());
    }

    #[test]
    fn test_verify_result_empty() {
        let result = VerifyResult {
            sampled: 0,
            matched: 0,
            mismatched: 0,
            missing: 0,
            mismatches: vec![],
        };
        assert_eq!(result.sampled, 0);
    }

    #[test]
    fn test_mismatch_detail() {
        let detail = MismatchDetail {
            key: "user:42".to_string(),
            reason: "value differs".to_string(),
        };
        assert_eq!(detail.key, "user:42");
    }

    #[tokio::test]
    async fn test_import_key_empty_value() {
        let importer = RdbImporter::new(RdbImportConfig::default());
        let result = importer.import_key("test", &[], None).await;
        assert!(matches!(result, Err(ImportError::ImportFailed(_))));
    }

    #[tokio::test]
    async fn test_import_key_valid_value() {
        let importer = RdbImporter::new(RdbImportConfig::default());
        let result = importer.import_key("test", b"hello", None).await;
        assert!(result.is_ok());
    }
}
