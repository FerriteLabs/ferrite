//! REST API endpoints for Ferrite Studio
//!
//! Provides HTTP endpoints for:
//! - Key browser operations (scan, get, set, delete)
//! - Server metrics and statistics
//! - Cluster information
//! - Slow query log
//! - Configuration management

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use super::StudioError;

/// API response wrapper
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiResponse<T> {
    /// Whether the request was successful
    pub success: bool,
    /// Response data (if successful)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<T>,
    /// Error message (if failed)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl<T> ApiResponse<T> {
    /// Create a successful response
    pub fn ok(data: T) -> Self {
        Self {
            success: true,
            data: Some(data),
            error: None,
        }
    }

    /// Create an error response
    pub fn err(message: impl Into<String>) -> Self {
        Self {
            success: false,
            data: None,
            error: Some(message.into()),
        }
    }
}

/// Key info for browser
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeyInfo {
    /// Key name
    pub key: String,
    /// Key type (string, list, set, hash, zset, stream)
    pub key_type: String,
    /// TTL in seconds (-1 if no expiry, -2 if not found)
    pub ttl: i64,
    /// Size in bytes (approximate)
    pub size: Option<usize>,
    /// Number of elements (for collections)
    pub length: Option<usize>,
}

/// Key value for browser
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeyValue {
    /// Key name
    pub key: String,
    /// Key type
    pub key_type: String,
    /// Value (string representation)
    pub value: serde_json::Value,
    /// TTL in seconds
    pub ttl: i64,
}

/// Scan result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScanResult {
    /// Next cursor (0 if done)
    pub cursor: u64,
    /// Keys found
    pub keys: Vec<KeyInfo>,
    /// Total count (approximate)
    pub total: Option<usize>,
}

/// Server info
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerInfo {
    /// Server version
    pub version: String,
    /// Uptime in seconds
    pub uptime_secs: u64,
    /// Number of connected clients
    pub connected_clients: usize,
    /// Total commands processed
    pub total_commands: u64,
    /// Operations per second
    pub ops_per_sec: f64,
    /// Total keys
    pub total_keys: usize,
    /// Memory usage in bytes
    pub used_memory: usize,
    /// Peak memory usage
    pub peak_memory: usize,
    /// Memory fragmentation ratio
    pub mem_fragmentation_ratio: f64,
    /// CPU usage (user + system)
    pub cpu_usage: f64,
    /// Server mode (standalone, cluster, sentinel)
    pub mode: String,
    /// Role (master, slave)
    pub role: String,
}

/// Memory info
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryInfo {
    /// Used memory in bytes
    pub used_memory: usize,
    /// Used memory human readable
    pub used_memory_human: String,
    /// Peak memory
    pub peak_memory: usize,
    /// Peak memory human readable
    pub peak_memory_human: String,
    /// Total system memory
    pub total_system_memory: usize,
    /// Memory fragmentation ratio
    pub fragmentation_ratio: f64,
    /// Memory by tier (mutable, readonly, disk)
    pub memory_by_tier: HashMap<String, usize>,
    /// Memory by database
    pub memory_by_db: HashMap<String, usize>,
}

/// Slow query entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlowQueryEntry {
    /// Query ID
    pub id: u64,
    /// Timestamp
    pub timestamp: u64,
    /// Duration in microseconds
    pub duration_us: u64,
    /// Command name
    pub command: String,
    /// Full command string
    pub full_command: String,
    /// Client address
    pub client_addr: Option<String>,
}

/// Cluster node info
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterNodeInfo {
    /// Node ID
    pub id: String,
    /// Node address
    pub addr: String,
    /// Role (master, replica)
    pub role: String,
    /// Master ID (if replica)
    pub master_id: Option<String>,
    /// Slot ranges
    pub slots: Vec<(u16, u16)>,
    /// Connection status
    pub status: String,
    /// Ping latency in ms
    pub ping_ms: Option<u64>,
}

/// Studio API handler
pub struct StudioApi {
    // In a real implementation, this would hold references to:
    // - The storage engine
    // - The server state
    // - The metrics collector
    // - The cluster manager
    _marker: std::marker::PhantomData<()>,
}

impl StudioApi {
    /// Create a new API handler
    pub fn new() -> Self {
        Self {
            _marker: std::marker::PhantomData,
        }
    }

    // Health Check

    /// Health check - returns server status
    pub async fn health_check(&self) -> Result<HealthStatus, StudioError> {
        let info = self.get_server_info().await?;
        Ok(HealthStatus {
            status: "ok".to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            uptime_secs: info.uptime_secs,
        })
    }

    // Metrics

    /// Get server metrics summary
    pub async fn get_metrics(&self) -> Result<MetricsSummary, StudioError> {
        let info = self.get_server_info().await?;
        let memory = self.get_memory_info().await?;
        Ok(MetricsSummary {
            ops_per_sec: info.ops_per_sec,
            total_commands: info.total_commands,
            connected_clients: info.connected_clients,
            total_keys: info.total_keys,
            used_memory: memory.used_memory,
            used_memory_human: memory.used_memory_human,
            fragmentation_ratio: memory.fragmentation_ratio,
            uptime_secs: info.uptime_secs,
        })
    }

    // Key Browser Operations

    /// Scan keys with pattern
    pub async fn scan_keys(
        &self,
        pattern: &str,
        cursor: u64,
        count: usize,
        _key_type: Option<&str>,
    ) -> Result<ScanResult, StudioError> {
        // Placeholder implementation
        let _ = (pattern, cursor, count);

        Ok(ScanResult {
            cursor: 0,
            keys: vec![
                KeyInfo {
                    key: "example:key:1".to_string(),
                    key_type: "string".to_string(),
                    ttl: -1,
                    size: Some(128),
                    length: None,
                },
                KeyInfo {
                    key: "example:list:1".to_string(),
                    key_type: "list".to_string(),
                    ttl: 3600,
                    size: Some(512),
                    length: Some(10),
                },
            ],
            total: Some(2),
        })
    }

    /// Get key value
    pub async fn get_key(&self, key: &str) -> Result<Option<KeyValue>, StudioError> {
        // Placeholder implementation
        Ok(Some(KeyValue {
            key: key.to_string(),
            key_type: "string".to_string(),
            value: serde_json::Value::String("example value".to_string()),
            ttl: -1,
        }))
    }

    /// Set key value
    pub async fn set_key(
        &self,
        key: &str,
        value: &str,
        ttl: Option<u64>,
    ) -> Result<(), StudioError> {
        let _ = (key, value, ttl);
        Ok(())
    }

    /// Delete key
    pub async fn delete_key(&self, key: &str) -> Result<bool, StudioError> {
        let _ = key;
        Ok(true)
    }

    /// Delete multiple keys
    pub async fn delete_keys(&self, keys: &[String]) -> Result<usize, StudioError> {
        Ok(keys.len())
    }

    // Server Info Operations

    /// Get server info
    pub async fn get_server_info(&self) -> Result<ServerInfo, StudioError> {
        Ok(ServerInfo {
            version: env!("CARGO_PKG_VERSION").to_string(),
            uptime_secs: 3600,
            connected_clients: 10,
            total_commands: 100000,
            ops_per_sec: 5000.0,
            total_keys: 1000,
            used_memory: 100 * 1024 * 1024,
            peak_memory: 150 * 1024 * 1024,
            mem_fragmentation_ratio: 1.1,
            cpu_usage: 15.5,
            mode: "standalone".to_string(),
            role: "master".to_string(),
        })
    }

    /// Get memory info
    pub async fn get_memory_info(&self) -> Result<MemoryInfo, StudioError> {
        let mut memory_by_tier = HashMap::new();
        memory_by_tier.insert("mutable".to_string(), 50 * 1024 * 1024);
        memory_by_tier.insert("readonly".to_string(), 30 * 1024 * 1024);
        memory_by_tier.insert("disk".to_string(), 20 * 1024 * 1024);

        let mut memory_by_db = HashMap::new();
        memory_by_db.insert("db0".to_string(), 80 * 1024 * 1024);
        memory_by_db.insert("db1".to_string(), 20 * 1024 * 1024);

        Ok(MemoryInfo {
            used_memory: 100 * 1024 * 1024,
            used_memory_human: "100.00MB".to_string(),
            peak_memory: 150 * 1024 * 1024,
            peak_memory_human: "150.00MB".to_string(),
            total_system_memory: 16 * 1024 * 1024 * 1024,
            fragmentation_ratio: 1.1,
            memory_by_tier,
            memory_by_db,
        })
    }

    // Slow Query Operations

    /// Get slow query log
    pub async fn get_slow_log(&self, count: usize) -> Result<Vec<SlowQueryEntry>, StudioError> {
        let _ = count;
        Ok(vec![SlowQueryEntry {
            id: 1,
            timestamp: 1700000000,
            duration_us: 50000,
            command: "KEYS".to_string(),
            full_command: "KEYS *".to_string(),
            client_addr: Some("127.0.0.1:12345".to_string()),
        }])
    }

    /// Clear slow query log
    pub async fn clear_slow_log(&self) -> Result<(), StudioError> {
        Ok(())
    }

    // Cluster Operations

    /// Get cluster info
    pub async fn get_cluster_info(&self) -> Result<Vec<ClusterNodeInfo>, StudioError> {
        Ok(vec![ClusterNodeInfo {
            id: "node1".to_string(),
            addr: "127.0.0.1:6379".to_string(),
            role: "master".to_string(),
            master_id: None,
            slots: vec![(0, 16383)],
            status: "connected".to_string(),
            ping_ms: Some(1),
        }])
    }

    // Config Operations

    /// Get config value
    pub async fn get_config(&self, param: &str) -> Result<Option<String>, StudioError> {
        let _ = param;
        Ok(Some("default".to_string()))
    }

    /// Set config value
    pub async fn set_config(&self, param: &str, value: &str) -> Result<(), StudioError> {
        let _ = (param, value);
        Ok(())
    }

    /// Get all config
    pub async fn get_all_config(&self) -> Result<HashMap<String, String>, StudioError> {
        let mut config = HashMap::new();
        config.insert("maxclients".to_string(), "10000".to_string());
        config.insert("timeout".to_string(), "0".to_string());
        config.insert("tcp-keepalive".to_string(), "300".to_string());
        Ok(config)
    }

    // Command execution

    /// Execute a raw command
    pub async fn execute_command(&self, command: &str) -> Result<serde_json::Value, StudioError> {
        let _ = command;
        // In production, this would parse and execute the command
        Ok(serde_json::Value::String("OK".to_string()))
    }

    // Key Browser Extended Operations

    /// Rename a key
    pub async fn rename_key(&self, key: &str, new_key: &str) -> Result<(), StudioError> {
        let _ = (key, new_key);
        Ok(())
    }

    /// Set TTL on a key (-1 to remove expiry)
    pub async fn set_ttl(&self, key: &str, ttl: i64) -> Result<(), StudioError> {
        let _ = (key, ttl);
        Ok(())
    }

    /// Get detailed key info including encoding and value
    pub async fn get_key_detail(&self, key: &str) -> Result<Option<KeyDetail>, StudioError> {
        Ok(Some(KeyDetail {
            key: key.to_string(),
            key_type: "string".to_string(),
            ttl: -1,
            size: Some(128),
            length: None,
            encoding: "embstr".to_string(),
            value: serde_json::Value::String("example value".to_string()),
        }))
    }
}

impl Default for StudioApi {
    fn default() -> Self {
        Self::new()
    }
}

/// Health check status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthStatus {
    /// Server status
    pub status: String,
    /// Server version
    pub version: String,
    /// Server uptime in seconds
    pub uptime_secs: u64,
}

/// Metrics summary
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsSummary {
    /// Operations per second
    pub ops_per_sec: f64,
    /// Total commands processed
    pub total_commands: u64,
    /// Connected clients
    pub connected_clients: usize,
    /// Total keys
    pub total_keys: usize,
    /// Used memory bytes
    pub used_memory: usize,
    /// Used memory human-readable
    pub used_memory_human: String,
    /// Fragmentation ratio
    pub fragmentation_ratio: f64,
    /// Server uptime in seconds
    pub uptime_secs: u64,
}

/// Request body for key operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SetKeyRequest {
    /// Key name
    pub key: String,
    /// Value to set
    pub value: String,
    /// TTL in seconds (optional)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ttl: Option<u64>,
}

/// Request body for command execution
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecuteCommandRequest {
    /// Command to execute
    pub command: String,
}

/// Request body for bulk delete
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteKeysRequest {
    /// Keys to delete
    pub keys: Vec<String>,
}

/// Request body for renaming a key
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RenameKeyRequest {
    /// Current key name
    pub key: String,
    /// New key name
    pub new_key: String,
}

/// Request body for setting TTL on a key
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SetTtlRequest {
    /// Key name
    pub key: String,
    /// TTL in seconds (-1 to remove expiry)
    pub ttl: i64,
}

/// Extended key info with encoding details for the key browser
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeyDetail {
    /// Key name
    pub key: String,
    /// Key type (string, list, set, hash, zset, stream)
    pub key_type: String,
    /// TTL in seconds (-1 if no expiry)
    pub ttl: i64,
    /// Approximate size in bytes
    pub size: Option<usize>,
    /// Number of elements (for collections)
    pub length: Option<usize>,
    /// Internal encoding (e.g. "embstr", "ziplist", "hashtable")
    pub encoding: String,
    /// The value (type-dependent)
    pub value: serde_json::Value,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_api_response_ok() {
        let response: ApiResponse<String> = ApiResponse::ok("test".to_string());
        assert!(response.success);
        assert_eq!(response.data, Some("test".to_string()));
        assert!(response.error.is_none());
    }

    #[test]
    fn test_api_response_err() {
        let response: ApiResponse<String> = ApiResponse::err("error message");
        assert!(!response.success);
        assert!(response.data.is_none());
        assert_eq!(response.error, Some("error message".to_string()));
    }

    #[tokio::test]
    async fn test_api_scan_keys() {
        let api = StudioApi::new();
        let result = api.scan_keys("*", 0, 10, None).await.unwrap();
        assert_eq!(result.cursor, 0);
        assert!(!result.keys.is_empty());
    }

    #[tokio::test]
    async fn test_api_get_server_info() {
        let api = StudioApi::new();
        let info = api.get_server_info().await.unwrap();
        assert!(!info.version.is_empty());
        assert!(info.ops_per_sec > 0.0);
    }

    #[tokio::test]
    async fn test_api_health_check() {
        let api = StudioApi::new();
        let status = api.health_check().await.unwrap();
        assert_eq!(status.status, "ok");
        assert!(!status.version.is_empty());
        assert!(status.uptime_secs > 0);
    }

    #[tokio::test]
    async fn test_api_get_metrics() {
        let api = StudioApi::new();
        let metrics = api.get_metrics().await.unwrap();
        assert!(metrics.ops_per_sec > 0.0);
        assert!(metrics.used_memory > 0);
        assert!(!metrics.used_memory_human.is_empty());
        assert!(metrics.uptime_secs > 0);
    }

    #[tokio::test]
    async fn test_api_get_memory_info() {
        let api = StudioApi::new();
        let info = api.get_memory_info().await.unwrap();
        assert!(info.used_memory > 0);
        assert!(!info.memory_by_tier.is_empty());
    }

    #[tokio::test]
    async fn test_api_key_operations() {
        let api = StudioApi::new();

        // Set key
        api.set_key("test_key", "test_value", None).await.unwrap();

        // Get key
        let value = api.get_key("test_key").await.unwrap();
        assert!(value.is_some());

        // Delete key
        let deleted = api.delete_key("test_key").await.unwrap();
        assert!(deleted);
    }

    #[tokio::test]
    async fn test_api_bulk_delete() {
        let api = StudioApi::new();
        let keys = vec!["key1".to_string(), "key2".to_string(), "key3".to_string()];
        let deleted = api.delete_keys(&keys).await.unwrap();
        assert_eq!(deleted, 3);
    }

    #[tokio::test]
    async fn test_api_slow_log() {
        let api = StudioApi::new();
        let log = api.get_slow_log(10).await.unwrap();
        assert!(!log.is_empty());
        api.clear_slow_log().await.unwrap();
    }

    #[tokio::test]
    async fn test_api_cluster_info() {
        let api = StudioApi::new();
        let info = api.get_cluster_info().await.unwrap();
        assert!(!info.is_empty());
        assert_eq!(info[0].role, "master");
    }

    #[tokio::test]
    async fn test_api_config() {
        let api = StudioApi::new();
        let all = api.get_all_config().await.unwrap();
        assert!(!all.is_empty());
        let val = api.get_config("maxclients").await.unwrap();
        assert!(val.is_some());
        api.set_config("maxclients", "20000").await.unwrap();
    }

    #[tokio::test]
    async fn test_api_execute_command() {
        let api = StudioApi::new();
        let result = api.execute_command("PING").await.unwrap();
        assert!(result.is_string());
    }

    #[test]
    fn test_api_default() {
        let api = StudioApi::default();
        // Ensure Default trait works
        let _ = api;
    }

    #[test]
    fn test_request_types_serialize() {
        let set_req = SetKeyRequest {
            key: "foo".to_string(),
            value: "bar".to_string(),
            ttl: Some(3600),
        };
        let json = serde_json::to_string(&set_req).unwrap();
        assert!(json.contains("foo"));

        let cmd_req = ExecuteCommandRequest {
            command: "PING".to_string(),
        };
        let json = serde_json::to_string(&cmd_req).unwrap();
        assert!(json.contains("PING"));

        let del_req = DeleteKeysRequest {
            keys: vec!["a".to_string(), "b".to_string()],
        };
        let json = serde_json::to_string(&del_req).unwrap();
        assert!(json.contains("\"a\""));

        let rename_req = RenameKeyRequest {
            key: "old".to_string(),
            new_key: "new".to_string(),
        };
        let json = serde_json::to_string(&rename_req).unwrap();
        assert!(json.contains("old"));
        assert!(json.contains("new"));

        let ttl_req = SetTtlRequest {
            key: "mykey".to_string(),
            ttl: 3600,
        };
        let json = serde_json::to_string(&ttl_req).unwrap();
        assert!(json.contains("mykey"));
        assert!(json.contains("3600"));
    }

    #[tokio::test]
    async fn test_api_rename_key() {
        let api = StudioApi::new();
        api.rename_key("old_key", "new_key").await.unwrap();
    }

    #[tokio::test]
    async fn test_api_set_ttl() {
        let api = StudioApi::new();
        api.set_ttl("test_key", 3600).await.unwrap();
        api.set_ttl("test_key", -1).await.unwrap();
    }

    #[tokio::test]
    async fn test_api_get_key_detail() {
        let api = StudioApi::new();
        let detail = api.get_key_detail("test_key").await.unwrap();
        assert!(detail.is_some());
        let detail = detail.unwrap();
        assert_eq!(detail.key, "test_key");
        assert!(!detail.encoding.is_empty());
    }

    #[test]
    fn test_key_detail_serialization() {
        let detail = KeyDetail {
            key: "foo".to_string(),
            key_type: "string".to_string(),
            ttl: -1,
            size: Some(128),
            length: None,
            encoding: "embstr".to_string(),
            value: serde_json::Value::String("bar".to_string()),
        };
        let json = serde_json::to_string(&detail).unwrap();
        assert!(json.contains("embstr"));
        let deserialized: KeyDetail = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.encoding, "embstr");
    }
}
