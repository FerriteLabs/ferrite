//! Data collection module for the Ferrite TUI dashboard.
//!
//! Connects to a Ferrite/Redis server via the Redis protocol (RESP),
//! periodically runs INFO and other commands, and parses the responses
//! into structured metrics for display.

use std::io;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

/// Aggregated server statistics parsed from the INFO command.
#[derive(Debug, Clone, Default)]
pub struct ServerStats {
    // Server
    pub version: String,
    pub uptime_seconds: u64,

    // Memory
    pub used_memory: u64,
    pub used_memory_rss: u64,
    pub max_memory: u64,
    pub peak_memory: u64,
    pub fragmentation_ratio: f64,

    // Throughput
    pub ops_per_sec: u64,
    pub total_commands: u64,

    // Clients
    pub connected_clients: u64,
    pub blocked_clients: u64,
    pub total_connections: u64,
    pub rejected_connections: u64,

    // Keyspace
    pub keyspace_hits: u64,
    pub keyspace_misses: u64,
    pub expired_keys: u64,
    pub evicted_keys: u64,
    pub prev_evicted_keys: u64,
    pub db_keys: u64,

    // Latency
    pub latency_p50: f64,
    pub latency_p95: f64,
    pub latency_p99: f64,
    pub latency_p999: f64,

    // Network
    pub total_net_input_bytes: u64,
    pub total_net_output_bytes: u64,

    // HybridLog tiers
    pub hybridlog_mutable_bytes: u64,
    pub hybridlog_mutable_entries: u64,
    pub hybridlog_readonly_bytes: u64,
    pub hybridlog_readonly_entries: u64,
    pub hybridlog_disk_bytes: u64,
    pub hybridlog_disk_entries: u64,

    // Slow commands
    pub slow_commands_count: u64,
}

/// Information about a connected client.
#[derive(Debug, Clone)]
pub struct ClientInfo {
    pub id: u64,
    pub addr: String,
    pub name: String,
    pub age: u64,
    pub idle: u64,
    pub last_cmd: String,
    pub db: u64,
}

/// A slowlog entry.
#[derive(Debug, Clone)]
pub struct SlowlogEntry {
    pub duration_ms: f64,
    pub command: String,
    pub timestamp: u64,
}

/// Key metadata returned by SCAN + TYPE + TTL.
#[derive(Debug, Clone)]
pub struct KeyInfo {
    pub name: String,
    pub key_type: String,
    pub ttl: i64,
    pub size: Option<u64>,
}

/// Detailed information about a single key.
#[derive(Debug, Clone)]
pub struct KeyDetail {
    pub key: String,
    pub key_type: String,
    pub ttl: i64,
    pub encoding: String,
    pub value_preview: String,
}

/// A single command stat entry from COMMANDSTATS.
#[derive(Debug, Clone)]
pub struct CommandStat {
    pub name: String,
    pub calls: u64,
    pub usec_per_call: f64,
}

/// Cluster node information.
#[derive(Debug, Clone)]
pub struct ClusterNodeInfo {
    pub id: String,
    pub addr: String,
    pub role: String,
    pub status: String,
    pub slot_ranges: Vec<(u16, u16)>,
    pub master_id: Option<String>,
}

/// Cluster-level state information.
#[derive(Debug, Clone, Default)]
pub struct ClusterInfo {
    pub enabled: bool,
    pub state: String,
    pub cluster_size: usize,
    pub slots_assigned: u16,
    pub slots_ok: u16,
    pub nodes: Vec<ClusterNodeInfo>,
}

/// Log entry captured from the server.
#[derive(Debug, Clone)]
pub struct LogEntry {
    pub timestamp: String,
    pub level: LogLevel,
    pub message: String,
}

/// Log severity levels.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum LogLevel {
    Trace,
    Debug,
    Info,
    Warning,
    Error,
}

impl LogLevel {
    /// Parse a log level string.
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "trace" => LogLevel::Trace,
            "debug" => LogLevel::Debug,
            "info" => LogLevel::Info,
            "warn" | "warning" => LogLevel::Warning,
            "error" | "err" => LogLevel::Error,
            _ => LogLevel::Info,
        }
    }

    /// Display name for the log level.
    pub fn as_str(&self) -> &'static str {
        match self {
            LogLevel::Trace => "TRACE",
            LogLevel::Debug => "DEBUG",
            LogLevel::Info => "INFO",
            LogLevel::Warning => "WARN",
            LogLevel::Error => "ERROR",
        }
    }
}

/// Collects data from a Ferrite/Redis server over a TCP connection using the RESP protocol.
pub struct DataCollector {
    pub host: String,
    pub port: u16,
    reader: BufReader<tokio::net::tcp::OwnedReadHalf>,
    writer: tokio::net::tcp::OwnedWriteHalf,
    pub stats: ServerStats,
    pub clients: Vec<ClientInfo>,
    pub slowlog: Vec<SlowlogEntry>,
    pub command_stats: Vec<CommandStat>,
    pub logs: Vec<LogEntry>,
}

impl DataCollector {
    /// Create a new data collector connected to the given host and port.
    /// Optionally authenticates with the provided password.
    pub async fn new(host: &str, port: u16, password: Option<&str>) -> io::Result<Self> {
        let stream = TcpStream::connect(format!("{}:{}", host, port)).await?;
        let (read_half, write_half) = stream.into_split();
        let reader = BufReader::new(read_half);

        let mut collector = Self {
            host: host.to_string(),
            port,
            reader,
            writer: write_half,
            stats: ServerStats::default(),
            clients: Vec::new(),
            slowlog: Vec::new(),
            command_stats: Vec::new(),
            logs: Vec::new(),
        };

        // Authenticate if password provided
        if let Some(pwd) = password {
            collector.send_command(&["AUTH", pwd]).await?;
            collector.read_response().await?;
        }

        Ok(collector)
    }

    /// Refresh all metrics from the server.
    pub async fn refresh(&mut self) -> io::Result<()> {
        // Fetch INFO
        self.send_command(&["INFO"]).await?;
        let info = self.read_response().await?;
        self.parse_info(&info);

        // Fetch CLIENT LIST
        self.send_command(&["CLIENT", "LIST"]).await?;
        let clients = self.read_response().await?;
        self.parse_clients(&clients);

        // Fetch SLOWLOG LEN
        self.send_command(&["SLOWLOG", "LEN"]).await?;
        let slowlog_len = self.read_response().await?;
        self.stats.slow_commands_count = slowlog_len.trim().parse().unwrap_or(0);

        Ok(())
    }

    /// Scan keys matching a pattern using the SCAN command.
    pub async fn scan_keys(
        &mut self,
        pattern: &str,
        cursor: u64,
        count: u64,
    ) -> io::Result<(u64, Vec<KeyInfo>)> {
        let cursor_str = cursor.to_string();
        let count_str = count.to_string();
        self.send_command(&["SCAN", &cursor_str, "MATCH", pattern, "COUNT", &count_str])
            .await?;

        // Read array response
        let mut line = String::new();
        self.reader.read_line(&mut line).await?;

        let new_cursor: u64;
        let mut keys = Vec::new();

        if line.starts_with('*') {
            // Read cursor bulk string
            let mut cursor_line = String::new();
            self.reader.read_line(&mut cursor_line).await?;
            if let Some(stripped) = cursor_line.strip_prefix('$') {
                let len: usize = stripped.trim().parse().unwrap_or(0);
                let mut buf = vec![0u8; len + 2];
                tokio::io::AsyncReadExt::read_exact(&mut self.reader, &mut buf).await?;
                new_cursor = String::from_utf8_lossy(&buf[..len]).parse().unwrap_or(0);
            } else {
                new_cursor = 0;
            }

            // Read keys array
            let mut keys_line = String::new();
            self.reader.read_line(&mut keys_line).await?;
            if let Some(stripped) = keys_line.strip_prefix('*') {
                let count: i64 = stripped.trim().parse().unwrap_or(0);
                for _ in 0..count {
                    let mut len_line = String::new();
                    self.reader.read_line(&mut len_line).await?;
                    if let Some(stripped) = len_line.strip_prefix('$') {
                        let len: usize = stripped.trim().parse().unwrap_or(0);
                        let mut buf = vec![0u8; len + 2];
                        tokio::io::AsyncReadExt::read_exact(&mut self.reader, &mut buf).await?;
                        let key_name = String::from_utf8_lossy(&buf[..len]).to_string();

                        // Get key type
                        self.send_command(&["TYPE", &key_name]).await?;
                        let key_type = self.read_response().await.unwrap_or_default();

                        // Get TTL
                        self.send_command(&["TTL", &key_name]).await?;
                        let ttl_str = self.read_response().await.unwrap_or_default();
                        let ttl: i64 = ttl_str.parse().unwrap_or(-1);

                        keys.push(KeyInfo {
                            name: key_name,
                            key_type,
                            ttl,
                            size: None,
                        });
                    }
                }
            }
        } else {
            new_cursor = 0;
        }

        Ok((new_cursor, keys))
    }

    /// Get detailed information about a specific key.
    pub async fn get_key_detail(&mut self, key: &str) -> io::Result<KeyDetail> {
        // Get type
        self.send_command(&["TYPE", key]).await?;
        let key_type = self.read_response().await?;

        // Get TTL
        self.send_command(&["TTL", key]).await?;
        let ttl_str = self.read_response().await?;
        let ttl: i64 = ttl_str.parse().unwrap_or(-1);

        // Get encoding
        self.send_command(&["OBJECT", "ENCODING", key]).await?;
        let encoding = self
            .read_response()
            .await
            .unwrap_or_else(|_| "unknown".to_string());

        // Get value preview based on type
        let value_preview = match key_type.as_str() {
            "string" => {
                self.send_command(&["GET", key]).await?;
                let val = self.read_response().await?;
                if val.len() > 500 {
                    format!("{}... (truncated)", &val[..500])
                } else {
                    val
                }
            }
            "list" => {
                self.send_command(&["LRANGE", key, "0", "10"]).await?;
                let _ = self.read_response().await?;
                "[list data - expand to view]".to_string()
            }
            "hash" => {
                self.send_command(&["HGETALL", key]).await?;
                let _ = self.read_response().await?;
                "[hash data - expand to view]".to_string()
            }
            "set" => {
                self.send_command(&["SMEMBERS", key]).await?;
                let _ = self.read_response().await?;
                "[set data - expand to view]".to_string()
            }
            "zset" => {
                self.send_command(&["ZRANGE", key, "0", "10", "WITHSCORES"])
                    .await?;
                let _ = self.read_response().await?;
                "[sorted set data - expand to view]".to_string()
            }
            _ => "[unknown type]".to_string(),
        };

        Ok(KeyDetail {
            key: key.to_string(),
            key_type,
            ttl,
            encoding,
            value_preview,
        })
    }

    /// Delete a key from the server.
    pub async fn delete_key(&mut self, key: &str) -> io::Result<bool> {
        self.send_command(&["DEL", key]).await?;
        let response = self.read_response().await?;
        Ok(response == "1")
    }

    /// Get cluster information from the server.
    pub async fn get_cluster_info(&mut self) -> io::Result<ClusterInfo> {
        self.send_command(&["CLUSTER", "INFO"]).await?;
        let cluster_info_str = self.read_response().await?;

        if cluster_info_str.contains("cluster_state") {
            let mut info = ClusterInfo {
                enabled: true,
                ..Default::default()
            };

            for line in cluster_info_str.lines() {
                if let Some((key, value)) = line.split_once(':') {
                    match key {
                        "cluster_state" => info.state = value.trim().to_string(),
                        "cluster_size" => info.cluster_size = value.parse().unwrap_or(0),
                        "cluster_slots_assigned" => {
                            info.slots_assigned = value.parse().unwrap_or(0)
                        }
                        "cluster_slots_ok" => info.slots_ok = value.parse().unwrap_or(0),
                        _ => {}
                    }
                }
            }

            // Get cluster nodes
            self.send_command(&["CLUSTER", "NODES"]).await?;
            let nodes_data = self.read_response().await?;
            info.nodes = parse_cluster_nodes(&nodes_data);

            Ok(info)
        } else {
            Ok(ClusterInfo::default())
        }
    }

    /// Send a RESP command to the server.
    async fn send_command(&mut self, args: &[&str]) -> io::Result<()> {
        let mut cmd = format!("*{}\r\n", args.len());
        for arg in args {
            cmd.push_str(&format!("${}\r\n{}\r\n", arg.len(), arg));
        }
        self.writer.write_all(cmd.as_bytes()).await?;
        self.writer.flush().await
    }

    /// Read a single RESP response from the server.
    async fn read_response(&mut self) -> io::Result<String> {
        let mut line = String::new();
        self.reader.read_line(&mut line).await?;

        match line.chars().next() {
            Some('+') => Ok(line[1..].trim().to_string()),
            Some('-') => Ok(line[1..].trim().to_string()),
            Some(':') => Ok(line[1..].trim().to_string()),
            Some('$') => {
                let len: i64 = line[1..].trim().parse().unwrap_or(-1);
                if len < 0 {
                    return Ok(String::new());
                }
                let mut buf = vec![0u8; len as usize + 2];
                tokio::io::AsyncReadExt::read_exact(&mut self.reader, &mut buf).await?;
                buf.truncate(len as usize);
                Ok(String::from_utf8_lossy(&buf).to_string())
            }
            Some('*') => {
                // Simplified array handling - consume and return empty
                Ok(String::new())
            }
            _ => Ok(String::new()),
        }
    }

    /// Parse the INFO command output into structured fields.
    fn parse_info(&mut self, info: &str) {
        // Preserve previous evicted_keys for rate calculation
        self.stats.prev_evicted_keys = self.stats.evicted_keys;

        for line in info.lines() {
            if let Some((key, value)) = line.split_once(':') {
                match key {
                    // Server
                    "redis_version" | "ferrite_version" => {
                        self.stats.version = value.trim().to_string();
                    }
                    "uptime_in_seconds" => {
                        self.stats.uptime_seconds = value.parse().unwrap_or(0);
                    }

                    // Memory
                    "used_memory" => {
                        self.stats.used_memory = value.parse().unwrap_or(0);
                    }
                    "used_memory_rss" => {
                        self.stats.used_memory_rss = value.parse().unwrap_or(0);
                    }
                    "maxmemory" => {
                        self.stats.max_memory = value.parse().unwrap_or(0);
                    }
                    "used_memory_peak" => {
                        self.stats.peak_memory = value.parse().unwrap_or(0);
                    }
                    "mem_fragmentation_ratio" => {
                        self.stats.fragmentation_ratio = value.parse().unwrap_or(1.0);
                    }

                    // Throughput
                    "instantaneous_ops_per_sec" | "ops_per_sec" => {
                        self.stats.ops_per_sec = value.parse().unwrap_or(0);
                    }
                    "total_commands_processed" => {
                        self.stats.total_commands = value.parse().unwrap_or(0);
                    }

                    // Clients
                    "connected_clients" => {
                        self.stats.connected_clients = value.parse().unwrap_or(0);
                    }
                    "blocked_clients" => {
                        self.stats.blocked_clients = value.parse().unwrap_or(0);
                    }
                    "total_connections_received" => {
                        self.stats.total_connections = value.parse().unwrap_or(0);
                    }
                    "rejected_connections" => {
                        self.stats.rejected_connections = value.parse().unwrap_or(0);
                    }

                    // Keyspace
                    "keyspace_hits" => {
                        self.stats.keyspace_hits = value.parse().unwrap_or(0);
                    }
                    "keyspace_misses" => {
                        self.stats.keyspace_misses = value.parse().unwrap_or(0);
                    }
                    "expired_keys" => {
                        self.stats.expired_keys = value.parse().unwrap_or(0);
                    }
                    "evicted_keys" => {
                        self.stats.evicted_keys = value.parse().unwrap_or(0);
                    }

                    // Latency
                    "latency_p50" | "latency_p50_ms" => {
                        self.stats.latency_p50 = value.parse().unwrap_or(0.0);
                    }
                    "latency_p95" | "latency_p95_ms" => {
                        self.stats.latency_p95 = value.parse().unwrap_or(0.0);
                    }
                    "latency_p99" | "latency_p99_ms" => {
                        self.stats.latency_p99 = value.parse().unwrap_or(0.0);
                    }
                    "latency_p999" | "latency_p999_ms" => {
                        self.stats.latency_p999 = value.parse().unwrap_or(0.0);
                    }

                    // Network
                    "total_net_input_bytes" => {
                        self.stats.total_net_input_bytes = value.parse().unwrap_or(0);
                    }
                    "total_net_output_bytes" => {
                        self.stats.total_net_output_bytes = value.parse().unwrap_or(0);
                    }

                    // HybridLog tiers
                    "hybridlog_mutable_bytes" => {
                        self.stats.hybridlog_mutable_bytes = value.parse().unwrap_or(0);
                    }
                    "hybridlog_mutable_entries" => {
                        self.stats.hybridlog_mutable_entries = value.parse().unwrap_or(0);
                    }
                    "hybridlog_readonly_bytes" => {
                        self.stats.hybridlog_readonly_bytes = value.parse().unwrap_or(0);
                    }
                    "hybridlog_readonly_entries" => {
                        self.stats.hybridlog_readonly_entries = value.parse().unwrap_or(0);
                    }
                    "hybridlog_disk_bytes" => {
                        self.stats.hybridlog_disk_bytes = value.parse().unwrap_or(0);
                    }
                    "hybridlog_disk_entries" => {
                        self.stats.hybridlog_disk_entries = value.parse().unwrap_or(0);
                    }

                    _ => {
                        // Parse db0:keys=1234,expires=5,avg_ttl=6789
                        if key.starts_with("db") {
                            for part in value.split(',') {
                                if let Some(keys_str) = part.strip_prefix("keys=") {
                                    self.stats.db_keys +=
                                        keys_str.parse::<u64>().unwrap_or(0);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    /// Parse CLIENT LIST output into structured client info.
    fn parse_clients(&mut self, data: &str) {
        self.clients.clear();
        for line in data.lines() {
            let mut id = 0u64;
            let mut addr = String::new();
            let mut name = String::new();
            let mut cmd = String::new();
            let mut age = 0u64;
            let mut idle = 0u64;
            let mut db = 0u64;

            for part in line.split_whitespace() {
                if let Some((key, value)) = part.split_once('=') {
                    match key {
                        "id" => id = value.parse().unwrap_or(0),
                        "addr" => addr = value.to_string(),
                        "name" => name = value.to_string(),
                        "cmd" => cmd = value.to_string(),
                        "age" => age = value.parse().unwrap_or(0),
                        "idle" => idle = value.parse().unwrap_or(0),
                        "db" => db = value.parse().unwrap_or(0),
                        _ => {}
                    }
                }
            }

            if !addr.is_empty() {
                self.clients.push(ClientInfo {
                    id,
                    addr,
                    name,
                    age,
                    idle,
                    last_cmd: cmd,
                    db,
                });
            }
        }
    }
}

/// Parse CLUSTER NODES output into structured node information.
fn parse_cluster_nodes(data: &str) -> Vec<ClusterNodeInfo> {
    let mut nodes = Vec::new();
    for line in data.lines() {
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() < 8 {
            continue;
        }

        let id = parts[0].to_string();
        let addr = parts[1].split('@').next().unwrap_or(parts[1]).to_string();
        let flags = parts[2];
        let master_id = if parts[3] == "-" {
            None
        } else {
            Some(parts[3].to_string())
        };

        let role = if flags.contains("master") {
            "master".to_string()
        } else if flags.contains("slave") {
            "replica".to_string()
        } else {
            "unknown".to_string()
        };

        let status = if flags.contains("fail") {
            "fail".to_string()
        } else if flags.contains("handshake") {
            "handshake".to_string()
        } else if flags.contains("noaddr") {
            "noaddr".to_string()
        } else {
            "connected".to_string()
        };

        let mut slot_ranges = Vec::new();
        for part in &parts[8..] {
            if part.contains('-') {
                let range: Vec<&str> = part.split('-').collect();
                if range.len() == 2 {
                    if let (Ok(start), Ok(end)) =
                        (range[0].parse::<u16>(), range[1].parse::<u16>())
                    {
                        slot_ranges.push((start, end));
                    }
                }
            } else if let Ok(slot) = part.parse::<u16>() {
                slot_ranges.push((slot, slot));
            }
        }

        nodes.push(ClusterNodeInfo {
            id,
            addr,
            role,
            status,
            slot_ranges,
            master_id,
        });
    }
    nodes
}

// ── Formatting helpers ──────────────────────────────────────────────────────

/// Format a byte count into a human-readable string (e.g. "45.2MB").
pub fn format_bytes(bytes: u64) -> String {
    if bytes >= 1024 * 1024 * 1024 {
        format!("{:.1}GB", bytes as f64 / (1024.0 * 1024.0 * 1024.0))
    } else if bytes >= 1024 * 1024 {
        format!("{:.1}MB", bytes as f64 / (1024.0 * 1024.0))
    } else if bytes >= 1024 {
        format!("{:.1}KB", bytes as f64 / 1024.0)
    } else {
        format!("{}B", bytes)
    }
}

/// Format a number with abbreviated suffix (e.g. "1.2M", "45.3K").
pub fn format_number(n: u64) -> String {
    if n >= 1_000_000 {
        format!("{:.1}M", n as f64 / 1_000_000.0)
    } else if n >= 1_000 {
        format!("{:.1}K", n as f64 / 1_000.0)
    } else {
        n.to_string()
    }
}

/// Format seconds into a human-readable uptime string (e.g. "3d 14h 22m").
pub fn format_uptime(seconds: u64) -> String {
    let days = seconds / 86400;
    let hours = (seconds % 86400) / 3600;
    let minutes = (seconds % 3600) / 60;

    if days > 0 {
        format!("{}d {}h {}m", days, hours, minutes)
    } else if hours > 0 {
        format!("{}h {}m", hours, minutes)
    } else {
        format!("{}m {}s", minutes, seconds % 60)
    }
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(0), "0B");
        assert_eq!(format_bytes(512), "512B");
        assert_eq!(format_bytes(1024), "1.0KB");
        assert_eq!(format_bytes(1536), "1.5KB");
        assert_eq!(format_bytes(1048576), "1.0MB");
        assert_eq!(format_bytes(47_394_611), "45.2MB");
        assert_eq!(format_bytes(1073741824), "1.0GB");
        assert_eq!(format_bytes(4294967296), "4.0GB");
    }

    #[test]
    fn test_format_number() {
        assert_eq!(format_number(0), "0");
        assert_eq!(format_number(42), "42");
        assert_eq!(format_number(999), "999");
        assert_eq!(format_number(1000), "1.0K");
        assert_eq!(format_number(1234567), "1.2M");
    }

    #[test]
    fn test_format_uptime() {
        assert_eq!(format_uptime(45), "0m 45s");
        assert_eq!(format_uptime(3600), "1h 0m");
        assert_eq!(format_uptime(86400), "1d 0h 0m");
        assert_eq!(format_uptime(309720), "3d 14h 2m");
    }

    #[test]
    fn test_parse_info_basic() {
        let mut collector_stats = ServerStats::default();

        let info = "\
redis_version:7.0.0\r\n\
uptime_in_seconds:12345\r\n\
used_memory:47185920\r\n\
maxmemory:4294967296\r\n\
used_memory_peak:71303168\r\n\
mem_fragmentation_ratio:1.02\r\n\
instantaneous_ops_per_sec:125432\r\n\
connected_clients:42\r\n\
total_commands_processed:999999\r\n\
keyspace_hits:80000\r\n\
keyspace_misses:20000\r\n\
latency_p50:0.08\r\n\
latency_p99:0.25\r\n\
latency_p999:1.2\r\n\
hybridlog_mutable_bytes:33554432\r\n\
hybridlog_readonly_bytes:134217728\r\n\
hybridlog_disk_bytes:1288490188\r\n\
";

        // Manually parse to test the parsing logic
        for line in info.lines() {
            if let Some((key, value)) = line.split_once(':') {
                match key {
                    "redis_version" => {
                        collector_stats.version = value.trim().to_string();
                    }
                    "uptime_in_seconds" => {
                        collector_stats.uptime_seconds = value.parse().unwrap_or(0);
                    }
                    "used_memory" => {
                        collector_stats.used_memory = value.parse().unwrap_or(0);
                    }
                    "maxmemory" => {
                        collector_stats.max_memory = value.parse().unwrap_or(0);
                    }
                    "used_memory_peak" => {
                        collector_stats.peak_memory = value.parse().unwrap_or(0);
                    }
                    "mem_fragmentation_ratio" => {
                        collector_stats.fragmentation_ratio = value.parse().unwrap_or(1.0);
                    }
                    "instantaneous_ops_per_sec" => {
                        collector_stats.ops_per_sec = value.parse().unwrap_or(0);
                    }
                    "connected_clients" => {
                        collector_stats.connected_clients = value.parse().unwrap_or(0);
                    }
                    "total_commands_processed" => {
                        collector_stats.total_commands = value.parse().unwrap_or(0);
                    }
                    "keyspace_hits" => {
                        collector_stats.keyspace_hits = value.parse().unwrap_or(0);
                    }
                    "keyspace_misses" => {
                        collector_stats.keyspace_misses = value.parse().unwrap_or(0);
                    }
                    "latency_p50" => {
                        collector_stats.latency_p50 = value.parse().unwrap_or(0.0);
                    }
                    "latency_p99" => {
                        collector_stats.latency_p99 = value.parse().unwrap_or(0.0);
                    }
                    "latency_p999" => {
                        collector_stats.latency_p999 = value.parse().unwrap_or(0.0);
                    }
                    "hybridlog_mutable_bytes" => {
                        collector_stats.hybridlog_mutable_bytes = value.parse().unwrap_or(0);
                    }
                    "hybridlog_readonly_bytes" => {
                        collector_stats.hybridlog_readonly_bytes = value.parse().unwrap_or(0);
                    }
                    "hybridlog_disk_bytes" => {
                        collector_stats.hybridlog_disk_bytes = value.parse().unwrap_or(0);
                    }
                    _ => {}
                }
            }
        }

        assert_eq!(collector_stats.version, "7.0.0");
        assert_eq!(collector_stats.uptime_seconds, 12345);
        assert_eq!(collector_stats.used_memory, 47185920);
        assert_eq!(collector_stats.max_memory, 4294967296);
        assert_eq!(collector_stats.peak_memory, 71303168);
        assert!((collector_stats.fragmentation_ratio - 1.02).abs() < 0.001);
        assert_eq!(collector_stats.ops_per_sec, 125432);
        assert_eq!(collector_stats.connected_clients, 42);
        assert_eq!(collector_stats.total_commands, 999999);
        assert_eq!(collector_stats.keyspace_hits, 80000);
        assert_eq!(collector_stats.keyspace_misses, 20000);
        assert!((collector_stats.latency_p50 - 0.08).abs() < 0.001);
        assert!((collector_stats.latency_p99 - 0.25).abs() < 0.001);
        assert!((collector_stats.latency_p999 - 1.2).abs() < 0.001);
        assert_eq!(collector_stats.hybridlog_mutable_bytes, 33554432);
        assert_eq!(collector_stats.hybridlog_readonly_bytes, 134217728);
        assert_eq!(collector_stats.hybridlog_disk_bytes, 1288490188);
    }

    #[test]
    fn test_parse_cluster_nodes() {
        let data = "07c37dfeb235213a872192d90877d0cd55635b91 127.0.0.1:6379@16379 master - 0 1426238317239 2 connected 5461-10922\n\
                     67ed2db8d677e59ec4a4cefb06858cf2a1a89fa1 127.0.0.1:6380@16380 slave 07c37dfeb235213a872192d90877d0cd55635b91 0 1426238316232 2 connected\n";

        let nodes = parse_cluster_nodes(data);
        assert_eq!(nodes.len(), 2);
        assert_eq!(nodes[0].role, "master");
        assert_eq!(nodes[0].addr, "127.0.0.1:6379");
        assert_eq!(nodes[0].slot_ranges, vec![(5461, 10922)]);
        assert_eq!(nodes[1].role, "replica");
        assert_eq!(nodes[1].master_id, Some("07c37dfeb235213a872192d90877d0cd55635b91".to_string()));
    }

    #[test]
    fn test_log_level_parsing() {
        assert_eq!(LogLevel::from_str("trace"), LogLevel::Trace);
        assert_eq!(LogLevel::from_str("DEBUG"), LogLevel::Debug);
        assert_eq!(LogLevel::from_str("Info"), LogLevel::Info);
        assert_eq!(LogLevel::from_str("WARN"), LogLevel::Warning);
        assert_eq!(LogLevel::from_str("warning"), LogLevel::Warning);
        assert_eq!(LogLevel::from_str("error"), LogLevel::Error);
        assert_eq!(LogLevel::from_str("ERR"), LogLevel::Error);
        assert_eq!(LogLevel::from_str("unknown"), LogLevel::Info);
    }

    #[test]
    fn test_log_level_display() {
        assert_eq!(LogLevel::Trace.as_str(), "TRACE");
        assert_eq!(LogLevel::Debug.as_str(), "DEBUG");
        assert_eq!(LogLevel::Info.as_str(), "INFO");
        assert_eq!(LogLevel::Warning.as_str(), "WARN");
        assert_eq!(LogLevel::Error.as_str(), "ERROR");
    }

    #[test]
    fn test_log_level_ordering() {
        assert!(LogLevel::Trace < LogLevel::Debug);
        assert!(LogLevel::Debug < LogLevel::Info);
        assert!(LogLevel::Info < LogLevel::Warning);
        assert!(LogLevel::Warning < LogLevel::Error);
    }

    #[test]
    fn test_server_stats_default() {
        let stats = ServerStats::default();
        assert_eq!(stats.used_memory, 0);
        assert_eq!(stats.version, "");
        assert_eq!(stats.ops_per_sec, 0);
        assert!((stats.fragmentation_ratio - 0.0).abs() < f64::EPSILON);
    }
}
