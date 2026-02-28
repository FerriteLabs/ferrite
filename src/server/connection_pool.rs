#![allow(dead_code)]
//! Native connection pooler and multiplexer
//!
//! Built-in proxy that multiplexes N client connections onto M backend
//! connections. Supports read/write splitting for replica routing.

use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::{Mutex, RwLock};
use thiserror::Error;

// ── Errors ───────────────────────────────────────────────────────────────────

/// Errors from the connection pool.
#[derive(Debug, Error)]
pub enum PoolError {
    /// Pool has reached its maximum capacity and no connections are available.
    #[error("connection pool exhausted")]
    Exhausted,

    /// Failed to establish a connection to the backend.
    #[error("connect failed: {0}")]
    ConnectFailed(String),

    /// Timed out waiting for a connection.
    #[error("connection acquisition timed out")]
    Timeout,

    /// Pool has been closed.
    #[error("connection pool is closed")]
    Closed,
}

// ── Configuration ────────────────────────────────────────────────────────────

/// Configuration for the connection pool.
#[derive(Debug, Clone)]
pub struct PoolConfig {
    /// Maximum number of backend connections.
    pub max_connections: u32,
    /// Minimum number of idle connections to keep warm.
    pub min_idle: u32,
    /// Maximum time a connection can sit idle before being reaped.
    pub max_idle_time: Duration,
    /// Timeout when establishing a new backend connection.
    pub connect_timeout: Duration,
    /// Timeout when waiting for a response from the backend.
    pub read_timeout: Duration,
    /// Whether to route read commands to replicas.
    pub enable_read_write_split: bool,
    /// Address of the primary backend.
    pub primary_addr: SocketAddr,
    /// Addresses of read-only replicas.
    pub replica_addrs: Vec<SocketAddr>,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            max_connections: 100,
            min_idle: 10,
            max_idle_time: Duration::from_secs(300),
            connect_timeout: Duration::from_secs(5),
            read_timeout: Duration::from_secs(30),
            enable_read_write_split: false,
            primary_addr: "127.0.0.1:6379".parse().expect("valid default addr"),
            replica_addrs: Vec::new(),
        }
    }
}

// ── Connection state ─────────────────────────────────────────────────────────

/// State of a pooled connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionState {
    /// Connection is actively in use by a client.
    Active,
    /// Connection is idle and available for reuse.
    Idle,
    /// Connection is being drained / closed.
    Closing,
}

/// A connection managed by the pool.
#[derive(Debug)]
pub struct PooledConnection {
    /// Unique identifier for this connection.
    pub id: u64,
    /// Remote address of the backend this connection talks to.
    pub addr: SocketAddr,
    /// When the connection was first established.
    pub created_at: Instant,
    /// When the connection was last used.
    pub last_used: Instant,
    /// Whether the connection targets a read-only replica.
    pub is_read_only: bool,
    /// Current lifecycle state.
    pub state: ConnectionState,
}

// ── Statistics ───────────────────────────────────────────────────────────────

/// Aggregated pool statistics.
#[derive(Debug, Clone)]
pub struct PoolStats {
    /// Total connections (active + idle).
    pub total_connections: u64,
    /// Connections currently checked out.
    pub active_connections: u64,
    /// Connections sitting idle.
    pub idle_connections: u64,
    /// Number of callers currently waiting for a connection.
    pub wait_count: u64,
    /// Lifetime count of successful acquisitions.
    pub total_acquired: u64,
    /// Lifetime count of returns.
    pub total_released: u64,
    /// Average wait time in milliseconds to acquire a connection.
    pub avg_wait_ms: f64,
    /// Maximum observed wait time in milliseconds.
    pub max_wait_ms: u64,
}

// ── Connection pool ──────────────────────────────────────────────────────────

/// Thread-safe connection pool.
pub struct ConnectionPool {
    config: RwLock<PoolConfig>,
    /// Idle primary connections.
    primary_idle: Mutex<VecDeque<PooledConnection>>,
    /// Idle replica connections.
    replica_idle: Mutex<VecDeque<PooledConnection>>,
    /// Counter for connection IDs.
    next_id: AtomicU64,
    /// Active connection count.
    active_count: AtomicU64,
    /// Lifetime acquired counter.
    total_acquired: AtomicU64,
    /// Lifetime released counter.
    total_released: AtomicU64,
    /// Wait count.
    wait_count: AtomicU64,
    /// Cumulative wait time in microseconds.
    total_wait_us: AtomicU64,
    /// Max wait time in microseconds.
    max_wait_us: AtomicU64,
    /// Whether the pool is closed.
    closed: Mutex<bool>,
}

impl ConnectionPool {
    /// Create a new connection pool with the given configuration.
    pub fn new(config: PoolConfig) -> Self {
        Self {
            config: RwLock::new(config),
            primary_idle: Mutex::new(VecDeque::new()),
            replica_idle: Mutex::new(VecDeque::new()),
            next_id: AtomicU64::new(1),
            active_count: AtomicU64::new(0),
            total_acquired: AtomicU64::new(0),
            total_released: AtomicU64::new(0),
            wait_count: AtomicU64::new(0),
            total_wait_us: AtomicU64::new(0),
            max_wait_us: AtomicU64::new(0),
            closed: Mutex::new(false),
        }
    }

    /// Acquire a connection from the pool.
    ///
    /// When `read_only` is `true` and read/write splitting is enabled, a
    /// replica connection is preferred. Falls back to the primary otherwise.
    pub fn get_connection(&self, read_only: bool) -> Result<PooledConnection, PoolError> {
        if *self.closed.lock() {
            return Err(PoolError::Closed);
        }

        let start = Instant::now();
        self.wait_count.fetch_add(1, Ordering::Relaxed);

        let config = self.config.read();

        // Try to get an idle connection from the appropriate queue
        let conn = if read_only && config.enable_read_write_split {
            self.try_get_idle(&self.replica_idle)
                .or_else(|| self.try_get_idle(&self.primary_idle))
        } else {
            self.try_get_idle(&self.primary_idle)
        };

        let conn = match conn {
            Some(mut c) => {
                c.state = ConnectionState::Active;
                c.last_used = Instant::now();
                c
            }
            None => {
                // Check capacity
                let total = self.active_count.load(Ordering::Relaxed)
                    + self.primary_idle.lock().len() as u64
                    + self.replica_idle.lock().len() as u64;
                if total >= u64::from(config.max_connections) {
                    self.wait_count.fetch_sub(1, Ordering::Relaxed);
                    return Err(PoolError::Exhausted);
                }

                // Create a new connection stub
                let addr = if read_only && config.enable_read_write_split {
                    config
                        .replica_addrs
                        .first()
                        .copied()
                        .unwrap_or(config.primary_addr)
                } else {
                    config.primary_addr
                };

                PooledConnection {
                    id: self.next_id.fetch_add(1, Ordering::Relaxed),
                    addr,
                    created_at: Instant::now(),
                    last_used: Instant::now(),
                    is_read_only: read_only && config.enable_read_write_split,
                    state: ConnectionState::Active,
                }
            }
        };

        self.active_count.fetch_add(1, Ordering::Relaxed);
        self.total_acquired.fetch_add(1, Ordering::Relaxed);
        self.wait_count.fetch_sub(1, Ordering::Relaxed);

        let elapsed_us = start.elapsed().as_micros() as u64;
        self.total_wait_us.fetch_add(elapsed_us, Ordering::Relaxed);
        // Update max with a simple CAS loop
        let mut current_max = self.max_wait_us.load(Ordering::Relaxed);
        while elapsed_us > current_max {
            match self.max_wait_us.compare_exchange_weak(
                current_max,
                elapsed_us,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => current_max = actual,
            }
        }

        Ok(conn)
    }

    /// Return a connection to the pool.
    pub fn return_connection(&self, mut conn: PooledConnection) {
        self.active_count.fetch_sub(1, Ordering::Relaxed);
        self.total_released.fetch_add(1, Ordering::Relaxed);

        if *self.closed.lock() {
            return; // Drop the connection
        }

        let config = self.config.read();

        // Check if the connection has been idle too long
        if conn.created_at.elapsed() > config.max_idle_time {
            return; // Drop stale connection
        }

        conn.state = ConnectionState::Idle;
        conn.last_used = Instant::now();

        if conn.is_read_only {
            self.replica_idle.lock().push_back(conn);
        } else {
            self.primary_idle.lock().push_back(conn);
        }
    }

    /// Get current pool statistics.
    pub fn stats(&self) -> PoolStats {
        let active = self.active_count.load(Ordering::Relaxed);
        let primary_idle = self.primary_idle.lock().len() as u64;
        let replica_idle = self.replica_idle.lock().len() as u64;
        let idle = primary_idle + replica_idle;
        let acquired = self.total_acquired.load(Ordering::Relaxed);
        let total_wait = self.total_wait_us.load(Ordering::Relaxed);
        let avg_wait_ms = if acquired > 0 {
            (total_wait as f64) / (acquired as f64) / 1000.0
        } else {
            0.0
        };

        PoolStats {
            total_connections: active + idle,
            active_connections: active,
            idle_connections: idle,
            wait_count: self.wait_count.load(Ordering::Relaxed),
            total_acquired: acquired,
            total_released: self.total_released.load(Ordering::Relaxed),
            avg_wait_ms,
            max_wait_ms: self.max_wait_us.load(Ordering::Relaxed) / 1000,
        }
    }

    /// Resize the pool to a new maximum.
    pub fn resize(&self, new_max: u32) {
        let mut config = self.config.write();
        config.max_connections = new_max;
    }

    /// Close all idle connections in the pool.
    pub fn drain(&self) {
        self.primary_idle.lock().clear();
        self.replica_idle.lock().clear();
    }

    /// Pop an idle connection from a queue, skipping stale entries.
    fn try_get_idle(
        &self,
        queue: &Mutex<VecDeque<PooledConnection>>,
    ) -> Option<PooledConnection> {
        let config = self.config.read();
        let mut q = queue.lock();
        while let Some(conn) = q.pop_front() {
            if conn.last_used.elapsed() <= config.max_idle_time {
                return Some(conn);
            }
            // else: connection is stale, drop it
        }
        None
    }
}

// ── Read/Write Splitter ──────────────────────────────────────────────────────

/// Type of Redis command with respect to data mutation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandType {
    /// Command only reads data.
    Read,
    /// Command writes or mutates data.
    Write,
    /// Administrative / meta command.
    Admin,
}

impl CommandType {
    /// Returns `true` if the command is read-only.
    pub fn is_read_only(self) -> bool {
        matches!(self, Self::Read | Self::Admin)
    }
}

/// Classifies Redis commands into read, write, or admin categories.
pub struct ReadWriteSplitter;

impl ReadWriteSplitter {
    /// Classify a command name as Read, Write, or Admin.
    pub fn classify_command(cmd: &str) -> CommandType {
        match cmd.to_uppercase().as_str() {
            // Read commands
            "GET" | "MGET" | "EXISTS" | "TYPE" | "TTL" | "PTTL" | "KEYS" | "SCAN" | "SCARD"
            | "SMEMBERS" | "SRANDMEMBER" | "SISMEMBER" | "LLEN" | "LRANGE" | "LINDEX"
            | "LPOS" | "HGET" | "HMGET" | "HGETALL" | "HLEN" | "HKEYS" | "HVALS" | "HEXISTS"
            | "ZRANGE" | "ZSCORE" | "ZCARD" | "ZRANK" | "ZREVRANK" | "ZCOUNT" | "ZLEXCOUNT"
            | "ZRANGEBYSCORE" | "ZRANGEBYLEX" | "ZREVRANGE" | "ZREVRANGEBYSCORE"
            | "ZREVRANGEBYLEX" | "ZMSCORE" | "STRLEN" | "GETRANGE" | "SUBSTR" | "RANDOMKEY"
            | "OBJECT" | "DUMP" | "XLEN" | "XRANGE" | "XREVRANGE" | "XINFO" | "XPENDING"
            | "BITCOUNT" | "BITPOS" | "GETBIT" | "PFCOUNT" | "GEODIST" | "GEOHASH"
            | "GEOPOS" | "GEORADIUS_RO" | "GEORADIUSBYMEMBER_RO" | "GEOSEARCH" | "SORT_RO"
            | "TOUCH" | "HSCAN" | "SSCAN" | "ZSCAN" | "OBJECT ENCODING"
            | "OBJECT REFCOUNT" | "OBJECT IDLETIME" | "OBJECT FREQ" | "OBJECT HELP"
            | "SINTERCARD" | "HRANDFIELD" => CommandType::Read,

            // Admin commands
            "INFO" | "DBSIZE" | "PING" | "ECHO" | "TIME" | "CONFIG" | "COMMAND" | "CLIENT"
            | "SLOWLOG" | "LATENCY" | "MEMORY" | "DEBUG" | "ACL" | "CLUSTER" | "WAIT"
            | "HELLO" | "RESET" | "QUIT" | "AUTH" | "SELECT" | "SWAPDB" => CommandType::Admin,

            // Everything else is a write
            _ => CommandType::Write,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn default_config() -> PoolConfig {
        PoolConfig::default()
    }

    #[test]
    fn test_pool_acquire_and_return() {
        let pool = ConnectionPool::new(default_config());
        let conn = pool.get_connection(false).expect("should acquire");
        assert_eq!(conn.state, ConnectionState::Active);
        pool.return_connection(conn);

        let stats = pool.stats();
        assert_eq!(stats.total_acquired, 1);
        assert_eq!(stats.total_released, 1);
        assert_eq!(stats.idle_connections, 1);
    }

    #[test]
    fn test_pool_exhaustion() {
        let mut cfg = default_config();
        cfg.max_connections = 1;
        let pool = ConnectionPool::new(cfg);

        let _conn = pool.get_connection(false).expect("should acquire");
        let result = pool.get_connection(false);
        assert!(result.is_err());
    }

    #[test]
    fn test_pool_drain() {
        let pool = ConnectionPool::new(default_config());
        let conn = pool.get_connection(false).expect("should acquire");
        pool.return_connection(conn);
        assert_eq!(pool.stats().idle_connections, 1);

        pool.drain();
        assert_eq!(pool.stats().idle_connections, 0);
    }

    #[test]
    fn test_pool_resize() {
        let pool = ConnectionPool::new(default_config());
        pool.resize(50);
        assert_eq!(pool.config.read().max_connections, 50);
    }

    #[test]
    fn test_pool_closed() {
        let pool = ConnectionPool::new(default_config());
        *pool.closed.lock() = true;
        assert!(pool.get_connection(false).is_err());
    }

    #[test]
    fn test_read_write_splitter_read() {
        assert_eq!(
            ReadWriteSplitter::classify_command("GET"),
            CommandType::Read
        );
        assert_eq!(
            ReadWriteSplitter::classify_command("mget"),
            CommandType::Read
        );
        assert_eq!(
            ReadWriteSplitter::classify_command("SCAN"),
            CommandType::Read
        );
        assert!(ReadWriteSplitter::classify_command("HGETALL")
            .is_read_only());
    }

    #[test]
    fn test_read_write_splitter_write() {
        assert_eq!(
            ReadWriteSplitter::classify_command("SET"),
            CommandType::Write
        );
        assert_eq!(
            ReadWriteSplitter::classify_command("DEL"),
            CommandType::Write
        );
        assert_eq!(
            ReadWriteSplitter::classify_command("LPUSH"),
            CommandType::Write
        );
    }

    #[test]
    fn test_read_write_splitter_admin() {
        assert_eq!(
            ReadWriteSplitter::classify_command("PING"),
            CommandType::Admin
        );
        assert_eq!(
            ReadWriteSplitter::classify_command("INFO"),
            CommandType::Admin
        );
        assert!(ReadWriteSplitter::classify_command("DBSIZE")
            .is_read_only());
    }

    #[test]
    fn test_read_only_routing() {
        let mut cfg = default_config();
        cfg.enable_read_write_split = true;
        cfg.replica_addrs = vec!["127.0.0.1:6380".parse().expect("valid addr")];
        let pool = ConnectionPool::new(cfg);

        let conn = pool.get_connection(true).expect("should acquire read-only");
        assert!(conn.is_read_only);
        assert_eq!(
            conn.addr,
            "127.0.0.1:6380".parse::<SocketAddr>().expect("valid addr")
        );
    }
}
