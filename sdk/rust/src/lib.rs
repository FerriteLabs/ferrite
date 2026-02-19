//! # Ferrite Client SDK for Rust
//!
//! Official Rust client library for [Ferrite](https://github.com/ferritelabs/ferrite),
//! a high-performance tiered-storage key-value store designed as a drop-in Redis
//! replacement.
//!
//! This crate wraps the battle-tested [`redis`](https://crates.io/crates/redis)
//! driver and adds first-class support for every Ferrite extension:
//!
//! * **Vector search** -- HNSW / IVF / flat similarity search
//! * **Semantic caching** -- cache by meaning, not exact keys
//! * **Time series** -- optimized time-stamped data ingestion and queries
//! * **Document store** -- JSON documents with secondary indexes
//! * **Graph** -- property graphs with Cypher-like queries
//! * **FerriteQL** -- SQL-like query language with joins and aggregations
//! * **Time-travel** -- point-in-time reads and full key history
//!
//! All standard Redis commands are available through the [`Deref`] implementation
//! to [`redis::aio::MultiplexedConnection`].
//!
//! # Quick Start
//!
//! ```rust,no_run
//! use ferrite_client::{FerriteClient, FerriteClientBuilder, Result};
//! use ferrite_client::commands::{FerriteCommands, VectorSearchOptions};
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     let mut client = FerriteClientBuilder::new()
//!         .host("localhost")
//!         .port(6379)
//!         .build()
//!         .await?;
//!
//!     // Standard Redis commands via the redis crate
//!     redis::AsyncCommands::set(&mut *client, "greeting", "Hello, Ferrite!").await?;
//!     let val: String = redis::AsyncCommands::get(&mut *client, "greeting").await?;
//!     println!("{}", val);
//!
//!     // Ferrite-specific: vector search
//!     client.vector_create_index("my_idx", 384, "cosine", "hnsw").await?;
//!     let embedding = vec![0.1_f32; 384];
//!     client.vector_set("my_idx", "doc:1", &embedding, None).await?;
//!     let results = client.vector_search(
//!         "my_idx",
//!         &embedding,
//!         &VectorSearchOptions::new().top_k(5),
//!     ).await?;
//!     println!("found {} results", results.len());
//!
//!     Ok(())
//! }
//! ```

pub mod commands;
pub mod error;

pub use error::{FerriteError, Result};

use std::ops::{Deref, DerefMut};

use commands::FerriteCommands;

// ---------------------------------------------------------------------------
// Builder
// ---------------------------------------------------------------------------

/// Builder for constructing a [`FerriteClient`] with custom connection
/// parameters.
///
/// # Examples
///
/// ```rust,no_run
/// # use ferrite_client::{FerriteClientBuilder, Result};
/// # async fn example() -> Result<()> {
/// let client = FerriteClientBuilder::new()
///     .host("db.example.com")
///     .port(6380)
///     .password("s3cret")
///     .tls(true)
///     .database(2)
///     .build()
///     .await?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct FerriteClientBuilder {
    host: String,
    port: u16,
    password: Option<String>,
    username: Option<String>,
    database: u16,
    tls: bool,
}

impl Default for FerriteClientBuilder {
    fn default() -> Self {
        Self {
            host: "localhost".to_string(),
            port: 6379,
            password: None,
            username: None,
            database: 0,
            tls: false,
        }
    }
}

impl FerriteClientBuilder {
    /// Create a new builder with default settings (localhost:6379, no auth, no TLS).
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the hostname or IP address of the Ferrite server.
    pub fn host(mut self, host: &str) -> Self {
        self.host = host.to_string();
        self
    }

    /// Set the port number. Defaults to `6379`.
    pub fn port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    /// Set the password for authentication.
    pub fn password(mut self, password: &str) -> Self {
        self.password = Some(password.to_string());
        self
    }

    /// Set the ACL username. Defaults to `"default"`.
    pub fn username(mut self, username: &str) -> Self {
        self.username = Some(username.to_string());
        self
    }

    /// Select a specific database index. Defaults to `0`.
    pub fn database(mut self, db: u16) -> Self {
        self.database = db;
        self
    }

    /// Enable or disable TLS. Defaults to `false`.
    pub fn tls(mut self, enabled: bool) -> Self {
        self.tls = enabled;
        self
    }

    /// Build and connect a [`FerriteClient`].
    pub async fn build(self) -> Result<FerriteClient> {
        let scheme = if self.tls { "rediss" } else { "redis" };
        let mut url = format!("{}://", scheme);

        if let Some(ref user) = self.username {
            url.push_str(user);
            url.push(':');
        }
        if let Some(ref pass) = self.password {
            if self.username.is_none() {
                // redis URLs encode password as `:password@`
                url.push(':');
            }
            url.push_str(pass);
            url.push('@');
        }
        url.push_str(&format!("{}:{}/{}", self.host, self.port, self.database));

        FerriteClient::connect(&url).await
    }
}

// ---------------------------------------------------------------------------
// Client
// ---------------------------------------------------------------------------

/// An async Ferrite client that wraps a multiplexed Redis connection.
///
/// Use [`FerriteClientBuilder`] for fine-grained configuration or
/// [`FerriteClient::connect`] to connect directly from a URL.
///
/// The client implements [`Deref`] to [`redis::aio::MultiplexedConnection`],
/// so any method from the [`redis`] crate can be called directly.
///
/// Ferrite-specific commands (vector search, semantic caching, etc.) are
/// available through the [`FerriteCommands`] trait which is automatically
/// implemented for this type.
pub struct FerriteClient {
    connection: redis::aio::MultiplexedConnection,
}

impl FerriteClient {
    /// Connect to a Ferrite (or Redis) server using a URL.
    ///
    /// The URL format follows the Redis convention:
    /// `redis://[username:password@]host[:port][/database]`
    ///
    /// Use `rediss://` for TLS connections.
    ///
    /// # Errors
    ///
    /// Returns [`FerriteError::Connection`] if the server is unreachable or
    /// authentication fails.
    pub async fn connect(url: &str) -> Result<Self> {
        let client = redis::Client::open(url).map_err(|e| {
            FerriteError::Connection(format!("failed to parse URL: {}", e))
        })?;

        let connection = client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| {
                FerriteError::Connection(format!("failed to connect: {}", e))
            })?;

        Ok(Self { connection })
    }

    /// Create a client from an existing [`redis::Client`].
    pub async fn from_redis_client(client: redis::Client) -> Result<Self> {
        let connection = client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| {
                FerriteError::Connection(format!("failed to connect: {}", e))
            })?;
        Ok(Self { connection })
    }

    /// Send an arbitrary raw command and return the raw [`redis::Value`].
    ///
    /// This is useful for Ferrite commands that are not yet covered by the
    /// typed helpers in [`FerriteCommands`].
    pub async fn execute_raw(
        &mut self,
        command: &str,
        args: &[&str],
    ) -> Result<redis::Value> {
        let mut cmd = redis::cmd(command);
        for arg in args {
            cmd.arg(*arg);
        }
        let val: redis::Value = cmd.query_async(&mut self.connection).await?;
        Ok(val)
    }

    /// Check that the connection to the Ferrite server is alive.
    pub async fn ping(&mut self) -> Result<String> {
        let pong: String = redis::cmd("PING")
            .query_async(&mut self.connection)
            .await?;
        Ok(pong)
    }
}

// Deref to the underlying connection so callers can use all redis::AsyncCommands.
impl Deref for FerriteClient {
    type Target = redis::aio::MultiplexedConnection;

    fn deref(&self) -> &Self::Target {
        &self.connection
    }
}

impl DerefMut for FerriteClient {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.connection
    }
}

// Implement FerriteCommands for the client.
impl FerriteCommands for FerriteClient {
    fn connection_mut(&mut self) -> &mut redis::aio::MultiplexedConnection {
        &mut self.connection
    }
}
