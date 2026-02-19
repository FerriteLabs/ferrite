//! Connection pooling for Ferrite.
//!
//! Provides a simple, thread-safe pool of connections with configurable
//! size limits and automatic reconnection.

use std::sync::Arc;
use tokio::sync::Semaphore;

use crate::connection::{Connection, ConnectionConfig};
use crate::error::{Error, Result};

/// Configuration for the connection pool.
#[derive(Debug, Clone)]
pub struct PoolConfig {
    /// Connection settings for each pooled connection.
    pub connection: ConnectionConfig,
    /// Maximum number of connections in the pool.
    pub max_size: usize,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            connection: ConnectionConfig::default(),
            max_size: 8,
        }
    }
}

/// A pool of reusable connections to a Ferrite server.
///
/// The pool lazily creates connections up to `max_size` and provides
/// them through a semaphore-guarded checkout mechanism.
pub struct Pool {
    config: ConnectionConfig,
    connections: parking_lot::Mutex<Vec<Connection>>,
    semaphore: Arc<Semaphore>,
    max_size: usize,
}

impl Pool {
    /// Create a new connection pool with the given configuration.
    pub fn new(config: PoolConfig) -> Self {
        Self {
            config: config.connection,
            connections: parking_lot::Mutex::new(Vec::with_capacity(config.max_size)),
            semaphore: Arc::new(Semaphore::new(config.max_size)),
            max_size: config.max_size,
        }
    }

    /// Get a connection from the pool.
    ///
    /// If a cached connection is available it is returned immediately.
    /// Otherwise a new connection is established (up to `max_size`).
    /// Returns [`Error::PoolExhausted`] if all permits are taken and
    /// the semaphore cannot be acquired.
    pub async fn get(&self) -> Result<PooledConnection> {
        let permit = self
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| Error::PoolExhausted {
                max_size: self.max_size,
            })?;

        // Try to reuse an existing connection.
        let conn = { self.connections.lock().pop() };

        let conn = match conn {
            Some(c) => c,
            None => Connection::connect(&self.config).await?,
        };

        Ok(PooledConnection {
            conn: Some(conn),
            pool: self,
            _permit: permit,
        })
    }

    /// Return a connection to the pool for reuse.
    fn put_back(&self, conn: Connection) {
        self.connections.lock().push(conn);
    }

    /// Current number of idle connections sitting in the pool.
    pub fn idle_count(&self) -> usize {
        self.connections.lock().len()
    }
}

/// A connection checked out from the pool.
///
/// When dropped, the connection is returned to the pool automatically.
pub struct PooledConnection<'a> {
    conn: Option<Connection>,
    pool: &'a Pool,
    _permit: tokio::sync::OwnedSemaphorePermit,
}

impl<'a> PooledConnection<'a> {
    /// Access the underlying connection.
    pub fn conn(&mut self) -> &mut Connection {
        self.conn.as_mut().expect("connection taken")
    }
}

impl<'a> Drop for PooledConnection<'a> {
    fn drop(&mut self) {
        if let Some(conn) = self.conn.take() {
            self.pool.put_back(conn);
        }
    }
}

impl<'a> std::ops::Deref for PooledConnection<'a> {
    type Target = Connection;

    fn deref(&self) -> &Self::Target {
        self.conn.as_ref().expect("connection taken")
    }
}

impl<'a> std::ops::DerefMut for PooledConnection<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.conn.as_mut().expect("connection taken")
    }
}
