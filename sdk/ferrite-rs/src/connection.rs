//! TCP and TLS connection management for Ferrite.
//!
//! This module provides the low-level connection abstraction over
//! TCP (and optionally TLS) streams, with buffered RESP protocol I/O.

use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::error::{Error, Result};
use crate::resp;
use crate::types::Value;

/// Default read/write buffer size (8 KB).
const DEFAULT_BUF_SIZE: usize = 8 * 1024;

/// Configuration for connecting to a Ferrite server.
#[derive(Debug, Clone)]
pub struct ConnectionConfig {
    /// Server host.
    pub host: String,
    /// Server port.
    pub port: u16,
    /// Optional password for AUTH.
    pub password: Option<String>,
    /// Database index to SELECT on connect.
    pub database: u8,
    /// Read/write buffer size in bytes.
    pub buffer_size: usize,
    /// Enable TLS (requires the `tls` feature).
    pub tls: bool,
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".into(),
            port: 6379,
            password: None,
            database: 0,
            buffer_size: DEFAULT_BUF_SIZE,
            tls: false,
        }
    }
}

impl ConnectionConfig {
    /// Create a config from a `host:port` address string.
    pub fn from_addr(host: impl Into<String>, port: u16) -> Self {
        Self {
            host: host.into(),
            port,
            ..Default::default()
        }
    }
}

/// A wrapper around a TCP (or TLS) stream with buffered RESP I/O.
pub struct Connection {
    stream: Stream,
    read_buf: BytesMut,
    write_buf: BytesMut,
}

/// Abstraction over plain TCP and TLS streams.
enum Stream {
    Tcp(TcpStream),
    #[cfg(feature = "tls")]
    Tls(tokio_rustls::client::TlsStream<TcpStream>),
}

impl Connection {
    /// Open a new connection using the given configuration.
    pub async fn connect(config: &ConnectionConfig) -> Result<Self> {
        let addr = format!("{}:{}", config.host, config.port);
        let tcp = TcpStream::connect(&addr).await?;
        tcp.set_nodelay(true).map_err(Error::Io)?;

        let stream = if config.tls {
            #[cfg(feature = "tls")]
            {
                use std::sync::Arc;
                use tokio_rustls::TlsConnector;

                let mut root_store = rustls::RootCertStore::empty();
                root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());

                let tls_config = rustls::ClientConfig::builder()
                    .with_root_certificates(root_store)
                    .with_no_client_auth();

                let connector = TlsConnector::from(Arc::new(tls_config));
                let domain = rustls::pki_types::ServerName::try_from(config.host.clone())
                    .map_err(|e| Error::Protocol(format!("invalid TLS server name: {}", e)))?;

                let tls_stream = connector.connect(domain, tcp).await.map_err(Error::Io)?;
                Stream::Tls(tls_stream)
            }
            #[cfg(not(feature = "tls"))]
            {
                return Err(Error::Protocol(
                    "TLS support requires the `tls` feature".into(),
                ));
            }
        } else {
            Stream::Tcp(tcp)
        };

        let mut conn = Self {
            stream,
            read_buf: BytesMut::with_capacity(config.buffer_size),
            write_buf: BytesMut::with_capacity(config.buffer_size),
        };

        // Authenticate if password is set.
        if let Some(ref password) = config.password {
            let reply = conn
                .execute(&[Bytes::from("AUTH"), Bytes::copy_from_slice(password.as_bytes())])
                .await?;
            match reply {
                Value::Status(ref s) if s == "OK" => {}
                _ => return Err(Error::Auth(format!("unexpected AUTH response: {}", reply))),
            }
        }

        // Select database if non-zero.
        if config.database != 0 {
            let reply = conn
                .execute(&[
                    Bytes::from("SELECT"),
                    Bytes::from(config.database.to_string()),
                ])
                .await?;
            match reply {
                Value::Status(ref s) if s == "OK" => {}
                _ => {
                    return Err(Error::Protocol(format!(
                        "unexpected SELECT response: {}",
                        reply
                    )))
                }
            }
        }

        Ok(conn)
    }

    /// Send a command and read the response.
    pub async fn execute(&mut self, args: &[Bytes]) -> Result<Value> {
        self.send_command(args).await?;
        self.read_response().await
    }

    /// Send a raw RESP command without reading a response.
    pub async fn send_command(&mut self, args: &[Bytes]) -> Result<()> {
        self.write_buf.clear();
        resp::encode_command(args, &mut self.write_buf);

        match &mut self.stream {
            Stream::Tcp(tcp) => {
                tcp.write_all(&self.write_buf).await?;
                tcp.flush().await?;
            }
            #[cfg(feature = "tls")]
            Stream::Tls(tls) => {
                use tokio::io::AsyncWriteExt;
                tls.write_all(&self.write_buf).await?;
                tls.flush().await?;
            }
        }

        self.write_buf.clear();
        Ok(())
    }

    /// Read a single RESP response from the connection.
    pub async fn read_response(&mut self) -> Result<Value> {
        loop {
            if let Some(value) = resp::decode_value(&mut self.read_buf)? {
                return Ok(value);
            }

            let n = match &mut self.stream {
                Stream::Tcp(tcp) => tcp.read_buf(&mut self.read_buf).await?,
                #[cfg(feature = "tls")]
                Stream::Tls(tls) => {
                    use tokio::io::AsyncReadExt;
                    tls.read_buf(&mut self.read_buf).await?
                }
            };

            if n == 0 {
                return Err(Error::ConnectionClosed);
            }
        }
    }
}
