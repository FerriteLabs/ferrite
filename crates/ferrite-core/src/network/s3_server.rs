//! S3-compatible HTTP server
//!
//! Binds an HTTP listener and routes requests through [`super::s3_compat::S3Service`].
//!
//! # Usage
//!
//! ```rust,ignore
//! use ferrite_core::network::s3_server::S3Server;
//! use ferrite_core::network::s3_compat::S3CompatConfig;
//!
//! let server = S3Server::new(S3CompatConfig::default());
//! server.start().await?;
//! ```

use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;

use bytes::Bytes;
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use tokio::net::TcpListener;
use tracing::{error, info};

use super::s3_compat::{parse_request, S3CompatConfig, S3Service};

/// S3-compatible HTTP server backed by callback functions.
///
/// The server is generic over store operations via closures, keeping it
/// decoupled from the concrete `Store` type.
pub struct S3Server {
    config: S3CompatConfig,
    service: Arc<S3Service>,
}

impl S3Server {
    /// Create a new S3 server with the given configuration.
    pub fn new(config: S3CompatConfig) -> Self {
        let service = Arc::new(S3Service::new(config.clone()));
        Self { config, service }
    }

    /// Start the HTTP listener. Returns the bound address (useful when port is 0).
    ///
    /// The `store_ops` closure factory produces per-request store operation closures.
    /// This design allows the caller to provide any store implementation.
    pub async fn start<F>(
        &self,
        store_ops: F,
    ) -> std::io::Result<SocketAddr>
    where
        F: Fn() -> StoreOps + Send + Sync + 'static,
    {
        let addr: SocketAddr = format!("{}:{}", self.config.bind, self.config.port)
            .parse()
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;

        let listener = TcpListener::bind(addr).await?;
        let bound_addr = listener.local_addr()?;
        info!("S3-compatible API listening on http://{}", bound_addr);

        let service = Arc::clone(&self.service);
        let store_ops = Arc::new(store_ops);

        tokio::spawn(async move {
            loop {
                let (stream, peer) = match listener.accept().await {
                    Ok(conn) => conn,
                    Err(e) => {
                        error!("S3 accept error: {}", e);
                        continue;
                    }
                };

                let svc = Arc::clone(&service);
                let ops_factory = Arc::clone(&store_ops);

                tokio::spawn(async move {
                    let svc = svc;
                    let handler = service_fn(move |req: Request<Incoming>| {
                        let svc = Arc::clone(&svc);
                        let ops = ops_factory();
                        async move {
                            Ok::<_, Infallible>(handle_s3_request(req, &svc, ops).await)
                        }
                    });

                    if let Err(e) = http1::Builder::new()
                        .serve_connection(hyper_util::rt::TokioIo::new(stream), handler)
                        .await
                    {
                        if !e.is_incomplete_message() {
                            error!("S3 connection error from {}: {}", peer, e);
                        }
                    }
                });
            }
        });

        Ok(bound_addr)
    }

    /// Returns the configured bind address.
    pub fn address(&self) -> String {
        format!("{}:{}", self.config.bind, self.config.port)
    }
}

/// Store operation callbacks for the S3 server.
pub struct StoreOps {
    /// Get a value by database index and key.
    pub get: Box<dyn Fn(u8, &str) -> Option<Vec<u8>> + Send + Sync>,
    /// Set a value by database index, key, and body.
    pub set: Box<dyn Fn(u8, &str, &[u8]) -> bool + Send + Sync>,
    /// Delete a key by database index and key.
    pub del: Box<dyn Fn(u8, &str) -> bool + Send + Sync>,
    /// Check existence by database index and key.
    pub exists: Box<dyn Fn(u8, &str) -> bool + Send + Sync>,
}

async fn handle_s3_request(
    req: Request<Incoming>,
    service: &S3Service,
    ops: StoreOps,
) -> Response<Full<Bytes>> {
    let method = req.method().as_str().to_uppercase();
    let path = req.uri().path().to_string();
    let query = req.uri().query().map(|q| q.to_string());

    let s3_req = match parse_request(&method, &path, query.as_deref()) {
        Ok(r) => r,
        Err(msg) => {
            return Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .header("Content-Type", "application/xml")
                .body(Full::new(Bytes::from(
                    super::s3_compat::error_xml("InvalidRequest", &msg, &path, "0"),
                )))
                .unwrap_or_default();
        }
    };

    // Collect request body for PUT operations
    let body_bytes = match &s3_req {
        super::s3_compat::S3Request::PutObject { .. } => {
            use http_body_util::BodyExt;
            match req.collect().await {
                Ok(collected) => collected.to_bytes().to_vec(),
                Err(e) => {
                    return Response::builder()
                        .status(StatusCode::BAD_REQUEST)
                        .header("Content-Type", "application/xml")
                        .body(Full::new(Bytes::from(
                            super::s3_compat::error_xml(
                                "IncompleteBody",
                                &format!("Failed to read request body: {}", e),
                                &path,
                                "0",
                            ),
                        )))
                        .unwrap_or_default();
                }
            }
        }
        _ => Vec::new(),
    };

    let resp = service.handle_request(
        &s3_req,
        |db, key| (ops.get)(db, key),
        |db, key, _empty| (ops.set)(db, key, &body_bytes),
        |db, key| (ops.del)(db, key),
        |db, key| (ops.exists)(db, key),
    );

    let mut builder = Response::builder().status(StatusCode::from_u16(resp.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR));

    if !resp.content_type.is_empty() {
        builder = builder.header("Content-Type", resp.content_type);
    }
    for (k, v) in &resp.headers {
        builder = builder.header(k.as_str(), v.as_str());
    }

    builder
        .body(Full::new(Bytes::from(resp.body)))
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_s3_server_address() {
        let config = S3CompatConfig {
            bind: "127.0.0.1".to_string(),
            port: 9000,
            ..S3CompatConfig::default()
        };
        let server = S3Server::new(config);
        assert_eq!(server.address(), "127.0.0.1:9000");
    }

    #[tokio::test]
    async fn test_s3_server_start_and_request() {
        let config = S3CompatConfig {
            bind: "127.0.0.1".to_string(),
            port: 0, // OS-assigned port
            ..S3CompatConfig::default()
        };
        let server = S3Server::new(config);

        let addr = server
            .start(|| StoreOps {
                get: Box::new(|_db, key| {
                    if key == "testkey" {
                        Some(b"testvalue".to_vec())
                    } else {
                        None
                    }
                }),
                set: Box::new(|_, _, _| true),
                del: Box::new(|_, _| true),
                exists: Box::new(|_, key| key == "testkey"),
            })
            .await
            .expect("server should start");

        // Give the server a moment to bind
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Test GET existing key
        let client = hyper_util::client::legacy::Client::builder(hyper_util::rt::TokioExecutor::new())
            .build_http::<Full<Bytes>>();
        let uri: hyper::Uri = format!("http://{}/default/testkey", addr).parse().unwrap();
        let resp = client.get(uri).await.expect("request should succeed");
        assert_eq!(resp.status(), 200);

        // Test GET missing key
        let uri: hyper::Uri = format!("http://{}/default/missing", addr).parse().unwrap();
        let resp = client.get(uri).await.expect("request should succeed");
        assert_eq!(resp.status(), 404);

        // Test ListBuckets
        let uri: hyper::Uri = format!("http://{}/", addr).parse().unwrap();
        let resp = client.get(uri).await.expect("request should succeed");
        assert_eq!(resp.status(), 200);
    }
}
