//! Prometheus metrics server
//!
//! This module implements an HTTP endpoint for Prometheus metrics exposition.

use std::net::SocketAddr;
use std::sync::Arc;

use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use tokio::net::TcpListener;
use tracing::{error, info};

use crate::error::Result;

/// HTTP body type for responses
type Body = http_body_util::Full<bytes::Bytes>;

/// Prometheus metrics server
pub struct MetricsServer {
    /// The Prometheus handle for rendering metrics
    handle: PrometheusHandle,

    /// Server address
    addr: SocketAddr,
}

impl MetricsServer {
    /// Create a new metrics server
    pub fn new(addr: SocketAddr) -> Result<Self> {
        let handle = PrometheusBuilder::new()
            .install_recorder()
            .map_err(|e| crate::error::FerriteError::Internal(e.to_string()))?;

        Ok(Self { handle, addr })
    }

    /// Run the metrics server
    pub async fn run(self) -> Result<()> {
        let listener = TcpListener::bind(self.addr).await?;
        info!("Metrics server listening on {}", self.addr);

        let handle = Arc::new(self.handle);

        loop {
            let (stream, _) = listener.accept().await?;
            let io = TokioIo::new(stream);
            let handle = handle.clone();

            tokio::spawn(async move {
                let service = service_fn(move |req| {
                    let handle = handle.clone();
                    async move { handle_request(req, handle).await }
                });

                if let Err(e) = http1::Builder::new().serve_connection(io, service).await {
                    error!("Error serving metrics connection: {}", e);
                }
            });
        }
    }

    /// Get the Prometheus handle for testing
    pub fn handle(&self) -> &PrometheusHandle {
        &self.handle
    }
}

/// Handle an incoming HTTP request
async fn handle_request(
    req: Request<Incoming>,
    handle: Arc<PrometheusHandle>,
) -> std::result::Result<Response<Body>, hyper::Error> {
    match (req.method(), req.uri().path()) {
        (&Method::GET, "/metrics") => {
            let metrics = handle.render();
            Ok(Response::builder()
                .status(StatusCode::OK)
                .header("Content-Type", "text/plain; charset=utf-8")
                .body(Body::new(bytes::Bytes::from(metrics)))
                .expect("valid response builder"))
        }
        (&Method::GET, "/health") => Ok(Response::builder()
            .status(StatusCode::OK)
            .body(Body::new(bytes::Bytes::from("OK")))
            .expect("valid response builder")),
        _ => Ok(Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::new(bytes::Bytes::from("Not Found")))
            .expect("valid response builder")),
    }
}
