//! Prometheus metrics server with GraphQL and anomaly endpoints
//!
//! This module implements the HTTP API endpoint serving:
//! - `GET  /metrics`   — Prometheus metrics exposition
//! - `GET  /health`    — Health check
//! - `POST /graphql`   — GraphQL query execution
//! - `GET  /graphql`   — GraphiQL interactive IDE
//! - `GET  /alerts`    — Active anomaly alerts (JSON)

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
use crate::storage::Store;

/// HTTP body type for responses
type Body = http_body_util::Full<bytes::Bytes>;

/// Shared context passed to each HTTP request handler.
#[derive(Clone)]
struct HttpContext {
    prometheus: Arc<PrometheusHandle>,
    store: Option<Arc<Store>>,
    anomaly: Option<Arc<super::anomaly::AnomalyDetector>>,
}

/// Prometheus metrics server
pub struct MetricsServer {
    /// The Prometheus handle for rendering metrics
    handle: PrometheusHandle,

    /// Server address
    addr: SocketAddr,

    /// Optional store reference for GraphQL
    store: Option<Arc<Store>>,

    /// Optional anomaly detector
    anomaly: Option<Arc<super::anomaly::AnomalyDetector>>,
}

impl MetricsServer {
    /// Create a new metrics server
    pub fn new(addr: SocketAddr) -> Result<Self> {
        let handle = PrometheusBuilder::new()
            .install_recorder()
            .map_err(|e| crate::error::FerriteError::Internal(e.to_string()))?;

        Ok(Self {
            handle,
            addr,
            store: None,
            anomaly: None,
        })
    }

    /// Attach a store to enable the GraphQL endpoint.
    pub fn with_store(mut self, store: Arc<Store>) -> Self {
        self.store = Some(store);
        self
    }

    /// Attach an anomaly detector to enable the /alerts endpoint.
    pub fn with_anomaly(mut self, detector: Arc<super::anomaly::AnomalyDetector>) -> Self {
        self.anomaly = Some(detector);
        self
    }

    /// Run the metrics server
    pub async fn run(self) -> Result<()> {
        let listener = TcpListener::bind(self.addr).await?;
        info!("Metrics server listening on {}", self.addr);

        if self.store.is_some() {
            info!("GraphQL endpoint enabled at POST /graphql");
        }

        let ctx = HttpContext {
            prometheus: Arc::new(self.handle),
            store: self.store,
            anomaly: self.anomaly,
        };

        loop {
            let (stream, _) = listener.accept().await?;
            let io = TokioIo::new(stream);
            let ctx = ctx.clone();

            tokio::spawn(async move {
                let service = service_fn(move |req| {
                    let ctx = ctx.clone();
                    async move { handle_request(req, ctx).await }
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
    ctx: HttpContext,
) -> std::result::Result<Response<Body>, hyper::Error> {
    match (req.method(), req.uri().path()) {
        // ── Prometheus metrics ──────────────────────────────────────
        (&Method::GET, "/metrics") => {
            let metrics = ctx.prometheus.render();
            Ok(json_or_text_response(
                StatusCode::OK,
                "text/plain; charset=utf-8",
                metrics,
            ))
        }

        // ── Health check ────────────────────────────────────────────
        (&Method::GET, "/health") => Ok(json_or_text_response(
            StatusCode::OK,
            "application/json",
            r#"{"status":"ok"}"#.to_string(),
        )),

        // ── GraphQL query (POST) ────────────────────────────────────
        (&Method::POST, "/graphql") => {
            let Some(store) = &ctx.store else {
                return Ok(json_or_text_response(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "application/json",
                    r#"{"error":"GraphQL endpoint not configured"}"#.to_string(),
                ));
            };

            // Read the request body
            use http_body_util::BodyExt;
            let body_bytes = match req.collect().await {
                Ok(collected) => collected.to_bytes(),
                Err(e) => {
                    let msg = format!(r#"{{"error":"Failed to read body: {}"}}"#, e);
                    return Ok(json_or_text_response(
                        StatusCode::BAD_REQUEST,
                        "application/json",
                        msg,
                    ));
                }
            };

            // Parse as GraphQL request
            let gql_request: crate::graphql::GraphQLRequest =
                match serde_json::from_slice(&body_bytes) {
                    Ok(r) => r,
                    Err(e) => {
                        let msg = format!(
                            r#"{{"error":"Invalid GraphQL JSON: {}"}}"#,
                            e.to_string().replace('"', "'")
                        );
                        return Ok(json_or_text_response(
                            StatusCode::BAD_REQUEST,
                            "application/json",
                            msg,
                        ));
                    }
                };

            // Execute
            let engine = crate::graphql::GraphQLEngine::new(store.clone());
            let response = engine.execute(gql_request);
            let json = serde_json::to_string(&response)
                .unwrap_or_else(|_| r#"{"error":"Failed to serialize response"}"#.to_string());

            Ok(json_or_text_response(
                StatusCode::OK,
                "application/json",
                json,
            ))
        }

        // ── GraphiQL IDE (GET) ──────────────────────────────────────
        (&Method::GET, "/graphql") => Ok(json_or_text_response(
            StatusCode::OK,
            "text/html; charset=utf-8",
            graphiql_html(),
        )),

        // ── Anomaly alerts ──────────────────────────────────────────
        (&Method::GET, "/alerts") => {
            let Some(detector) = &ctx.anomaly else {
                return Ok(json_or_text_response(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "application/json",
                    r#"{"error":"Anomaly detector not configured"}"#.to_string(),
                ));
            };

            let alerts = detector.get_alerts(None);
            let summary = detector.summary();
            let json = serde_json::to_string(&serde_json::json!({
                "summary": summary,
                "alerts": alerts,
            }))
            .unwrap_or_else(|_| r#"{"error":"Failed to serialize alerts"}"#.to_string());

            Ok(json_or_text_response(
                StatusCode::OK,
                "application/json",
                json,
            ))
        }

        // ── 404 ─────────────────────────────────────────────────────
        _ => Ok(json_or_text_response(
            StatusCode::NOT_FOUND,
            "application/json",
            r#"{"error":"Not Found"}"#.to_string(),
        )),
    }
}

/// Build an HTTP response with the given status, content type, and body.
fn json_or_text_response(status: StatusCode, content_type: &str, body: String) -> Response<Body> {
    Response::builder()
        .status(status)
        .header("Content-Type", content_type)
        .header("Access-Control-Allow-Origin", "*")
        .header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        .header("Access-Control-Allow-Headers", "Content-Type")
        .body(Body::new(bytes::Bytes::from(body)))
        .expect("valid response builder")
}

/// Minimal GraphiQL HTML page for interactive GraphQL exploration.
fn graphiql_html() -> String {
    r#"<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <title>Ferrite GraphiQL</title>
  <style>body { margin: 0; height: 100vh; }</style>
  <link rel="stylesheet" href="https://unpkg.com/graphiql@3/graphiql.min.css" />
</head>
<body>
  <div id="graphiql" style="height: 100vh;"></div>
  <script crossorigin src="https://unpkg.com/react@18/umd/react.production.min.js"></script>
  <script crossorigin src="https://unpkg.com/react-dom@18/umd/react-dom.production.min.js"></script>
  <script crossorigin src="https://unpkg.com/graphiql@3/graphiql.min.js"></script>
  <script>
    const fetcher = GraphiQL.createFetcher({ url: window.location.href });
    ReactDOM.createRoot(document.getElementById('graphiql'))
      .render(React.createElement(GraphiQL, { fetcher }));
  </script>
</body>
</html>"#
        .to_string()
}
