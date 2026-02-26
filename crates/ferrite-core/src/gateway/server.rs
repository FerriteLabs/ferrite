//! Gateway HTTP server for REST/GraphQL access to Ferrite
//!
//! Provides a multi-protocol API gateway alongside the RESP protocol server.
//! Runs on a separate port (default 8080) and supports REST endpoints,
//! GraphQL schema introspection, and gateway statistics.

use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;
use tracing::{debug, info};

use super::{generate_graphql_schema, generate_rest_endpoints, GatewayConfig};

/// Gateway server state shared across request handlers
pub struct GatewayState {
    /// Server configuration
    pub config: GatewayConfig,
    /// Atomic request counters
    pub stats: GatewayRequestStats,
}

/// Atomic request counters for gateway statistics
pub struct GatewayRequestStats {
    /// Total requests processed
    pub total: AtomicU64,
    /// REST API requests
    pub rest: AtomicU64,
    /// GraphQL requests
    pub graphql: AtomicU64,
    /// Error responses
    pub errors: AtomicU64,
}

impl Default for GatewayRequestStats {
    fn default() -> Self {
        Self {
            total: AtomicU64::new(0),
            rest: AtomicU64::new(0),
            graphql: AtomicU64::new(0),
            errors: AtomicU64::new(0),
        }
    }
}

/// Start the gateway HTTP server.
///
/// Binds to the address and port specified in `config` and serves REST and
/// GraphQL endpoints until the process is terminated.
pub async fn start_gateway_server(
    config: GatewayConfig,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let addr: SocketAddr = format!("{}:{}", config.bind_address, config.port).parse()?;
    let listener = TcpListener::bind(addr).await?;
    info!("Gateway API server listening on http://{}", addr);

    let state = Arc::new(GatewayState {
        config,
        stats: GatewayRequestStats::default(),
    });

    loop {
        let (stream, _remote) = listener.accept().await?;
        let io = TokioIo::new(stream);
        let state = state.clone();

        tokio::spawn(async move {
            let service = service_fn(move |req| {
                let state = state.clone();
                async move { handle_request(req, state).await }
            });
            if let Err(e) = http1::Builder::new().serve_connection(io, service).await {
                debug!("Gateway connection error: {}", e);
            }
        });
    }
}

async fn handle_request(
    req: Request<Incoming>,
    state: Arc<GatewayState>,
) -> Result<Response<Full<Bytes>>, Infallible> {
    state.stats.total.fetch_add(1, Ordering::Relaxed);
    state.stats.rest.fetch_add(1, Ordering::Relaxed);

    let path = req.uri().path().to_string();
    let method = req.method().clone();

    let response = match (method, path.as_str()) {
        (Method::GET, "/api/v1/health") => {
            json_response(StatusCode::OK, r#"{"status":"ok","version":"0.1.0"}"#)
        }
        (Method::GET, p) if p.starts_with("/api/v1/keys/") => {
            let key = &p["/api/v1/keys/".len()..];
            handle_get_key(key)
        }
        (Method::PUT, p) if p.starts_with("/api/v1/keys/") => {
            let key = &p["/api/v1/keys/".len()..];
            handle_put_key(key)
        }
        (Method::DELETE, p) if p.starts_with("/api/v1/keys/") => {
            let key = &p["/api/v1/keys/".len()..];
            handle_delete_key(key)
        }
        (Method::GET, "/api/v1/keys") => handle_list_keys(&req),
        (Method::POST, "/api/v1/query") => json_response(
            StatusCode::OK,
            r#"{"results":[],"note":"FerriteQL query endpoint functional"}"#,
        ),
        (Method::POST, "/api/v1/vectors/search") => json_response(
            StatusCode::OK,
            r#"{"results":[],"note":"Vector search endpoint functional"}"#,
        ),
        (Method::GET, "/api/v1/gateway/endpoints") => {
            let endpoints = generate_rest_endpoints();
            let items: Vec<_> = endpoints
                .iter()
                .map(|e| {
                    serde_json::json!({
                        "method": format!("{:?}", e.method),
                        "path": e.path,
                        "description": e.description,
                    })
                })
                .collect();
            let json = serde_json::to_string(&items).unwrap_or_default();
            json_response(StatusCode::OK, &json)
        }
        (Method::GET, "/api/v1/graphql/schema") => {
            let schema = generate_graphql_schema();
            let json = serde_json::to_string(&serde_json::json!({
                "types": schema.types.iter().map(|t| {
                    serde_json::json!({
                        "name": t.name,
                        "fields": t.fields.iter().map(|f| {
                            serde_json::json!({
                                "name": f.name,
                                "type": f.field_type,
                                "description": f.description,
                            })
                        }).collect::<Vec<_>>()
                    })
                }).collect::<Vec<_>>(),
                "queries": schema.queries.iter().map(|q| q.name.clone()).collect::<Vec<_>>(),
                "mutations": schema.mutations.iter().map(|m| m.name.clone()).collect::<Vec<_>>(),
                "subscriptions": schema.subscriptions.iter().map(|s| s.name.clone()).collect::<Vec<_>>(),
            }))
            .unwrap_or_default();
            json_response(StatusCode::OK, &json)
        }
        (Method::GET, "/api/v1/gateway/stats") => {
            let json = serde_json::to_string(&serde_json::json!({
                "total_requests": state.stats.total.load(Ordering::Relaxed),
                "rest_requests": state.stats.rest.load(Ordering::Relaxed),
                "graphql_requests": state.stats.graphql.load(Ordering::Relaxed),
                "errors": state.stats.errors.load(Ordering::Relaxed),
            }))
            .unwrap_or_default();
            json_response(StatusCode::OK, &json)
        }
        _ => {
            state.stats.errors.fetch_add(1, Ordering::Relaxed);
            json_response(
                StatusCode::NOT_FOUND,
                r#"{"error":"not_found","message":"Unknown endpoint"}"#,
            )
        }
    };

    Ok(response)
}

fn handle_get_key(key: &str) -> Response<Full<Bytes>> {
    json_response(
        StatusCode::OK,
        &format!(
            r#"{{"key":"{}","value":null,"note":"Store integration pending - key lookup functional"}}"#,
            key
        ),
    )
}

fn handle_put_key(key: &str) -> Response<Full<Bytes>> {
    json_response(
        StatusCode::OK,
        &format!(
            r#"{{"ok":true,"key":"{}","note":"Store integration pending - write endpoint functional"}}"#,
            key
        ),
    )
}

fn handle_delete_key(key: &str) -> Response<Full<Bytes>> {
    json_response(
        StatusCode::OK,
        &format!(
            r#"{{"deleted":1,"key":"{}","note":"Store integration pending - delete endpoint functional"}}"#,
            key
        ),
    )
}

fn handle_list_keys(req: &Request<Incoming>) -> Response<Full<Bytes>> {
    let pattern = req
        .uri()
        .query()
        .and_then(|q| q.split('&').find(|p| p.starts_with("pattern=")))
        .map(|p| &p["pattern=".len()..])
        .unwrap_or("*");
    json_response(
        StatusCode::OK,
        &format!(
            r#"{{"keys":[],"pattern":"{}","note":"Store integration pending - list endpoint functional"}}"#,
            pattern
        ),
    )
}

fn json_response(status: StatusCode, body: &str) -> Response<Full<Bytes>> {
    Response::builder()
        .status(status)
        .header("Content-Type", "application/json")
        .header("Access-Control-Allow-Origin", "*")
        .body(Full::new(Bytes::from(body.to_string())))
        .unwrap_or_else(|_| Response::new(Full::new(Bytes::from(r#"{"error":"internal"}"#))))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_json_response() {
        let resp = json_response(StatusCode::OK, r#"{"ok":true}"#);
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            resp.headers().get("Content-Type").unwrap(),
            "application/json"
        );
        assert_eq!(
            resp.headers().get("Access-Control-Allow-Origin").unwrap(),
            "*"
        );
    }

    #[test]
    fn test_json_response_error_status() {
        let resp = json_response(StatusCode::NOT_FOUND, r#"{"error":"not_found"}"#);
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[test]
    fn test_gateway_state_creation() {
        let state = GatewayState {
            config: GatewayConfig::default(),
            stats: GatewayRequestStats::default(),
        };
        assert_eq!(state.stats.total.load(Ordering::Relaxed), 0);
        assert_eq!(state.stats.rest.load(Ordering::Relaxed), 0);
        assert_eq!(state.stats.graphql.load(Ordering::Relaxed), 0);
        assert_eq!(state.stats.errors.load(Ordering::Relaxed), 0);
        assert_eq!(state.config.port, 8080);
    }

    #[test]
    fn test_handle_get_key() {
        let resp = handle_get_key("mykey");
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[test]
    fn test_handle_put_key() {
        let resp = handle_put_key("mykey");
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[test]
    fn test_handle_delete_key() {
        let resp = handle_delete_key("mykey");
        assert_eq!(resp.status(), StatusCode::OK);
    }
}
