//! HTTP Server for Ferrite Studio
//!
//! Implements a lightweight HTTP server using hyper for serving:
//! - Static web assets (embedded HTML/CSS/JS)
//! - REST API endpoints
//! - WebSocket connections for real-time updates

use std::convert::Infallible;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;

use http_body_util::Full;
use hyper::body::{Bytes, Incoming};
use hyper::server::conn::http1;
use hyper::service::Service;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;
use tokio::sync::oneshot;

use super::api::{
    ApiResponse, DeleteKeysRequest, ExecuteCommandRequest, RenameKeyRequest, SetKeyRequest,
    SetTtlRequest, StudioApi,
};
use super::auth::{AuthManager, LoginRequest};
use super::console::Console;
use super::metrics::MetricsCollector;
use super::server_info::ServerInfoPanel;
use super::{StudioConfig, StudioError};

/// Ferrite Studio server
pub struct Studio {
    config: StudioConfig,
    api: Arc<StudioApi>,
    auth: Arc<AuthManager>,
    console: Arc<Console>,
    metrics_collector: Arc<MetricsCollector>,
    shutdown_tx: Option<oneshot::Sender<()>>,
}

impl Studio {
    /// Create a new studio instance
    pub fn new(config: StudioConfig) -> Self {
        let auth_config = super::auth::AuthConfig {
            enabled: config.auth_enabled,
            ..Default::default()
        };
        Self {
            config,
            api: Arc::new(StudioApi::new()),
            auth: Arc::new(AuthManager::new(auth_config)),
            console: Arc::new(Console::new()),
            metrics_collector: Arc::new(MetricsCollector::new()),
            shutdown_tx: None,
        }
    }

    /// Create with default development configuration
    pub fn development() -> Self {
        Self::new(StudioConfig::development())
    }

    /// Get the configuration
    pub fn config(&self) -> &StudioConfig {
        &self.config
    }

    /// Get the API handler
    pub fn api(&self) -> &StudioApi {
        &self.api
    }

    /// Get the authentication manager
    pub fn auth(&self) -> &AuthManager {
        &self.auth
    }

    /// Get the console
    pub fn console(&self) -> &Console {
        &self.console
    }

    /// Get the metrics collector
    pub fn metrics_collector(&self) -> &MetricsCollector {
        &self.metrics_collector
    }

    /// Get the bind address
    pub fn bind_addr(&self) -> String {
        self.config.bind_addr()
    }

    /// Check if studio is enabled
    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    /// Start the studio server
    pub async fn start(&mut self) -> Result<(), StudioError> {
        if !self.config.enabled {
            return Ok(());
        }

        let addr = self.config.bind_addr();
        let socket_addr: SocketAddr = addr
            .parse()
            .map_err(|e| StudioError::Server(format!("Invalid address: {}", e)))?;

        tracing::info!("Starting Ferrite Studio at http://{}", addr);

        let listener = TcpListener::bind(socket_addr)
            .await
            .map_err(|e| StudioError::Server(format!("Failed to bind: {}", e)))?;

        let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
        self.shutdown_tx = Some(shutdown_tx);

        let api = self.api.clone();
        let auth = self.auth.clone();
        let console = self.console.clone();
        let metrics_collector = self.metrics_collector.clone();
        let config = self.config.clone();

        // Spawn the server in a background task
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    result = listener.accept() => {
                        match result {
                            Ok((stream, _addr)) => {
                                let io = TokioIo::new(stream);
                                let api = api.clone();
                                let auth = auth.clone();
                                let console = console.clone();
                                let metrics_collector = metrics_collector.clone();
                                let config = config.clone();

                                tokio::spawn(async move {
                                    let service = StudioService::new(
                                        api, auth, console, metrics_collector, config,
                                    );
                                    if let Err(e) = http1::Builder::new()
                                        .serve_connection(io, service)
                                        .await
                                    {
                                        tracing::debug!("Studio connection error: {:?}", e);
                                    }
                                });
                            }
                            Err(e) => {
                                tracing::warn!("Studio accept error: {:?}", e);
                            }
                        }
                    }
                    _ = &mut shutdown_rx => {
                        tracing::info!("Studio server shutting down");
                        break;
                    }
                }
            }
        });

        Ok(())
    }

    /// Stop the studio server
    pub async fn stop(&mut self) -> Result<(), StudioError> {
        tracing::info!("Stopping Ferrite Studio");
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        Ok(())
    }

    /// Generate the embedded HTML for the studio UI
    pub fn generate_index_html() -> &'static str {
        INDEX_HTML
    }

    /// Generate the embedded CSS
    pub fn generate_css() -> &'static str {
        STUDIO_CSS
    }

    /// Generate the embedded JavaScript
    pub fn generate_js() -> &'static str {
        STUDIO_JS
    }
}

/// HTTP service for handling studio requests
struct StudioService {
    api: Arc<StudioApi>,
    auth: Arc<AuthManager>,
    console: Arc<Console>,
    metrics_collector: Arc<MetricsCollector>,
    config: StudioConfig,
}

impl StudioService {
    fn new(
        api: Arc<StudioApi>,
        auth: Arc<AuthManager>,
        console: Arc<Console>,
        metrics_collector: Arc<MetricsCollector>,
        config: StudioConfig,
    ) -> Self {
        Self {
            api,
            auth,
            console,
            metrics_collector,
            config,
        }
    }

    /// Handle an incoming HTTP request
    async fn handle_request(
        api: Arc<StudioApi>,
        auth: Arc<AuthManager>,
        console: Arc<Console>,
        metrics_collector: Arc<MetricsCollector>,
        config: StudioConfig,
        req: Request<Incoming>,
    ) -> Result<Response<Full<Bytes>>, Infallible> {
        let method = req.method().clone();
        let path = req.uri().path().to_string();

        // Auth endpoints are always accessible (login/logout).
        // Other API endpoints check session if auth is enabled.
        let is_auth_endpoint = path.starts_with("/api/auth/");
        let is_static = !path.starts_with("/api/");

        if config.auth_enabled && !is_auth_endpoint && !is_static && auth.is_enabled() {
            // Check for session cookie or Bearer token.
            let session_id = req
                .headers()
                .get("Authorization")
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.strip_prefix("Bearer "))
                .or_else(|| {
                    req.headers()
                        .get("X-Session-Id")
                        .and_then(|v| v.to_str().ok())
                });

            // Fall back to legacy API key check.
            let has_api_key = config.api_key.as_ref().is_some_and(|expected| {
                req.headers()
                    .get("Authorization")
                    .and_then(|v| v.to_str().ok())
                    .map(|s| s.strip_prefix("Bearer ").unwrap_or(s))
                    == Some(expected.as_str())
            });

            if !has_api_key {
                if let Some(sid) = session_id {
                    if auth.validate_session(sid).is_err() {
                        return Ok(Self::json_response(
                            StatusCode::UNAUTHORIZED,
                            &ApiResponse::<()>::err("Invalid or expired session"),
                        ));
                    }
                } else {
                    return Ok(Self::json_response(
                        StatusCode::UNAUTHORIZED,
                        &ApiResponse::<()>::err("Authentication required"),
                    ));
                }
            }
        }

        // Add CORS headers if enabled
        let cors_enabled = config.cors_enabled;

        // Handle preflight requests
        if method == Method::OPTIONS {
            return Ok(Self::cors_response(cors_enabled));
        }

        // Route the request
        let response = match (method, path.as_str()) {
            // Static assets
            (Method::GET, "/") | (Method::GET, "/index.html") => Self::html_response(INDEX_HTML),
            (Method::GET, "/static/studio.css") => Self::css_response(STUDIO_CSS),
            (Method::GET, "/static/studio.js") => Self::js_response(STUDIO_JS),

            // API endpoints
            (Method::GET, "/api/health") => match api.health_check().await {
                Ok(status) => Self::json_response(StatusCode::OK, &ApiResponse::ok(status)),
                Err(e) => Self::json_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    &ApiResponse::<()>::err(e.to_string()),
                ),
            },
            (Method::GET, "/api/metrics") => match api.get_metrics().await {
                Ok(metrics) => Self::json_response(StatusCode::OK, &ApiResponse::ok(metrics)),
                Err(e) => Self::json_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    &ApiResponse::<()>::err(e.to_string()),
                ),
            },
            (Method::GET, "/api/server/info") => match api.get_server_info().await {
                Ok(info) => Self::json_response(StatusCode::OK, &ApiResponse::ok(info)),
                Err(e) => Self::json_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    &ApiResponse::<()>::err(e.to_string()),
                ),
            },
            (Method::GET, "/api/server/memory") => match api.get_memory_info().await {
                Ok(info) => Self::json_response(StatusCode::OK, &ApiResponse::ok(info)),
                Err(e) => Self::json_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    &ApiResponse::<()>::err(e.to_string()),
                ),
            },
            (Method::GET, path) if path.starts_with("/api/keys/detail/") => {
                let key = path.strip_prefix("/api/keys/detail/").unwrap_or("");
                let key = urlencoding::decode(key).unwrap_or_default();
                match api.get_key_detail(&key).await {
                    Ok(Some(detail)) => {
                        Self::json_response(StatusCode::OK, &ApiResponse::ok(detail))
                    }
                    Ok(None) => Self::json_response(
                        StatusCode::NOT_FOUND,
                        &ApiResponse::<()>::err("Key not found"),
                    ),
                    Err(e) => Self::json_response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        &ApiResponse::<()>::err(e.to_string()),
                    ),
                }
            }
            (Method::GET, path) if path.starts_with("/api/keys") => {
                Self::handle_keys_get(api, &req, path).await
            }
            (Method::POST, "/api/keys") => Self::handle_keys_post(api, req).await,
            (Method::DELETE, path) if path.starts_with("/api/keys/") => {
                let key = path.strip_prefix("/api/keys/").unwrap_or("");
                let key = urlencoding::decode(key).unwrap_or_default();
                match api.delete_key(&key).await {
                    Ok(deleted) => {
                        let result = serde_json::json!({ "deleted": deleted });
                        Self::json_response(StatusCode::OK, &ApiResponse::ok(result))
                    }
                    Err(e) => Self::json_response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        &ApiResponse::<()>::err(e.to_string()),
                    ),
                }
            }
            (Method::GET, "/api/slowlog") => match api.get_slow_log(100).await {
                Ok(entries) => Self::json_response(StatusCode::OK, &ApiResponse::ok(entries)),
                Err(e) => Self::json_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    &ApiResponse::<()>::err(e.to_string()),
                ),
            },
            (Method::DELETE, "/api/slowlog") => match api.clear_slow_log().await {
                Ok(()) => Self::json_response(StatusCode::OK, &ApiResponse::ok(())),
                Err(e) => Self::json_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    &ApiResponse::<()>::err(e.to_string()),
                ),
            },
            (Method::GET, "/api/cluster") => match api.get_cluster_info().await {
                Ok(info) => Self::json_response(StatusCode::OK, &ApiResponse::ok(info)),
                Err(e) => Self::json_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    &ApiResponse::<()>::err(e.to_string()),
                ),
            },
            (Method::GET, "/api/config") => match api.get_all_config().await {
                Ok(config) => Self::json_response(StatusCode::OK, &ApiResponse::ok(config)),
                Err(e) => Self::json_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    &ApiResponse::<()>::err(e.to_string()),
                ),
            },
            (Method::POST, "/api/keys/delete") => Self::handle_keys_bulk_delete(api, req).await,
            (Method::POST, "/api/keys/rename") => Self::handle_keys_rename(api, req).await,
            (Method::POST, "/api/keys/ttl") => Self::handle_keys_set_ttl(api, req).await,
            (Method::POST, "/api/command") => Self::handle_command_post(api, req).await,

            // Auth endpoints
            (Method::POST, "/api/auth/login") => Self::handle_login(auth, req).await,
            (Method::POST, "/api/auth/logout") => Self::handle_logout(auth, req).await,

            // Console endpoints
            (Method::GET, "/api/console/commands") => {
                let cmds: Vec<_> = console.available_commands().into_iter().cloned().collect();
                Self::json_response(StatusCode::OK, &ApiResponse::ok(cmds))
            }
            (Method::GET, "/api/console/history") => {
                let history = console.get_history(100);
                Self::json_response(StatusCode::OK, &ApiResponse::ok(history))
            }
            (Method::GET, path) if path.starts_with("/api/console/autocomplete") => {
                let query_str = req.uri().query().unwrap_or("");
                let input = query_str
                    .split('&')
                    .filter_map(|s| s.split_once('='))
                    .find(|(k, _)| *k == "input")
                    .map(|(_, v)| urlencoding::decode(v).unwrap_or_default().to_string())
                    .unwrap_or_default();
                let suggestions = console.autocomplete(&input);
                Self::json_response(StatusCode::OK, &ApiResponse::ok(suggestions))
            }

            // Metrics SSE / snapshot endpoint
            (Method::GET, "/api/metrics/snapshot") => {
                let snap = metrics_collector.snapshot();
                Self::json_response(StatusCode::OK, &ApiResponse::ok(snap))
            }

            // Server info panel
            (Method::GET, "/api/server/panel") => {
                let panel = ServerInfoPanel::placeholder();
                Self::json_response(StatusCode::OK, &ApiResponse::ok(panel))
            }

            // 404 for everything else
            _ => Self::json_response(StatusCode::NOT_FOUND, &ApiResponse::<()>::err("Not found")),
        };

        // Add CORS headers to response
        if cors_enabled {
            Ok(Self::add_cors_headers(response))
        } else {
            Ok(response)
        }
    }

    /// Handle GET /api/keys requests
    async fn handle_keys_get(
        api: Arc<StudioApi>,
        req: &Request<Incoming>,
        path: &str,
    ) -> Response<Full<Bytes>> {
        // Check if it's a specific key request
        if path.starts_with("/api/keys/") && path.len() > "/api/keys/".len() {
            let key = path.strip_prefix("/api/keys/").unwrap_or("");
            let key = urlencoding::decode(key).unwrap_or_default();
            match api.get_key(&key).await {
                Ok(Some(value)) => Self::json_response(StatusCode::OK, &ApiResponse::ok(value)),
                Ok(None) => Self::json_response(
                    StatusCode::NOT_FOUND,
                    &ApiResponse::<()>::err("Key not found"),
                ),
                Err(e) => Self::json_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    &ApiResponse::<()>::err(e.to_string()),
                ),
            }
        } else {
            // Scan keys with pattern
            let query = req.uri().query().unwrap_or("");
            let params: Vec<(&str, &str)> =
                query.split('&').filter_map(|s| s.split_once('=')).collect();

            let pattern = params
                .iter()
                .find(|(k, _)| *k == "pattern")
                .map(|(_, v)| urlencoding::decode(v).unwrap_or_default().to_string())
                .unwrap_or_else(|| "*".to_string());

            let cursor = params
                .iter()
                .find(|(k, _)| *k == "cursor")
                .and_then(|(_, v)| v.parse().ok())
                .unwrap_or(0);

            let count = params
                .iter()
                .find(|(k, _)| *k == "count")
                .and_then(|(_, v)| v.parse().ok())
                .unwrap_or(100);

            match api.scan_keys(&pattern, cursor, count, None).await {
                Ok(result) => Self::json_response(StatusCode::OK, &ApiResponse::ok(result)),
                Err(e) => Self::json_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    &ApiResponse::<()>::err(e.to_string()),
                ),
            }
        }
    }

    /// Handle POST /api/keys requests
    async fn handle_keys_post(
        api: Arc<StudioApi>,
        req: Request<Incoming>,
    ) -> Response<Full<Bytes>> {
        // Read the request body
        let body = match Self::read_body(req).await {
            Ok(b) => b,
            Err(e) => {
                return Self::json_response(StatusCode::BAD_REQUEST, &ApiResponse::<()>::err(e));
            }
        };

        // Parse the request
        let set_req: SetKeyRequest = match serde_json::from_slice(&body) {
            Ok(r) => r,
            Err(e) => {
                return Self::json_response(
                    StatusCode::BAD_REQUEST,
                    &ApiResponse::<()>::err(format!("Invalid JSON: {}", e)),
                );
            }
        };

        match api.set_key(&set_req.key, &set_req.value, set_req.ttl).await {
            Ok(()) => Self::json_response(StatusCode::OK, &ApiResponse::ok(())),
            Err(e) => Self::json_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                &ApiResponse::<()>::err(e.to_string()),
            ),
        }
    }

    /// Handle POST /api/command requests
    async fn handle_command_post(
        api: Arc<StudioApi>,
        req: Request<Incoming>,
    ) -> Response<Full<Bytes>> {
        let body = match Self::read_body(req).await {
            Ok(b) => b,
            Err(e) => {
                return Self::json_response(StatusCode::BAD_REQUEST, &ApiResponse::<()>::err(e));
            }
        };

        let cmd_req: ExecuteCommandRequest = match serde_json::from_slice(&body) {
            Ok(r) => r,
            Err(e) => {
                return Self::json_response(
                    StatusCode::BAD_REQUEST,
                    &ApiResponse::<()>::err(format!("Invalid JSON: {}", e)),
                );
            }
        };

        match api.execute_command(&cmd_req.command).await {
            Ok(result) => Self::json_response(StatusCode::OK, &ApiResponse::ok(result)),
            Err(e) => Self::json_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                &ApiResponse::<()>::err(e.to_string()),
            ),
        }
    }

    /// Handle POST /api/keys/delete (bulk delete)
    async fn handle_keys_bulk_delete(
        api: Arc<StudioApi>,
        req: Request<Incoming>,
    ) -> Response<Full<Bytes>> {
        let body = match Self::read_body(req).await {
            Ok(b) => b,
            Err(e) => {
                return Self::json_response(StatusCode::BAD_REQUEST, &ApiResponse::<()>::err(e));
            }
        };

        let del_req: DeleteKeysRequest = match serde_json::from_slice(&body) {
            Ok(r) => r,
            Err(e) => {
                return Self::json_response(
                    StatusCode::BAD_REQUEST,
                    &ApiResponse::<()>::err(format!("Invalid JSON: {}", e)),
                );
            }
        };

        match api.delete_keys(&del_req.keys).await {
            Ok(deleted) => {
                let result = serde_json::json!({ "deleted": deleted });
                Self::json_response(StatusCode::OK, &ApiResponse::ok(result))
            }
            Err(e) => Self::json_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                &ApiResponse::<()>::err(e.to_string()),
            ),
        }
    }

    /// Handle POST /api/keys/rename
    async fn handle_keys_rename(
        api: Arc<StudioApi>,
        req: Request<Incoming>,
    ) -> Response<Full<Bytes>> {
        let body = match Self::read_body(req).await {
            Ok(b) => b,
            Err(e) => {
                return Self::json_response(StatusCode::BAD_REQUEST, &ApiResponse::<()>::err(e));
            }
        };

        let rename_req: RenameKeyRequest = match serde_json::from_slice(&body) {
            Ok(r) => r,
            Err(e) => {
                return Self::json_response(
                    StatusCode::BAD_REQUEST,
                    &ApiResponse::<()>::err(format!("Invalid JSON: {}", e)),
                );
            }
        };

        match api.rename_key(&rename_req.key, &rename_req.new_key).await {
            Ok(()) => Self::json_response(StatusCode::OK, &ApiResponse::ok(())),
            Err(e) => Self::json_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                &ApiResponse::<()>::err(e.to_string()),
            ),
        }
    }

    /// Handle POST /api/keys/ttl
    async fn handle_keys_set_ttl(
        api: Arc<StudioApi>,
        req: Request<Incoming>,
    ) -> Response<Full<Bytes>> {
        let body = match Self::read_body(req).await {
            Ok(b) => b,
            Err(e) => {
                return Self::json_response(StatusCode::BAD_REQUEST, &ApiResponse::<()>::err(e));
            }
        };

        let ttl_req: SetTtlRequest = match serde_json::from_slice(&body) {
            Ok(r) => r,
            Err(e) => {
                return Self::json_response(
                    StatusCode::BAD_REQUEST,
                    &ApiResponse::<()>::err(format!("Invalid JSON: {}", e)),
                );
            }
        };

        match api.set_ttl(&ttl_req.key, ttl_req.ttl).await {
            Ok(()) => Self::json_response(StatusCode::OK, &ApiResponse::ok(())),
            Err(e) => Self::json_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                &ApiResponse::<()>::err(e.to_string()),
            ),
        }
    }

    /// Handle POST /api/auth/login
    async fn handle_login(
        auth: Arc<AuthManager>,
        req: Request<Incoming>,
    ) -> Response<Full<Bytes>> {
        let body = match Self::read_body(req).await {
            Ok(b) => b,
            Err(e) => {
                return Self::json_response(StatusCode::BAD_REQUEST, &ApiResponse::<()>::err(e));
            }
        };

        let login_req: LoginRequest = match serde_json::from_slice(&body) {
            Ok(r) => r,
            Err(e) => {
                return Self::json_response(
                    StatusCode::BAD_REQUEST,
                    &ApiResponse::<()>::err(format!("Invalid JSON: {}", e)),
                );
            }
        };

        match auth.login(&login_req) {
            Ok(resp) => Self::json_response(StatusCode::OK, &ApiResponse::ok(resp)),
            Err(e) => Self::json_response(
                StatusCode::UNAUTHORIZED,
                &ApiResponse::<()>::err(e.to_string()),
            ),
        }
    }

    /// Handle POST /api/auth/logout
    async fn handle_logout(
        auth: Arc<AuthManager>,
        req: Request<Incoming>,
    ) -> Response<Full<Bytes>> {
        let session_id = req
            .headers()
            .get("X-Session-Id")
            .or_else(|| req.headers().get("Authorization"))
            .and_then(|v| v.to_str().ok())
            .map(|s| s.strip_prefix("Bearer ").unwrap_or(s).to_string());

        match session_id {
            Some(sid) => {
                let removed = auth.logout(&sid);
                let result = serde_json::json!({ "logged_out": removed });
                Self::json_response(StatusCode::OK, &ApiResponse::ok(result))
            }
            None => Self::json_response(
                StatusCode::BAD_REQUEST,
                &ApiResponse::<()>::err("No session ID provided"),
            ),
        }
    }

    /// Read request body
    async fn read_body(req: Request<Incoming>) -> Result<Vec<u8>, String> {
        use http_body_util::BodyExt;

        let body = req.into_body();
        match body.collect().await {
            Ok(collected) => Ok(collected.to_bytes().to_vec()),
            Err(e) => Err(format!("Failed to read body: {}", e)),
        }
    }

    /// Create a JSON response
    fn json_response<T: serde::Serialize>(status: StatusCode, data: &T) -> Response<Full<Bytes>> {
        let body = serde_json::to_string(data).unwrap_or_else(|_| "{}".to_string());
        Response::builder()
            .status(status)
            .header("Content-Type", "application/json")
            .body(Full::new(Bytes::from(body)))
            .expect("valid HTTP response")
    }

    /// Create an HTML response
    fn html_response(html: &str) -> Response<Full<Bytes>> {
        Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", "text/html; charset=utf-8")
            .body(Full::new(Bytes::from(html.to_string())))
            .expect("valid HTTP response")
    }

    /// Create a CSS response
    fn css_response(css: &str) -> Response<Full<Bytes>> {
        Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", "text/css; charset=utf-8")
            .body(Full::new(Bytes::from(css.to_string())))
            .expect("valid HTTP response")
    }

    /// Create a JavaScript response
    fn js_response(js: &str) -> Response<Full<Bytes>> {
        Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", "application/javascript; charset=utf-8")
            .body(Full::new(Bytes::from(js.to_string())))
            .expect("valid HTTP response")
    }

    /// Create a CORS preflight response
    fn cors_response(enabled: bool) -> Response<Full<Bytes>> {
        let mut builder = Response::builder().status(StatusCode::NO_CONTENT);
        if enabled {
            builder = builder
                .header("Access-Control-Allow-Origin", "*")
                .header(
                    "Access-Control-Allow-Methods",
                    "GET, POST, PUT, DELETE, OPTIONS",
                )
                .header(
                    "Access-Control-Allow-Headers",
                    "Content-Type, Authorization",
                )
                .header("Access-Control-Max-Age", "86400");
        }
        builder
            .body(Full::new(Bytes::new()))
            .expect("valid HTTP response")
    }

    /// Add CORS headers to a response
    fn add_cors_headers(mut response: Response<Full<Bytes>>) -> Response<Full<Bytes>> {
        response.headers_mut().insert(
            "Access-Control-Allow-Origin",
            "*".parse().expect("valid header value"),
        );
        response
    }
}

impl Service<Request<Incoming>> for StudioService {
    type Response = Response<Full<Bytes>>;
    type Error = Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn call(&self, req: Request<Incoming>) -> Self::Future {
        let api = self.api.clone();
        let auth = self.auth.clone();
        let console = self.console.clone();
        let metrics_collector = self.metrics_collector.clone();
        let config = self.config.clone();

        Box::pin(async move {
            Self::handle_request(api, auth, console, metrics_collector, config, req).await
        })
    }
}

/// Embedded index.html for the studio UI
const INDEX_HTML: &str = r##"<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Ferrite Studio</title>
    <link rel="stylesheet" href="/static/studio.css">
</head>
<body>
    <div id="app">
        <header class="header">
            <div class="logo">
                <h1>Ferrite Studio</h1>
            </div>
            <nav class="nav">
                <a href="#" class="nav-link active" data-tab="dashboard">Dashboard</a>
                <a href="#" class="nav-link" data-tab="keys">Key Browser</a>
                <a href="#" class="nav-link" data-tab="console">Console</a>
                <a href="#" class="nav-link" data-tab="serverinfo">Server Info</a>
                <a href="#" class="nav-link" data-tab="slowlog">Slow Log</a>
                <a href="#" class="nav-link" data-tab="cluster">Cluster</a>
                <a href="#" class="nav-link" data-tab="config">Config</a>
            </nav>
        </header>

        <main class="main">
            <!-- Dashboard Tab -->
            <section id="dashboard" class="tab-content active">
                <div class="dashboard-grid">
                    <div class="metric-card">
                        <h3>Operations/sec</h3>
                        <div class="metric-value" id="ops-per-sec">-</div>
                    </div>
                    <div class="metric-card">
                        <h3>Connected Clients</h3>
                        <div class="metric-value" id="connected-clients">-</div>
                    </div>
                    <div class="metric-card">
                        <h3>Memory Used</h3>
                        <div class="metric-value" id="memory-used">-</div>
                    </div>
                    <div class="metric-card">
                        <h3>Total Keys</h3>
                        <div class="metric-value" id="total-keys">-</div>
                    </div>
                </div>

                <div class="chart-container">
                    <h3>Memory by Tier</h3>
                    <div id="memory-chart" class="chart"></div>
                </div>
            </section>

            <!-- Key Browser Tab -->
            <section id="keys" class="tab-content">
                <div class="toolbar">
                    <input type="text" id="key-pattern" placeholder="Pattern (e.g., user:*)" value="*">
                    <button id="scan-btn" class="btn btn-primary">Scan</button>
                    <button id="add-key-btn" class="btn btn-secondary">Add Key</button>
                </div>

                <div class="key-list" id="key-list">
                    <table>
                        <thead>
                            <tr>
                                <th>Key</th>
                                <th>Type</th>
                                <th>TTL</th>
                                <th>Actions</th>
                            </tr>
                        </thead>
                        <tbody id="key-table-body">
                        </tbody>
                    </table>
                </div>
            </section>

            <!-- Console Tab -->
            <section id="console" class="tab-content">
                <div class="console-container">
                    <div class="console-output" id="console-output"></div>
                    <div class="console-input-row">
                        <span class="console-prompt">ferrite&gt;</span>
                        <input type="text" id="console-input" placeholder="Enter command..." autocomplete="off">
                        <button id="console-run-btn" class="btn btn-primary">Run</button>
                    </div>
                    <div class="console-suggestions" id="console-suggestions"></div>
                </div>
            </section>

            <!-- Server Info Tab -->
            <section id="serverinfo" class="tab-content">
                <div class="dashboard-grid" id="server-info-panel">
                    <!-- Server info will be rendered here -->
                </div>
                <div class="alerts-container" id="alerts-container">
                    <!-- Alerts will be rendered here -->
                </div>
            </section>

            <!-- Slow Log Tab -->
            <section id="slowlog" class="tab-content">
                <div class="toolbar">
                    <button id="refresh-slowlog-btn" class="btn btn-primary">Refresh</button>
                    <button id="clear-slowlog-btn" class="btn btn-danger">Clear</button>
                </div>

                <div class="slowlog-list" id="slowlog-list">
                    <table>
                        <thead>
                            <tr>
                                <th>ID</th>
                                <th>Time</th>
                                <th>Duration</th>
                                <th>Command</th>
                                <th>Client</th>
                            </tr>
                        </thead>
                        <tbody id="slowlog-table-body">
                        </tbody>
                    </table>
                </div>
            </section>

            <!-- Cluster Tab -->
            <section id="cluster" class="tab-content">
                <div class="cluster-topology" id="cluster-topology">
                    <!-- Cluster visualization will be rendered here -->
                </div>
            </section>

            <!-- Config Tab -->
            <section id="config" class="tab-content">
                <div class="config-form" id="config-form">
                    <!-- Config form will be rendered here -->
                </div>
            </section>
        </main>

        <footer class="footer">
            <span>Ferrite v<span id="version">-</span></span>
            <span>Uptime: <span id="uptime">-</span></span>
        </footer>
    </div>

    <!-- Modal for key details -->
    <div id="modal" class="modal">
        <div class="modal-content">
            <span class="close">&times;</span>
            <div id="modal-body"></div>
        </div>
    </div>

    <script src="/static/studio.js"></script>
</body>
</html>
"##;

/// Embedded CSS for the studio UI
const STUDIO_CSS: &str = r##"
:root {
    --bg-primary: #1a1a2e;
    --bg-secondary: #16213e;
    --bg-card: #0f3460;
    --text-primary: #ffffff;
    --text-secondary: #a0a0a0;
    --accent: #e94560;
    --accent-hover: #ff6b6b;
    --success: #00d9a5;
    --warning: #ffc107;
    --danger: #dc3545;
    --border: #2a2a4a;
}

* {
    margin: 0;
    padding: 0;
    box-sizing: border-box;
}

body {
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, sans-serif;
    background: var(--bg-primary);
    color: var(--text-primary);
    min-height: 100vh;
}

#app {
    display: flex;
    flex-direction: column;
    min-height: 100vh;
}

.header {
    background: var(--bg-secondary);
    padding: 1rem 2rem;
    display: flex;
    justify-content: space-between;
    align-items: center;
    border-bottom: 1px solid var(--border);
}

.logo h1 {
    font-size: 1.5rem;
    color: var(--accent);
}

.nav {
    display: flex;
    gap: 1rem;
}

.nav-link {
    color: var(--text-secondary);
    text-decoration: none;
    padding: 0.5rem 1rem;
    border-radius: 4px;
    transition: all 0.2s;
}

.nav-link:hover, .nav-link.active {
    color: var(--text-primary);
    background: var(--bg-card);
}

.main {
    flex: 1;
    padding: 2rem;
}

.tab-content {
    display: none;
}

.tab-content.active {
    display: block;
}

.dashboard-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
    gap: 1.5rem;
    margin-bottom: 2rem;
}

.metric-card {
    background: var(--bg-card);
    padding: 1.5rem;
    border-radius: 8px;
    text-align: center;
}

.metric-card h3 {
    font-size: 0.875rem;
    color: var(--text-secondary);
    margin-bottom: 0.5rem;
}

.metric-value {
    font-size: 2rem;
    font-weight: bold;
    color: var(--accent);
}

.chart-container {
    background: var(--bg-card);
    padding: 1.5rem;
    border-radius: 8px;
}

.chart-container h3 {
    margin-bottom: 1rem;
}

.toolbar {
    display: flex;
    gap: 1rem;
    margin-bottom: 1.5rem;
}

input[type="text"] {
    background: var(--bg-secondary);
    border: 1px solid var(--border);
    color: var(--text-primary);
    padding: 0.5rem 1rem;
    border-radius: 4px;
    flex: 1;
}

.btn {
    padding: 0.5rem 1rem;
    border: none;
    border-radius: 4px;
    cursor: pointer;
    font-weight: 500;
    transition: all 0.2s;
}

.btn-primary {
    background: var(--accent);
    color: white;
}

.btn-primary:hover {
    background: var(--accent-hover);
}

.btn-secondary {
    background: var(--bg-card);
    color: var(--text-primary);
}

.btn-danger {
    background: var(--danger);
    color: white;
}

table {
    width: 100%;
    border-collapse: collapse;
    background: var(--bg-card);
    border-radius: 8px;
    overflow: hidden;
}

th, td {
    padding: 1rem;
    text-align: left;
    border-bottom: 1px solid var(--border);
}

th {
    background: var(--bg-secondary);
    font-weight: 500;
}

tr:hover {
    background: var(--bg-secondary);
}

.footer {
    background: var(--bg-secondary);
    padding: 1rem 2rem;
    display: flex;
    justify-content: space-between;
    border-top: 1px solid var(--border);
    color: var(--text-secondary);
    font-size: 0.875rem;
}

.modal {
    display: none;
    position: fixed;
    top: 0;
    left: 0;
    width: 100%;
    height: 100%;
    background: rgba(0, 0, 0, 0.8);
    justify-content: center;
    align-items: center;
}

.modal.active {
    display: flex;
}

.modal-content {
    background: var(--bg-card);
    padding: 2rem;
    border-radius: 8px;
    max-width: 600px;
    width: 90%;
    max-height: 80vh;
    overflow-y: auto;
}

.close {
    float: right;
    font-size: 1.5rem;
    cursor: pointer;
    color: var(--text-secondary);
}

.close:hover {
    color: var(--text-primary);
}

.console-container {
    background: var(--bg-card);
    border-radius: 8px;
    padding: 1rem;
    height: 70vh;
    display: flex;
    flex-direction: column;
}

.console-output {
    flex: 1;
    overflow-y: auto;
    font-family: 'Menlo', 'Monaco', 'Courier New', monospace;
    font-size: 0.875rem;
    line-height: 1.5;
    padding: 0.5rem;
    white-space: pre-wrap;
    color: var(--text-secondary);
}

.console-output .cmd { color: var(--accent); }
.console-output .ok { color: var(--success); }
.console-output .err { color: var(--danger); }
.console-output .int { color: var(--warning); }

.console-input-row {
    display: flex;
    gap: 0.5rem;
    align-items: center;
    margin-top: 0.5rem;
}

.console-prompt {
    color: var(--accent);
    font-family: 'Menlo', 'Monaco', 'Courier New', monospace;
    font-weight: bold;
}

.console-suggestions {
    display: flex;
    flex-wrap: wrap;
    gap: 0.25rem;
    margin-top: 0.25rem;
    font-size: 0.75rem;
}

.console-suggestions span {
    background: var(--bg-secondary);
    padding: 0.125rem 0.5rem;
    border-radius: 3px;
    cursor: pointer;
    color: var(--text-secondary);
}

.console-suggestions span:hover {
    color: var(--text-primary);
    background: var(--bg-card);
}

.alerts-container {
    margin-top: 1.5rem;
}

.alert-item {
    padding: 0.75rem 1rem;
    border-radius: 4px;
    margin-bottom: 0.5rem;
    border-left: 4px solid;
}

.alert-item.critical { border-color: var(--danger); background: rgba(220, 53, 69, 0.1); }
.alert-item.warning { border-color: var(--warning); background: rgba(255, 193, 7, 0.1); }
.alert-item.info { border-color: #17a2b8; background: rgba(23, 162, 184, 0.1); }
"##;

/// Embedded JavaScript for the studio UI
const STUDIO_JS: &str = r##"
// Ferrite Studio JavaScript

const API_BASE = '/api';

// Tab navigation
document.querySelectorAll('.nav-link').forEach(link => {
    link.addEventListener('click', (e) => {
        e.preventDefault();
        const tab = e.target.dataset.tab;

        // Update active nav link
        document.querySelectorAll('.nav-link').forEach(l => l.classList.remove('active'));
        e.target.classList.add('active');

        // Show active tab content
        document.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));
        document.getElementById(tab).classList.add('active');

        // Load tab data
        loadTabData(tab);
    });
});

// Load data for active tab
async function loadTabData(tab) {
    switch (tab) {
        case 'dashboard':
            await loadDashboard();
            break;
        case 'keys':
            await loadKeys();
            break;
        case 'console':
            // Console is always ready
            break;
        case 'serverinfo':
            await loadServerInfo();
            break;
        case 'slowlog':
            await loadSlowLog();
            break;
        case 'cluster':
            await loadCluster();
            break;
        case 'config':
            await loadConfig();
            break;
    }
}

// Dashboard
async function loadDashboard() {
    try {
        const info = await fetch(`${API_BASE}/server/info`).then(r => r.json());
        if (info.success) {
            document.getElementById('ops-per-sec').textContent = info.data.ops_per_sec.toFixed(0);
            document.getElementById('connected-clients').textContent = info.data.connected_clients;
            document.getElementById('memory-used').textContent = formatBytes(info.data.used_memory);
            document.getElementById('total-keys').textContent = info.data.total_keys;
            document.getElementById('version').textContent = info.data.version;
            document.getElementById('uptime').textContent = formatUptime(info.data.uptime_secs);
        }
    } catch (e) {
        console.error('Failed to load dashboard:', e);
    }
}

// Keys
async function loadKeys() {
    const pattern = document.getElementById('key-pattern').value || '*';
    try {
        const result = await fetch(`${API_BASE}/keys?pattern=${encodeURIComponent(pattern)}`).then(r => r.json());
        if (result.success) {
            const tbody = document.getElementById('key-table-body');
            tbody.innerHTML = result.data.keys.map(key => `
                <tr>
                    <td>${escapeHtml(key.key)}</td>
                    <td>${key.key_type}</td>
                    <td>${key.ttl === -1 ? 'None' : key.ttl + 's'}</td>
                    <td>
                        <button class="btn btn-secondary" onclick="viewKey('${escapeHtml(key.key)}')">View</button>
                        <button class="btn btn-danger" onclick="deleteKey('${escapeHtml(key.key)}')">Delete</button>
                    </td>
                </tr>
            `).join('');
        }
    } catch (e) {
        console.error('Failed to load keys:', e);
    }
}

// Slow Log
async function loadSlowLog() {
    try {
        const result = await fetch(`${API_BASE}/slowlog`).then(r => r.json());
        if (result.success) {
            const tbody = document.getElementById('slowlog-table-body');
            tbody.innerHTML = result.data.map(entry => `
                <tr>
                    <td>${entry.id}</td>
                    <td>${new Date(entry.timestamp * 1000).toLocaleString()}</td>
                    <td>${(entry.duration_us / 1000).toFixed(2)}ms</td>
                    <td><code>${escapeHtml(entry.full_command)}</code></td>
                    <td>${entry.client_addr || '-'}</td>
                </tr>
            `).join('');
        }
    } catch (e) {
        console.error('Failed to load slow log:', e);
    }
}

// Cluster
async function loadCluster() {
    try {
        const result = await fetch(`${API_BASE}/cluster`).then(r => r.json());
        if (result.success) {
            const container = document.getElementById('cluster-topology');
            container.innerHTML = result.data.map(node => `
                <div class="cluster-node ${node.role}">
                    <div class="node-header">
                        <span class="node-role">${node.role.toUpperCase()}</span>
                        <span class="node-status ${node.status}">${node.status}</span>
                    </div>
                    <div class="node-addr">${node.addr}</div>
                    <div class="node-slots">Slots: ${node.slots.map(s => `${s[0]}-${s[1]}`).join(', ')}</div>
                </div>
            `).join('');
        }
    } catch (e) {
        console.error('Failed to load cluster:', e);
    }
}

// Config
async function loadConfig() {
    try {
        const result = await fetch(`${API_BASE}/config`).then(r => r.json());
        if (result.success) {
            const container = document.getElementById('config-form');
            container.innerHTML = `
                <table>
                    <thead><tr><th>Parameter</th><th>Value</th></tr></thead>
                    <tbody>
                        ${Object.entries(result.data).map(([k, v]) => `
                            <tr>
                                <td>${escapeHtml(k)}</td>
                                <td><input type="text" value="${escapeHtml(v)}" data-param="${escapeHtml(k)}"></td>
                            </tr>
                        `).join('')}
                    </tbody>
                </table>
            `;
        }
    } catch (e) {
        console.error('Failed to load config:', e);
    }
}

// View key details
async function viewKey(key) {
    try {
        const result = await fetch(`${API_BASE}/keys/${encodeURIComponent(key)}`).then(r => r.json());
        if (result.success && result.data) {
            const modal = document.getElementById('modal');
            const body = document.getElementById('modal-body');
            body.innerHTML = `
                <h2>Key: ${escapeHtml(result.data.key)}</h2>
                <p><strong>Type:</strong> ${result.data.key_type}</p>
                <p><strong>TTL:</strong> ${result.data.ttl === -1 ? 'None' : result.data.ttl + 's'}</p>
                <p><strong>Value:</strong></p>
                <pre>${JSON.stringify(result.data.value, null, 2)}</pre>
            `;
            modal.classList.add('active');
        }
    } catch (e) {
        console.error('Failed to view key:', e);
    }
}

// Delete key
async function deleteKey(key) {
    if (!confirm(`Delete key "${key}"?`)) return;
    try {
        await fetch(`${API_BASE}/keys/${encodeURIComponent(key)}`, { method: 'DELETE' });
        loadKeys();
    } catch (e) {
        console.error('Failed to delete key:', e);
    }
}

// Utility functions
function formatBytes(bytes) {
    if (bytes >= 1024 * 1024 * 1024) return (bytes / (1024 * 1024 * 1024)).toFixed(2) + ' GB';
    if (bytes >= 1024 * 1024) return (bytes / (1024 * 1024)).toFixed(2) + ' MB';
    if (bytes >= 1024) return (bytes / 1024).toFixed(2) + ' KB';
    return bytes + ' B';
}

function formatUptime(seconds) {
    const days = Math.floor(seconds / 86400);
    const hours = Math.floor((seconds % 86400) / 3600);
    const mins = Math.floor((seconds % 3600) / 60);
    if (days > 0) return `${days}d ${hours}h`;
    if (hours > 0) return `${hours}h ${mins}m`;
    return `${mins}m`;
}

function escapeHtml(str) {
    const div = document.createElement('div');
    div.textContent = str;
    return div.innerHTML;
}

// Modal close
document.querySelector('.close').addEventListener('click', () => {
    document.getElementById('modal').classList.remove('active');
});

// Console
const consoleInput = document.getElementById('console-input');
const consoleOutput = document.getElementById('console-output');
const consoleSuggestions = document.getElementById('console-suggestions');
let consoleHistory = [];
let historyIndex = -1;

async function runConsoleCommand() {
    const cmd = consoleInput.value.trim();
    if (!cmd) return;
    consoleHistory.push(cmd);
    historyIndex = consoleHistory.length;
    consoleOutput.innerHTML += `<span class="cmd">ferrite&gt; ${escapeHtml(cmd)}</span>\n`;
    consoleInput.value = '';
    consoleSuggestions.innerHTML = '';
    try {
        const result = await fetch(`${API_BASE}/command`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ command: cmd }),
        }).then(r => r.json());
        if (result.success) {
            const val = result.data;
            const cls = typeof val === 'string' && val.startsWith('ERR') ? 'err' : 'ok';
            consoleOutput.innerHTML += `<span class="${cls}">${escapeHtml(JSON.stringify(val))}</span>\n`;
        } else {
            consoleOutput.innerHTML += `<span class="err">(error) ${escapeHtml(result.error || 'Unknown error')}</span>\n`;
        }
    } catch (e) {
        consoleOutput.innerHTML += `<span class="err">(error) ${escapeHtml(e.message)}</span>\n`;
    }
    consoleOutput.scrollTop = consoleOutput.scrollHeight;
}

if (consoleInput) {
    consoleInput.addEventListener('keydown', async (e) => {
        if (e.key === 'Enter') {
            e.preventDefault();
            await runConsoleCommand();
        } else if (e.key === 'ArrowUp') {
            e.preventDefault();
            if (historyIndex > 0) { historyIndex--; consoleInput.value = consoleHistory[historyIndex]; }
        } else if (e.key === 'ArrowDown') {
            e.preventDefault();
            if (historyIndex < consoleHistory.length - 1) { historyIndex++; consoleInput.value = consoleHistory[historyIndex]; }
            else { historyIndex = consoleHistory.length; consoleInput.value = ''; }
        } else if (e.key === 'Tab') {
            e.preventDefault();
            try {
                const result = await fetch(`${API_BASE}/console/autocomplete?input=${encodeURIComponent(consoleInput.value)}`).then(r => r.json());
                if (result.success && result.data.length > 0) {
                    if (result.data.length === 1) {
                        consoleInput.value = result.data[0] + ' ';
                        consoleSuggestions.innerHTML = '';
                    } else {
                        consoleSuggestions.innerHTML = result.data.slice(0, 20).map(s =>
                            `<span onclick="document.getElementById('console-input').value='${s} '">${s}</span>`
                        ).join('');
                    }
                }
            } catch (_) {}
        }
    });
}

document.getElementById('console-run-btn')?.addEventListener('click', runConsoleCommand);

// Server Info
async function loadServerInfo() {
    try {
        const result = await fetch(`${API_BASE}/server/panel`).then(r => r.json());
        if (result.success) {
            const d = result.data;
            const panel = document.getElementById('server-info-panel');
            panel.innerHTML = `
                <div class="metric-card"><h3>Version</h3><div class="metric-value">${escapeHtml(d.version)}</div></div>
                <div class="metric-card"><h3>Mode</h3><div class="metric-value">${escapeHtml(d.mode)}</div></div>
                <div class="metric-card"><h3>Role</h3><div class="metric-value">${escapeHtml(d.role)}</div></div>
                <div class="metric-card"><h3>Uptime</h3><div class="metric-value">${formatUptime(d.uptime_seconds)}</div></div>
                <div class="metric-card"><h3>OS</h3><div class="metric-value">${escapeHtml(d.os)}</div></div>
                <div class="metric-card"><h3>Databases</h3><div class="metric-value">${d.databases}</div></div>
                <div class="metric-card"><h3>Replication</h3><div class="metric-value">${escapeHtml(d.replication.role)}${d.replication.enabled ? ' (active)' : ''}</div></div>
                <div class="metric-card"><h3>Persistence</h3><div class="metric-value">${d.persistence.aof_enabled ? 'AOF' : ''}${d.persistence.checkpoint_enabled ? ' + Checkpoint' : ''}</div></div>
            `;
            const alertsContainer = document.getElementById('alerts-container');
            if (d.alerts && d.alerts.length > 0) {
                alertsContainer.innerHTML = d.alerts.map(a =>
                    `<div class="alert-item ${a.level.toLowerCase()}"><strong>[${a.category}]</strong> ${escapeHtml(a.message)}</div>`
                ).join('');
            } else {
                alertsContainer.innerHTML = '<div class="alert-item info">All systems operational</div>';
            }
        }
    } catch (e) {
        console.error('Failed to load server info:', e);
    }
}

// Event listeners
document.getElementById('scan-btn')?.addEventListener('click', loadKeys);
document.getElementById('refresh-slowlog-btn')?.addEventListener('click', loadSlowLog);

// Initial load
loadDashboard();

// Auto-refresh dashboard every 5 seconds
setInterval(() => {
    if (document.getElementById('dashboard').classList.contains('active')) {
        loadDashboard();
    }
}, 5000);
"##;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_studio_creation() {
        let studio = Studio::new(StudioConfig::default());
        assert!(!studio.is_enabled());
    }

    #[test]
    fn test_studio_development() {
        let studio = Studio::development();
        assert!(studio.is_enabled());
        assert!(!studio.config().auth_enabled);
    }

    #[test]
    fn test_embedded_html() {
        let html = Studio::generate_index_html();
        assert!(html.contains("Ferrite Studio"));
        assert!(html.contains("dashboard"));
    }

    #[test]
    fn test_embedded_css() {
        let css = Studio::generate_css();
        assert!(css.contains("--bg-primary"));
        assert!(css.contains(".metric-card"));
    }

    #[test]
    fn test_embedded_js() {
        let js = Studio::generate_js();
        assert!(js.contains("loadDashboard"));
        assert!(js.contains("API_BASE"));
    }

    #[test]
    fn test_studio_disabled_start() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let mut studio = Studio::new(StudioConfig::default());
            // Disabled studio should return Ok without starting
            studio.start().await.unwrap();
        });
    }

    #[test]
    fn test_studio_stop() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let mut studio = Studio::new(StudioConfig::default());
            // Stop without start should be fine
            studio.stop().await.unwrap();
        });
    }

    #[test]
    fn test_studio_bind_addr() {
        let studio = Studio::new(StudioConfig::default());
        assert_eq!(studio.bind_addr(), "127.0.0.1:8080");
    }

    #[test]
    fn test_studio_api_accessor() {
        let studio = Studio::new(StudioConfig::default());
        let _api = studio.api();
    }

    #[test]
    fn test_json_response_helper() {
        let response = StudioService::json_response(
            StatusCode::OK,
            &ApiResponse::ok("test".to_string()),
        );
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[test]
    fn test_html_response_helper() {
        let response = StudioService::html_response("<h1>Test</h1>");
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[test]
    fn test_css_response_helper() {
        let response = StudioService::css_response("body { color: red; }");
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[test]
    fn test_js_response_helper() {
        let response = StudioService::js_response("console.log('test');");
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[test]
    fn test_cors_response() {
        let response = StudioService::cors_response(true);
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        assert!(response.headers().contains_key("Access-Control-Allow-Origin"));
    }

    #[test]
    fn test_cors_response_disabled() {
        let response = StudioService::cors_response(false);
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        assert!(!response.headers().contains_key("Access-Control-Allow-Origin"));
    }

    #[test]
    fn test_not_found_response() {
        let response = StudioService::json_response(
            StatusCode::NOT_FOUND,
            &ApiResponse::<()>::err("Not found"),
        );
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[test]
    fn test_html_contains_nav_tabs() {
        let html = Studio::generate_index_html();
        assert!(html.contains("data-tab=\"dashboard\""));
        assert!(html.contains("data-tab=\"keys\""));
        assert!(html.contains("data-tab=\"console\""));
        assert!(html.contains("data-tab=\"serverinfo\""));
        assert!(html.contains("data-tab=\"slowlog\""));
        assert!(html.contains("data-tab=\"cluster\""));
        assert!(html.contains("data-tab=\"config\""));
    }

    #[test]
    fn test_js_contains_all_loaders() {
        let js = Studio::generate_js();
        assert!(js.contains("loadKeys"));
        assert!(js.contains("loadSlowLog"));
        assert!(js.contains("loadCluster"));
        assert!(js.contains("loadConfig"));
        assert!(js.contains("loadServerInfo"));
        assert!(js.contains("runConsoleCommand"));
        assert!(js.contains("viewKey"));
        assert!(js.contains("deleteKey"));
    }

    // Integration routing tests using real TCP connections

    async fn send_http_request(port: u16, request: &str) -> String {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let mut stream =
            tokio::net::TcpStream::connect(format!("127.0.0.1:{}", port))
                .await
                .unwrap();
        stream.write_all(request.as_bytes()).await.unwrap();

        let mut buf = Vec::new();
        stream.read_to_end(&mut buf).await.unwrap();
        String::from_utf8_lossy(&buf).to_string()
    }

    async fn start_test_server() -> u16 {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .unwrap();
        let port = listener.local_addr().unwrap().port();

        let api = Arc::new(StudioApi::new());
        let auth = Arc::new(AuthManager::new(super::super::auth::AuthConfig {
            enabled: false,
            ..Default::default()
        }));
        let console = Arc::new(Console::new());
        let metrics_collector = Arc::new(MetricsCollector::new());
        let config = StudioConfig::development();

        tokio::spawn(async move {
            // Accept connections until the listener is dropped
            while let Ok((stream, _)) = listener.accept().await {
                let io = TokioIo::new(stream);
                let api = api.clone();
                let auth = auth.clone();
                let console = console.clone();
                let metrics_collector = metrics_collector.clone();
                let config = config.clone();
                tokio::spawn(async move {
                    let service = StudioService::new(
                        api, auth, console, metrics_collector, config,
                    );
                    let _ = http1::Builder::new()
                        .serve_connection(io, service)
                        .await;
                });
            }
        });

        // Give the server a moment to start accepting
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        port
    }

    #[tokio::test]
    async fn test_route_health_endpoint() {
        let port = start_test_server().await;
        let response = send_http_request(
            port,
            "GET /api/health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
        )
        .await;
        assert!(response.contains("200 OK"));
        assert!(response.contains("\"status\":\"ok\""));
        assert!(response.contains("uptime_secs"));
    }

    #[tokio::test]
    async fn test_route_metrics_endpoint() {
        let port = start_test_server().await;
        let response = send_http_request(
            port,
            "GET /api/metrics HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
        )
        .await;
        assert!(response.contains("200 OK"));
        assert!(response.contains("ops_per_sec"));
        assert!(response.contains("uptime_secs"));
        assert!(response.contains("total_commands"));
    }

    #[tokio::test]
    async fn test_route_server_info() {
        let port = start_test_server().await;
        let response = send_http_request(
            port,
            "GET /api/server/info HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
        )
        .await;
        assert!(response.contains("200 OK"));
        assert!(response.contains("version"));
    }

    #[tokio::test]
    async fn test_route_404_unknown_path() {
        let port = start_test_server().await;
        let response = send_http_request(
            port,
            "GET /api/nonexistent HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
        )
        .await;
        assert!(response.contains("404"));
        assert!(response.contains("Not found"));
    }

    #[tokio::test]
    async fn test_route_index_html() {
        let port = start_test_server().await;
        let response = send_http_request(
            port,
            "GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
        )
        .await;
        assert!(response.contains("200 OK"));
        assert!(response.contains("Ferrite Studio"));
    }

    #[tokio::test]
    async fn test_route_cors_preflight() {
        let port = start_test_server().await;
        let response = send_http_request(
            port,
            "OPTIONS /api/health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
        )
        .await;
        assert!(response.contains("204"));
        assert!(response.contains("access-control-allow-origin"));
    }

    #[tokio::test]
    async fn test_route_slowlog() {
        let port = start_test_server().await;
        let response = send_http_request(
            port,
            "GET /api/slowlog HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
        )
        .await;
        assert!(response.contains("200 OK"));
    }

    #[tokio::test]
    async fn test_route_cluster() {
        let port = start_test_server().await;
        let response = send_http_request(
            port,
            "GET /api/cluster HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
        )
        .await;
        assert!(response.contains("200 OK"));
    }

    #[tokio::test]
    async fn test_route_config() {
        let port = start_test_server().await;
        let response = send_http_request(
            port,
            "GET /api/config HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
        )
        .await;
        assert!(response.contains("200 OK"));
    }

    #[tokio::test]
    async fn test_route_keys_scan() {
        let port = start_test_server().await;
        let response = send_http_request(
            port,
            "GET /api/keys?pattern=*&cursor=0 HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
        )
        .await;
        assert!(response.contains("200 OK"));
        assert!(response.contains("keys"));
    }

    #[tokio::test]
    async fn test_route_keys_get_specific() {
        let port = start_test_server().await;
        let response = send_http_request(
            port,
            "GET /api/keys/mykey HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
        )
        .await;
        assert!(response.contains("200 OK"));
    }

    #[tokio::test]
    async fn test_route_server_memory() {
        let port = start_test_server().await;
        let response = send_http_request(
            port,
            "GET /api/server/memory HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
        )
        .await;
        assert!(response.contains("200 OK"));
        assert!(response.contains("used_memory"));
    }

    #[tokio::test]
    async fn test_route_auth_login() {
        let port = start_test_server().await;
        let body = r#"{"username":"admin","password":"admin"}"#;
        let request = format!(
            "POST /api/auth/login HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        );
        let response = send_http_request(port, &request).await;
        // Auth is disabled in test server, so login returns error
        assert!(response.contains("401") || response.contains("200"));
    }

    #[tokio::test]
    async fn test_route_console_commands() {
        let port = start_test_server().await;
        let response = send_http_request(
            port,
            "GET /api/console/commands HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
        )
        .await;
        assert!(response.contains("200 OK"));
        assert!(response.contains("GET"));
    }

    #[tokio::test]
    async fn test_route_console_autocomplete() {
        let port = start_test_server().await;
        let response = send_http_request(
            port,
            "GET /api/console/autocomplete?input=GE HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
        )
        .await;
        assert!(response.contains("200 OK"));
        assert!(response.contains("GET"));
    }

    #[tokio::test]
    async fn test_route_metrics_snapshot() {
        let port = start_test_server().await;
        let response = send_http_request(
            port,
            "GET /api/metrics/snapshot HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
        )
        .await;
        assert!(response.contains("200 OK"));
        assert!(response.contains("ops_per_sec"));
    }

    #[tokio::test]
    async fn test_route_server_panel() {
        let port = start_test_server().await;
        let response = send_http_request(
            port,
            "GET /api/server/panel HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
        )
        .await;
        assert!(response.contains("200 OK"));
        assert!(response.contains("replication"));
        assert!(response.contains("persistence"));
    }

    #[tokio::test]
    async fn test_route_key_detail() {
        let port = start_test_server().await;
        let response = send_http_request(
            port,
            "GET /api/keys/detail/mykey HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
        )
        .await;
        assert!(response.contains("200 OK"));
        assert!(response.contains("encoding"));
    }

    #[tokio::test]
    async fn test_route_console_history() {
        let port = start_test_server().await;
        let response = send_http_request(
            port,
            "GET /api/console/history HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
        )
        .await;
        assert!(response.contains("200 OK"));
    }

    #[test]
    fn test_studio_auth_accessor() {
        let studio = Studio::new(StudioConfig::default());
        assert!(studio.auth().is_enabled());
    }

    #[test]
    fn test_studio_console_accessor() {
        let studio = Studio::new(StudioConfig::default());
        assert!(studio.console().available_commands().len() > 0);
    }

    #[test]
    fn test_studio_metrics_accessor() {
        let studio = Studio::new(StudioConfig::default());
        let snap = studio.metrics_collector().snapshot();
        assert!(!snap.version.is_empty());
    }
}
