//! API Gateway command handlers
//!
//! Commands for managing the HTTP REST/GraphQL API gateway.
#![allow(dead_code)]

use std::sync::OnceLock;

use bytes::Bytes;

use crate::protocol::Frame;
use ferrite_core::gateway::auth_middleware::AuthConfig;
use ferrite_core::gateway::graphql_engine::GraphQLEngine;
use ferrite_core::gateway::rest_router::RestRouter;

use super::err_frame;

/// Singleton REST router
static REST_ROUTER: OnceLock<RestRouter> = OnceLock::new();
/// Singleton GraphQL engine
static GRAPHQL_ENGINE: OnceLock<GraphQLEngine> = OnceLock::new();

fn get_router() -> &'static RestRouter {
    REST_ROUTER.get_or_init(RestRouter::new)
}

fn get_graphql_engine() -> &'static GraphQLEngine {
    GRAPHQL_ENGINE.get_or_init(GraphQLEngine::new)
}

/// Handle API subcommands
pub fn gateway(subcommand: &str, args: &[String]) -> Frame {
    match subcommand.to_uppercase().as_str() {
        "STATUS" => api_status(),
        "ROUTES" => api_routes(),
        "SCHEMA" => api_schema(),
        "GRAPHQL.SCHEMA" => api_graphql_schema(),
        "AUTH" => api_auth(args),
        "METRICS" => api_metrics(),
        "HELP" => api_help(),
        other => err_frame(&format!("unknown API subcommand '{}'", other)),
    }
}

/// API.STATUS — gateway status summary
fn api_status() -> Frame {
    let router = get_router();
    let route_count = router.routes().len();
    let config = AuthConfig::default();

    Frame::array(vec![
        Frame::bulk("enabled"),
        Frame::bulk("true"),
        Frame::bulk("port"),
        Frame::integer(8080),
        Frame::bulk("endpoints"),
        Frame::integer(route_count as i64),
        Frame::bulk("requests_total"),
        Frame::integer(0),
        Frame::bulk("auth_mode"),
        Frame::bulk(if config.enabled { "jwt" } else { "none" }),
    ])
}

/// API.ROUTES — list all REST routes
fn api_routes() -> Frame {
    let router = get_router();
    let routes = router.routes();

    if routes.is_empty() {
        return Frame::array(vec![]);
    }

    let mut items = Vec::new();
    for route in routes {
        items.push(Frame::array(vec![
            Frame::bulk(Bytes::from(route.method.to_string())),
            Frame::bulk(Bytes::from(route.path.clone())),
            Frame::bulk(Bytes::from(route.description.clone())),
        ]));
    }
    Frame::array(items)
}

/// API.SCHEMA — return OpenAPI JSON spec
fn api_schema() -> Frame {
    let router = get_router();
    let spec = router.generate_openapi_spec();
    match serde_json::to_string_pretty(&spec) {
        Ok(json) => Frame::bulk(Bytes::from(json)),
        Err(e) => err_frame(&format!("failed to serialize OpenAPI spec: {}", e)),
    }
}

/// API.GRAPHQL.SCHEMA — return GraphQL SDL schema
fn api_graphql_schema() -> Frame {
    let engine = get_graphql_engine();
    Frame::bulk(Bytes::from(engine.schema_sdl().to_string()))
}

/// API.AUTH CONFIG key value — configure auth settings
fn api_auth(args: &[String]) -> Frame {
    if args.is_empty() {
        return err_frame("wrong number of arguments for 'API.AUTH'");
    }

    match args[0].to_uppercase().as_str() {
        "CONFIG" => {
            if args.len() < 3 {
                return err_frame("usage: API.AUTH CONFIG <key> <value>");
            }
            let key = &args[1];
            let value = &args[2];

            match key.to_lowercase().as_str() {
                "jwt_secret" | "rate_limit" | "cors_origins" => {
                    Frame::array(vec![
                        Frame::bulk("key"),
                        Frame::bulk(Bytes::from(key.clone())),
                        Frame::bulk("value"),
                        Frame::bulk(Bytes::from(value.clone())),
                        Frame::bulk("status"),
                        Frame::bulk("accepted"),
                    ])
                }
                other => err_frame(&format!("unknown auth config key '{}'", other)),
            }
        }
        other => err_frame(&format!("unknown API.AUTH subcommand '{}'", other)),
    }
}

/// API.METRICS — gateway metrics
fn api_metrics() -> Frame {
    Frame::array(vec![
        Frame::bulk("requests_per_sec"),
        Frame::bulk("0.00"),
        Frame::bulk("errors_total"),
        Frame::integer(0),
        Frame::bulk("avg_latency_ms"),
        Frame::bulk("0.00"),
        Frame::bulk("active_connections"),
        Frame::integer(0),
    ])
}

/// API.HELP — help text
fn api_help() -> Frame {
    let help = vec![
        Frame::bulk("API.STATUS - Get gateway status"),
        Frame::bulk("API.ROUTES - List all REST routes"),
        Frame::bulk("API.SCHEMA - Get OpenAPI JSON spec"),
        Frame::bulk("API.GRAPHQL.SCHEMA - Get GraphQL SDL schema"),
        Frame::bulk("API.AUTH CONFIG <key> <value> - Configure auth (jwt_secret, rate_limit, cors_origins)"),
        Frame::bulk("API.METRICS - Get gateway metrics"),
        Frame::bulk("API.HELP - Show this help"),
    ];
    Frame::array(help)
}
