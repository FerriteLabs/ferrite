#![allow(dead_code)]
//! Integration point for starting the HTTP gateway alongside the RESP server.

use std::sync::Arc;

use crate::storage::Store;

use super::http_gateway::{GatewayError, HttpGateway, HttpGatewayConfig};

/// Start the HTTP API gateway.
///
/// Creates an [`HttpGateway`] and runs it until the task is cancelled or
/// an error occurs. This function is intended to be spawned as a
/// background task alongside the main RESP server.
pub async fn start_http_gateway(
    config: HttpGatewayConfig,
    store: Arc<Store>,
) -> Result<(), GatewayError> {
    let gateway = HttpGateway::new(config, store);
    gateway.start().await
}
