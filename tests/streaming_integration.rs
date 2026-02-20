//! Integration tests for ferrite-streaming.
//!
//! Validates the stream engine, event construction, CDC capture,
//! and pipeline builder APIs.

use ferrite_streaming::cdc::CdcEngine;
use ferrite_streaming::streaming::{StreamConfig, StreamEngine, StreamEvent};

#[test]
fn test_stream_event_creation() {
    let event = StreamEvent::new(
        Some("user:1".to_string()),
        serde_json::json!({"name": "Alice"}),
    );
    assert_eq!(event.key.as_deref(), Some("user:1"));
}

#[test]
fn test_stream_event_with_metadata() {
    let event = StreamEvent::new(
        Some("key".to_string()),
        serde_json::json!("value"),
    )
    .with_header("source", "test")
    .with_partition(0);

    assert_eq!(event.get_str("source"), Some("test"));
}

#[test]
fn test_stream_config_defaults() {
    let config = StreamConfig::default();
    assert!(
        config.num_threads > 0,
        "should have positive thread count"
    );
}

#[test]
fn test_stream_engine_creation() {
    let config = StreamConfig::default();
    let engine = StreamEngine::new(config);
    // Engine should be created without errors (no async needed)
    let _ = engine;
}

#[tokio::test]
async fn test_cdc_engine_creation() {
    let engine = CdcEngine::default();
    let info = engine.info().await;
    assert_eq!(
        info.subscriptions, 0,
        "new CDC engine should have no subscriptions"
    );
}

#[tokio::test]
async fn test_cdc_subscribe() {
    let engine = CdcEngine::default();
    let result = engine
        .subscribe(
            "test_sub",
            vec!["user:*".to_string()],
            Default::default(),
        )
        .await;
    assert!(result.is_ok(), "subscribe should succeed");

    let info = engine.info().await;
    assert_eq!(info.subscriptions, 1, "should have one subscription");
}
