//! PROXY.* command handlers for managing the smart proxy
//!
//! PROXY.STATUS, PROXY.UPSTREAM, PROXY.DISCOVER, PROXY.FEATURES,
//! PROXY.CACHE, PROXY.INTERCEPT, PROXY.STATS, PROXY.HELP
#![allow(dead_code)]

use std::sync::OnceLock;

use bytes::Bytes;

use crate::protocol::Frame;
use crate::server::smart_proxy::{SmartProxy, SmartProxyConfig};

use super::err_frame;

/// Global smart proxy singleton.
static SMART_PROXY: OnceLock<SmartProxy> = OnceLock::new();

fn get_proxy() -> &'static SmartProxy {
    SMART_PROXY.get_or_init(|| SmartProxy::new(SmartProxyConfig::default()))
}

/// Helper to create a bulk frame from an owned String.
fn bulk(s: impl Into<String>) -> Frame {
    Frame::Bulk(Some(Bytes::from(s.into())))
}

/// Dispatch a `PROXY` subcommand.
pub fn proxy_command(subcommand: &str, args: &[String]) -> Frame {
    match subcommand.to_uppercase().as_str() {
        "STATUS" => proxy_status(),
        "UPSTREAM" => proxy_upstream(),
        "DISCOVER" => proxy_discover(),
        "FEATURES" => proxy_features(args),
        "CACHE" => proxy_cache(args),
        "INTERCEPT" => proxy_intercept(args),
        "STATS" => proxy_stats(),
        "HELP" => proxy_help(),
        _ => err_frame(&format!("unknown PROXY subcommand '{}'", subcommand)),
    }
}

/// PROXY.STATUS — return current proxy state.
fn proxy_status() -> Frame {
    let proxy = get_proxy();
    let stats = proxy.stats();
    let mut items = Vec::new();
    items.push(bulk("clients_connected"));
    items.push(Frame::Integer(i64::from(stats.clients_connected)));
    items.push(bulk("uptime_secs"));
    items.push(Frame::Integer(stats.uptime_secs as i64));
    items.push(bulk("topology"));
    items.push(bulk(stats.discovered_topology));
    Frame::Array(Some(items))
}

/// PROXY.UPSTREAM — show upstream address and topology.
fn proxy_upstream() -> Frame {
    let proxy = get_proxy();
    let stats = proxy.stats();
    let mut items = Vec::new();
    items.push(bulk("upstream_addr"));
    items.push(bulk("127.0.0.1:6379"));
    items.push(bulk("topology"));
    items.push(bulk(stats.discovered_topology));
    items.push(bulk("latency_avg_us"));
    items.push(Frame::Integer(stats.upstream_latency_avg_us as i64));
    Frame::Array(Some(items))
}

/// PROXY.DISCOVER — trigger upstream topology discovery.
fn proxy_discover() -> Frame {
    let proxy = get_proxy();
    match proxy.discover_upstream() {
        Ok(topo) => bulk(format!("OK discovered: {}", topo)),
        Err(e) => err_frame(&format!("discovery failed: {}", e)),
    }
}

/// PROXY.FEATURES [ENABLE|DISABLE feature]
fn proxy_features(args: &[String]) -> Frame {
    if args.is_empty() {
        let proxy = get_proxy();
        let stats = proxy.stats();
        let mut items = Vec::new();
        items.push(bulk("commands_intercepted"));
        items.push(Frame::Integer(stats.commands_intercepted as i64));
        items.push(bulk("commands_forwarded"));
        items.push(Frame::Integer(stats.commands_forwarded as i64));
        return Frame::Array(Some(items));
    }

    let action = args[0].to_uppercase();
    let feature = args.get(1).map(|s| s.to_uppercase()).unwrap_or_default();

    match action.as_str() {
        "ENABLE" | "DISABLE" => {
            let valid = matches!(
                feature.as_str(),
                "TIERING" | "VECTOR_SEARCH" | "OBSERVABILITY" | "CACHING" | "METRICS"
            );
            if valid {
                bulk(format!("OK {} {}", action, feature))
            } else {
                err_frame(&format!("unknown feature '{}'", feature))
            }
        }
        _ => err_frame("usage: PROXY.FEATURES [ENABLE|DISABLE feature]"),
    }
}

/// PROXY.CACHE [HIT_RATE|FLUSH|SIZE]
fn proxy_cache(args: &[String]) -> Frame {
    let proxy = get_proxy();
    let sub = args
        .first()
        .map(|s| s.to_uppercase())
        .unwrap_or_else(|| "SIZE".to_string());

    match sub.as_str() {
        "HIT_RATE" => {
            let stats = proxy.stats();
            bulk(format!("{:.4}", stats.cache_hit_rate))
        }
        "FLUSH" => {
            let flushed = proxy.flush_cache();
            Frame::Integer(flushed as i64)
        }
        "SIZE" => Frame::Integer(proxy.cache_size() as i64),
        _ => err_frame("usage: PROXY.CACHE [HIT_RATE|FLUSH|SIZE]"),
    }
}

/// PROXY.INTERCEPT [SHOW|ADD|REMOVE pattern]
fn proxy_intercept(args: &[String]) -> Frame {
    let proxy = get_proxy();
    let sub = args
        .first()
        .map(|s| s.to_uppercase())
        .unwrap_or_else(|| "SHOW".to_string());

    match sub.as_str() {
        "SHOW" => {
            let rules = proxy.list_intercept_rules();
            let items: Vec<Frame> = rules
                .iter()
                .map(|(k, v)| bulk(format!("{} -> {:?}", k, v)))
                .collect();
            Frame::Array(Some(items))
        }
        "ADD" => {
            if args.len() < 2 {
                return err_frame("usage: PROXY.INTERCEPT ADD pattern");
            }
            let pattern = args[1].to_uppercase();
            proxy.add_intercept_rule(
                pattern.clone(),
                crate::server::smart_proxy::InterceptAction::HandleLocally,
            );
            bulk(format!("OK added {}", pattern))
        }
        "REMOVE" => {
            if args.len() < 2 {
                return err_frame("usage: PROXY.INTERCEPT REMOVE pattern");
            }
            let pattern = args[1].to_uppercase();
            if proxy.remove_intercept_rule(&pattern) {
                bulk(format!("OK removed {}", pattern))
            } else {
                err_frame(&format!("rule '{}' not found", pattern))
            }
        }
        _ => err_frame("usage: PROXY.INTERCEPT [SHOW|ADD|REMOVE pattern]"),
    }
}

/// PROXY.STATS — return detailed proxy statistics.
fn proxy_stats() -> Frame {
    let proxy = get_proxy();
    let stats = proxy.stats();
    let mut items = Vec::new();

    items.push(bulk("commands_forwarded"));
    items.push(Frame::Integer(stats.commands_forwarded as i64));
    items.push(bulk("commands_intercepted"));
    items.push(Frame::Integer(stats.commands_intercepted as i64));
    items.push(bulk("commands_cached"));
    items.push(Frame::Integer(stats.commands_cached as i64));
    items.push(bulk("cache_hit_rate"));
    items.push(bulk(format!("{:.4}", stats.cache_hit_rate)));
    items.push(bulk("upstream_latency_avg_us"));
    items.push(Frame::Integer(stats.upstream_latency_avg_us as i64));
    items.push(bulk("clients_connected"));
    items.push(Frame::Integer(i64::from(stats.clients_connected)));
    items.push(bulk("uptime_secs"));
    items.push(Frame::Integer(stats.uptime_secs as i64));
    items.push(bulk("discovered_topology"));
    items.push(bulk(stats.discovered_topology));

    Frame::Array(Some(items))
}

/// PROXY.HELP — return help text.
fn proxy_help() -> Frame {
    let help = vec![
        "PROXY.STATUS - Show proxy status",
        "PROXY.UPSTREAM - Show upstream info and topology",
        "PROXY.DISCOVER - Trigger upstream topology discovery",
        "PROXY.FEATURES [ENABLE|DISABLE feature] - Manage feature toggles",
        "PROXY.CACHE [HIT_RATE|FLUSH|SIZE] - Manage local cache",
        "PROXY.INTERCEPT [SHOW|ADD|REMOVE pattern] - Manage intercept rules",
        "PROXY.STATS - Show detailed statistics",
        "PROXY.HELP - Show this help",
    ];
    let items: Vec<Frame> = help.iter().map(|s| bulk(s.to_string())).collect();
    Frame::Array(Some(items))
}
