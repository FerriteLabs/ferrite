//! MARKETPLACE.* command handlers
//!
//! MARKETPLACE.INSTALL, MARKETPLACE.UNINSTALL, MARKETPLACE.ENABLE,
//! MARKETPLACE.DISABLE, MARKETPLACE.UPDATE, MARKETPLACE.LIST,
//! MARKETPLACE.SEARCH, MARKETPLACE.INFO, MARKETPLACE.VERIFY,
//! MARKETPLACE.STATS, MARKETPLACE.HELP
#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::OnceLock;

use bytes::Bytes;

use crate::protocol::Frame;
use ferrite_plugins::marketplace::registry::{
    ExtensionRegistry, RegistryConfig,
};

use super::err_frame;

/// Global extension registry singleton.
static EXTENSION_REGISTRY: OnceLock<ExtensionRegistry> = OnceLock::new();

fn get_registry() -> &'static ExtensionRegistry {
    EXTENSION_REGISTRY.get_or_init(|| ExtensionRegistry::new(RegistryConfig::default()))
}

/// Dispatch a `MARKETPLACE` subcommand.
pub fn marketplace_command(subcommand: &str, args: &[String]) -> Frame {
    match subcommand.to_uppercase().as_str() {
        "INSTALL" => handle_install(args),
        "UNINSTALL" => handle_uninstall(args),
        "ENABLE" => handle_enable(args),
        "DISABLE" => handle_disable(args),
        "UPDATE" => handle_update(args),
        "LIST" => handle_list(),
        "SEARCH" => handle_search(args),
        "INFO" => handle_info(args),
        "VERIFY" => handle_verify(args),
        "STATS" => handle_stats(),
        "HELP" | "" => handle_help(),
        other => err_frame(&format!("unknown MARKETPLACE subcommand '{}'", other)),
    }
}

// ---------------------------------------------------------------------------
// MARKETPLACE.INSTALL
// ---------------------------------------------------------------------------

fn handle_install(args: &[String]) -> Frame {
    let name = match args.first() {
        Some(n) => n.as_str(),
        None => return err_frame("MARKETPLACE.INSTALL requires an extension name"),
    };
    let version = args.get(1).map(|s| s.as_str());

    match get_registry().install(name, version) {
        Ok(ext) => {
            let mut map = HashMap::new();
            map.insert(
                Bytes::from_static(b"name"),
                Frame::Bulk(Some(Bytes::from(ext.manifest.name.clone()))),
            );
            map.insert(
                Bytes::from_static(b"version"),
                Frame::Bulk(Some(Bytes::from(ext.manifest.version.clone()))),
            );
            map.insert(
                Bytes::from_static(b"status"),
                Frame::Bulk(Some(Bytes::from("installed"))),
            );
            Frame::Map(map)
        }
        Err(e) => err_frame(&format!("MARKETPLACE.INSTALL failed: {}", e)),
    }
}

// ---------------------------------------------------------------------------
// MARKETPLACE.UNINSTALL
// ---------------------------------------------------------------------------

fn handle_uninstall(args: &[String]) -> Frame {
    let name = match args.first() {
        Some(n) => n.as_str(),
        None => return err_frame("MARKETPLACE.UNINSTALL requires an extension name"),
    };
    match get_registry().uninstall(name) {
        Ok(()) => Frame::Simple(Bytes::from("OK")),
        Err(e) => err_frame(&format!("MARKETPLACE.UNINSTALL failed: {}", e)),
    }
}

// ---------------------------------------------------------------------------
// MARKETPLACE.ENABLE / DISABLE
// ---------------------------------------------------------------------------

fn handle_enable(args: &[String]) -> Frame {
    let name = match args.first() {
        Some(n) => n.as_str(),
        None => return err_frame("MARKETPLACE.ENABLE requires an extension name"),
    };
    match get_registry().enable(name) {
        Ok(()) => Frame::Simple(Bytes::from("OK")),
        Err(e) => err_frame(&format!("MARKETPLACE.ENABLE failed: {}", e)),
    }
}

fn handle_disable(args: &[String]) -> Frame {
    let name = match args.first() {
        Some(n) => n.as_str(),
        None => return err_frame("MARKETPLACE.DISABLE requires an extension name"),
    };
    match get_registry().disable(name) {
        Ok(()) => Frame::Simple(Bytes::from("OK")),
        Err(e) => err_frame(&format!("MARKETPLACE.DISABLE failed: {}", e)),
    }
}

// ---------------------------------------------------------------------------
// MARKETPLACE.UPDATE
// ---------------------------------------------------------------------------

fn handle_update(args: &[String]) -> Frame {
    let name = match args.first() {
        Some(n) => n.as_str(),
        None => return err_frame("MARKETPLACE.UPDATE requires an extension name"),
    };
    match get_registry().update(name) {
        Ok(ext) => {
            let mut map = HashMap::new();
            map.insert(
                Bytes::from_static(b"name"),
                Frame::Bulk(Some(Bytes::from(ext.manifest.name.clone()))),
            );
            map.insert(
                Bytes::from_static(b"version"),
                Frame::Bulk(Some(Bytes::from(ext.manifest.version.clone()))),
            );
            map.insert(
                Bytes::from_static(b"status"),
                Frame::Bulk(Some(Bytes::from("updated"))),
            );
            Frame::Map(map)
        }
        Err(e) => err_frame(&format!("MARKETPLACE.UPDATE failed: {}", e)),
    }
}

// ---------------------------------------------------------------------------
// MARKETPLACE.LIST
// ---------------------------------------------------------------------------

fn handle_list() -> Frame {
    let extensions = get_registry().list_installed();
    let items: Vec<Frame> = extensions
        .iter()
        .map(|ext| {
            let mut map = HashMap::new();
            map.insert(
                Bytes::from_static(b"name"),
                Frame::Bulk(Some(Bytes::from(ext.manifest.name.clone()))),
            );
            map.insert(
                Bytes::from_static(b"version"),
                Frame::Bulk(Some(Bytes::from(ext.manifest.version.clone()))),
            );
            map.insert(
                Bytes::from_static(b"enabled"),
                Frame::Boolean(ext.enabled),
            );
            map.insert(
                Bytes::from_static(b"invocations"),
                Frame::Integer(ext.invocations as i64),
            );
            Frame::Map(map)
        })
        .collect();
    Frame::Array(Some(items))
}

// ---------------------------------------------------------------------------
// MARKETPLACE.SEARCH
// ---------------------------------------------------------------------------

fn handle_search(args: &[String]) -> Frame {
    let query = match args.first() {
        Some(q) => q.as_str(),
        None => return err_frame("MARKETPLACE.SEARCH requires a query string"),
    };
    let results = get_registry().search(query);
    let items: Vec<Frame> = results
        .iter()
        .map(|m| {
            let mut map = HashMap::new();
            map.insert(
                Bytes::from_static(b"name"),
                Frame::Bulk(Some(Bytes::from(m.name.clone()))),
            );
            map.insert(
                Bytes::from_static(b"version"),
                Frame::Bulk(Some(Bytes::from(m.version.clone()))),
            );
            map.insert(
                Bytes::from_static(b"description"),
                Frame::Bulk(Some(Bytes::from(m.description.clone()))),
            );
            map.insert(
                Bytes::from_static(b"author"),
                Frame::Bulk(Some(Bytes::from(m.author.clone()))),
            );
            Frame::Map(map)
        })
        .collect();
    Frame::Array(Some(items))
}

// ---------------------------------------------------------------------------
// MARKETPLACE.INFO
// ---------------------------------------------------------------------------

fn handle_info(args: &[String]) -> Frame {
    let name = match args.first() {
        Some(n) => n.as_str(),
        None => return err_frame("MARKETPLACE.INFO requires an extension name"),
    };
    match get_registry().info(name) {
        Some(info) => {
            let mut map = HashMap::new();
            map.insert(
                Bytes::from_static(b"name"),
                Frame::Bulk(Some(Bytes::from(info.manifest.name.clone()))),
            );
            map.insert(
                Bytes::from_static(b"version"),
                Frame::Bulk(Some(Bytes::from(info.manifest.version.clone()))),
            );
            map.insert(
                Bytes::from_static(b"description"),
                Frame::Bulk(Some(Bytes::from(info.manifest.description.clone()))),
            );
            map.insert(
                Bytes::from_static(b"author"),
                Frame::Bulk(Some(Bytes::from(info.manifest.author.clone()))),
            );
            map.insert(
                Bytes::from_static(b"installed"),
                Frame::Boolean(info.installed),
            );
            map.insert(
                Bytes::from_static(b"downloads"),
                Frame::Integer(info.downloads as i64),
            );
            map.insert(
                Bytes::from_static(b"rating"),
                Frame::Double(info.rating),
            );
            Frame::Map(map)
        }
        None => err_frame(&format!("extension '{}' not found", name)),
    }
}

// ---------------------------------------------------------------------------
// MARKETPLACE.VERIFY
// ---------------------------------------------------------------------------

fn handle_verify(args: &[String]) -> Frame {
    let name = match args.first() {
        Some(n) => n.as_str(),
        None => return err_frame("MARKETPLACE.VERIFY requires an extension name"),
    };
    let result = get_registry().verify(name);
    let mut map = HashMap::new();
    map.insert(
        Bytes::from_static(b"name"),
        Frame::Bulk(Some(Bytes::from(result.name.clone()))),
    );
    map.insert(
        Bytes::from_static(b"checksum_valid"),
        Frame::Boolean(result.checksum_valid),
    );
    map.insert(
        Bytes::from_static(b"signature_valid"),
        Frame::Boolean(result.signature_valid),
    );
    map.insert(
        Bytes::from_static(b"permissions_ok"),
        Frame::Boolean(result.permissions_ok),
    );
    let issues: Vec<Frame> = result
        .issues
        .iter()
        .map(|i| Frame::Bulk(Some(Bytes::from(i.clone()))))
        .collect();
    map.insert(
        Bytes::from_static(b"issues"),
        Frame::Array(Some(issues)),
    );
    Frame::Map(map)
}

// ---------------------------------------------------------------------------
// MARKETPLACE.STATS
// ---------------------------------------------------------------------------

fn handle_stats() -> Frame {
    let stats = get_registry().stats();
    let mut map = HashMap::new();
    map.insert(
        Bytes::from_static(b"installed"),
        Frame::Integer(stats.installed as i64),
    );
    map.insert(
        Bytes::from_static(b"active"),
        Frame::Integer(stats.active as i64),
    );
    map.insert(
        Bytes::from_static(b"total_invocations"),
        Frame::Integer(stats.total_invocations as i64),
    );
    map.insert(
        Bytes::from_static(b"total_errors"),
        Frame::Integer(stats.total_errors as i64),
    );
    map.insert(
        Bytes::from_static(b"cache_size_bytes"),
        Frame::Integer(stats.cache_size_bytes as i64),
    );
    Frame::Map(map)
}

// ---------------------------------------------------------------------------
// MARKETPLACE.HELP
// ---------------------------------------------------------------------------

fn handle_help() -> Frame {
    let help = vec![
        "MARKETPLACE.INSTALL <name> [version]",
        "MARKETPLACE.UNINSTALL <name>",
        "MARKETPLACE.ENABLE <name>",
        "MARKETPLACE.DISABLE <name>",
        "MARKETPLACE.UPDATE <name>",
        "MARKETPLACE.LIST",
        "MARKETPLACE.SEARCH <query>",
        "MARKETPLACE.INFO <name>",
        "MARKETPLACE.VERIFY <name>",
        "MARKETPLACE.STATS",
        "MARKETPLACE.HELP",
    ];
    Frame::Array(Some(
        help.iter().map(|s| Frame::Bulk(Some(Bytes::from(*s)))).collect(),
    ))
}
