//! Built-in Plugin Templates
//!
//! Provides template manifests and example WASM plugin skeletons for common
//! plugin types. These help developers bootstrap new plugins quickly.
//!
//! # Available Templates
//!
//! - **Rate Limiter** – Token-bucket rate limiting per key
//! - **Custom Data Type** – Define a new Redis-like data structure
//! - **External Connector** – Import/export data from external sources

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Template Definition
// ---------------------------------------------------------------------------

/// A plugin template that can be used to scaffold a new plugin project.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PluginTemplate {
    /// Template identifier.
    pub id: String,
    /// Human-readable name.
    pub name: String,
    /// Description of what this template provides.
    pub description: String,
    /// The TOML manifest content for `ferrite-plugin.toml`.
    pub manifest_toml: String,
    /// Example Rust source for the WASM plugin.
    pub example_source: String,
    /// Category this template belongs to.
    pub category: String,
    /// Tags for discovery.
    pub tags: Vec<String>,
}

// ---------------------------------------------------------------------------
// Built-in templates
// ---------------------------------------------------------------------------

/// Returns all built-in plugin templates.
pub fn builtin_templates() -> Vec<PluginTemplate> {
    vec![
        rate_limiter_template(),
        custom_data_type_template(),
        external_connector_template(),
    ]
}

/// Find a built-in template by ID.
pub fn get_template(id: &str) -> Option<PluginTemplate> {
    builtin_templates().into_iter().find(|t| t.id == id)
}

/// List available template IDs and names.
pub fn list_templates() -> Vec<(String, String)> {
    builtin_templates()
        .into_iter()
        .map(|t| (t.id, t.name))
        .collect()
}

// ---------------------------------------------------------------------------
// Rate Limiter Template
// ---------------------------------------------------------------------------

fn rate_limiter_template() -> PluginTemplate {
    PluginTemplate {
        id: "rate-limiter".to_string(),
        name: "Rate Limiter Plugin".to_string(),
        description: "Token-bucket rate limiting. Tracks request counts per key with \
                      configurable window and limit."
            .to_string(),
        category: "security".to_string(),
        tags: vec![
            "rate-limit".to_string(),
            "security".to_string(),
            "throttle".to_string(),
        ],
        manifest_toml: r#"[module]
name = "my-rate-limiter"
version = "0.1.0"
author = "Your Name"
description = "Token-bucket rate limiting for Ferrite"
license = "MIT"
categories = ["security"]
tags = ["rate-limit", "throttle"]
min_ferrite_version = "0.1.0"

[dependencies]
"#
        .to_string(),
        example_source: r#"//! Rate Limiter WASM Plugin
//!
//! Commands:
//!   RATELIMIT.CHECK <key> <limit> <window_secs>
//!   RATELIMIT.RESET <key>
//!
//! Compile: cargo build --target wasm32-unknown-unknown --release

use ferrite_wasm_sdk::*;

/// Check whether a request is allowed under the rate limit.
///
/// Returns 1 if allowed, 0 if rate-limited.
#[ferrite_function]
fn ratelimit_check(key: &str, limit: i64, window_secs: i64) -> Result<i64, String> {
    let counter_key = format!("ratelimit:{}", key);

    // Get current count
    let count: i64 = ferrite::get(&counter_key)
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);

    if count >= limit {
        return Ok(0); // Rate limited
    }

    // Increment counter
    ferrite::incr(&counter_key)?;

    // Set expiry on first request in window
    if count == 0 {
        ferrite::expire(&counter_key, window_secs)?;
    }

    Ok(1) // Allowed
}

/// Reset the rate limit counter for a key.
#[ferrite_function]
fn ratelimit_reset(key: &str) -> Result<i64, String> {
    let counter_key = format!("ratelimit:{}", key);
    ferrite::del(&counter_key)?;
    Ok(1)
}
"#
        .to_string(),
    }
}

// ---------------------------------------------------------------------------
// Custom Data Type Template
// ---------------------------------------------------------------------------

fn custom_data_type_template() -> PluginTemplate {
    PluginTemplate {
        id: "custom-data-type".to_string(),
        name: "Custom Data Type Plugin".to_string(),
        description: "Define a new Redis-compatible data type backed by WASM. \
                      Includes encode/decode, command handlers, and RDB persistence hooks."
            .to_string(),
        category: "data-processing".to_string(),
        tags: vec![
            "data-type".to_string(),
            "custom".to_string(),
            "structure".to_string(),
        ],
        manifest_toml: r#"[module]
name = "my-custom-type"
version = "0.1.0"
author = "Your Name"
description = "Custom data type for Ferrite"
license = "MIT"
categories = ["data-processing"]
tags = ["data-type", "custom"]
min_ferrite_version = "0.1.0"

[dependencies]
"#
        .to_string(),
        example_source: r#"//! Custom Data Type WASM Plugin
//!
//! Implements a Bloom Filter data type with commands:
//!   BF.ADD <key> <item>
//!   BF.EXISTS <key> <item>
//!   BF.INFO <key>
//!
//! Compile: cargo build --target wasm32-unknown-unknown --release

use ferrite_wasm_sdk::*;

/// Add an item to a Bloom filter stored at the given key.
///
/// Creates the filter if it doesn't exist. Returns 1 if the item
/// was newly added, 0 if it might already exist.
#[ferrite_function]
fn bf_add(key: &str, item: &str) -> Result<i64, String> {
    // Load existing filter data or create new
    let filter_key = format!("bf:{}", key);
    let mut bits: Vec<u8> = ferrite::get(&filter_key)
        .map(|v| v.into_bytes())
        .unwrap_or_else(|| vec![0u8; 128]); // 1024-bit filter

    let h1 = hash_fnv(item.as_bytes(), 0) as usize % (bits.len() * 8);
    let h2 = hash_fnv(item.as_bytes(), 1) as usize % (bits.len() * 8);

    let was_set = get_bit(&bits, h1) && get_bit(&bits, h2);

    set_bit(&mut bits, h1);
    set_bit(&mut bits, h2);

    ferrite::set(&filter_key, &String::from_utf8_lossy(&bits))?;

    Ok(if was_set { 0 } else { 1 })
}

/// Check if an item might exist in the Bloom filter.
///
/// Returns 1 if the item might exist, 0 if it definitely does not.
#[ferrite_function]
fn bf_exists(key: &str, item: &str) -> Result<i64, String> {
    let filter_key = format!("bf:{}", key);
    let bits: Vec<u8> = ferrite::get(&filter_key)
        .map(|v| v.into_bytes())
        .unwrap_or_default();

    if bits.is_empty() {
        return Ok(0);
    }

    let h1 = hash_fnv(item.as_bytes(), 0) as usize % (bits.len() * 8);
    let h2 = hash_fnv(item.as_bytes(), 1) as usize % (bits.len() * 8);

    Ok(if get_bit(&bits, h1) && get_bit(&bits, h2) { 1 } else { 0 })
}

fn hash_fnv(data: &[u8], seed: u64) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325u64.wrapping_add(seed);
    for &b in data {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

fn get_bit(bits: &[u8], pos: usize) -> bool {
    bits[pos / 8] & (1 << (pos % 8)) != 0
}

fn set_bit(bits: &mut [u8], pos: usize) {
    bits[pos / 8] |= 1 << (pos % 8);
}
"#
        .to_string(),
    }
}

// ---------------------------------------------------------------------------
// External Connector Template
// ---------------------------------------------------------------------------

fn external_connector_template() -> PluginTemplate {
    PluginTemplate {
        id: "external-connector".to_string(),
        name: "External Data Source Connector".to_string(),
        description: "Connect Ferrite to an external data source. Implements fetch, \
                      cache, and refresh patterns for remote APIs or databases."
            .to_string(),
        category: "connector".to_string(),
        tags: vec![
            "connector".to_string(),
            "import".to_string(),
            "external".to_string(),
        ],
        manifest_toml: r#"[module]
name = "my-connector"
version = "0.1.0"
author = "Your Name"
description = "External data source connector for Ferrite"
license = "MIT"
categories = ["data-processing"]
tags = ["connector", "import", "external"]
min_ferrite_version = "0.1.0"

[dependencies]
"#
        .to_string(),
        example_source: r#"//! External Connector WASM Plugin
//!
//! Commands:
//!   CONNECTOR.FETCH <source_key> <url>
//!   CONNECTOR.REFRESH <source_key>
//!   CONNECTOR.STATUS <source_key>
//!
//! Compile: cargo build --target wasm32-unknown-unknown --release

use ferrite_wasm_sdk::*;

/// Fetch data from an external source and cache it locally.
///
/// Stores the fetched data under `cache:{source_key}` with a TTL.
/// Returns the cached value.
#[ferrite_function]
fn connector_fetch(source_key: &str, url: &str) -> Result<String, String> {
    let cache_key = format!("cache:{}", source_key);
    let meta_key = format!("cache:{}:meta", source_key);

    // Check if we have a cached value
    if let Some(cached) = ferrite::get(&cache_key) {
        return Ok(cached);
    }

    // In a real plugin with network permissions, this would HTTP GET the URL.
    // For this template we simulate with a placeholder.
    let data = format!("{{\"source\": \"{}\", \"url\": \"{}\", \"ts\": {}}}", 
                        source_key, url, ferrite::time_ms());

    // Cache with 5-minute TTL
    ferrite::set(&cache_key, &data)?;
    ferrite::expire(&cache_key, 300)?;

    // Store metadata
    ferrite::hset(&meta_key, "url", url)?;
    ferrite::hset(&meta_key, "last_fetch", &ferrite::time_ms().to_string())?;
    ferrite::hset(&meta_key, "status", "ok")?;

    Ok(data)
}

/// Force refresh the cached data for a source.
#[ferrite_function]
fn connector_refresh(source_key: &str) -> Result<i64, String> {
    let cache_key = format!("cache:{}", source_key);
    ferrite::del(&cache_key)?;
    Ok(1)
}

/// Get the status of a cached data source.
///
/// Returns JSON with url, last_fetch, and status.
#[ferrite_function]
fn connector_status(source_key: &str) -> Result<String, String> {
    let meta_key = format!("cache:{}:meta", source_key);
    let cache_key = format!("cache:{}", source_key);

    let url = ferrite::hget(&meta_key, "url").unwrap_or_default();
    let last_fetch = ferrite::hget(&meta_key, "last_fetch").unwrap_or_default();
    let status = ferrite::hget(&meta_key, "status").unwrap_or_else(|| "unknown".to_string());
    let ttl = ferrite::ttl(&cache_key).unwrap_or(-1);

    Ok(format!(
        "{{\"url\": \"{}\", \"last_fetch\": {}, \"status\": \"{}\", \"ttl\": {}}}",
        url, last_fetch, status, ttl
    ))
}
"#
        .to_string(),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builtin_templates_exist() {
        let templates = builtin_templates();
        assert_eq!(templates.len(), 3);
    }

    #[test]
    fn test_get_template_by_id() {
        let t = get_template("rate-limiter");
        assert!(t.is_some());
        assert_eq!(t.unwrap().name, "Rate Limiter Plugin");

        let t = get_template("custom-data-type");
        assert!(t.is_some());

        let t = get_template("external-connector");
        assert!(t.is_some());

        assert!(get_template("nonexistent").is_none());
    }

    #[test]
    fn test_list_templates() {
        let list = list_templates();
        assert_eq!(list.len(), 3);
        assert!(list.iter().any(|(id, _)| id == "rate-limiter"));
    }

    #[test]
    fn test_template_manifest_is_valid_toml() {
        for template in builtin_templates() {
            let parsed: Result<toml::Value, _> = toml::from_str(&template.manifest_toml);
            assert!(
                parsed.is_ok(),
                "Template '{}' has invalid TOML manifest: {:?}",
                template.id,
                parsed.err()
            );
        }
    }

    #[test]
    fn test_template_has_required_fields() {
        for template in builtin_templates() {
            assert!(!template.id.is_empty());
            assert!(!template.name.is_empty());
            assert!(!template.description.is_empty());
            assert!(!template.manifest_toml.is_empty());
            assert!(!template.example_source.is_empty());
            assert!(!template.category.is_empty());
        }
    }
}
