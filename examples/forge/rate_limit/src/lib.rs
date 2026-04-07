//! Token-bucket rate limiter — Ferrite Forge WASM module.
//!
//! Each key gets an independent token bucket persisted in KV. Tokens are
//! lazily refilled on every call based on wall-clock time from the `time`
//! host import.

// When building with `cargo component`, uncomment:
// wit_bindgen::generate!({
//     world: "function",
//     path: "../../../crates/ferrite-forge/wit/ferrite.wit",
// });

// ---------------------------------------------------------------------------
// Stub host imports — replaced by real bindings at build time
// ---------------------------------------------------------------------------
mod host {
    pub mod kv {
        pub fn get(_key: &[u8]) -> Option<Vec<u8>> { unimplemented!() }
        pub fn set(_key: &[u8], _value: &[u8]) -> Result<(), String> { unimplemented!() }
        pub fn expire(_key: &[u8], _ttl_ms: u64) -> Result<bool, String> { unimplemented!() }
    }
    pub mod time {
        pub fn now_ms() -> u64 { unimplemented!() }
    }
    pub mod log {
        pub fn info(_msg: &str) {}
    }
}

// ---------------------------------------------------------------------------
// Domain types
// ---------------------------------------------------------------------------
struct Config {
    capacity: u64,
    refill_rate: f64, // tokens per millisecond
}

struct Bucket {
    tokens: f64,
    last_refill_ms: u64,
}

impl Bucket {
    fn from_bytes(bytes: &[u8]) -> Self {
        // In a real build, deserialize with serde / miniserde.
        // Stub: default to a full bucket.
        let _ = bytes;
        Bucket { tokens: 0.0, last_refill_ms: 0 }
    }

    fn to_bytes(&self) -> Vec<u8> {
        format!(r#"{{"tokens":{},"last_refill_ms":{}}}"#, self.tokens, self.last_refill_ms)
            .into_bytes()
    }
}

// ---------------------------------------------------------------------------
// Entry point — exported as `process` in the WIT world
// ---------------------------------------------------------------------------
/// Rate-limit check for a single key.
///
/// `input` is a JSON object: `{"capacity": 10, "refill_rate": 1.0}`
/// Returns JSON: `{"allowed": true, "remaining": 9}`
pub fn process(input: Vec<u8>) -> Result<Vec<u8>, String> {
    let config = parse_config(&input)?;
    let now = host::time::now_ms();

    // Build the per-key storage key (the FN.CALL key is prepended by the runtime).
    let state_key = b"__rl:state";

    let mut bucket = match host::kv::get(state_key) {
        Some(bytes) => Bucket::from_bytes(&bytes),
        None => Bucket {
            tokens: config.capacity as f64,
            last_refill_ms: now,
        },
    };

    // Refill tokens based on elapsed time.
    let elapsed = now.saturating_sub(bucket.last_refill_ms);
    bucket.tokens = (bucket.tokens + elapsed as f64 * config.refill_rate)
        .min(config.capacity as f64);
    bucket.last_refill_ms = now;

    let allowed = bucket.tokens >= 1.0;
    if allowed {
        bucket.tokens -= 1.0;
    }

    host::kv::set(state_key, &bucket.to_bytes())?;

    // Expire idle buckets after 2× the fill-up time.
    let full_refill_ms = (config.capacity as f64 / config.refill_rate) as u64;
    let _ = host::kv::expire(state_key, full_refill_ms * 2);

    host::log::info(&format!("rate_limit: allowed={allowed}, remaining={}", bucket.tokens as u64));

    let response = format!(
        r#"{{"allowed":{},"remaining":{}}}"#,
        allowed,
        bucket.tokens as u64
    );
    Ok(response.into_bytes())
}

fn parse_config(input: &[u8]) -> Result<Config, String> {
    // Minimal hand-rolled parse for the scaffold. In a real module, use serde.
    let s = core::str::from_utf8(input).map_err(|e| e.to_string())?;
    let capacity = extract_u64(s, "capacity")?;
    let refill_rate = extract_f64(s, "refill_rate")?;
    Ok(Config { capacity, refill_rate })
}

fn extract_u64(json: &str, key: &str) -> Result<u64, String> {
    let needle = format!(r#""{}":"#, key);
    let start = json.find(&needle).ok_or(format!("missing key: {key}"))? + needle.len();
    let end = json[start..].find([',', '}']).unwrap_or(json.len() - start);
    json[start..start + end].trim().parse().map_err(|e: std::num::ParseIntError| e.to_string())
}

fn extract_f64(json: &str, key: &str) -> Result<f64, String> {
    let needle = format!(r#""{}":"#, key);
    let start = json.find(&needle).ok_or(format!("missing key: {key}"))? + needle.len();
    let end = json[start..].find([',', '}']).unwrap_or(json.len() - start);
    json[start..start + end].trim().parse().map_err(|e: std::num::ParseFloatError| e.to_string())
}
