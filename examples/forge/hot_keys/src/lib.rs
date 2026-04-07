//! Streaming top-K hot key detector using the Space-Saving algorithm —
//! Ferrite Forge WASM module.
//!
//! Maintains a fixed-size summary that identifies the most frequent keys
//! with bounded error, using O(k) memory regardless of key cardinality.

// wit_bindgen::generate!({
//     world: "function",
//     path: "../../../crates/ferrite-forge/wit/ferrite.wit",
// });

mod host {
    pub mod kv {
        pub fn get(_key: &[u8]) -> Option<Vec<u8>> { unimplemented!() }
        pub fn set(_key: &[u8], _value: &[u8]) -> Result<(), String> { unimplemented!() }
    }
    pub mod log {
        pub fn info(_msg: &str) {}
    }
}

// ---------------------------------------------------------------------------
// Space-Saving data structure
// ---------------------------------------------------------------------------

/// A single counter in the Space-Saving summary.
struct Counter {
    key: Vec<u8>,
    count: u64,
    /// Maximum overcount error — the true count is in [count - error, count].
    error: u64,
}

/// Fixed-size Space-Saving summary for approximate frequent items.
struct SpaceSaving {
    counters: Vec<Counter>,
    capacity: usize,
    total: u64,
}

impl SpaceSaving {
    fn new(k: usize) -> Self {
        SpaceSaving {
            counters: Vec::with_capacity(k),
            capacity: k,
            total: 0,
        }
    }

    fn from_bytes(bytes: &[u8], k: usize) -> Self {
        // Stub: deserialize from a compact binary or JSON format.
        let _ = bytes;
        Self::new(k)
    }

    fn to_bytes(&self) -> Vec<u8> {
        // Stub: serialize to a compact format.
        format!(r#"{{"total":{},"counters":{}}}"#, self.total, self.counters.len()).into_bytes()
    }

    /// Record `count` observations of `key`.
    fn observe(&mut self, key: &[u8], count: u64) {
        self.total += count;

        // Check if key already has a counter.
        if let Some(idx) = self.counters.iter().position(|c| c.key == key) {
            self.counters[idx].count += count;
            return;
        }

        // If we have spare capacity, add a new counter.
        if self.counters.len() < self.capacity {
            self.counters.push(Counter {
                key: key.to_vec(),
                count,
                error: 0,
            });
            return;
        }

        // Replace the minimum counter (Space-Saving eviction).
        if let Some(min_idx) = self.min_counter_index() {
            let min_count = self.counters[min_idx].count;
            self.counters[min_idx] = Counter {
                key: key.to_vec(),
                count: min_count + count,
                error: min_count,
            };
        }
    }

    /// Return indices sorted by count descending.
    fn top(&self, n: usize) -> Vec<&Counter> {
        let mut sorted: Vec<&Counter> = self.counters.iter().collect();
        sorted.sort_by(|a, b| b.count.cmp(&a.count));
        sorted.truncate(n);
        sorted
    }

    fn min_counter_index(&self) -> Option<usize> {
        self.counters.iter().enumerate()
            .min_by_key(|(_, c)| c.count)
            .map(|(i, _)| i)
    }
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------
/// Record a key observation or query the top-K.
///
/// `input` JSON:
///   - Observe mode: `{"k": 100, "count": 1}`
///   - Query mode:   `{"k": 100, "top": 10}`
///
/// In observe mode the FN.CALL key is the observed key.
/// In query mode (key = `__query`), returns the current top-K.
pub fn process(input: Vec<u8>) -> Result<Vec<u8>, String> {
    let s = core::str::from_utf8(&input).map_err(|e| e.to_string())?;
    let k = extract_u32(s, "k").unwrap_or(100) as usize;

    let state_key = b"__hot_keys:state";

    let mut ss = match host::kv::get(state_key) {
        Some(bytes) => SpaceSaving::from_bytes(&bytes, k),
        None => SpaceSaving::new(k),
    };

    // Check if this is a query or an observation.
    if let Ok(top_n) = extract_u32(s, "top") {
        // Query mode — return current top-K.
        let top = ss.top(top_n as usize);
        let entries: Vec<String> = top.iter().map(|c| {
            format!(
                r#"{{"key":"{}","count":{},"error":{}}}"#,
                String::from_utf8_lossy(&c.key),
                c.count,
                c.error,
            )
        }).collect();

        let response = format!(
            r#"{{"top_keys":[{}],"total":{}}}"#,
            entries.join(","),
            ss.total,
        );
        return Ok(response.into_bytes());
    }

    // Observe mode — record the call key.
    let count = extract_u64(s, "count").unwrap_or(1);

    // The FN.CALL key is passed as a prefix by the runtime; here we
    // use a placeholder. In a real module, the runtime injects the key.
    let observed_key = b"__call_key";
    ss.observe(observed_key, count);

    host::kv::set(state_key, &ss.to_bytes())?;
    host::log::info(&format!("hot_keys: recorded {count} observations, total={}", ss.total));

    let response = format!(r#"{{"recorded":true,"total":{}}}"#, ss.total);
    Ok(response.into_bytes())
}

fn extract_u32(json: &str, key: &str) -> Result<u32, String> {
    let needle = format!(r#""{}":"#, key);
    let start = json.find(&needle).ok_or(format!("missing key: {key}"))? + needle.len();
    let end = json[start..].find([',', '}']).unwrap_or(json.len() - start);
    json[start..start + end].trim().parse().map_err(|e: std::num::ParseIntError| e.to_string())
}

fn extract_u64(json: &str, key: &str) -> Result<u64, String> {
    let needle = format!(r#""{}":"#, key);
    let start = json.find(&needle).ok_or(format!("missing key: {key}"))? + needle.len();
    let end = json[start..].find([',', '}']).unwrap_or(json.len() - start);
    json[start..start + end].trim().parse().map_err(|e: std::num::ParseIntError| e.to_string())
}
