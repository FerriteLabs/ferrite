//! Last-write-wins-by-source-rank merge for Chronicle — Ferrite Forge WASM
//! module.
//!
//! Resolves conflicting writes during Chronicle replication by comparing
//! timestamps, then source ranks, then values for deterministic ordering.

// wit_bindgen::generate!({
//     world: "function",
//     path: "../../../crates/ferrite-forge/wit/ferrite.wit",
// });

mod host {
    pub mod log {
        pub fn info(_msg: &str) {}
    }
}

// ---------------------------------------------------------------------------
// Domain types
// ---------------------------------------------------------------------------
struct ReplicaValue {
    value: Vec<u8>,
    timestamp_ms: u64,
    source_rank: u32,
}

#[derive(Debug)]
enum Winner {
    Local,
    Remote,
}

impl Winner {
    fn as_str(&self) -> &'static str {
        match self {
            Winner::Local => "local",
            Winner::Remote => "remote",
        }
    }
}

// ---------------------------------------------------------------------------
// Merge logic
// ---------------------------------------------------------------------------

/// Determine which replica value wins using LWW-by-source-rank.
///
/// Ordering:
///   1. Higher timestamp wins.
///   2. On tie: lower source_rank wins (higher priority).
///   3. On tie: lexicographically greater value wins (determinism).
fn resolve(local: &ReplicaValue, remote: &ReplicaValue) -> (Winner, &'static str) {
    if local.timestamp_ms != remote.timestamp_ms {
        if local.timestamp_ms > remote.timestamp_ms {
            (Winner::Local, "later timestamp")
        } else {
            (Winner::Remote, "later timestamp")
        }
    } else if local.source_rank != remote.source_rank {
        // Lower rank = higher priority.
        if local.source_rank < remote.source_rank {
            (Winner::Local, "lower source rank")
        } else {
            (Winner::Remote, "lower source rank")
        }
    } else {
        // Deterministic tie-break on value bytes.
        if local.value >= remote.value {
            (Winner::Local, "lexicographic tie-break")
        } else {
            (Winner::Remote, "lexicographic tie-break")
        }
    }
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------
/// Merge two conflicting replica values.
///
/// `input`: JSON with `local` and `remote` objects, each containing
/// `value`, `ts`, and `source_rank`.
/// Returns JSON: `{"winner":"local"|"remote","value":"...","reason":"..."}`
pub fn process(input: Vec<u8>) -> Result<Vec<u8>, String> {
    let s = core::str::from_utf8(&input).map_err(|e| e.to_string())?;

    let local = parse_replica(s, "local")?;
    let remote = parse_replica(s, "remote")?;

    let (winner, reason) = resolve(&local, &remote);

    let winning_value = match winner {
        Winner::Local => &local.value,
        Winner::Remote => &remote.value,
    };

    let value_str = String::from_utf8_lossy(winning_value);
    host::log::info(&format!(
        "custom_merge: winner={}, reason={reason}",
        winner.as_str()
    ));

    let response = format!(
        r#"{{"winner":"{}","value":"{}","reason":"{}"}}"#,
        winner.as_str(),
        value_str,
        reason,
    );
    Ok(response.into_bytes())
}

fn parse_replica(json: &str, key: &str) -> Result<ReplicaValue, String> {
    // Stub: in a real module, use serde or a lightweight JSON parser.
    let _ = json;
    let _ = key;
    Ok(ReplicaValue {
        value: b"stub".to_vec(),
        timestamp_ms: 0,
        source_rank: 0,
    })
}
