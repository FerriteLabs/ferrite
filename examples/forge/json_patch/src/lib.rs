//! RFC 6902 JSON Patch on KV-stored JSON — Ferrite Forge WASM module.
//!
//! Reads a JSON document from KV, applies a patch array, writes back, and
//! returns the result. All operations are atomic within a single FN.CALL.

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
        pub fn warn(_msg: &str) {}
    }
}

// ---------------------------------------------------------------------------
// Patch operations (RFC 6902)
// ---------------------------------------------------------------------------
#[derive(Debug)]
enum PatchOp {
    Add { path: String, value: String },
    Remove { path: String },
    Replace { path: String, value: String },
    Move { from: String, path: String },
    Copy { from: String, path: String },
    Test { path: String, value: String },
}

// ---------------------------------------------------------------------------
// JSON Pointer resolution (RFC 6901)
// ---------------------------------------------------------------------------

/// Resolve a JSON Pointer path into segments.
fn parse_pointer(path: &str) -> Vec<&str> {
    if path.is_empty() || path == "/" {
        return vec![];
    }
    path.strip_prefix('/').unwrap_or(path).split('/').collect()
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------
/// Apply an RFC 6902 JSON Patch to the document stored at the call key.
///
/// `input`: JSON array of patch operations.
/// Returns the full patched document as JSON bytes.
pub fn process(input: Vec<u8>) -> Result<Vec<u8>, String> {
    let patch_str = core::str::from_utf8(&input).map_err(|e| e.to_string())?;

    // The FN.CALL key is the document key — read current state.
    let doc_key = b"__self";
    let doc_bytes = host::kv::get(doc_key)
        .ok_or("document not found at call key")?;
    let doc = core::str::from_utf8(&doc_bytes).map_err(|e| e.to_string())?;

    let ops = parse_patch(patch_str)?;

    // Apply operations sequentially. In a real module this would manipulate
    // a parsed JSON tree; here we show the structure.
    let mut document = doc.to_string();
    for (i, op) in ops.iter().enumerate() {
        document = apply_op(&document, op)
            .map_err(|e| format!("patch op {} failed: {e}", i))?;
    }

    // Write back.
    host::kv::set(doc_key, document.as_bytes())?;
    host::log::info(&format!("json_patch: applied {} operations", ops.len()));

    Ok(document.into_bytes())
}

fn parse_patch(json: &str) -> Result<Vec<PatchOp>, String> {
    // Stub: in a real module, parse the JSON array into PatchOp variants.
    // For the scaffold we return an empty vec.
    let _ = json;
    Ok(vec![])
}

fn apply_op(document: &str, op: &PatchOp) -> Result<String, String> {
    // Stub: in a real module, parse the document into a JSON tree, apply
    // the operation using JSON Pointer resolution, and serialize back.
    match op {
        PatchOp::Add { path, value } => {
            let _segments = parse_pointer(path);
            let _ = value;
            Ok(document.to_string())
        }
        PatchOp::Remove { path } => {
            let _segments = parse_pointer(path);
            Ok(document.to_string())
        }
        PatchOp::Replace { path, value } => {
            let _segments = parse_pointer(path);
            let _ = value;
            Ok(document.to_string())
        }
        PatchOp::Move { from, path } => {
            let _from_seg = parse_pointer(from);
            let _to_seg = parse_pointer(path);
            Ok(document.to_string())
        }
        PatchOp::Copy { from, path } => {
            let _from_seg = parse_pointer(from);
            let _to_seg = parse_pointer(path);
            Ok(document.to_string())
        }
        PatchOp::Test { path, value } => {
            let _segments = parse_pointer(path);
            let _ = value;
            // Would compare the value at path with the expected value.
            Ok(document.to_string())
        }
    }
}
