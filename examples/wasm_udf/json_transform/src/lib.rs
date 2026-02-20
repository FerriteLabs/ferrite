//! # JSON Transform UDF
//!
//! A WASM user-defined function that performs lightweight JSON transformations
//! without requiring a full JSON parsing library.
//!
//! ## Building
//!
//! ```bash
//! rustup target add wasm32-unknown-unknown
//! cargo build --target wasm32-unknown-unknown --release
//! ```
//!
//! ## Supported Operations
//!
//! | Function | Description |
//! |----------|-------------|
//! | `count_keys` | Count top-level keys in a JSON object |
//! | `has_field` | Check if a JSON object contains a given field name |
//! | `extract_string_field` | Extract a string value for a given key |
//!
//! All functions operate on UTF-8 JSON text written to the shared buffer
//! by the host before invocation.

/// Shared buffer for passing data between host and WASM.
static mut BUFFER: [u8; 8192] = [0u8; 8192];

/// Returns a pointer to the shared buffer.
#[no_mangle]
pub extern "C" fn get_buffer_ptr() -> i32 {
    // SAFETY: BUFFER address is stable; single-threaded WASM execution.
    unsafe { BUFFER.as_ptr() as i32 }
}

/// Returns the capacity of the shared buffer.
#[no_mangle]
pub extern "C" fn get_buffer_cap() -> i32 {
    8192
}

/// Counts top-level keys in a JSON object stored in the shared buffer.
///
/// The first `len` bytes of the buffer must contain a UTF-8 JSON object.
/// Returns the number of top-level keys, or `-1` on error.
#[no_mangle]
pub extern "C" fn count_keys(len: i32) -> i32 {
    if len <= 0 || len as usize > 8192 {
        return -1;
    }
    // SAFETY: single-threaded WASM model.
    let data = unsafe { &BUFFER[..len as usize] };
    let s = match core::str::from_utf8(data) {
        Ok(s) => s.trim(),
        Err(_) => return -1,
    };
    if !s.starts_with('{') || !s.ends_with('}') {
        return -1;
    }

    json_count_top_level_keys(s)
}

/// Checks whether the JSON object in the buffer contains a field whose name
/// matches the `field_len` bytes immediately following the JSON data.
///
/// Buffer layout: `[json_data (json_len bytes)][field_name (field_len bytes)]`
///
/// Returns `1` if the field exists, `0` if not, `-1` on error.
#[no_mangle]
pub extern "C" fn has_field(json_len: i32, field_len: i32) -> i32 {
    let jl = json_len as usize;
    let fl = field_len as usize;
    if json_len <= 0 || field_len <= 0 || jl + fl > 8192 {
        return -1;
    }
    // SAFETY: single-threaded WASM model.
    let data = unsafe { &BUFFER[..jl + fl] };
    let json_str = match core::str::from_utf8(&data[..jl]) {
        Ok(s) => s.trim(),
        Err(_) => return -1,
    };
    let field_name = match core::str::from_utf8(&data[jl..jl + fl]) {
        Ok(s) => s.trim(),
        Err(_) => return -1,
    };

    // Simple search for `"field_name":`
    let pattern = format!("\"{}\"", field_name);
    if json_str.contains(&pattern) {
        1
    } else {
        0
    }
}

/// Extracts the string value of a named field from the JSON object in the
/// buffer and writes it back to the beginning of the buffer.
///
/// Buffer layout on entry: `[json_data (json_len)][field_name (field_len)]`
/// Buffer layout on exit:  `[extracted_value ...]`
///
/// Returns the byte length of the extracted value, `0` if not found, or
/// `-1` on error.
#[no_mangle]
pub extern "C" fn extract_string_field(json_len: i32, field_len: i32) -> i32 {
    let jl = json_len as usize;
    let fl = field_len as usize;
    if json_len <= 0 || field_len <= 0 || jl + fl > 8192 {
        return -1;
    }

    // SAFETY: single-threaded WASM model; bounds validated above (jl + fl <= 8192).
    // Host writes JSON data followed by field name before calling this function.
    let (json_bytes, field_bytes) = unsafe {
        let data = &BUFFER[..jl + fl];
        (data[..jl].to_vec(), data[jl..jl + fl].to_vec())
    };

    let json_str = match core::str::from_utf8(&json_bytes) {
        Ok(s) => s,
        Err(_) => return -1,
    };
    let field_name = match core::str::from_utf8(&field_bytes) {
        Ok(s) => s.trim(),
        Err(_) => return -1,
    };

    match extract_json_string_value(json_str, field_name) {
        Some(value) => {
            let vb = value.as_bytes();
            if vb.len() > 8192 {
                return -1;
            }
            // SAFETY: writing result back to buffer; vb.len() <= 8192 checked above.
            unsafe {
                BUFFER[..vb.len()].copy_from_slice(vb);
            }
            vb.len() as i32
        }
        None => 0,
    }
}

// ---------------------------------------------------------------------------
// Internal helpers (no-std JSON parsing)
// ---------------------------------------------------------------------------

/// Counts top-level keys in a JSON object string (very simplified parser).
fn json_count_top_level_keys(s: &str) -> i32 {
    let s = s.trim();
    if s.len() < 2 {
        return 0;
    }
    let inner = &s[1..s.len() - 1]; // strip { }
    if inner.trim().is_empty() {
        return 0;
    }

    let mut count = 0i32;
    let mut depth = 0i32;
    let mut in_string = false;
    let mut prev = '\0';

    for ch in inner.chars() {
        match ch {
            '"' if prev != '\\' => {
                if !in_string && depth == 0 {
                    count += 1;
                }
                in_string = !in_string;
            }
            '{' | '[' if !in_string => depth += 1,
            '}' | ']' if !in_string => depth -= 1,
            _ => {}
        }
        prev = ch;
    }

    // Each key is a quoted string at depth 0 followed by ':'.
    // We counted opening quotes at depth 0 — that double-counts keys+values.
    // A simpler heuristic: count commas at depth 0 + 1 for non-empty objects.
    // Let's use the comma approach instead for correctness:
    let mut comma_count = 0i32;
    depth = 0;
    in_string = false;
    prev = '\0';

    for ch in inner.chars() {
        match ch {
            '"' if prev != '\\' => in_string = !in_string,
            '{' | '[' if !in_string => depth += 1,
            '}' | ']' if !in_string => depth -= 1,
            ',' if !in_string && depth == 0 => comma_count += 1,
            _ => {}
        }
        prev = ch;
    }

    comma_count + 1
}

/// Extracts a string value for a key from a JSON object (simplified).
fn extract_json_string_value<'a>(json: &'a str, key: &str) -> Option<&'a str> {
    let pattern = format!("\"{}\"", key);
    let key_pos = json.find(&pattern)?;
    let after_key = &json[key_pos + pattern.len()..];

    // Skip whitespace and ':'
    let after_colon = after_key.trim_start().strip_prefix(':')?;
    let after_colon = after_colon.trim_start();

    // Expect a quoted string value
    if !after_colon.starts_with('"') {
        return None;
    }
    let value_start = 1; // skip opening quote
    let rest = &after_colon[value_start..];
    let mut end = 0;
    let mut prev = '\0';
    for ch in rest.chars() {
        if ch == '"' && prev != '\\' {
            break;
        }
        end += ch.len_utf8();
        prev = ch;
    }

    Some(&rest[..end])
}

/// Default entry point — returns 0 (success).
#[no_mangle]
pub extern "C" fn main() -> i32 {
    0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_count_keys_empty() {
        assert_eq!(json_count_top_level_keys("{}"), 0);
    }

    #[test]
    fn test_count_keys_simple() {
        assert_eq!(
            json_count_top_level_keys(r#"{"a":1,"b":2,"c":3}"#),
            3
        );
    }

    #[test]
    fn test_count_keys_nested() {
        assert_eq!(
            json_count_top_level_keys(r#"{"a":{"x":1},"b":[1,2]}"#),
            2
        );
    }

    #[test]
    fn test_extract_string_value() {
        let json = r#"{"name":"Alice","age":"30"}"#;
        assert_eq!(extract_json_string_value(json, "name"), Some("Alice"));
        assert_eq!(extract_json_string_value(json, "age"), Some("30"));
        assert_eq!(extract_json_string_value(json, "missing"), None);
    }

    #[test]
    fn test_extract_string_value_with_spaces() {
        let json = r#"{ "name" : "Bob" }"#;
        assert_eq!(extract_json_string_value(json, "name"), Some("Bob"));
    }
}
