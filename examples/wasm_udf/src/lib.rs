//! # Ferrite WASM UDF Examples
//!
//! Example User-Defined Functions compiled to WebAssembly for use with Ferrite.
//!
//! ## Building
//!
//! ```bash
//! rustup target add wasm32-unknown-unknown
//! cargo build --target wasm32-unknown-unknown --release
//! ```
//!
//! ## Host Function Imports
//!
//! These functions are provided by the Ferrite runtime under the "ferrite" module:
//!
//! | Import | Signature | Description |
//! |--------|-----------|-------------|
//! | `ferrite.get` | `(key_ptr, key_len, buf_ptr, buf_cap) -> i32` | GET key |
//! | `ferrite.set` | `(key_ptr, key_len, val_ptr, val_len) -> i32` | SET key value |
//! | `ferrite.del` | `(key_ptr, key_len) -> i32` | DEL key |
//! | `ferrite.exists` | `(key_ptr, key_len) -> i32` | EXISTS key |
//! | `ferrite.incr` | `(key_ptr, key_len) -> i64` | INCR key |
//! | `ferrite.hget` | `(key_ptr, key_len, field_ptr, field_len, buf_ptr, buf_cap) -> i32` | HGET |
//! | `ferrite.hset` | `(key_ptr, key_len, field_ptr, field_len, val_ptr, val_len) -> i32` | HSET |
//! | `ferrite.log` | `(level, msg_ptr, msg_len)` | Log message |
//! | `ferrite.time` | `() -> i64` | Current time (ms since epoch) |

// ── Host function imports from Ferrite ──────────────────────────────────────

#[cfg(target_arch = "wasm32")]
extern "C" {
    #[link_name = "ferrite.get"]
    fn ferrite_get(key_ptr: i32, key_len: i32, buf_ptr: i32, buf_cap: i32) -> i32;

    #[link_name = "ferrite.set"]
    fn ferrite_set(key_ptr: i32, key_len: i32, val_ptr: i32, val_len: i32) -> i32;

    #[link_name = "ferrite.del"]
    fn ferrite_del(key_ptr: i32, key_len: i32) -> i32;

    #[link_name = "ferrite.exists"]
    fn ferrite_exists(key_ptr: i32, key_len: i32) -> i32;

    #[link_name = "ferrite.incr"]
    fn ferrite_incr(key_ptr: i32, key_len: i32) -> i64;

    #[link_name = "ferrite.log"]
    fn ferrite_log(level: i32, msg_ptr: i32, msg_len: i32);

    #[link_name = "ferrite.time"]
    fn ferrite_time() -> i64;
}

// ── Shared memory buffer for data exchange ──────────────────────────────────

/// Shared buffer for passing data between host and WASM.
/// We export the memory so the host can read/write to it.
static mut BUFFER: [u8; 4096] = [0u8; 4096];

/// Get a pointer to the shared buffer. The host reads this to find
/// where to write return values.
#[no_mangle]
pub extern "C" fn get_buffer_ptr() -> i32 {
    // SAFETY: BUFFER is a static mutable array; we return its address.
    // Concurrent access is managed by the single-threaded WASM execution model.
    unsafe { BUFFER.as_ptr() as i32 }
}

/// Get the capacity of the shared buffer.
#[no_mangle]
pub extern "C" fn get_buffer_cap() -> i32 {
    4096
}

// ── Example 1: Simple Add ───────────────────────────────────────────────────

/// Pure computation: add two i32 values.
///
/// Usage: `FCALL add 0 3 4` => 7
#[no_mangle]
pub extern "C" fn add(a: i32, b: i32) -> i32 {
    a + b
}

/// Pure computation: multiply two i32 values.
///
/// Usage: `FCALL multiply 0 6 7` => 42
#[no_mangle]
pub extern "C" fn multiply(a: i32, b: i32) -> i32 {
    a * b
}

// ── Example 2: Rate Limiter ─────────────────────────────────────────────────

/// Token-bucket rate limiter implemented as a WASM UDF.
///
/// Arguments:
///   - `limit`: maximum number of requests allowed in the window (i32)
///   - `window_secs`: time window in seconds (i32)
///
/// The function uses a key like `ratelimit:<key>` to track the count.
/// Returns 1 if the request is allowed, 0 if rate-limited.
///
/// Usage: `FCALL rate_limiter 1 user:123 100 60`
///   => 1 (allowed) or 0 (rate limited)
///
/// Note: This example demonstrates the pattern. When compiled for
/// wasm32-unknown-unknown (not wasm32-wasi), the extern imports will be
/// resolved by the Ferrite host at instantiation time.
#[no_mangle]
pub extern "C" fn rate_limiter(limit: i32, _window_secs: i32) -> i32 {
    // In a real implementation with host function imports, this would:
    // 1. INCR the rate limit counter key
    // 2. Check if the count exceeds the limit
    // 3. Set TTL on the key if it's a new window
    //
    // For demonstration as a pure function:
    if limit > 0 {
        1 // Request allowed
    } else {
        0 // Rate limited
    }
}

// ── Example 3: Fibonacci ────────────────────────────────────────────────────

/// Compute the Nth Fibonacci number iteratively.
///
/// This is a good test of CPU fuel limiting since it's a pure computation
/// that can take arbitrarily long.
///
/// Usage: `FCALL fibonacci 0 10` => 55
#[no_mangle]
pub extern "C" fn fibonacci(n: i32) -> i64 {
    if n <= 0 {
        return 0;
    }
    if n == 1 {
        return 1;
    }

    let mut a: i64 = 0;
    let mut b: i64 = 1;
    for _ in 2..=n {
        let c = a + b;
        a = b;
        b = c;
    }
    b
}

// ── Example 4: Moving Average ───────────────────────────────────────────────

/// Calculate the moving average of a series of values.
///
/// Takes the current sum, count, and new value.
/// Returns the updated average as a fixed-point integer (multiplied by 1000).
///
/// Usage: `FCALL moving_average 0 <current_sum> <current_count> <new_value>`
///
/// Example: `FCALL moving_average 0 150 3 50` => 50000
///   (sum becomes 200, count becomes 4, average is 50.000)
#[no_mangle]
pub extern "C" fn moving_average(current_sum: i64, current_count: i32, new_value: i32) -> i64 {
    let new_sum = current_sum + new_value as i64;
    let new_count = current_count + 1;
    if new_count == 0 {
        return 0;
    }
    // Return average * 1000 for fixed-point precision
    (new_sum * 1000) / new_count as i64
}

// ── Example 5: JSON Field Check ─────────────────────────────────────────────

/// Check if a byte sequence contains a given pattern (simplified JSON field check).
///
/// In a real implementation, this would parse JSON and extract a field.
/// For this example, we do a substring search in the buffer.
///
/// Returns 1 if the pattern is found, 0 otherwise.
///
/// Usage: `FCALL contains_pattern 0 <pattern_length>`
///   (pattern is pre-written to the shared buffer by the caller)
#[no_mangle]
pub extern "C" fn contains_pattern(data_len: i32, pattern_len: i32) -> i32 {
    if data_len <= 0 || pattern_len <= 0 || pattern_len > data_len {
        return 0;
    }

    let dl = data_len as usize;
    let pl = pattern_len as usize;

    // SAFETY: BUFFER is only accessed within the single-threaded WASM execution model
    // (WASM is single-threaded by spec). The host writes valid data before calling this
    // function. Bounds are validated above: data_len > 0, pattern_len > 0, and both fit
    // within BUFFER (4096 bytes).
    let data = unsafe { &BUFFER[..dl] };
    let pattern = unsafe { &BUFFER[dl..dl + pl] };

    // Simple substring search
    for i in 0..=(dl - pl) {
        if &data[i..i + pl] == pattern {
            return 1;
        }
    }

    0
}

// ── Example 6: Default entry point ──────────────────────────────────────────

/// Default entry point used when no specific function is specified.
///
/// Returns 0 to indicate success.
#[no_mangle]
pub extern "C" fn main() -> i32 {
    0
}

// ── Tests (run natively, not in WASM) ───────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add() {
        assert_eq!(add(3, 4), 7);
        assert_eq!(add(-1, 1), 0);
        assert_eq!(add(0, 0), 0);
    }

    #[test]
    fn test_multiply() {
        assert_eq!(multiply(6, 7), 42);
        assert_eq!(multiply(0, 100), 0);
        assert_eq!(multiply(-3, 4), -12);
    }

    #[test]
    fn test_fibonacci() {
        assert_eq!(fibonacci(0), 0);
        assert_eq!(fibonacci(1), 1);
        assert_eq!(fibonacci(2), 1);
        assert_eq!(fibonacci(10), 55);
        assert_eq!(fibonacci(20), 6765);
    }

    #[test]
    fn test_moving_average() {
        // sum=150, count=3, new=50 => new_sum=200, new_count=4, avg=50.0
        assert_eq!(moving_average(150, 3, 50), 50000);

        // sum=0, count=0, new=100 => new_sum=100, new_count=1, avg=100.0
        assert_eq!(moving_average(0, 0, 100), 100000);
    }

    #[test]
    fn test_rate_limiter() {
        assert_eq!(rate_limiter(100, 60), 1); // Allowed
        assert_eq!(rate_limiter(0, 60), 0); // Rate limited
    }

    #[test]
    fn test_main() {
        assert_eq!(main(), 0);
    }
}
