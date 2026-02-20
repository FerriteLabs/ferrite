//! # Email Validation UDF
//!
//! A simple WASM user-defined function that validates email addresses.
//!
//! ## Building
//!
//! ```bash
//! rustup target add wasm32-unknown-unknown
//! cargo build --target wasm32-unknown-unknown --release
//! ```
//!
//! ## Usage in Ferrite
//!
//! ```text
//! FUNCTION.LOAD validate_email ./validate_email.wasm
//! FCALL validate_email 0 "user@example.com"    # => 1 (valid)
//! FCALL validate_email 0 "not-an-email"        # => 0 (invalid)
//! ```

/// Shared buffer for passing string data between host and WASM.
static mut BUFFER: [u8; 4096] = [0u8; 4096];

/// Returns a pointer to the shared buffer.
#[no_mangle]
pub extern "C" fn get_buffer_ptr() -> i32 {
    // SAFETY: BUFFER address is stable; single-threaded WASM execution.
    unsafe { BUFFER.as_ptr() as i32 }
}

/// Returns the capacity of the shared buffer.
#[no_mangle]
pub extern "C" fn get_buffer_cap() -> i32 {
    4096
}

/// Validates that the first `len` bytes of the shared buffer contain a
/// plausible email address.
///
/// Returns `1` for valid, `0` for invalid.
#[no_mangle]
pub extern "C" fn validate_email(len: i32) -> i32 {
    if len <= 0 || len as usize > 4096 {
        return 0;
    }

    // SAFETY: single-threaded WASM model; host writes data before calling.
    let email = unsafe { &BUFFER[..len as usize] };

    let s = match core::str::from_utf8(email) {
        Ok(s) => s,
        Err(_) => return 0,
    };

    if is_valid_email(s) {
        1
    } else {
        0
    }
}

/// Checks basic email format: `local@domain.tld`.
fn is_valid_email(email: &str) -> bool {
    let email = email.trim();
    if email.is_empty() || email.len() > 254 {
        return false;
    }

    // Must contain exactly one '@'
    let parts: Vec<&str> = email.splitn(2, '@').collect();
    if parts.len() != 2 {
        return false;
    }

    let local = parts[0];
    let domain = parts[1];

    // Local part checks
    if local.is_empty() || local.len() > 64 {
        return false;
    }
    if local.starts_with('.') || local.ends_with('.') {
        return false;
    }
    if local.contains("..") {
        return false;
    }

    // Domain checks
    if domain.is_empty() || domain.len() > 253 {
        return false;
    }
    if !domain.contains('.') {
        return false;
    }
    if domain.starts_with('.') || domain.ends_with('.') {
        return false;
    }
    if domain.starts_with('-') || domain.ends_with('-') {
        return false;
    }

    // TLD must be at least 2 characters
    let tld = domain.rsplit('.').next().unwrap_or("");
    if tld.len() < 2 {
        return false;
    }

    // All domain chars must be alphanumeric, '-', or '.'
    domain
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '.')
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
    fn test_valid_emails() {
        assert!(is_valid_email("user@example.com"));
        assert!(is_valid_email("alice.bob@sub.domain.org"));
        assert!(is_valid_email("test+tag@gmail.com"));
        assert!(is_valid_email("a@b.co"));
    }

    #[test]
    fn test_invalid_emails() {
        assert!(!is_valid_email(""));
        assert!(!is_valid_email("no-at-sign"));
        assert!(!is_valid_email("@no-local.com"));
        assert!(!is_valid_email("no-domain@"));
        assert!(!is_valid_email("user@.com"));
        assert!(!is_valid_email("user@domain."));
        assert!(!is_valid_email("user@-domain.com"));
        assert!(!is_valid_email("user@domain.c")); // TLD too short
        assert!(!is_valid_email("user@domain")); // no TLD dot
        assert!(!is_valid_email("user..name@domain.com")); // consecutive dots
    }
}
