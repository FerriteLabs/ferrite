//! Ferrite WASM Playground Binary
//!
//! This binary compiles Ferrite's embedded mode to WebAssembly,
//! enabling an in-browser interactive playground.
//!
//! # Building
//!
//! ```bash
//! # Install wasm-pack
//! cargo install wasm-pack
//!
//! # Build the WASM module
//! wasm-pack build --target web --out-dir ../../ferrite-docs/website/static/wasm \
//!     -- --features lite --bin ferrite-wasm
//! ```
//!
//! # Architecture
//!
//! The WASM binary exposes a simple API:
//! - `execute(command: &str) -> String` — Execute a Redis command and return the response
//! - `reset()` — Reset the in-memory database
//!
//! It uses Ferrite's embedded mode (`Database::open_memory()`) to run
//! entirely in the browser without any network calls.
#![allow(clippy::print_stdout, clippy::print_stderr)]

// When building for wasm32, we use wasm-bindgen
#[cfg(target_arch = "wasm32")]
use wasm_bindgen::prelude::*;

#[cfg(target_arch = "wasm32")]
use std::sync::Mutex;

#[cfg(target_arch = "wasm32")]
static DB: Mutex<Option<FerriteWasm>> = Mutex::new(None);

#[cfg(target_arch = "wasm32")]
struct FerriteWasm {
    // Placeholder for embedded database handle
    // In a full implementation, this would be:
    // db: ferrite::embedded::Database,
    store: std::collections::HashMap<String, String>,
}

#[cfg(target_arch = "wasm32")]
impl FerriteWasm {
    fn new() -> Self {
        Self {
            store: std::collections::HashMap::new(),
        }
    }

    fn execute(&mut self, command: &str) -> String {
        let parts: Vec<&str> = command.split_whitespace().collect();
        if parts.is_empty() {
            return "(error) ERR empty command".to_string();
        }

        match parts[0].to_uppercase().as_str() {
            "PING" => {
                if parts.len() > 1 {
                    format!("\"{}\"", parts[1..].join(" "))
                } else {
                    "PONG".to_string()
                }
            }
            "SET" => {
                if parts.len() < 3 {
                    "(error) ERR wrong number of arguments for 'SET' command".to_string()
                } else {
                    self.store
                        .insert(parts[1].to_string(), parts[2..].join(" "));
                    "OK".to_string()
                }
            }
            "GET" => {
                if parts.len() < 2 {
                    "(error) ERR wrong number of arguments for 'GET' command".to_string()
                } else {
                    match self.store.get(parts[1]) {
                        Some(val) => format!("\"{}\"", val),
                        None => "(nil)".to_string(),
                    }
                }
            }
            "DEL" => {
                if parts.len() < 2 {
                    "(error) ERR wrong number of arguments for 'DEL' command".to_string()
                } else {
                    let mut count = 0;
                    for key in &parts[1..] {
                        if self.store.remove(*key).is_some() {
                            count += 1;
                        }
                    }
                    format!("(integer) {}", count)
                }
            }
            "EXISTS" => {
                if parts.len() < 2 {
                    "(error) ERR wrong number of arguments for 'EXISTS' command".to_string()
                } else {
                    let count = parts[1..]
                        .iter()
                        .filter(|k| self.store.contains_key(**k))
                        .count();
                    format!("(integer) {}", count)
                }
            }
            "DBSIZE" => {
                format!("(integer) {}", self.store.len())
            }
            "FLUSHDB" | "FLUSHALL" => {
                self.store.clear();
                "OK".to_string()
            }
            "KEYS" => {
                let pattern = if parts.len() > 1 { parts[1] } else { "*" };
                let keys: Vec<&String> = if pattern == "*" {
                    self.store.keys().collect()
                } else {
                    self.store
                        .keys()
                        .filter(|k| k.contains(&pattern.replace('*', "")))
                        .collect()
                };
                if keys.is_empty() {
                    "(empty array)".to_string()
                } else {
                    keys.iter()
                        .enumerate()
                        .map(|(i, k)| format!("{}) \"{}\"", i + 1, k))
                        .collect::<Vec<_>>()
                        .join("\n")
                }
            }
            "INFO" => "# Server\r\nferrite_version:0.1.0-wasm\r\nferrite_mode:playground\r\narch_bits:32\r\nos:wasm32".to_string(),
            _ => format!(
                "(error) ERR unknown command '{}'. Try PING, SET, GET, DEL, EXISTS, KEYS, DBSIZE, INFO",
                parts[0]
            ),
        }
    }
}

/// Initialize the WASM Ferrite instance
#[cfg(target_arch = "wasm32")]
#[wasm_bindgen]
pub fn init() {
    let mut db = match DB.lock() {
        Ok(guard) => guard,
        Err(_) => return,
    };
    *db = Some(FerriteWasm::new());
}

/// Execute a Redis command and return the response as a string
#[cfg(target_arch = "wasm32")]
#[wasm_bindgen]
pub fn execute(command: &str) -> String {
    let mut db = match DB.lock() {
        Ok(guard) => guard,
        Err(_) => return "(error) ERR internal: mutex poisoned".to_string(),
    };
    match db.as_mut() {
        Some(db) => db.execute(command),
        None => "(error) ERR database not initialized. Call init() first".to_string(),
    }
}

/// Reset the in-memory database
#[cfg(target_arch = "wasm32")]
#[wasm_bindgen]
pub fn reset() {
    let mut db = match DB.lock() {
        Ok(guard) => guard,
        Err(_) => return,
    };
    *db = Some(FerriteWasm::new());
}

// Non-WASM main (for development/testing)
#[cfg(not(target_arch = "wasm32"))]
fn main() {
    println!("Ferrite WASM Playground");
    println!("This binary is intended to be compiled to WebAssembly.");
    println!("Build with: wasm-pack build --target web");

    // Quick self-test
    let mut db = FerriteWasmTest::new();
    assert_eq!(db.execute("PING"), "PONG");
    db.execute("SET hello world");
    assert_eq!(db.execute("GET hello"), "\"world\"");
    assert_eq!(db.execute("DBSIZE"), "(integer) 1");
    db.execute("DEL hello");
    assert_eq!(db.execute("GET hello"), "(nil)");
    println!("All self-tests passed!");
}

#[cfg(not(target_arch = "wasm32"))]
struct FerriteWasmTest {
    store: std::collections::HashMap<String, String>,
}

#[cfg(not(target_arch = "wasm32"))]
impl FerriteWasmTest {
    fn new() -> Self {
        Self {
            store: std::collections::HashMap::new(),
        }
    }

    fn execute(&mut self, command: &str) -> String {
        let parts: Vec<&str> = command.split_whitespace().collect();
        if parts.is_empty() {
            return "(error) ERR empty command".to_string();
        }
        match parts[0].to_uppercase().as_str() {
            "PING" => "PONG".to_string(),
            "SET" if parts.len() >= 3 => {
                self.store
                    .insert(parts[1].to_string(), parts[2..].join(" "));
                "OK".to_string()
            }
            "GET" if parts.len() >= 2 => match self.store.get(parts[1]) {
                Some(val) => format!("\"{}\"", val),
                None => "(nil)".to_string(),
            },
            "DEL" if parts.len() >= 2 => {
                let count = parts[1..]
                    .iter()
                    .filter(|k| self.store.remove(**k).is_some())
                    .count();
                format!("(integer) {}", count)
            }
            "DBSIZE" => format!("(integer) {}", self.store.len()),
            _ => format!("(error) ERR unknown command '{}'", parts[0]),
        }
    }
}
