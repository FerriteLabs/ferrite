#![allow(clippy::unwrap_used)]
//! Integration test harness for end-to-end testing of Ferrite.
//!
//! Uses the embedded `Database` API to exercise the full command surface
//! without needing a running TCP server, keeping tests fast and self-contained.
//!
//! Run with: `cargo test --test integration_harness`

use ferrite_core::embedded::{Database, EmbeddedError, KeyType};
use std::thread;
use std::time::Duration;

// ============================================================================
// TestResult — unified result type for integration test assertions
// ============================================================================

/// Represents the possible results of a simulated RESP command.
#[derive(Debug, Clone, PartialEq)]
enum TestResult {
    Ok(String),
    Int(i64),
    Bulk(String),
    Array(Vec<TestResult>),
    Nil,
    Err(String),
}

// ============================================================================
// TestServer — wraps an in-memory embedded Database
// ============================================================================

/// Helper to create and manage an in-memory Ferrite database for testing.
struct TestServer {
    port: u16,
    db: Option<Database>,
}

impl TestServer {
    /// Configure a test server on the given port (used only for addressing).
    fn new(port: u16) -> Self {
        Self { port, db: None }
    }

    /// Start the server by creating an in-memory Database.
    fn start(&mut self) {
        self.db = Some(Database::memory().expect("failed to create in-memory database"));
    }

    /// Stop the server by dropping the database.
    fn stop(&mut self) {
        self.db.take();
    }

    /// Return the address string for this test server.
    fn addr(&self) -> String {
        format!("127.0.0.1:{}", self.port)
    }

    /// Get a reference to the underlying database.
    fn database(&self) -> &Database {
        self.db.as_ref().expect("server not started")
    }
}

// ============================================================================
// TestClient — executes commands against an embedded Database
// ============================================================================

/// A test client that executes commands directly against the embedded Database.
struct TestClient<'a> {
    db: &'a Database,
    _addr: String,
}

impl<'a> TestClient<'a> {
    /// Connect to a test server (takes a reference to its database).
    fn connect(addr: &str, server: &'a TestServer) -> Self {
        Self {
            db: server.database(),
            _addr: addr.to_string(),
        }
    }

    /// Execute a single RESP-style command against the database.
    fn command(&mut self, args: &[&str]) -> TestResult {
        if args.is_empty() {
            return TestResult::Err("empty command".into());
        }

        let cmd = args[0].to_uppercase();
        match cmd.as_str() {
            "PING" => TestResult::Ok("PONG".into()),

            "SET" => self.cmd_set(args),
            "GET" => self.cmd_get(args),
            "DEL" => self.cmd_del(args),
            "EXISTS" => self.cmd_exists(args),
            "INCR" => self.cmd_incr(args),
            "DECR" => self.cmd_decr(args),
            "INCRBY" => self.cmd_incrby(args),
            "MGET" => self.cmd_mget(args),
            "MSET" => self.cmd_mset(args),
            "APPEND" => self.cmd_append(args),
            "STRLEN" => self.cmd_strlen(args),
            "EXPIRE" => self.cmd_expire(args),
            "TTL" => self.cmd_ttl(args),
            "SETNX" => self.cmd_setnx(args),
            "DBSIZE" => self.cmd_dbsize(),
            "FLUSHDB" => self.cmd_flushdb(),
            "INFO" => self.cmd_info(),
            "TYPE" => self.cmd_type(args),
            "RENAME" => self.cmd_rename(args),

            // List commands
            "LPUSH" => self.cmd_lpush(args),
            "RPUSH" => self.cmd_rpush(args),
            "LPOP" => self.cmd_lpop(args),
            "RPOP" => self.cmd_rpop(args),
            "LRANGE" => self.cmd_lrange(args),
            "LLEN" => self.cmd_llen(args),

            // Hash commands
            "HSET" => self.cmd_hset(args),
            "HGET" => self.cmd_hget(args),
            "HDEL" => self.cmd_hdel(args),
            "HGETALL" => self.cmd_hgetall(args),
            "HLEN" => self.cmd_hlen(args),

            // Set commands
            "SADD" => self.cmd_sadd(args),
            "SREM" => self.cmd_srem(args),
            "SMEMBERS" => self.cmd_smembers(args),
            "SCARD" => self.cmd_scard(args),
            "SISMEMBER" => self.cmd_sismember(args),

            _ => TestResult::Err(format!("unknown command: {cmd}")),
        }
    }

    /// Execute a pipeline of commands, returning results in order.
    fn pipeline(&mut self, commands: Vec<Vec<&str>>) -> Vec<TestResult> {
        commands.iter().map(|cmd| self.command(cmd)).collect()
    }

    // -- String commands -----------------------------------------------------

    fn cmd_set(&self, args: &[&str]) -> TestResult {
        if args.len() < 3 {
            return TestResult::Err("SET requires key and value".into());
        }
        let key = args[1];
        let value = args[2];

        // Parse optional flags: EX <seconds>, NX
        let mut i = 3;
        let mut ex: Option<u64> = None;
        let mut nx = false;
        while i < args.len() {
            match args[i].to_uppercase().as_str() {
                "EX" => {
                    i += 1;
                    if i < args.len() {
                        ex = args[i].parse().ok();
                    }
                }
                "NX" => nx = true,
                _ => {}
            }
            i += 1;
        }

        if nx {
            match self.db.set_nx(key, value) {
                Ok(true) => TestResult::Ok("OK".into()),
                Ok(false) => TestResult::Nil,
                Err(e) => TestResult::Err(e.to_string()),
            }
        } else if let Some(seconds) = ex {
            match self.db.set_ex(key, value, Duration::from_secs(seconds)) {
                Ok(()) => TestResult::Ok("OK".into()),
                Err(e) => TestResult::Err(e.to_string()),
            }
        } else {
            match self.db.set(key, value) {
                Ok(()) => TestResult::Ok("OK".into()),
                Err(e) => TestResult::Err(e.to_string()),
            }
        }
    }

    fn cmd_get(&self, args: &[&str]) -> TestResult {
        if args.len() < 2 {
            return TestResult::Err("GET requires a key".into());
        }
        match self.db.get(args[1]) {
            Ok(Some(v)) => TestResult::Bulk(String::from_utf8_lossy(&v).into()),
            Ok(None) => TestResult::Nil,
            Err(e) => TestResult::Err(e.to_string()),
        }
    }

    fn cmd_del(&self, args: &[&str]) -> TestResult {
        if args.len() < 2 {
            return TestResult::Err("DEL requires at least one key".into());
        }
        match self.db.del(&args[1..]) {
            Ok(n) => TestResult::Int(n as i64),
            Err(e) => TestResult::Err(e.to_string()),
        }
    }

    fn cmd_exists(&self, args: &[&str]) -> TestResult {
        if args.len() < 2 {
            return TestResult::Err("EXISTS requires at least one key".into());
        }
        match self.db.exists(&args[1..]) {
            Ok(n) => TestResult::Int(n as i64),
            Err(e) => TestResult::Err(e.to_string()),
        }
    }

    fn cmd_incr(&self, args: &[&str]) -> TestResult {
        if args.len() < 2 {
            return TestResult::Err("INCR requires a key".into());
        }
        match self.db.incr(args[1]) {
            Ok(v) => TestResult::Int(v),
            Err(e) => TestResult::Err(e.to_string()),
        }
    }

    fn cmd_decr(&self, args: &[&str]) -> TestResult {
        if args.len() < 2 {
            return TestResult::Err("DECR requires a key".into());
        }
        match self.db.decr(args[1]) {
            Ok(v) => TestResult::Int(v),
            Err(e) => TestResult::Err(e.to_string()),
        }
    }

    fn cmd_incrby(&self, args: &[&str]) -> TestResult {
        if args.len() < 3 {
            return TestResult::Err("INCRBY requires key and increment".into());
        }
        let delta: i64 = match args[2].parse() {
            Ok(v) => v,
            Err(_) => return TestResult::Err("value is not an integer".into()),
        };
        match self.db.incr_by(args[1], delta) {
            Ok(v) => TestResult::Int(v),
            Err(e) => TestResult::Err(e.to_string()),
        }
    }

    fn cmd_mget(&self, args: &[&str]) -> TestResult {
        if args.len() < 2 {
            return TestResult::Err("MGET requires at least one key".into());
        }
        match self.db.mget(&args[1..]) {
            Ok(values) => {
                let results: Vec<TestResult> = values
                    .into_iter()
                    .map(|v| match v {
                        Some(data) => TestResult::Bulk(String::from_utf8_lossy(&data).into()),
                        None => TestResult::Nil,
                    })
                    .collect();
                TestResult::Array(results)
            }
            Err(e) => TestResult::Err(e.to_string()),
        }
    }

    fn cmd_mset(&self, args: &[&str]) -> TestResult {
        if args.len() < 3 || (args.len() - 1) % 2 != 0 {
            return TestResult::Err("MSET requires key-value pairs".into());
        }
        let pairs: Vec<(&str, &str)> = args[1..].chunks(2).map(|c| (c[0], c[1])).collect();
        match self.db.mset(&pairs) {
            Ok(()) => TestResult::Ok("OK".into()),
            Err(e) => TestResult::Err(e.to_string()),
        }
    }

    fn cmd_append(&self, args: &[&str]) -> TestResult {
        if args.len() < 3 {
            return TestResult::Err("APPEND requires key and value".into());
        }
        match self.db.append(args[1], args[2]) {
            Ok(len) => TestResult::Int(len as i64),
            Err(e) => TestResult::Err(e.to_string()),
        }
    }

    fn cmd_strlen(&self, args: &[&str]) -> TestResult {
        if args.len() < 2 {
            return TestResult::Err("STRLEN requires a key".into());
        }
        match self.db.strlen(args[1]) {
            Ok(len) => TestResult::Int(len as i64),
            Err(e) => TestResult::Err(e.to_string()),
        }
    }

    fn cmd_expire(&self, args: &[&str]) -> TestResult {
        if args.len() < 3 {
            return TestResult::Err("EXPIRE requires key and seconds".into());
        }
        let secs: u64 = match args[2].parse() {
            Ok(v) => v,
            Err(_) => return TestResult::Err("value is not an integer".into()),
        };
        match self.db.expire(args[1], Duration::from_secs(secs)) {
            Ok(true) => TestResult::Int(1),
            Ok(false) => TestResult::Int(0),
            Err(e) => TestResult::Err(e.to_string()),
        }
    }

    fn cmd_ttl(&self, args: &[&str]) -> TestResult {
        if args.len() < 2 {
            return TestResult::Err("TTL requires a key".into());
        }
        match self.db.ttl(args[1]) {
            Ok(Some(dur)) => TestResult::Int(dur.as_secs() as i64),
            Ok(None) => TestResult::Int(-1),
            Err(_) => TestResult::Int(-2),
        }
    }

    fn cmd_setnx(&self, args: &[&str]) -> TestResult {
        if args.len() < 3 {
            return TestResult::Err("SETNX requires key and value".into());
        }
        match self.db.set_nx(args[1], args[2]) {
            Ok(true) => TestResult::Int(1),
            Ok(false) => TestResult::Int(0),
            Err(e) => TestResult::Err(e.to_string()),
        }
    }

    fn cmd_dbsize(&self) -> TestResult {
        match self.db.dbsize() {
            Ok(n) => TestResult::Int(n as i64),
            Err(e) => TestResult::Err(e.to_string()),
        }
    }

    fn cmd_flushdb(&self) -> TestResult {
        match self.db.flushdb() {
            Ok(()) => TestResult::Ok("OK".into()),
            Err(e) => TestResult::Err(e.to_string()),
        }
    }

    fn cmd_info(&self) -> TestResult {
        // Use database stats as INFO response
        let stats = self.db.stats();
        TestResult::Bulk(format!("{stats:?}"))
    }

    fn cmd_type(&self, args: &[&str]) -> TestResult {
        if args.len() < 2 {
            return TestResult::Err("TYPE requires a key".into());
        }
        match self.db.key_type(args[1]) {
            Ok(Some(kt)) => {
                let name = match kt {
                    KeyType::String => "string",
                    KeyType::List => "list",
                    KeyType::Hash => "hash",
                    KeyType::Set => "set",
                    KeyType::ZSet => "zset",
                    KeyType::Stream => "stream",
                    KeyType::HyperLogLog => "hyperloglog",
                };
                TestResult::Ok(name.into())
            }
            Ok(None) => TestResult::Ok("none".into()),
            Err(e) => TestResult::Err(e.to_string()),
        }
    }

    fn cmd_rename(&self, args: &[&str]) -> TestResult {
        if args.len() < 3 {
            return TestResult::Err("RENAME requires old and new key".into());
        }
        match self.db.rename(args[1], args[2]) {
            Ok(()) => TestResult::Ok("OK".into()),
            Err(e) => TestResult::Err(e.to_string()),
        }
    }

    // -- List commands -------------------------------------------------------

    fn cmd_lpush(&self, args: &[&str]) -> TestResult {
        if args.len() < 3 {
            return TestResult::Err("LPUSH requires key and at least one value".into());
        }
        match self.db.lpush(args[1], &args[2..]) {
            Ok(len) => TestResult::Int(len as i64),
            Err(e) => TestResult::Err(e.to_string()),
        }
    }

    fn cmd_rpush(&self, args: &[&str]) -> TestResult {
        if args.len() < 3 {
            return TestResult::Err("RPUSH requires key and at least one value".into());
        }
        match self.db.rpush(args[1], &args[2..]) {
            Ok(len) => TestResult::Int(len as i64),
            Err(e) => TestResult::Err(e.to_string()),
        }
    }

    fn cmd_lpop(&self, args: &[&str]) -> TestResult {
        if args.len() < 2 {
            return TestResult::Err("LPOP requires a key".into());
        }
        match self.db.lpop(args[1]) {
            Ok(Some(v)) => TestResult::Bulk(String::from_utf8_lossy(&v).into()),
            Ok(None) => TestResult::Nil,
            Err(e) => TestResult::Err(e.to_string()),
        }
    }

    fn cmd_rpop(&self, args: &[&str]) -> TestResult {
        if args.len() < 2 {
            return TestResult::Err("RPOP requires a key".into());
        }
        match self.db.rpop(args[1]) {
            Ok(Some(v)) => TestResult::Bulk(String::from_utf8_lossy(&v).into()),
            Ok(None) => TestResult::Nil,
            Err(e) => TestResult::Err(e.to_string()),
        }
    }

    fn cmd_lrange(&self, args: &[&str]) -> TestResult {
        if args.len() < 4 {
            return TestResult::Err("LRANGE requires key, start, stop".into());
        }
        let start: i64 = args[2].parse().unwrap_or(0);
        let stop: i64 = args[3].parse().unwrap_or(-1);
        match self.db.lrange(args[1], start, stop) {
            Ok(items) => {
                let results: Vec<TestResult> = items
                    .into_iter()
                    .map(|v| TestResult::Bulk(String::from_utf8_lossy(&v).into()))
                    .collect();
                TestResult::Array(results)
            }
            Err(e) => TestResult::Err(e.to_string()),
        }
    }

    fn cmd_llen(&self, args: &[&str]) -> TestResult {
        if args.len() < 2 {
            return TestResult::Err("LLEN requires a key".into());
        }
        match self.db.llen(args[1]) {
            Ok(n) => TestResult::Int(n as i64),
            Err(e) => TestResult::Err(e.to_string()),
        }
    }

    // -- Hash commands -------------------------------------------------------

    fn cmd_hset(&self, args: &[&str]) -> TestResult {
        if args.len() < 4 {
            return TestResult::Err("HSET requires key, field, value".into());
        }
        match self.db.hset(args[1], args[2], args[3]) {
            Ok(new) => TestResult::Int(if new { 1 } else { 0 }),
            Err(e) => TestResult::Err(e.to_string()),
        }
    }

    fn cmd_hget(&self, args: &[&str]) -> TestResult {
        if args.len() < 3 {
            return TestResult::Err("HGET requires key and field".into());
        }
        match self.db.hget(args[1], args[2]) {
            Ok(Some(v)) => TestResult::Bulk(String::from_utf8_lossy(&v).into()),
            Ok(None) => TestResult::Nil,
            Err(e) => TestResult::Err(e.to_string()),
        }
    }

    fn cmd_hdel(&self, args: &[&str]) -> TestResult {
        if args.len() < 3 {
            return TestResult::Err("HDEL requires key and at least one field".into());
        }
        match self.db.hdel(args[1], &args[2..]) {
            Ok(n) => TestResult::Int(n as i64),
            Err(e) => TestResult::Err(e.to_string()),
        }
    }

    fn cmd_hgetall(&self, args: &[&str]) -> TestResult {
        if args.len() < 2 {
            return TestResult::Err("HGETALL requires a key".into());
        }
        match self.db.hgetall(args[1]) {
            Ok(map) => {
                let mut results: Vec<TestResult> = Vec::with_capacity(map.len() * 2);
                // Sort by key for deterministic output
                let mut entries: Vec<_> = map.into_iter().collect();
                entries.sort_by(|a, b| a.0.cmp(&b.0));
                for (k, v) in entries {
                    results.push(TestResult::Bulk(String::from_utf8_lossy(&k).into()));
                    results.push(TestResult::Bulk(String::from_utf8_lossy(&v).into()));
                }
                TestResult::Array(results)
            }
            Err(e) => TestResult::Err(e.to_string()),
        }
    }

    fn cmd_hlen(&self, args: &[&str]) -> TestResult {
        if args.len() < 2 {
            return TestResult::Err("HLEN requires a key".into());
        }
        match self.db.hlen(args[1]) {
            Ok(n) => TestResult::Int(n as i64),
            Err(e) => TestResult::Err(e.to_string()),
        }
    }

    // -- Set commands --------------------------------------------------------

    fn cmd_sadd(&self, args: &[&str]) -> TestResult {
        if args.len() < 3 {
            return TestResult::Err("SADD requires key and at least one member".into());
        }
        match self.db.sadd(args[1], &args[2..]) {
            Ok(n) => TestResult::Int(n as i64),
            Err(e) => TestResult::Err(e.to_string()),
        }
    }

    fn cmd_srem(&self, args: &[&str]) -> TestResult {
        if args.len() < 3 {
            return TestResult::Err("SREM requires key and at least one member".into());
        }
        match self.db.srem(args[1], &args[2..]) {
            Ok(n) => TestResult::Int(n as i64),
            Err(e) => TestResult::Err(e.to_string()),
        }
    }

    fn cmd_smembers(&self, args: &[&str]) -> TestResult {
        if args.len() < 2 {
            return TestResult::Err("SMEMBERS requires a key".into());
        }
        match self.db.smembers(args[1]) {
            Ok(members) => {
                let mut results: Vec<TestResult> = members
                    .into_iter()
                    .map(|v| TestResult::Bulk(String::from_utf8_lossy(&v).into()))
                    .collect();
                // Sort for deterministic output
                results.sort_by(|a, b| {
                    if let (TestResult::Bulk(a), TestResult::Bulk(b)) = (a, b) {
                        a.cmp(b)
                    } else {
                        std::cmp::Ordering::Equal
                    }
                });
                TestResult::Array(results)
            }
            Err(e) => TestResult::Err(e.to_string()),
        }
    }

    fn cmd_scard(&self, args: &[&str]) -> TestResult {
        if args.len() < 2 {
            return TestResult::Err("SCARD requires a key".into());
        }
        match self.db.scard(args[1]) {
            Ok(n) => TestResult::Int(n as i64),
            Err(e) => TestResult::Err(e.to_string()),
        }
    }

    fn cmd_sismember(&self, args: &[&str]) -> TestResult {
        if args.len() < 3 {
            return TestResult::Err("SISMEMBER requires key and member".into());
        }
        match self.db.sismember(args[1], args[2]) {
            Ok(true) => TestResult::Int(1),
            Ok(false) => TestResult::Int(0),
            Err(e) => TestResult::Err(e.to_string()),
        }
    }
}

// ============================================================================
// Helper: create a fresh server + client pair for each test
// ============================================================================

fn setup() -> (TestServer, u16) {
    static PORT: std::sync::atomic::AtomicU16 = std::sync::atomic::AtomicU16::new(16379);
    let port = PORT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let mut server = TestServer::new(port);
    server.start();
    (server, port)
}

// ============================================================================
// Core operation tests
// ============================================================================

#[test]
fn test_ping_pong() {
    let (server, _) = setup();
    let mut client = TestClient::connect(&server.addr(), &server);
    assert_eq!(client.command(&["PING"]), TestResult::Ok("PONG".into()));
}

#[test]
fn test_set_get() {
    let (server, _) = setup();
    let mut client = TestClient::connect(&server.addr(), &server);

    assert_eq!(
        client.command(&["SET", "mykey", "hello"]),
        TestResult::Ok("OK".into())
    );
    assert_eq!(
        client.command(&["GET", "mykey"]),
        TestResult::Bulk("hello".into())
    );
}

#[test]
fn test_set_get_delete() {
    let (server, _) = setup();
    let mut client = TestClient::connect(&server.addr(), &server);

    client.command(&["SET", "k", "v"]);
    assert_eq!(client.command(&["GET", "k"]), TestResult::Bulk("v".into()));
    assert_eq!(client.command(&["DEL", "k"]), TestResult::Int(1));
    assert_eq!(client.command(&["GET", "k"]), TestResult::Nil);
}

#[test]
fn test_exists() {
    let (server, _) = setup();
    let mut client = TestClient::connect(&server.addr(), &server);

    client.command(&["SET", "a", "1"]);
    assert_eq!(client.command(&["EXISTS", "a"]), TestResult::Int(1));
    assert_eq!(client.command(&["EXISTS", "b"]), TestResult::Int(0));
    // Multiple keys
    assert_eq!(client.command(&["EXISTS", "a", "b"]), TestResult::Int(1));
}

#[test]
fn test_incr_decr() {
    let (server, _) = setup();
    let mut client = TestClient::connect(&server.addr(), &server);

    // INCR on non-existent key starts at 0, returns 1
    assert_eq!(client.command(&["INCR", "counter"]), TestResult::Int(1));
    assert_eq!(client.command(&["INCR", "counter"]), TestResult::Int(2));
    assert_eq!(client.command(&["DECR", "counter"]), TestResult::Int(1));
    assert_eq!(
        client.command(&["INCRBY", "counter", "10"]),
        TestResult::Int(11)
    );
}

#[test]
fn test_mget_mset() {
    let (server, _) = setup();
    let mut client = TestClient::connect(&server.addr(), &server);

    assert_eq!(
        client.command(&["MSET", "k1", "v1", "k2", "v2", "k3", "v3"]),
        TestResult::Ok("OK".into())
    );
    assert_eq!(
        client.command(&["MGET", "k1", "k2", "k3", "missing"]),
        TestResult::Array(vec![
            TestResult::Bulk("v1".into()),
            TestResult::Bulk("v2".into()),
            TestResult::Bulk("v3".into()),
            TestResult::Nil,
        ])
    );
}

#[test]
fn test_append_strlen() {
    let (server, _) = setup();
    let mut client = TestClient::connect(&server.addr(), &server);

    assert_eq!(
        client.command(&["APPEND", "greeting", "Hello"]),
        TestResult::Int(5)
    );
    assert_eq!(
        client.command(&["APPEND", "greeting", " World"]),
        TestResult::Int(11)
    );
    assert_eq!(client.command(&["STRLEN", "greeting"]), TestResult::Int(11));
    assert_eq!(
        client.command(&["GET", "greeting"]),
        TestResult::Bulk("Hello World".into())
    );
}

#[test]
fn test_expire_ttl() {
    let (server, _) = setup();
    let mut client = TestClient::connect(&server.addr(), &server);

    client.command(&["SET", "temp", "data"]);
    // No expiry yet
    assert_eq!(client.command(&["TTL", "temp"]), TestResult::Int(-1));
    // Set 2-second expiry
    assert_eq!(client.command(&["EXPIRE", "temp", "2"]), TestResult::Int(1));
    // TTL should be non-negative (may be 0 if sub-second resolution rounds down)
    let ttl = client.command(&["TTL", "temp"]);
    match ttl {
        TestResult::Int(v) => assert!(v >= 0, "TTL should be non-negative, got {v}"),
        other => panic!("expected Int, got {other:?}"),
    }
}

#[test]
fn test_set_with_ex() {
    let (server, _) = setup();
    let mut client = TestClient::connect(&server.addr(), &server);

    assert_eq!(
        client.command(&["SET", "expiring", "data", "EX", "10"]),
        TestResult::Ok("OK".into())
    );
    assert_eq!(
        client.command(&["GET", "expiring"]),
        TestResult::Bulk("data".into())
    );
    let ttl = client.command(&["TTL", "expiring"]);
    match ttl {
        TestResult::Int(v) => assert!(v >= 0 && v <= 10, "TTL should be 0..=10, got {v}"),
        other => panic!("expected Int, got {other:?}"),
    }
}

#[test]
fn test_setnx() {
    let (server, _) = setup();
    let mut client = TestClient::connect(&server.addr(), &server);

    // SETNX on new key succeeds
    assert_eq!(
        client.command(&["SETNX", "unique", "first"]),
        TestResult::Int(1)
    );
    // SETNX on existing key fails
    assert_eq!(
        client.command(&["SETNX", "unique", "second"]),
        TestResult::Int(0)
    );
    // Original value preserved
    assert_eq!(
        client.command(&["GET", "unique"]),
        TestResult::Bulk("first".into())
    );
}

// ============================================================================
// Data structure tests
// ============================================================================

#[test]
fn test_list_operations() {
    let (server, _) = setup();
    let mut client = TestClient::connect(&server.addr(), &server);

    // LPUSH adds to head
    assert_eq!(
        client.command(&["LPUSH", "mylist", "b", "a"]),
        TestResult::Int(2)
    );
    // RPUSH adds to tail
    assert_eq!(
        client.command(&["RPUSH", "mylist", "c"]),
        TestResult::Int(3)
    );
    assert_eq!(client.command(&["LLEN", "mylist"]), TestResult::Int(3));

    // LRANGE returns full list
    let range = client.command(&["LRANGE", "mylist", "0", "-1"]);
    if let TestResult::Array(items) = &range {
        assert_eq!(items.len(), 3);
    } else {
        panic!("expected Array, got {range:?}");
    }

    // LPOP/RPOP
    let left = client.command(&["LPOP", "mylist"]);
    assert!(matches!(left, TestResult::Bulk(_)));
    let right = client.command(&["RPOP", "mylist"]);
    assert!(matches!(right, TestResult::Bulk(_)));
    assert_eq!(client.command(&["LLEN", "mylist"]), TestResult::Int(1));
}

#[test]
fn test_hash_operations() {
    let (server, _) = setup();
    let mut client = TestClient::connect(&server.addr(), &server);

    assert_eq!(
        client.command(&["HSET", "user:1", "name", "Alice"]),
        TestResult::Int(1)
    );
    assert_eq!(
        client.command(&["HSET", "user:1", "age", "30"]),
        TestResult::Int(1)
    );
    assert_eq!(
        client.command(&["HGET", "user:1", "name"]),
        TestResult::Bulk("Alice".into())
    );
    assert_eq!(client.command(&["HLEN", "user:1"]), TestResult::Int(2));

    // HGETALL returns field-value pairs
    let all = client.command(&["HGETALL", "user:1"]);
    if let TestResult::Array(items) = &all {
        assert_eq!(items.len(), 4); // 2 fields × 2 (field + value)
    } else {
        panic!("expected Array, got {all:?}");
    }

    // HDEL
    assert_eq!(
        client.command(&["HDEL", "user:1", "age"]),
        TestResult::Int(1)
    );
    assert_eq!(client.command(&["HLEN", "user:1"]), TestResult::Int(1));
}

#[test]
fn test_set_operations() {
    let (server, _) = setup();
    let mut client = TestClient::connect(&server.addr(), &server);

    assert_eq!(
        client.command(&["SADD", "fruits", "apple", "banana", "cherry"]),
        TestResult::Int(3)
    );
    assert_eq!(client.command(&["SCARD", "fruits"]), TestResult::Int(3));
    assert_eq!(
        client.command(&["SISMEMBER", "fruits", "apple"]),
        TestResult::Int(1)
    );
    assert_eq!(
        client.command(&["SISMEMBER", "fruits", "grape"]),
        TestResult::Int(0)
    );

    // SREM
    assert_eq!(
        client.command(&["SREM", "fruits", "banana"]),
        TestResult::Int(1)
    );
    assert_eq!(client.command(&["SCARD", "fruits"]), TestResult::Int(2));

    // SMEMBERS
    let members = client.command(&["SMEMBERS", "fruits"]);
    if let TestResult::Array(items) = &members {
        assert_eq!(items.len(), 2);
    } else {
        panic!("expected Array, got {members:?}");
    }
}

#[test]
fn test_sorted_set_operations() {
    // Sorted set operations are not yet implemented in the embedded Database API.
    // This test verifies that the raw Store-level SortedSet Value type works.
    use bytes::Bytes;
    use ferrite_core::storage::{Store, Value};
    use ordered_float::OrderedFloat;
    use std::collections::{BTreeMap, HashMap};

    let store = Store::new(16);

    // Build a sorted set manually via the Value enum
    let mut by_score = BTreeMap::new();
    let mut by_member = HashMap::new();
    for (member, score) in [("alice", 100.0), ("bob", 200.0), ("charlie", 150.0)] {
        let m = Bytes::from(member);
        by_score.insert((OrderedFloat(score), m.clone()), ());
        by_member.insert(m, score);
    }
    store.set(
        0,
        Bytes::from("leaderboard"),
        Value::SortedSet {
            by_score,
            by_member,
        },
    );

    // Verify it stored correctly
    let val = store.get(0, &Bytes::from("leaderboard"));
    assert!(val.is_some());
    if let Some(Value::SortedSet {
        by_member,
        by_score,
    }) = val
    {
        // ZCARD equivalent
        assert_eq!(by_member.len(), 3);
        // ZSCORE equivalent
        assert_eq!(by_member.get(&Bytes::from("alice")), Some(&100.0));
        // ZRANGE equivalent (by_score is ordered)
        let ranked: Vec<_> = by_score.keys().map(|(_, m)| m.clone()).collect();
        assert_eq!(ranked[0], Bytes::from("alice")); // score 100
        assert_eq!(ranked[1], Bytes::from("charlie")); // score 150
        assert_eq!(ranked[2], Bytes::from("bob")); // score 200
                                                   // ZRANK equivalent
        let rank = ranked.iter().position(|m| m == &Bytes::from("charlie"));
        assert_eq!(rank, Some(1));
    } else {
        panic!("expected SortedSet value");
    }
}

// ============================================================================
// Server command tests
// ============================================================================

#[test]
fn test_dbsize() {
    let (server, _) = setup();
    let mut client = TestClient::connect(&server.addr(), &server);

    assert_eq!(client.command(&["DBSIZE"]), TestResult::Int(0));
    client.command(&["SET", "a", "1"]);
    client.command(&["SET", "b", "2"]);
    assert_eq!(client.command(&["DBSIZE"]), TestResult::Int(2));
    client.command(&["DEL", "a"]);
    assert_eq!(client.command(&["DBSIZE"]), TestResult::Int(1));
}

#[test]
fn test_flushdb() {
    let (server, _) = setup();
    let mut client = TestClient::connect(&server.addr(), &server);

    client.command(&["SET", "x", "1"]);
    client.command(&["SET", "y", "2"]);
    assert_eq!(client.command(&["DBSIZE"]), TestResult::Int(2));
    assert_eq!(client.command(&["FLUSHDB"]), TestResult::Ok("OK".into()));
    assert_eq!(client.command(&["DBSIZE"]), TestResult::Int(0));
}

#[test]
fn test_info() {
    let (server, _) = setup();
    let mut client = TestClient::connect(&server.addr(), &server);

    let result = client.command(&["INFO"]);
    // INFO should return a bulk string with server stats
    assert!(
        matches!(result, TestResult::Bulk(_)),
        "expected Bulk, got {result:?}"
    );
}

#[test]
fn test_select_database() {
    // SELECT requires &mut Database, so we call it directly
    let mut db = Database::memory().unwrap();

    db.set("key_in_0", "value0").unwrap();
    assert!(db.get("key_in_0").unwrap().is_some());

    // Switch to database 1
    db.select(1).unwrap();
    // key_in_0 should not exist in database 1
    assert!(db.get("key_in_0").unwrap().is_none());

    db.set("key_in_1", "value1").unwrap();

    // Switch back to database 0
    db.select(0).unwrap();
    assert_eq!(db.get("key_in_0").unwrap(), Some(b"value0".to_vec()));
    // key_in_1 should not exist in database 0
    assert!(db.get("key_in_1").unwrap().is_none());
}

#[test]
fn test_type_command() {
    let (server, _) = setup();
    let mut client = TestClient::connect(&server.addr(), &server);

    // Non-existent key
    assert_eq!(
        client.command(&["TYPE", "missing"]),
        TestResult::Ok("none".into())
    );

    // String
    client.command(&["SET", "str_key", "hello"]);
    assert_eq!(
        client.command(&["TYPE", "str_key"]),
        TestResult::Ok("string".into())
    );

    // List
    client.command(&["LPUSH", "list_key", "a"]);
    assert_eq!(
        client.command(&["TYPE", "list_key"]),
        TestResult::Ok("list".into())
    );

    // Hash
    client.command(&["HSET", "hash_key", "f", "v"]);
    assert_eq!(
        client.command(&["TYPE", "hash_key"]),
        TestResult::Ok("hash".into())
    );

    // Set
    client.command(&["SADD", "set_key", "x"]);
    assert_eq!(
        client.command(&["TYPE", "set_key"]),
        TestResult::Ok("set".into())
    );
}

#[test]
fn test_rename() {
    let (server, _) = setup();
    let mut client = TestClient::connect(&server.addr(), &server);

    client.command(&["SET", "old_name", "data"]);
    assert_eq!(
        client.command(&["RENAME", "old_name", "new_name"]),
        TestResult::Ok("OK".into())
    );
    assert_eq!(client.command(&["GET", "old_name"]), TestResult::Nil);
    assert_eq!(
        client.command(&["GET", "new_name"]),
        TestResult::Bulk("data".into())
    );
}

// ============================================================================
// Pipeline test
// ============================================================================

#[test]
fn test_pipeline_execution() {
    let (server, _) = setup();
    let mut client = TestClient::connect(&server.addr(), &server);

    let results = client.pipeline(vec![
        vec!["SET", "p1", "val1"],
        vec!["SET", "p2", "val2"],
        vec!["GET", "p1"],
        vec!["GET", "p2"],
        vec!["DEL", "p1", "p2"],
        vec!["DBSIZE"],
    ]);

    assert_eq!(results.len(), 6);
    assert_eq!(results[0], TestResult::Ok("OK".into()));
    assert_eq!(results[1], TestResult::Ok("OK".into()));
    assert_eq!(results[2], TestResult::Bulk("val1".into()));
    assert_eq!(results[3], TestResult::Bulk("val2".into()));
    assert_eq!(results[4], TestResult::Int(2));
    assert_eq!(results[5], TestResult::Int(0));
}

// ============================================================================
// Edge case tests
// ============================================================================

#[test]
fn test_get_nonexistent_key() {
    let (server, _) = setup();
    let mut client = TestClient::connect(&server.addr(), &server);

    assert_eq!(client.command(&["GET", "no_such_key"]), TestResult::Nil);
}

#[test]
fn test_del_multiple_keys() {
    let (server, _) = setup();
    let mut client = TestClient::connect(&server.addr(), &server);

    client.command(&["SET", "a", "1"]);
    client.command(&["SET", "b", "2"]);
    client.command(&["SET", "c", "3"]);
    // DEL returns count of keys actually deleted
    assert_eq!(
        client.command(&["DEL", "a", "b", "nonexistent"]),
        TestResult::Int(2)
    );
}

#[test]
fn test_overwrite_value() {
    let (server, _) = setup();
    let mut client = TestClient::connect(&server.addr(), &server);

    client.command(&["SET", "key", "first"]);
    assert_eq!(
        client.command(&["GET", "key"]),
        TestResult::Bulk("first".into())
    );
    client.command(&["SET", "key", "second"]);
    assert_eq!(
        client.command(&["GET", "key"]),
        TestResult::Bulk("second".into())
    );
}
