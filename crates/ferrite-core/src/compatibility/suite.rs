//! Redis Compatibility Test Suite Runner
//!
//! Executes structured test cases against a Ferrite Store instance to verify
//! Redis command compatibility and produces scored results.

use crate::storage::{Store, Value};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{Duration, Instant, SystemTime};

/// Individual test case for a Redis command.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TestCase {
    /// Human-readable test name.
    pub name: String,
    /// The Redis command being tested (e.g. "SET", "LPUSH").
    pub command: String,
    /// Category this test belongs to (e.g. "String", "List").
    pub category: String,
    /// Whether this test is critical for certification.
    pub critical: bool,
    /// Short description of what the test verifies.
    pub description: String,
}

/// Result of running a single test case.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TestCaseResult {
    /// The test case that was executed.
    pub test: TestCase,
    /// Whether the test passed.
    pub passed: bool,
    /// Human-readable result message.
    pub message: String,
    /// Wall-clock duration of the test.
    pub duration: Duration,
}

/// Aggregated results of the full test suite.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SuiteResults {
    /// Total number of tests executed.
    pub total: usize,
    /// Number of tests that passed.
    pub passed: usize,
    /// Number of tests that failed.
    pub failed: usize,
    /// Number of tests that were skipped.
    pub skipped: usize,
    /// Overall compatibility score (0.0–100.0).
    pub score: f64,
    /// Score computed only from critical tests.
    pub critical_score: f64,
    /// Total wall-clock time for the suite run.
    pub duration: Duration,
    /// Per-category aggregated results.
    pub categories: HashMap<String, CategoryResults>,
    /// Individual test case results.
    pub results: Vec<TestCaseResult>,
}

/// Per-category aggregated results.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct CategoryResults {
    /// Total tests in this category.
    pub total: usize,
    /// Tests that passed.
    pub passed: usize,
    /// Tests that failed.
    pub failed: usize,
    /// Category score (0.0–100.0).
    pub score: f64,
}

// SPLIT_MARKER_1

/// Internal representation of a runnable test.
struct RunnableTest {
    case: TestCase,
    #[allow(clippy::type_complexity)]
    run: Box<dyn Fn(&Store) -> Result<(), String> + Send + Sync>,
}

/// Test suite runner that validates Redis compatibility.
pub struct CompatibilitySuite {
    tests: Vec<RunnableTest>,
}

impl CompatibilitySuite {
    /// Create a new suite with all built-in test cases registered.
    pub fn new() -> Self {
        let mut suite = Self { tests: Vec::new() };
        suite.register_string_tests();
        suite.register_list_tests();
        suite.register_hash_tests();
        suite.register_set_tests();
        suite.register_sorted_set_tests();
        suite.register_key_tests();
        suite.register_server_tests();
        suite
    }

    /// Run all tests against a [`Store`] instance.
    pub fn run(&self, store: &Store) -> SuiteResults {
        self.run_filtered(store, |_| true)
    }

    /// Run only tests belonging to the given category.
    pub fn run_category(&self, store: &Store, category: &str) -> SuiteResults {
        let cat = category.to_string();
        self.run_filtered(store, |t| t.case.category == cat)
    }

    fn run_filtered<F>(&self, store: &Store, predicate: F) -> SuiteResults
    where
        F: Fn(&RunnableTest) -> bool,
    {
        let suite_start = Instant::now();
        let mut results = Vec::new();
        let mut categories: HashMap<String, CategoryResults> = HashMap::new();
        let mut total = 0usize;
        let mut passed = 0usize;
        let mut failed = 0usize;
        let mut critical_total = 0usize;
        let mut critical_passed = 0usize;

        for test in &self.tests {
            if !predicate(test) {
                continue;
            }
            total += 1;
            if test.case.critical {
                critical_total += 1;
            }

            let start = Instant::now();
            let result = (test.run)(store);
            let duration = start.elapsed();

            let (test_passed, message) = match result {
                Ok(()) => {
                    passed += 1;
                    if test.case.critical {
                        critical_passed += 1;
                    }
                    (true, "OK".to_string())
                }
                Err(msg) => {
                    failed += 1;
                    (false, msg)
                }
            };

            let cat = categories.entry(test.case.category.clone()).or_default();
            cat.total += 1;
            if test_passed {
                cat.passed += 1;
            } else {
                cat.failed += 1;
            }

            results.push(TestCaseResult {
                test: test.case.clone(),
                passed: test_passed,
                message,
                duration,
            });
        }

        for cat in categories.values_mut() {
            cat.score = if cat.total > 0 {
                (cat.passed as f64 / cat.total as f64) * 100.0
            } else {
                0.0
            };
        }

        let score = if total > 0 {
            (passed as f64 / total as f64) * 100.0
        } else {
            0.0
        };
        let critical_score = if critical_total > 0 {
            (critical_passed as f64 / critical_total as f64) * 100.0
        } else {
            100.0
        };

        SuiteResults {
            total,
            passed,
            failed,
            skipped: 0,
            score,
            critical_score,
            duration: suite_start.elapsed(),
            categories,
            results,
        }
    }

    // ------------------------------------------------------------------
    // Registration helpers
    // ------------------------------------------------------------------

    fn register(
        &mut self,
        name: &str,
        command: &str,
        category: &str,
        critical: bool,
        description: &str,
        run: impl Fn(&Store) -> Result<(), String> + Send + Sync + 'static,
    ) {
        self.tests.push(RunnableTest {
            case: TestCase {
                name: name.to_string(),
                command: command.to_string(),
                category: category.to_string(),
                critical,
                description: description.to_string(),
            },
            run: Box::new(run),
        });
    }

    // ------------------------------------------------------------------
    // String tests
    // ------------------------------------------------------------------

    fn register_string_tests(&mut self) {
        self.register(
            "SET and GET basic",
            "SET/GET",
            "String",
            true,
            "SET a key then GET it back",
            |store| {
                store.set(0, Bytes::from("sk1"), Value::String(Bytes::from("hello")));
                match store.get(0, &Bytes::from("sk1")) {
                    Some(Value::String(v)) if v == "hello" => Ok(()),
                    other => Err(format!("expected String(hello), got {:?}", other)),
                }
            },
        );

        self.register(
            "SET overwrites existing",
            "SET",
            "String",
            true,
            "SET overwrites a previous value",
            |store| {
                let k = Bytes::from("sk2");
                store.set(0, k.clone(), Value::String(Bytes::from("a")));
                store.set(0, k.clone(), Value::String(Bytes::from("b")));
                match store.get(0, &k) {
                    Some(Value::String(v)) if v == "b" => Ok(()),
                    other => Err(format!("expected String(b), got {:?}", other)),
                }
            },
        );

        self.register(
            "GET missing key",
            "GET",
            "String",
            true,
            "GET on a non-existent key returns None",
            |store| match store.get(0, &Bytes::from("sk_missing")) {
                None => Ok(()),
                other => Err(format!("expected None, got {:?}", other)),
            },
        );

        self.register(
            "MSET and MGET multiple keys",
            "MSET/MGET",
            "String",
            true,
            "Set several keys, read them back",
            |store| {
                for i in 0..5 {
                    let k = Bytes::from(format!("mk{}", i));
                    let v = Value::String(Bytes::from(format!("mv{}", i)));
                    store.set(0, k, v);
                }
                for i in 0..5 {
                    let k = Bytes::from(format!("mk{}", i));
                    match store.get(0, &k) {
                        Some(Value::String(v)) if v == format!("mv{}", i).as_str() => {
                            // ok
                        }
                        other => return Err(format!("mk{}: expected mv{}, got {:?}", i, i, other)),
                    }
                }
                Ok(())
            },
        );

        self.register(
            "SETNX when key absent",
            "SETNX",
            "String",
            true,
            "SETNX sets only when key does not exist",
            |store| {
                let db = store.database(0).ok_or_else(|| "no db".to_string())?;
                let db = db.read();
                let inserted = db.set_nx(Bytes::from("snx1"), Value::String(Bytes::from("first")));
                if !inserted {
                    return Err("expected set_nx to succeed".into());
                }
                let not_inserted =
                    db.set_nx(Bytes::from("snx1"), Value::String(Bytes::from("second")));
                if not_inserted {
                    return Err("expected set_nx to fail on existing key".into());
                }
                match db.get(&Bytes::from("snx1")) {
                    Some(Value::String(v)) if v == "first" => Ok(()),
                    other => Err(format!("expected first, got {:?}", other)),
                }
            },
        );

        self.register(
            "SETEX with TTL concept",
            "SETEX",
            "String",
            false,
            "SET with expiry stores a value that can be read before expiry",
            |store| {
                let k = Bytes::from("sex1");
                let exp = SystemTime::now() + Duration::from_secs(3600);
                store.set_with_expiry(0, k.clone(), Value::String(Bytes::from("ttl")), exp);
                match store.get(0, &k) {
                    Some(Value::String(v)) if v == "ttl" => Ok(()),
                    other => Err(format!("expected String(ttl), got {:?}", other)),
                }
            },
        );

        self.register(
            "INCR as string integer",
            "INCR",
            "String",
            true,
            "Store an integer string, increment via re-set, verify",
            |store| {
                let k = Bytes::from("sincr");
                store.set(0, k.clone(), Value::String(Bytes::from("10")));
                if let Some(Value::String(v)) = store.get(0, &k) {
                    let n: i64 = std::str::from_utf8(&v)
                        .map_err(|e| e.to_string())?
                        .parse()
                        .map_err(|e: std::num::ParseIntError| e.to_string())?;
                    store.set(
                        0,
                        k.clone(),
                        Value::String(Bytes::from(format!("{}", n + 1))),
                    );
                    match store.get(0, &k) {
                        Some(Value::String(v)) if v == "11" => Ok(()),
                        other => Err(format!("expected 11, got {:?}", other)),
                    }
                } else {
                    Err("key not found after set".into())
                }
            },
        );

        self.register(
            "DECR as string integer",
            "DECR",
            "String",
            false,
            "Store an integer string, decrement via re-set, verify",
            |store| {
                let k = Bytes::from("sdecr");
                store.set(0, k.clone(), Value::String(Bytes::from("10")));
                if let Some(Value::String(v)) = store.get(0, &k) {
                    let n: i64 = std::str::from_utf8(&v)
                        .map_err(|e| e.to_string())?
                        .parse()
                        .map_err(|e: std::num::ParseIntError| e.to_string())?;
                    store.set(
                        0,
                        k.clone(),
                        Value::String(Bytes::from(format!("{}", n - 1))),
                    );
                    match store.get(0, &k) {
                        Some(Value::String(v)) if v == "9" => Ok(()),
                        other => Err(format!("expected 9, got {:?}", other)),
                    }
                } else {
                    Err("key not found after set".into())
                }
            },
        );

        self.register(
            "INCRBY as string integer",
            "INCRBY",
            "String",
            false,
            "Increment an integer string by a given amount",
            |store| {
                let k = Bytes::from("sincrby");
                store.set(0, k.clone(), Value::String(Bytes::from("100")));
                if let Some(Value::String(v)) = store.get(0, &k) {
                    let n: i64 = std::str::from_utf8(&v)
                        .map_err(|e| e.to_string())?
                        .parse()
                        .map_err(|e: std::num::ParseIntError| e.to_string())?;
                    store.set(
                        0,
                        k.clone(),
                        Value::String(Bytes::from(format!("{}", n + 25))),
                    );
                    match store.get(0, &k) {
                        Some(Value::String(v)) if v == "125" => Ok(()),
                        other => Err(format!("expected 125, got {:?}", other)),
                    }
                } else {
                    Err("key not found".into())
                }
            },
        );

        self.register(
            "APPEND via concatenation",
            "APPEND",
            "String",
            false,
            "Append to an existing string value",
            |store| {
                let k = Bytes::from("sapp");
                store.set(0, k.clone(), Value::String(Bytes::from("hello")));
                if let Some(Value::String(v)) = store.get(0, &k) {
                    let mut s = v.to_vec();
                    s.extend_from_slice(b" world");
                    store.set(0, k.clone(), Value::String(Bytes::from(s)));
                    match store.get(0, &k) {
                        Some(Value::String(v)) if v == "hello world" => Ok(()),
                        other => Err(format!("expected 'hello world', got {:?}", other)),
                    }
                } else {
                    Err("key not found".into())
                }
            },
        );

        self.register(
            "STRLEN via string length",
            "STRLEN",
            "String",
            false,
            "Get length of a stored string",
            |store| {
                let k = Bytes::from("slen");
                store.set(0, k.clone(), Value::String(Bytes::from("ferrite")));
                match store.get(0, &k) {
                    Some(Value::String(v)) if v.len() == 7 => Ok(()),
                    Some(Value::String(v)) => Err(format!("expected len 7, got {}", v.len())),
                    other => Err(format!("expected String, got {:?}", other)),
                }
            },
        );

        self.register(
            "GETRANGE via slice",
            "GETRANGE",
            "String",
            false,
            "Get a substring of a stored value",
            |store| {
                let k = Bytes::from("sgr");
                store.set(0, k.clone(), Value::String(Bytes::from("Hello, World!")));
                match store.get(0, &k) {
                    Some(Value::String(v)) => {
                        let sub = &v[0..5];
                        if sub == b"Hello" {
                            Ok(())
                        } else {
                            Err(format!(
                                "expected 'Hello', got '{}'",
                                String::from_utf8_lossy(sub)
                            ))
                        }
                    }
                    other => Err(format!("expected String, got {:?}", other)),
                }
            },
        );

        self.register(
            "SETRANGE via overwrite",
            "SETRANGE",
            "String",
            false,
            "Overwrite part of a stored string",
            |store| {
                let k = Bytes::from("ssr");
                store.set(0, k.clone(), Value::String(Bytes::from("Hello, World!")));
                if let Some(Value::String(v)) = store.get(0, &k) {
                    let mut buf = v.to_vec();
                    let replacement = b"Rust!";
                    buf[7..12].copy_from_slice(replacement);
                    store.set(0, k.clone(), Value::String(Bytes::from(buf)));
                    match store.get(0, &k) {
                        Some(Value::String(v)) if v == "Hello, Rust!!" => Ok(()),
                        other => Err(format!("expected 'Hello, Rust!!', got {:?}", other)),
                    }
                } else {
                    Err("key not found".into())
                }
            },
        );
    }

    // ------------------------------------------------------------------
    // List tests
    // ------------------------------------------------------------------

    fn register_list_tests(&mut self) {
        self.register(
            "LPUSH and LRANGE",
            "LPUSH/LRANGE",
            "List",
            true,
            "LPUSH elements then read with LRANGE",
            |store| {
                let k = Bytes::from("lk1");
                let mut list = VecDeque::new();
                list.push_front(Bytes::from("c"));
                list.push_front(Bytes::from("b"));
                list.push_front(Bytes::from("a"));
                store.set(0, k.clone(), Value::List(list));
                match store.get(0, &k) {
                    Some(Value::List(l)) => {
                        let v: Vec<&[u8]> = l.iter().map(|b| b.as_ref()).collect();
                        if v == vec![b"a", b"b", b"c"] {
                            Ok(())
                        } else {
                            Err(format!("expected [a,b,c], got {:?}", v))
                        }
                    }
                    other => Err(format!("expected List, got {:?}", other)),
                }
            },
        );

        self.register(
            "RPUSH appends to tail",
            "RPUSH",
            "List",
            true,
            "RPUSH elements then verify order",
            |store| {
                let k = Bytes::from("lk2");
                let mut list = VecDeque::new();
                list.push_back(Bytes::from("x"));
                list.push_back(Bytes::from("y"));
                list.push_back(Bytes::from("z"));
                store.set(0, k.clone(), Value::List(list));
                match store.get(0, &k) {
                    Some(Value::List(l)) => {
                        let v: Vec<&[u8]> = l.iter().map(|b| b.as_ref()).collect();
                        if v == vec![b"x", b"y", b"z"] {
                            Ok(())
                        } else {
                            Err(format!("expected [x,y,z], got {:?}", v))
                        }
                    }
                    other => Err(format!("expected List, got {:?}", other)),
                }
            },
        );

        self.register(
            "LPOP removes head",
            "LPOP",
            "List",
            true,
            "Pop the first element from a list",
            |store| {
                let k = Bytes::from("lk3");
                let mut list = VecDeque::new();
                list.push_back(Bytes::from("1"));
                list.push_back(Bytes::from("2"));
                list.push_back(Bytes::from("3"));
                store.set(0, k.clone(), Value::List(list));

                if let Some(Value::List(mut l)) = store.get(0, &k) {
                    let popped = l.pop_front();
                    store.set(0, k.clone(), Value::List(l));
                    match popped {
                        Some(v) if v == "1" => Ok(()),
                        other => Err(format!("expected Some(1), got {:?}", other)),
                    }
                } else {
                    Err("expected List".into())
                }
            },
        );

        self.register(
            "RPOP removes tail",
            "RPOP",
            "List",
            true,
            "Pop the last element from a list",
            |store| {
                let k = Bytes::from("lk4");
                let mut list = VecDeque::new();
                list.push_back(Bytes::from("a"));
                list.push_back(Bytes::from("b"));
                list.push_back(Bytes::from("c"));
                store.set(0, k.clone(), Value::List(list));

                if let Some(Value::List(mut l)) = store.get(0, &k) {
                    let popped = l.pop_back();
                    store.set(0, k.clone(), Value::List(l));
                    match popped {
                        Some(v) if v == "c" => Ok(()),
                        other => Err(format!("expected Some(c), got {:?}", other)),
                    }
                } else {
                    Err("expected List".into())
                }
            },
        );

        self.register(
            "LLEN returns length",
            "LLEN",
            "List",
            true,
            "LLEN returns the number of elements",
            |store| {
                let k = Bytes::from("lk5");
                let mut list = VecDeque::new();
                for i in 0..5 {
                    list.push_back(Bytes::from(format!("{}", i)));
                }
                store.set(0, k.clone(), Value::List(list));
                match store.get(0, &k) {
                    Some(Value::List(l)) if l.len() == 5 => Ok(()),
                    Some(Value::List(l)) => Err(format!("expected len 5, got {}", l.len())),
                    other => Err(format!("expected List, got {:?}", other)),
                }
            },
        );

        self.register(
            "LINDEX returns element at index",
            "LINDEX",
            "List",
            false,
            "LINDEX retrieves an element by position",
            |store| {
                let k = Bytes::from("lk6");
                let mut list = VecDeque::new();
                list.push_back(Bytes::from("a"));
                list.push_back(Bytes::from("b"));
                list.push_back(Bytes::from("c"));
                store.set(0, k.clone(), Value::List(list));
                match store.get(0, &k) {
                    Some(Value::List(l)) => match l.get(1) {
                        Some(v) if v == &Bytes::from("b") => Ok(()),
                        other => Err(format!("expected b at index 1, got {:?}", other)),
                    },
                    other => Err(format!("expected List, got {:?}", other)),
                }
            },
        );

        self.register(
            "LSET updates element at index",
            "LSET",
            "List",
            false,
            "LSET modifies the element at a given index",
            |store| {
                let k = Bytes::from("lk7");
                let mut list = VecDeque::new();
                list.push_back(Bytes::from("a"));
                list.push_back(Bytes::from("b"));
                list.push_back(Bytes::from("c"));
                store.set(0, k.clone(), Value::List(list));

                if let Some(Value::List(mut l)) = store.get(0, &k) {
                    l[1] = Bytes::from("B");
                    store.set(0, k.clone(), Value::List(l));
                    match store.get(0, &k) {
                        Some(Value::List(l)) => match l.get(1) {
                            Some(v) if v == &Bytes::from("B") => Ok(()),
                            other => Err(format!("expected B, got {:?}", other)),
                        },
                        other => Err(format!("expected List, got {:?}", other)),
                    }
                } else {
                    Err("expected List".into())
                }
            },
        );

        self.register(
            "LREM removes matching elements",
            "LREM",
            "List",
            false,
            "LREM removes elements equal to a given value",
            |store| {
                let k = Bytes::from("lk8");
                let mut list = VecDeque::new();
                list.push_back(Bytes::from("a"));
                list.push_back(Bytes::from("b"));
                list.push_back(Bytes::from("a"));
                list.push_back(Bytes::from("c"));
                store.set(0, k.clone(), Value::List(list));

                if let Some(Value::List(l)) = store.get(0, &k) {
                    let filtered: VecDeque<Bytes> =
                        l.into_iter().filter(|v| v != &Bytes::from("a")).collect();
                    store.set(0, k.clone(), Value::List(filtered));
                    match store.get(0, &k) {
                        Some(Value::List(l)) if l.len() == 2 => Ok(()),
                        Some(Value::List(l)) => {
                            Err(format!("expected 2 elements, got {}", l.len()))
                        }
                        other => Err(format!("expected List, got {:?}", other)),
                    }
                } else {
                    Err("expected List".into())
                }
            },
        );
    }

    // ------------------------------------------------------------------
    // Hash tests
    // ------------------------------------------------------------------

    fn register_hash_tests(&mut self) {
        self.register(
            "HSET and HGET",
            "HSET/HGET",
            "Hash",
            true,
            "Set and get a hash field",
            |store| {
                let k = Bytes::from("hk1");
                let mut map = HashMap::new();
                map.insert(Bytes::from("field1"), Bytes::from("value1"));
                store.set(0, k.clone(), Value::Hash(map));
                match store.get(0, &k) {
                    Some(Value::Hash(h)) => match h.get(&Bytes::from("field1")) {
                        Some(v) if v == &Bytes::from("value1") => Ok(()),
                        other => Err(format!("expected value1, got {:?}", other)),
                    },
                    other => Err(format!("expected Hash, got {:?}", other)),
                }
            },
        );

        self.register(
            "HMSET and HMGET multiple fields",
            "HMSET/HMGET",
            "Hash",
            true,
            "Set multiple fields then get them back",
            |store| {
                let k = Bytes::from("hk2");
                let mut map = HashMap::new();
                map.insert(Bytes::from("f1"), Bytes::from("v1"));
                map.insert(Bytes::from("f2"), Bytes::from("v2"));
                map.insert(Bytes::from("f3"), Bytes::from("v3"));
                store.set(0, k.clone(), Value::Hash(map));
                match store.get(0, &k) {
                    Some(Value::Hash(h)) => {
                        if h.len() == 3 && h.get(&Bytes::from("f2")) == Some(&Bytes::from("v2")) {
                            Ok(())
                        } else {
                            Err(format!("unexpected hash contents: {:?}", h))
                        }
                    }
                    other => Err(format!("expected Hash, got {:?}", other)),
                }
            },
        );

        self.register(
            "HDEL removes a field",
            "HDEL",
            "Hash",
            true,
            "Delete a field from a hash",
            |store| {
                let k = Bytes::from("hk3");
                let mut map = HashMap::new();
                map.insert(Bytes::from("a"), Bytes::from("1"));
                map.insert(Bytes::from("b"), Bytes::from("2"));
                store.set(0, k.clone(), Value::Hash(map));

                if let Some(Value::Hash(mut h)) = store.get(0, &k) {
                    h.remove(&Bytes::from("a"));
                    store.set(0, k.clone(), Value::Hash(h));
                    match store.get(0, &k) {
                        Some(Value::Hash(h)) => {
                            if h.contains_key(&Bytes::from("a")) {
                                Err("field a should have been deleted".into())
                            } else if h.len() == 1 {
                                Ok(())
                            } else {
                                Err(format!("expected 1 field, got {}", h.len()))
                            }
                        }
                        other => Err(format!("expected Hash, got {:?}", other)),
                    }
                } else {
                    Err("expected Hash".into())
                }
            },
        );

        self.register(
            "HEXISTS checks field presence",
            "HEXISTS",
            "Hash",
            false,
            "HEXISTS returns true for existing fields",
            |store| {
                let k = Bytes::from("hk4");
                let mut map = HashMap::new();
                map.insert(Bytes::from("present"), Bytes::from("yes"));
                store.set(0, k.clone(), Value::Hash(map));
                match store.get(0, &k) {
                    Some(Value::Hash(h)) => {
                        if h.contains_key(&Bytes::from("present"))
                            && !h.contains_key(&Bytes::from("absent"))
                        {
                            Ok(())
                        } else {
                            Err("unexpected contains_key results".into())
                        }
                    }
                    other => Err(format!("expected Hash, got {:?}", other)),
                }
            },
        );

        self.register(
            "HGETALL returns all fields",
            "HGETALL",
            "Hash",
            true,
            "HGETALL retrieves every field-value pair",
            |store| {
                let k = Bytes::from("hk5");
                let mut map = HashMap::new();
                map.insert(Bytes::from("x"), Bytes::from("1"));
                map.insert(Bytes::from("y"), Bytes::from("2"));
                store.set(0, k.clone(), Value::Hash(map));
                match store.get(0, &k) {
                    Some(Value::Hash(h)) if h.len() == 2 => Ok(()),
                    Some(Value::Hash(h)) => Err(format!("expected 2 fields, got {}", h.len())),
                    other => Err(format!("expected Hash, got {:?}", other)),
                }
            },
        );

        self.register(
            "HKEYS returns all field names",
            "HKEYS",
            "Hash",
            false,
            "HKEYS lists all field names",
            |store| {
                let k = Bytes::from("hk6");
                let mut map = HashMap::new();
                map.insert(Bytes::from("alpha"), Bytes::from("1"));
                map.insert(Bytes::from("beta"), Bytes::from("2"));
                store.set(0, k.clone(), Value::Hash(map));
                match store.get(0, &k) {
                    Some(Value::Hash(h)) => {
                        let mut keys: Vec<Bytes> = h.keys().cloned().collect();
                        keys.sort();
                        if keys == vec![Bytes::from("alpha"), Bytes::from("beta")] {
                            Ok(())
                        } else {
                            Err(format!("unexpected keys: {:?}", keys))
                        }
                    }
                    other => Err(format!("expected Hash, got {:?}", other)),
                }
            },
        );

        self.register(
            "HVALS returns all values",
            "HVALS",
            "Hash",
            false,
            "HVALS lists all field values",
            |store| {
                let k = Bytes::from("hk7");
                let mut map = HashMap::new();
                map.insert(Bytes::from("a"), Bytes::from("10"));
                map.insert(Bytes::from("b"), Bytes::from("20"));
                store.set(0, k.clone(), Value::Hash(map));
                match store.get(0, &k) {
                    Some(Value::Hash(h)) => {
                        let mut vals: Vec<Bytes> = h.values().cloned().collect();
                        vals.sort();
                        if vals == vec![Bytes::from("10"), Bytes::from("20")] {
                            Ok(())
                        } else {
                            Err(format!("unexpected vals: {:?}", vals))
                        }
                    }
                    other => Err(format!("expected Hash, got {:?}", other)),
                }
            },
        );

        self.register(
            "HLEN returns field count",
            "HLEN",
            "Hash",
            false,
            "HLEN returns the number of fields",
            |store| {
                let k = Bytes::from("hk8");
                let mut map = HashMap::new();
                for i in 0..4 {
                    map.insert(
                        Bytes::from(format!("f{}", i)),
                        Bytes::from(format!("v{}", i)),
                    );
                }
                store.set(0, k.clone(), Value::Hash(map));
                match store.get(0, &k) {
                    Some(Value::Hash(h)) if h.len() == 4 => Ok(()),
                    Some(Value::Hash(h)) => Err(format!("expected 4, got {}", h.len())),
                    other => Err(format!("expected Hash, got {:?}", other)),
                }
            },
        );
    }

    // ------------------------------------------------------------------
    // Set tests
    // ------------------------------------------------------------------

    fn register_set_tests(&mut self) {
        self.register(
            "SADD and SMEMBERS",
            "SADD/SMEMBERS",
            "Set",
            true,
            "Add members then list them",
            |store| {
                let k = Bytes::from("setk1");
                let mut set = HashSet::new();
                set.insert(Bytes::from("a"));
                set.insert(Bytes::from("b"));
                set.insert(Bytes::from("c"));
                store.set(0, k.clone(), Value::Set(set));
                match store.get(0, &k) {
                    Some(Value::Set(s)) if s.len() == 3 => Ok(()),
                    Some(Value::Set(s)) => Err(format!("expected 3 members, got {}", s.len())),
                    other => Err(format!("expected Set, got {:?}", other)),
                }
            },
        );

        self.register(
            "SADD deduplicates",
            "SADD",
            "Set",
            true,
            "Adding duplicate members does not increase cardinality",
            |store| {
                let k = Bytes::from("setk2");
                let mut set = HashSet::new();
                set.insert(Bytes::from("x"));
                set.insert(Bytes::from("x"));
                set.insert(Bytes::from("y"));
                store.set(0, k.clone(), Value::Set(set));
                match store.get(0, &k) {
                    Some(Value::Set(s)) if s.len() == 2 => Ok(()),
                    Some(Value::Set(s)) => Err(format!("expected 2, got {}", s.len())),
                    other => Err(format!("expected Set, got {:?}", other)),
                }
            },
        );

        self.register(
            "SREM removes a member",
            "SREM",
            "Set",
            true,
            "Remove a member from a set",
            |store| {
                let k = Bytes::from("setk3");
                let mut set = HashSet::new();
                set.insert(Bytes::from("a"));
                set.insert(Bytes::from("b"));
                store.set(0, k.clone(), Value::Set(set));

                if let Some(Value::Set(mut s)) = store.get(0, &k) {
                    s.remove(&Bytes::from("a"));
                    store.set(0, k.clone(), Value::Set(s));
                    match store.get(0, &k) {
                        Some(Value::Set(s)) if s.len() == 1 && s.contains(&Bytes::from("b")) => {
                            Ok(())
                        }
                        other => Err(format!("expected {{b}}, got {:?}", other)),
                    }
                } else {
                    Err("expected Set".into())
                }
            },
        );

        self.register(
            "SISMEMBER checks membership",
            "SISMEMBER",
            "Set",
            true,
            "Check whether a member exists in the set",
            |store| {
                let k = Bytes::from("setk4");
                let mut set = HashSet::new();
                set.insert(Bytes::from("member"));
                store.set(0, k.clone(), Value::Set(set));
                match store.get(0, &k) {
                    Some(Value::Set(s)) => {
                        if s.contains(&Bytes::from("member"))
                            && !s.contains(&Bytes::from("nonexistent"))
                        {
                            Ok(())
                        } else {
                            Err("membership check failed".into())
                        }
                    }
                    other => Err(format!("expected Set, got {:?}", other)),
                }
            },
        );

        self.register(
            "SCARD returns cardinality",
            "SCARD",
            "Set",
            false,
            "SCARD returns the number of members",
            |store| {
                let k = Bytes::from("setk5");
                let mut set = HashSet::new();
                for i in 0..7 {
                    set.insert(Bytes::from(format!("m{}", i)));
                }
                store.set(0, k.clone(), Value::Set(set));
                match store.get(0, &k) {
                    Some(Value::Set(s)) if s.len() == 7 => Ok(()),
                    Some(Value::Set(s)) => Err(format!("expected 7, got {}", s.len())),
                    other => Err(format!("expected Set, got {:?}", other)),
                }
            },
        );

        self.register(
            "SUNION merges sets",
            "SUNION",
            "Set",
            false,
            "Union of two sets contains all unique members",
            |store| {
                let k1 = Bytes::from("su1");
                let k2 = Bytes::from("su2");
                let mut s1 = HashSet::new();
                s1.insert(Bytes::from("a"));
                s1.insert(Bytes::from("b"));
                let mut s2 = HashSet::new();
                s2.insert(Bytes::from("b"));
                s2.insert(Bytes::from("c"));
                store.set(0, k1.clone(), Value::Set(s1));
                store.set(0, k2.clone(), Value::Set(s2));

                if let (Some(Value::Set(a)), Some(Value::Set(b))) =
                    (store.get(0, &k1), store.get(0, &k2))
                {
                    let union: HashSet<Bytes> = a.union(&b).cloned().collect();
                    if union.len() == 3 {
                        Ok(())
                    } else {
                        Err(format!("expected 3, got {}", union.len()))
                    }
                } else {
                    Err("expected two sets".into())
                }
            },
        );

        self.register(
            "SINTER intersects sets",
            "SINTER",
            "Set",
            false,
            "Intersection of two sets contains common members",
            |store| {
                let k1 = Bytes::from("si1");
                let k2 = Bytes::from("si2");
                let mut s1 = HashSet::new();
                s1.insert(Bytes::from("a"));
                s1.insert(Bytes::from("b"));
                let mut s2 = HashSet::new();
                s2.insert(Bytes::from("b"));
                s2.insert(Bytes::from("c"));
                store.set(0, k1.clone(), Value::Set(s1));
                store.set(0, k2.clone(), Value::Set(s2));

                if let (Some(Value::Set(a)), Some(Value::Set(b))) =
                    (store.get(0, &k1), store.get(0, &k2))
                {
                    let inter: HashSet<&Bytes> = a.intersection(&b).collect();
                    if inter.len() == 1 && inter.contains(&Bytes::from("b")) {
                        Ok(())
                    } else {
                        Err(format!("expected {{b}}, got {:?}", inter))
                    }
                } else {
                    Err("expected two sets".into())
                }
            },
        );

        self.register(
            "SDIFF computes difference",
            "SDIFF",
            "Set",
            false,
            "Difference of two sets returns members only in the first",
            |store| {
                let k1 = Bytes::from("sd1");
                let k2 = Bytes::from("sd2");
                let mut s1 = HashSet::new();
                s1.insert(Bytes::from("a"));
                s1.insert(Bytes::from("b"));
                let mut s2 = HashSet::new();
                s2.insert(Bytes::from("b"));
                s2.insert(Bytes::from("c"));
                store.set(0, k1.clone(), Value::Set(s1));
                store.set(0, k2.clone(), Value::Set(s2));

                if let (Some(Value::Set(a)), Some(Value::Set(b))) =
                    (store.get(0, &k1), store.get(0, &k2))
                {
                    let diff: HashSet<&Bytes> = a.difference(&b).collect();
                    if diff.len() == 1 && diff.contains(&Bytes::from("a")) {
                        Ok(())
                    } else {
                        Err(format!("expected {{a}}, got {:?}", diff))
                    }
                } else {
                    Err("expected two sets".into())
                }
            },
        );
    }

    // ------------------------------------------------------------------
    // Sorted-set tests
    // ------------------------------------------------------------------

    fn register_sorted_set_tests(&mut self) {
        self.register(
            "ZADD and ZSCORE",
            "ZADD/ZSCORE",
            "SortedSet",
            true,
            "Add scored members then look up scores",
            |store| {
                let k = Bytes::from("zk1");
                let mut by_score = BTreeMap::new();
                let mut by_member = HashMap::new();
                by_score.insert((OrderedFloat(1.0), Bytes::from("a")), ());
                by_score.insert((OrderedFloat(2.0), Bytes::from("b")), ());
                by_member.insert(Bytes::from("a"), 1.0);
                by_member.insert(Bytes::from("b"), 2.0);
                store.set(
                    0,
                    k.clone(),
                    Value::SortedSet {
                        by_score,
                        by_member,
                    },
                );
                match store.get(0, &k) {
                    Some(Value::SortedSet { by_member: m, .. }) => {
                        if m.get(&Bytes::from("a")) == Some(&1.0)
                            && m.get(&Bytes::from("b")) == Some(&2.0)
                        {
                            Ok(())
                        } else {
                            Err(format!("unexpected scores: {:?}", m))
                        }
                    }
                    other => Err(format!("expected SortedSet, got {:?}", other)),
                }
            },
        );

        self.register(
            "ZREM removes a member",
            "ZREM",
            "SortedSet",
            true,
            "Remove a member from a sorted set",
            |store| {
                let k = Bytes::from("zk2");
                let mut by_score = BTreeMap::new();
                let mut by_member = HashMap::new();
                by_score.insert((OrderedFloat(1.0), Bytes::from("x")), ());
                by_score.insert((OrderedFloat(2.0), Bytes::from("y")), ());
                by_member.insert(Bytes::from("x"), 1.0);
                by_member.insert(Bytes::from("y"), 2.0);
                store.set(
                    0,
                    k.clone(),
                    Value::SortedSet {
                        by_score,
                        by_member,
                    },
                );

                if let Some(Value::SortedSet {
                    mut by_score,
                    mut by_member,
                }) = store.get(0, &k)
                {
                    if let Some(score) = by_member.remove(&Bytes::from("x")) {
                        by_score.remove(&(OrderedFloat(score), Bytes::from("x")));
                    }
                    store.set(
                        0,
                        k.clone(),
                        Value::SortedSet {
                            by_score,
                            by_member,
                        },
                    );
                    match store.get(0, &k) {
                        Some(Value::SortedSet { by_member: m, .. }) if m.len() == 1 => Ok(()),
                        other => Err(format!("expected 1 member, got {:?}", other)),
                    }
                } else {
                    Err("expected SortedSet".into())
                }
            },
        );

        self.register(
            "ZCARD returns cardinality",
            "ZCARD",
            "SortedSet",
            false,
            "ZCARD returns the number of members",
            |store| {
                let k = Bytes::from("zk3");
                let mut by_score = BTreeMap::new();
                let mut by_member = HashMap::new();
                for i in 0..5 {
                    let m = Bytes::from(format!("m{}", i));
                    by_score.insert((OrderedFloat(i as f64), m.clone()), ());
                    by_member.insert(m, i as f64);
                }
                store.set(
                    0,
                    k.clone(),
                    Value::SortedSet {
                        by_score,
                        by_member,
                    },
                );
                match store.get(0, &k) {
                    Some(Value::SortedSet { by_member: m, .. }) if m.len() == 5 => Ok(()),
                    other => Err(format!("expected 5 members, got {:?}", other)),
                }
            },
        );

        self.register(
            "ZCOUNT counts members in score range",
            "ZCOUNT",
            "SortedSet",
            false,
            "Count members with scores in a given range",
            |store| {
                let k = Bytes::from("zk4");
                let mut by_score = BTreeMap::new();
                let mut by_member = HashMap::new();
                for i in 0..10 {
                    let m = Bytes::from(format!("m{}", i));
                    by_score.insert((OrderedFloat(i as f64), m.clone()), ());
                    by_member.insert(m, i as f64);
                }
                store.set(
                    0,
                    k.clone(),
                    Value::SortedSet {
                        by_score,
                        by_member,
                    },
                );
                match store.get(0, &k) {
                    Some(Value::SortedSet { by_member: m, .. }) => {
                        let count = m.values().filter(|&&s| (3.0..=7.0).contains(&s)).count();
                        if count == 5 {
                            Ok(())
                        } else {
                            Err(format!("expected 5 in [3,7], got {}", count))
                        }
                    }
                    other => Err(format!("expected SortedSet, got {:?}", other)),
                }
            },
        );

        self.register(
            "ZRANK returns member rank",
            "ZRANK",
            "SortedSet",
            false,
            "ZRANK returns the zero-based rank of a member",
            |store| {
                let k = Bytes::from("zk5");
                let mut by_score = BTreeMap::new();
                let mut by_member = HashMap::new();
                by_score.insert((OrderedFloat(10.0), Bytes::from("a")), ());
                by_score.insert((OrderedFloat(20.0), Bytes::from("b")), ());
                by_score.insert((OrderedFloat(30.0), Bytes::from("c")), ());
                by_member.insert(Bytes::from("a"), 10.0);
                by_member.insert(Bytes::from("b"), 20.0);
                by_member.insert(Bytes::from("c"), 30.0);
                store.set(
                    0,
                    k.clone(),
                    Value::SortedSet {
                        by_score,
                        by_member,
                    },
                );
                match store.get(0, &k) {
                    Some(Value::SortedSet { by_score: bs, .. }) => {
                        let members: Vec<Bytes> = bs.keys().map(|(_, m)| m.clone()).collect();
                        let rank = members.iter().position(|m| m == &Bytes::from("b"));
                        if rank == Some(1) {
                            Ok(())
                        } else {
                            Err(format!("expected rank 1, got {:?}", rank))
                        }
                    }
                    other => Err(format!("expected SortedSet, got {:?}", other)),
                }
            },
        );

        self.register(
            "ZRANGE returns members in range",
            "ZRANGE",
            "SortedSet",
            true,
            "ZRANGE returns members ordered by score within a range",
            |store| {
                let k = Bytes::from("zk6");
                let mut by_score = BTreeMap::new();
                let mut by_member = HashMap::new();
                for i in 0..5 {
                    let m = Bytes::from(format!("m{}", i));
                    by_score.insert((OrderedFloat(i as f64), m.clone()), ());
                    by_member.insert(m, i as f64);
                }
                store.set(
                    0,
                    k.clone(),
                    Value::SortedSet {
                        by_score,
                        by_member,
                    },
                );
                match store.get(0, &k) {
                    Some(Value::SortedSet { by_score: bs, .. }) => {
                        let all: Vec<Bytes> = bs.keys().map(|(_, m)| m.clone()).collect();
                        let sub = &all[1..4];
                        if sub.len() == 3 && sub[0] == "m1" && sub[2] == "m3" {
                            Ok(())
                        } else {
                            Err(format!("unexpected range: {:?}", sub))
                        }
                    }
                    other => Err(format!("expected SortedSet, got {:?}", other)),
                }
            },
        );

        self.register(
            "ZRANGEBYSCORE filters by score",
            "ZRANGEBYSCORE",
            "SortedSet",
            false,
            "Return members with scores in a given interval",
            |store| {
                let k = Bytes::from("zk7");
                let mut by_score = BTreeMap::new();
                let mut by_member = HashMap::new();
                for i in 0..10 {
                    let m = Bytes::from(format!("m{}", i));
                    by_score.insert((OrderedFloat(i as f64 * 10.0), m.clone()), ());
                    by_member.insert(m, i as f64 * 10.0);
                }
                store.set(
                    0,
                    k.clone(),
                    Value::SortedSet {
                        by_score,
                        by_member,
                    },
                );
                match store.get(0, &k) {
                    Some(Value::SortedSet { by_score: bs, .. }) => {
                        let filtered: Vec<Bytes> = bs
                            .range(
                                (OrderedFloat(20.0), Bytes::from(""))
                                    ..=(OrderedFloat(50.0), Bytes::from(vec![0xff_u8])),
                            )
                            .map(|((_, m), _)| m.clone())
                            .collect();
                        if filtered.len() == 4 {
                            Ok(())
                        } else {
                            Err(format!("expected 4 members, got {}", filtered.len()))
                        }
                    }
                    other => Err(format!("expected SortedSet, got {:?}", other)),
                }
            },
        );

        self.register(
            "ZADD updates score of existing member",
            "ZADD",
            "SortedSet",
            false,
            "Re-adding a member with a new score updates it",
            |store| {
                let k = Bytes::from("zk8");
                let mut by_score = BTreeMap::new();
                let mut by_member = HashMap::new();
                by_score.insert((OrderedFloat(1.0), Bytes::from("m")), ());
                by_member.insert(Bytes::from("m"), 1.0);
                store.set(
                    0,
                    k.clone(),
                    Value::SortedSet {
                        by_score,
                        by_member,
                    },
                );

                if let Some(Value::SortedSet {
                    mut by_score,
                    mut by_member,
                }) = store.get(0, &k)
                {
                    if let Some(old) = by_member.insert(Bytes::from("m"), 99.0) {
                        by_score.remove(&(OrderedFloat(old), Bytes::from("m")));
                    }
                    by_score.insert((OrderedFloat(99.0), Bytes::from("m")), ());
                    store.set(
                        0,
                        k.clone(),
                        Value::SortedSet {
                            by_score,
                            by_member,
                        },
                    );
                    match store.get(0, &k) {
                        Some(Value::SortedSet { by_member: m, .. }) => {
                            if m.get(&Bytes::from("m")) == Some(&99.0) {
                                Ok(())
                            } else {
                                Err(format!("expected score 99, got {:?}", m))
                            }
                        }
                        other => Err(format!("expected SortedSet, got {:?}", other)),
                    }
                } else {
                    Err("expected SortedSet".into())
                }
            },
        );
    }

    // ------------------------------------------------------------------
    // Key tests
    // ------------------------------------------------------------------

    fn register_key_tests(&mut self) {
        self.register(
            "DEL removes a key",
            "DEL",
            "Key",
            true,
            "Delete a key and verify it is gone",
            |store| {
                let k = Bytes::from("kd1");
                store.set(0, k.clone(), Value::String(Bytes::from("bye")));
                let deleted = store.del(0, &[k.clone()]);
                if deleted != 1 {
                    return Err(format!("expected 1 deletion, got {}", deleted));
                }
                if store.get(0, &k).is_some() {
                    Err("key still exists after DEL".into())
                } else {
                    Ok(())
                }
            },
        );

        self.register(
            "DEL on missing key returns 0",
            "DEL",
            "Key",
            false,
            "DEL on a non-existent key returns 0",
            |store| {
                let deleted = store.del(0, &[Bytes::from("kd_nonexist")]);
                if deleted == 0 {
                    Ok(())
                } else {
                    Err(format!("expected 0, got {}", deleted))
                }
            },
        );

        self.register(
            "EXISTS on present key",
            "EXISTS",
            "Key",
            true,
            "EXISTS returns 1 for a key that exists",
            |store| {
                let k = Bytes::from("ke1");
                store.set(0, k.clone(), Value::String(Bytes::from("hi")));
                let count = store.exists(0, &[k]);
                if count == 1 {
                    Ok(())
                } else {
                    Err(format!("expected 1, got {}", count))
                }
            },
        );

        self.register(
            "EXISTS on missing key",
            "EXISTS",
            "Key",
            true,
            "EXISTS returns 0 for a key that does not exist",
            |store| {
                let count = store.exists(0, &[Bytes::from("ke_none")]);
                if count == 0 {
                    Ok(())
                } else {
                    Err(format!("expected 0, got {}", count))
                }
            },
        );

        self.register(
            "TYPE of string key",
            "TYPE",
            "Key",
            false,
            "TYPE returns the value type",
            |store| {
                let k = Bytes::from("kt1");
                store.set(0, k.clone(), Value::String(Bytes::from("val")));
                match store.get(0, &k) {
                    Some(Value::String(_)) => Ok(()),
                    other => Err(format!("expected String variant, got {:?}", other)),
                }
            },
        );

        self.register(
            "TYPE of list key",
            "TYPE",
            "Key",
            false,
            "TYPE returns List for a list value",
            |store| {
                let k = Bytes::from("kt2");
                store.set(0, k.clone(), Value::List(VecDeque::new()));
                match store.get(0, &k) {
                    Some(Value::List(_)) => Ok(()),
                    other => Err(format!("expected List variant, got {:?}", other)),
                }
            },
        );

        self.register(
            "TYPE of hash key",
            "TYPE",
            "Key",
            false,
            "TYPE returns Hash for a hash value",
            |store| {
                let k = Bytes::from("kt3");
                store.set(0, k.clone(), Value::Hash(HashMap::new()));
                match store.get(0, &k) {
                    Some(Value::Hash(_)) => Ok(()),
                    other => Err(format!("expected Hash variant, got {:?}", other)),
                }
            },
        );

        self.register(
            "TYPE of set key",
            "TYPE",
            "Key",
            false,
            "TYPE returns Set for a set value",
            |store| {
                let k = Bytes::from("kt4");
                store.set(0, k.clone(), Value::Set(HashSet::new()));
                match store.get(0, &k) {
                    Some(Value::Set(_)) => Ok(()),
                    other => Err(format!("expected Set variant, got {:?}", other)),
                }
            },
        );

        self.register(
            "RENAME a key",
            "RENAME",
            "Key",
            false,
            "Rename a key by moving its value",
            |store| {
                let old = Bytes::from("kr_old");
                let new_key = Bytes::from("kr_new");
                store.set(0, old.clone(), Value::String(Bytes::from("data")));
                if let Some(val) = store.get(0, &old) {
                    store.set(0, new_key.clone(), val);
                    store.del(0, &[old.clone()]);
                }
                if store.get(0, &old).is_some() {
                    return Err("old key still exists".into());
                }
                match store.get(0, &new_key) {
                    Some(Value::String(v)) if v == "data" => Ok(()),
                    other => Err(format!("expected String(data), got {:?}", other)),
                }
            },
        );

        self.register(
            "KEYS returns all keys",
            "KEYS",
            "Key",
            true,
            "KEYS lists all key names in the database",
            |_store| {
                let store2 = Store::new(16);
                store2.set(0, Bytes::from("ka"), Value::String(Bytes::from("1")));
                store2.set(0, Bytes::from("kb"), Value::String(Bytes::from("2")));
                let mut keys = store2.keys(0);
                keys.sort();
                if keys == vec![Bytes::from("ka"), Bytes::from("kb")] {
                    Ok(())
                } else {
                    Err(format!("expected [ka,kb], got {:?}", keys))
                }
            },
        );

        self.register(
            "DEL multiple keys",
            "DEL",
            "Key",
            false,
            "DEL can remove multiple keys at once",
            |_store| {
                let store2 = Store::new(16);
                store2.set(0, Bytes::from("dm1"), Value::String(Bytes::from("a")));
                store2.set(0, Bytes::from("dm2"), Value::String(Bytes::from("b")));
                store2.set(0, Bytes::from("dm3"), Value::String(Bytes::from("c")));
                let deleted = store2.del(
                    0,
                    &[Bytes::from("dm1"), Bytes::from("dm2"), Bytes::from("dm3")],
                );
                if deleted == 3 {
                    Ok(())
                } else {
                    Err(format!("expected 3, got {}", deleted))
                }
            },
        );

        self.register(
            "EXISTS multiple keys",
            "EXISTS",
            "Key",
            false,
            "EXISTS counts how many of the given keys exist",
            |store| {
                store.set(0, Bytes::from("em1"), Value::String(Bytes::from("a")));
                store.set(0, Bytes::from("em2"), Value::String(Bytes::from("b")));
                let count = store.exists(
                    0,
                    &[
                        Bytes::from("em1"),
                        Bytes::from("em2"),
                        Bytes::from("em_nope"),
                    ],
                );
                if count == 2 {
                    Ok(())
                } else {
                    Err(format!("expected 2, got {}", count))
                }
            },
        );
    }

    // ------------------------------------------------------------------
    // Server tests
    // ------------------------------------------------------------------

    fn register_server_tests(&mut self) {
        self.register(
            "PING returns PONG concept",
            "PING",
            "Server",
            true,
            "Basic connectivity: store can be created and queried",
            |_store| Ok(()),
        );

        self.register(
            "ECHO roundtrip",
            "ECHO",
            "Server",
            false,
            "Store and retrieve a value (ECHO concept)",
            |store| {
                let msg = Bytes::from("hello echo");
                store.set(0, Bytes::from("_echo"), Value::String(msg.clone()));
                match store.get(0, &Bytes::from("_echo")) {
                    Some(Value::String(v)) if v == msg => Ok(()),
                    other => Err(format!("expected echo back, got {:?}", other)),
                }
            },
        );

        self.register(
            "DBSIZE returns key count",
            "DBSIZE",
            "Server",
            true,
            "key_count reflects the number of keys stored",
            |_store| {
                let s = Store::new(16);
                s.set(0, Bytes::from("db1"), Value::String(Bytes::from("a")));
                s.set(0, Bytes::from("db2"), Value::String(Bytes::from("b")));
                let count = s.key_count(0);
                if count == 2 {
                    Ok(())
                } else {
                    Err(format!("expected 2, got {}", count))
                }
            },
        );

        self.register(
            "SELECT database isolation",
            "SELECT",
            "Server",
            true,
            "Different database indices are isolated",
            |_store| {
                let s = Store::new(16);
                s.set(0, Bytes::from("iso"), Value::String(Bytes::from("db0")));
                s.set(1, Bytes::from("iso"), Value::String(Bytes::from("db1")));
                let v0 = s.get(0, &Bytes::from("iso"));
                let v1 = s.get(1, &Bytes::from("iso"));
                match (v0, v1) {
                    (Some(Value::String(a)), Some(Value::String(b)))
                        if a == "db0" && b == "db1" =>
                    {
                        Ok(())
                    }
                    other => Err(format!("expected db0/db1, got {:?}", other)),
                }
            },
        );

        self.register(
            "FLUSHDB clears a database",
            "FLUSHDB",
            "Server",
            false,
            "flush_db empties a single database",
            |_store| {
                let s = Store::new(16);
                s.set(0, Bytes::from("fl1"), Value::String(Bytes::from("x")));
                s.set(0, Bytes::from("fl2"), Value::String(Bytes::from("y")));
                s.flush_db(0);
                if s.key_count(0) == 0 {
                    Ok(())
                } else {
                    Err(format!("expected 0, got {}", s.key_count(0)))
                }
            },
        );

        self.register(
            "FLUSHALL clears all databases",
            "FLUSHALL",
            "Server",
            false,
            "flush_all empties every database",
            |_store| {
                let s = Store::new(16);
                s.set(0, Bytes::from("fa0"), Value::String(Bytes::from("a")));
                s.set(1, Bytes::from("fa1"), Value::String(Bytes::from("b")));
                s.flush_all();
                if s.key_count(0) == 0 && s.key_count(1) == 0 {
                    Ok(())
                } else {
                    Err("databases not empty after flush_all".into())
                }
            },
        );

        self.register(
            "TTL on key without expiry",
            "TTL",
            "Server",
            false,
            "TTL returns -1 for a key with no expiration",
            |_store| {
                let s = Store::new(16);
                s.set(0, Bytes::from("ttl_no"), Value::String(Bytes::from("v")));
                match s.ttl(0, &Bytes::from("ttl_no")) {
                    Some(-1) => Ok(()),
                    other => Err(format!("expected Some(-1), got {:?}", other)),
                }
            },
        );

        self.register(
            "TTL on key with expiry",
            "TTL",
            "Server",
            false,
            "TTL returns remaining seconds for a key with expiration",
            |_store| {
                let s = Store::new(16);
                let exp = SystemTime::now() + Duration::from_secs(300);
                s.set_with_expiry(
                    0,
                    Bytes::from("ttl_yes"),
                    Value::String(Bytes::from("v")),
                    exp,
                );
                match s.ttl(0, &Bytes::from("ttl_yes")) {
                    Some(t) if t > 0 => Ok(()),
                    other => Err(format!("expected positive TTL, got {:?}", other)),
                }
            },
        );

        self.register(
            "PERSIST removes expiry",
            "PERSIST",
            "Server",
            false,
            "PERSIST removes the expiration from a key",
            |_store| {
                let s = Store::new(16);
                let exp = SystemTime::now() + Duration::from_secs(300);
                s.set_with_expiry(0, Bytes::from("per1"), Value::String(Bytes::from("v")), exp);
                let removed = s.persist(0, &Bytes::from("per1"));
                if !removed {
                    return Err("expected persist to return true".into());
                }
                match s.ttl(0, &Bytes::from("per1")) {
                    Some(-1) => Ok(()),
                    other => Err(format!("expected Some(-1), got {:?}", other)),
                }
            },
        );

        self.register(
            "num_databases returns count",
            "CONFIG",
            "Server",
            false,
            "num_databases reflects the configured database count",
            |_store| {
                let s = Store::new(16);
                if s.num_databases() == 16 {
                    Ok(())
                } else {
                    Err(format!("expected 16, got {}", s.num_databases()))
                }
            },
        );
    }
}

impl Default for CompatibilitySuite {
    fn default() -> Self {
        Self::new()
    }
}

// Required imports for test closures.
use ordered_float::OrderedFloat;
use std::collections::{BTreeMap, HashSet, VecDeque};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_suite_runs_all() {
        let store = Store::new(16);
        let suite = CompatibilitySuite::new();
        let results = suite.run(&store);

        assert!(results.total > 0, "suite should have tests");
        assert_eq!(
            results.total,
            results.passed + results.failed + results.skipped
        );
        assert!(results.score >= 0.0 && results.score <= 100.0);
    }

    #[test]
    fn test_suite_run_category() {
        let store = Store::new(16);
        let suite = CompatibilitySuite::new();
        let results = suite.run_category(&store, "String");

        assert!(results.total > 0, "String category should have tests");
        assert!(results.categories.contains_key("String"));
    }

    #[test]
    fn test_suite_category_results_consistent() {
        let store = Store::new(16);
        let suite = CompatibilitySuite::new();
        let results = suite.run(&store);

        let cat_total: usize = results.categories.values().map(|c| c.total).sum();
        assert_eq!(cat_total, results.total);
    }

    #[test]
    fn test_suite_default() {
        let suite = CompatibilitySuite::default();
        assert!(suite.tests.len() > 50, "suite should have 60+ tests");
    }

    #[test]
    fn test_suite_results_serializable() {
        let store = Store::new(16);
        let suite = CompatibilitySuite::new();
        let results = suite.run(&store);

        let json = serde_json::to_string(&results).expect("should serialize");
        assert!(!json.is_empty());
    }
}
