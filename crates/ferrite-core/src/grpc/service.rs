//! Service definitions and handler mapping for the gRPC API.
//!
//! Defines the [`FerriteService`] trait (analogous to a tonic generated service
//! trait) and [`FerriteServiceImpl`] which fulfils every method by delegating to
//! the underlying [`Store`].

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use bytes::Bytes;

use crate::storage::{Store, Value};

use super::types::*;
use super::GrpcError;

// ---------------------------------------------------------------------------
// Service trait
// ---------------------------------------------------------------------------

/// Trait modelling a gRPC service for Ferrite.
///
/// Each method corresponds to a unary RPC. Streaming RPCs (e.g. subscribe)
/// are left for a future tonic integration.
pub trait FerriteService: Send + Sync {
    /// Retrieve a single key.
    fn get(&self, req: GetRequest) -> Result<GetResponse, GrpcError>;
    /// Set a key to a value.
    fn set(&self, req: SetRequest) -> Result<SetResponse, GrpcError>;
    /// Delete one or more keys.
    fn delete(&self, req: DeleteRequest) -> Result<DeleteResponse, GrpcError>;
    /// Check existence of one or more keys.
    fn exists(&self, req: ExistsRequest) -> Result<ExistsResponse, GrpcError>;
    /// Retrieve multiple keys at once.
    fn mget(&self, req: MGetRequest) -> Result<MGetResponse, GrpcError>;
    /// Set multiple key-value pairs at once.
    fn mset(&self, req: MSetRequest) -> Result<MSetResponse, GrpcError>;
    /// Incrementally iterate over keys.
    fn scan(&self, req: ScanRequest) -> Result<ScanResponse, GrpcError>;
    /// Increment a key by an integer amount.
    fn incr(&self, req: IncrRequest) -> Result<IncrResponse, GrpcError>;
    /// Push values onto a list.
    fn list_push(&self, req: ListPushRequest) -> Result<ListPushResponse, GrpcError>;
    /// Pop values from a list.
    fn list_pop(&self, req: ListPopRequest) -> Result<ListPopResponse, GrpcError>;
    /// Set fields on a hash.
    fn hash_set(&self, req: HashSetRequest) -> Result<HashSetResponse, GrpcError>;
    /// Get fields from a hash.
    fn hash_get(&self, req: HashGetRequest) -> Result<HashGetResponse, GrpcError>;
    /// Ping health-check.
    fn ping(&self, req: PingRequest) -> Result<PingResponse, GrpcError>;
    /// Retrieve server information.
    fn info(&self, req: InfoRequest) -> Result<InfoResponse, GrpcError>;
    /// Execute a batch of operations.
    fn batch(&self, req: BatchRequest) -> Result<BatchResponse, GrpcError>;
}

// ---------------------------------------------------------------------------
// Implementation backed by Store
// ---------------------------------------------------------------------------

/// Concrete implementation of [`FerriteService`] backed by a [`Store`].
pub struct FerriteServiceImpl {
    store: Arc<Store>,
}

impl FerriteServiceImpl {
    /// Create a new service implementation wrapping the given store.
    pub fn new(store: Arc<Store>) -> Self {
        Self { store }
    }

    /// Helper: extract bytes from a [`Value::String`].
    fn value_to_bytes(value: &Value) -> Vec<u8> {
        match value {
            Value::String(b) => b.to_vec(),
            _ => Vec::new(),
        }
    }
}

impl FerriteService for FerriteServiceImpl {
    fn get(&self, req: GetRequest) -> Result<GetResponse, GrpcError> {
        let key = Bytes::from(req.key);
        match self.store.get(req.database, &key) {
            Some(value) => Ok(GetResponse {
                value: Some(Self::value_to_bytes(&value)),
                found: true,
            }),
            None => Ok(GetResponse {
                value: None,
                found: false,
            }),
        }
    }

    fn set(&self, req: SetRequest) -> Result<SetResponse, GrpcError> {
        if req.key.is_empty() {
            return Err(GrpcError::InvalidRequest("key must not be empty".into()));
        }

        let key = Bytes::from(req.key);
        let db = req.database;

        // Handle NX / XX semantics.
        let existing = self.store.get(db, &key);
        if req.nx && existing.is_some() {
            return Ok(SetResponse {
                success: false,
                old_value: None,
            });
        }
        if req.xx && existing.is_none() {
            return Ok(SetResponse {
                success: false,
                old_value: None,
            });
        }

        let old_value = existing.map(|v| Self::value_to_bytes(&v));
        let value = Value::String(Bytes::from(req.value));

        match req.ttl_ms {
            Some(ms) => {
                let expires_at = SystemTime::now() + Duration::from_millis(ms);
                self.store.set_with_expiry(db, key, value, expires_at);
            }
            None => {
                self.store.set(db, key, value);
            }
        }

        Ok(SetResponse {
            success: true,
            old_value,
        })
    }

    fn delete(&self, req: DeleteRequest) -> Result<DeleteResponse, GrpcError> {
        let keys: Vec<Bytes> = req.keys.into_iter().map(Bytes::from).collect();
        let deleted = self.store.del(req.database, &keys);
        Ok(DeleteResponse {
            deleted_count: deleted as u64,
        })
    }

    fn exists(&self, req: ExistsRequest) -> Result<ExistsResponse, GrpcError> {
        let keys: Vec<Bytes> = req.keys.into_iter().map(Bytes::from).collect();
        let count = self.store.exists(req.database, &keys);
        Ok(ExistsResponse {
            exist_count: count as u64,
        })
    }

    fn mget(&self, req: MGetRequest) -> Result<MGetResponse, GrpcError> {
        let values = req
            .keys
            .iter()
            .map(|k| {
                let key = Bytes::from(k.clone());
                self.store
                    .get(req.database, &key)
                    .map(|v| Self::value_to_bytes(&v))
            })
            .collect();
        Ok(MGetResponse { values })
    }

    fn mset(&self, req: MSetRequest) -> Result<MSetResponse, GrpcError> {
        for entry in req.entries {
            let key = Bytes::from(entry.key);
            let value = Value::String(Bytes::from(entry.value));
            self.store.set(req.database, key, value);
        }
        Ok(MSetResponse { success: true })
    }

    fn scan(&self, req: ScanRequest) -> Result<ScanResponse, GrpcError> {
        let all_keys = self.store.keys(req.database);
        let count = req.count.unwrap_or(10) as usize;
        let start = req.cursor as usize;

        let filtered: Vec<String> = all_keys
            .into_iter()
            .skip(start)
            .filter(|k| {
                if let Some(ref pat) = req.pattern {
                    let key_str = String::from_utf8_lossy(k);
                    simple_pattern_match(pat, &key_str)
                } else {
                    true
                }
            })
            .take(count)
            .map(|k| String::from_utf8_lossy(k.as_ref()).into_owned())
            .collect();

        let next_cursor = if filtered.len() < count {
            0 // iteration complete
        } else {
            (start + filtered.len()) as u64
        };

        Ok(ScanResponse {
            cursor: next_cursor,
            keys: filtered,
        })
    }

    fn incr(&self, req: IncrRequest) -> Result<IncrResponse, GrpcError> {
        let key = Bytes::from(req.key);
        let db = req.database;

        let current = self.store.get(db, &key);
        let current_val: i64 = match current {
            Some(Value::String(ref b)) => {
                let s = std::str::from_utf8(b).map_err(|_| {
                    GrpcError::InvalidRequest("value is not a valid integer".into())
                })?;
                s.parse::<i64>()
                    .map_err(|_| GrpcError::InvalidRequest("value is not a valid integer".into()))?
            }
            Some(_) => {
                return Err(GrpcError::InvalidRequest(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                ));
            }
            None => 0,
        };

        let new_val = current_val
            .checked_add(req.increment)
            .ok_or_else(|| GrpcError::InvalidRequest("increment would overflow".into()))?;

        let value = Value::String(Bytes::from(new_val.to_string()));
        self.store.set(db, key, value);

        Ok(IncrResponse { value: new_val })
    }

    fn list_push(&self, req: ListPushRequest) -> Result<ListPushResponse, GrpcError> {
        let key = Bytes::from(req.key);
        let db = req.database;

        let mut list = match self.store.get(db, &key) {
            Some(Value::List(l)) => l,
            Some(_) => {
                return Err(GrpcError::InvalidRequest(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                ));
            }
            None => VecDeque::new(),
        };

        for v in req.values {
            if req.left {
                list.push_front(Bytes::from(v));
            } else {
                list.push_back(Bytes::from(v));
            }
        }

        let length = list.len() as u64;
        self.store.set(db, key, Value::List(list));

        Ok(ListPushResponse { length })
    }

    fn list_pop(&self, req: ListPopRequest) -> Result<ListPopResponse, GrpcError> {
        let key = Bytes::from(req.key);
        let db = req.database;

        let mut list = match self.store.get(db, &key) {
            Some(Value::List(l)) => l,
            Some(_) => {
                return Err(GrpcError::InvalidRequest(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                ));
            }
            None => {
                return Ok(ListPopResponse { values: vec![] });
            }
        };

        let count = req.count as usize;
        let mut values = Vec::with_capacity(count);
        for _ in 0..count {
            let popped = if req.left {
                list.pop_front()
            } else {
                list.pop_back()
            };
            match popped {
                Some(v) => values.push(v.to_vec()),
                None => break,
            }
        }

        if list.is_empty() {
            // Remove the key when the list is empty (Redis semantics).
            self.store.del(db, &[key]);
        } else {
            self.store.set(db, key, Value::List(list));
        }

        Ok(ListPopResponse { values })
    }

    fn hash_set(&self, req: HashSetRequest) -> Result<HashSetResponse, GrpcError> {
        let key = Bytes::from(req.key);
        let db = req.database;

        let mut hash = match self.store.get(db, &key) {
            Some(Value::Hash(h)) => h,
            Some(_) => {
                return Err(GrpcError::InvalidRequest(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                ));
            }
            None => HashMap::new(),
        };

        let mut new_fields = 0u64;
        for field in req.fields {
            let field_key = Bytes::from(field.key);
            if !hash.contains_key(&field_key) {
                new_fields += 1;
            }
            hash.insert(field_key, Bytes::from(field.value));
        }

        self.store.set(db, key, Value::Hash(hash));

        Ok(HashSetResponse { new_fields })
    }

    fn hash_get(&self, req: HashGetRequest) -> Result<HashGetResponse, GrpcError> {
        let key = Bytes::from(req.key);
        let db = req.database;

        match self.store.get(db, &key) {
            Some(Value::Hash(h)) => {
                let values = req
                    .fields
                    .iter()
                    .map(|f| h.get(f.as_bytes()).map(|v: &Bytes| v.to_vec()))
                    .collect();
                Ok(HashGetResponse { values })
            }
            Some(_) => Err(GrpcError::InvalidRequest(
                "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
            )),
            None => Ok(HashGetResponse {
                values: req.fields.iter().map(|_| None).collect(),
            }),
        }
    }

    fn ping(&self, req: PingRequest) -> Result<PingResponse, GrpcError> {
        Ok(PingResponse {
            message: req.message.unwrap_or_else(|| "PONG".to_string()),
        })
    }

    fn info(&self, _req: InfoRequest) -> Result<InfoResponse, GrpcError> {
        let mut info = String::new();
        info.push_str("# Server\r\n");
        info.push_str("ferrite_version:0.1.0\r\n");
        info.push_str(&format!("num_databases:{}\r\n", self.store.num_databases()));
        Ok(InfoResponse { info })
    }

    fn batch(&self, req: BatchRequest) -> Result<BatchResponse, GrpcError> {
        let results = req
            .operations
            .into_iter()
            .map(|op| match op {
                Operation::Get(r) => match self.get(r) {
                    Ok(resp) => OperationResult {
                        success: true,
                        error: None,
                        data: resp.value,
                    },
                    Err(e) => OperationResult {
                        success: false,
                        error: Some(e.to_string()),
                        data: None,
                    },
                },
                Operation::Set(r) => match self.set(r) {
                    Ok(resp) => OperationResult {
                        success: resp.success,
                        error: None,
                        data: None,
                    },
                    Err(e) => OperationResult {
                        success: false,
                        error: Some(e.to_string()),
                        data: None,
                    },
                },
                Operation::Delete(r) => match self.delete(r) {
                    Ok(resp) => OperationResult {
                        success: true,
                        error: None,
                        data: Some(resp.deleted_count.to_le_bytes().to_vec()),
                    },
                    Err(e) => OperationResult {
                        success: false,
                        error: Some(e.to_string()),
                        data: None,
                    },
                },
                Operation::Exists(r) => match self.exists(r) {
                    Ok(resp) => OperationResult {
                        success: true,
                        error: None,
                        data: Some(resp.exist_count.to_le_bytes().to_vec()),
                    },
                    Err(e) => OperationResult {
                        success: false,
                        error: Some(e.to_string()),
                        data: None,
                    },
                },
                Operation::Incr(r) => match self.incr(r) {
                    Ok(resp) => OperationResult {
                        success: true,
                        error: None,
                        data: Some(resp.value.to_le_bytes().to_vec()),
                    },
                    Err(e) => OperationResult {
                        success: false,
                        error: Some(e.to_string()),
                        data: None,
                    },
                },
            })
            .collect();

        Ok(BatchResponse { results })
    }
}

// ---------------------------------------------------------------------------
// Service registry
// ---------------------------------------------------------------------------

/// Registry that maps service names to [`FerriteService`] implementations.
///
/// This is a simple lookup table that can be used by a transport layer (e.g.
/// tonic) to route incoming RPCs to the correct handler.
pub struct ServiceRegistry {
    services: HashMap<String, Arc<dyn FerriteService>>,
}

impl ServiceRegistry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self {
            services: HashMap::new(),
        }
    }

    /// Register a service under the given name.
    pub fn register(&mut self, name: impl Into<String>, service: Arc<dyn FerriteService>) {
        self.services.insert(name.into(), service);
    }

    /// Look up a service by name.
    pub fn get(&self, name: &str) -> Option<Arc<dyn FerriteService>> {
        self.services.get(name).cloned()
    }

    /// List all registered service names.
    pub fn list(&self) -> Vec<&str> {
        self.services.keys().map(|s| s.as_str()).collect()
    }
}

impl Default for ServiceRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Very small glob-style pattern matcher supporting `*` and `?`.
fn simple_pattern_match(pattern: &str, input: &str) -> bool {
    let mut pi = pattern.chars().peekable();
    let mut ii = input.chars().peekable();

    fn do_match(
        pat: &mut std::iter::Peekable<std::str::Chars<'_>>,
        inp: &mut std::iter::Peekable<std::str::Chars<'_>>,
    ) -> bool {
        loop {
            match (pat.peek(), inp.peek()) {
                (None, None) => return true,
                (Some('*'), _) => {
                    pat.next();
                    // Try matching the rest from every position.
                    let remaining_pat: String = pat.collect();
                    let remaining_inp: String = std::iter::once(' ') // placeholder
                        .chain(inp.by_ref())
                        .skip(1) // remove placeholder
                        .collect();
                    // Brute-force: try each suffix.
                    for i in 0..=remaining_inp.len() {
                        if simple_pattern_match(&remaining_pat, &remaining_inp[i..]) {
                            return true;
                        }
                    }
                    return false;
                }
                (Some('?'), Some(_)) => {
                    pat.next();
                    inp.next();
                }
                (Some(a), Some(b)) if *a == *b => {
                    pat.next();
                    inp.next();
                }
                _ => return false,
            }
        }
    }

    do_match(&mut pi, &mut ii)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_service() -> FerriteServiceImpl {
        let store = Arc::new(Store::new(16));
        FerriteServiceImpl::new(store)
    }

    #[test]
    fn test_ping() {
        let svc = make_service();
        let resp = svc.ping(PingRequest { message: None }).unwrap();
        assert_eq!(resp.message, "PONG");

        let resp = svc
            .ping(PingRequest {
                message: Some("hello".into()),
            })
            .unwrap();
        assert_eq!(resp.message, "hello");
    }

    #[test]
    fn test_set_and_get() {
        let svc = make_service();

        let set_resp = svc
            .set(SetRequest {
                key: "foo".into(),
                value: b"bar".to_vec(),
                database: 0,
                ttl_ms: None,
                nx: false,
                xx: false,
            })
            .unwrap();
        assert!(set_resp.success);
        assert!(set_resp.old_value.is_none());

        let get_resp = svc
            .get(GetRequest {
                key: "foo".into(),
                database: 0,
            })
            .unwrap();
        assert!(get_resp.found);
        assert_eq!(get_resp.value.unwrap(), b"bar");
    }

    #[test]
    fn test_set_nx_xx() {
        let svc = make_service();

        // NX succeeds when key is absent.
        let resp = svc
            .set(SetRequest {
                key: "k".into(),
                value: b"v1".to_vec(),
                database: 0,
                ttl_ms: None,
                nx: true,
                xx: false,
            })
            .unwrap();
        assert!(resp.success);

        // NX fails when key exists.
        let resp = svc
            .set(SetRequest {
                key: "k".into(),
                value: b"v2".to_vec(),
                database: 0,
                ttl_ms: None,
                nx: true,
                xx: false,
            })
            .unwrap();
        assert!(!resp.success);

        // XX succeeds when key exists.
        let resp = svc
            .set(SetRequest {
                key: "k".into(),
                value: b"v3".to_vec(),
                database: 0,
                ttl_ms: None,
                nx: false,
                xx: true,
            })
            .unwrap();
        assert!(resp.success);

        // XX fails when key is absent.
        let resp = svc
            .set(SetRequest {
                key: "absent".into(),
                value: b"v".to_vec(),
                database: 0,
                ttl_ms: None,
                nx: false,
                xx: true,
            })
            .unwrap();
        assert!(!resp.success);
    }

    #[test]
    fn test_delete() {
        let svc = make_service();

        svc.set(SetRequest {
            key: "a".into(),
            value: b"1".to_vec(),
            database: 0,
            ttl_ms: None,
            nx: false,
            xx: false,
        })
        .unwrap();

        svc.set(SetRequest {
            key: "b".into(),
            value: b"2".to_vec(),
            database: 0,
            ttl_ms: None,
            nx: false,
            xx: false,
        })
        .unwrap();

        let resp = svc
            .delete(DeleteRequest {
                keys: vec!["a".into(), "b".into(), "c".into()],
                database: 0,
            })
            .unwrap();
        assert_eq!(resp.deleted_count, 2);
    }

    #[test]
    fn test_exists() {
        let svc = make_service();

        svc.set(SetRequest {
            key: "x".into(),
            value: b"1".to_vec(),
            database: 0,
            ttl_ms: None,
            nx: false,
            xx: false,
        })
        .unwrap();

        let resp = svc
            .exists(ExistsRequest {
                keys: vec!["x".into(), "y".into()],
                database: 0,
            })
            .unwrap();
        assert_eq!(resp.exist_count, 1);
    }

    #[test]
    fn test_incr() {
        let svc = make_service();

        // Increment a non-existent key starts from 0.
        let resp = svc
            .incr(IncrRequest {
                key: "counter".into(),
                increment: 5,
                database: 0,
            })
            .unwrap();
        assert_eq!(resp.value, 5);

        // Subsequent increment.
        let resp = svc
            .incr(IncrRequest {
                key: "counter".into(),
                increment: -2,
                database: 0,
            })
            .unwrap();
        assert_eq!(resp.value, 3);
    }

    #[test]
    fn test_mget_mset() {
        let svc = make_service();

        svc.mset(MSetRequest {
            entries: vec![
                KeyValue {
                    key: "k1".into(),
                    value: b"v1".to_vec(),
                },
                KeyValue {
                    key: "k2".into(),
                    value: b"v2".to_vec(),
                },
            ],
            database: 0,
        })
        .unwrap();

        let resp = svc
            .mget(MGetRequest {
                keys: vec!["k1".into(), "k2".into(), "k3".into()],
                database: 0,
            })
            .unwrap();
        assert_eq!(resp.values.len(), 3);
        assert_eq!(resp.values[0].as_deref(), Some(b"v1".as_ref()));
        assert_eq!(resp.values[1].as_deref(), Some(b"v2".as_ref()));
        assert!(resp.values[2].is_none());
    }

    #[test]
    fn test_list_push_and_pop() {
        let svc = make_service();

        let resp = svc
            .list_push(ListPushRequest {
                key: "mylist".into(),
                values: vec![b"a".to_vec(), b"b".to_vec()],
                left: true,
                database: 0,
            })
            .unwrap();
        assert_eq!(resp.length, 2);

        let resp = svc
            .list_pop(ListPopRequest {
                key: "mylist".into(),
                count: 1,
                left: true,
                database: 0,
            })
            .unwrap();
        assert_eq!(resp.values.len(), 1);
        // LPUSH pushes each element to the front, so "b" is at front.
        assert_eq!(resp.values[0], b"b");
    }

    #[test]
    fn test_hash_set_and_get() {
        let svc = make_service();

        let resp = svc
            .hash_set(HashSetRequest {
                key: "myhash".into(),
                fields: vec![
                    KeyValue {
                        key: "f1".into(),
                        value: b"v1".to_vec(),
                    },
                    KeyValue {
                        key: "f2".into(),
                        value: b"v2".to_vec(),
                    },
                ],
                database: 0,
            })
            .unwrap();
        assert_eq!(resp.new_fields, 2);

        let resp = svc
            .hash_get(HashGetRequest {
                key: "myhash".into(),
                fields: vec!["f1".into(), "f3".into()],
                database: 0,
            })
            .unwrap();
        assert_eq!(resp.values.len(), 2);
        assert_eq!(resp.values[0].as_deref(), Some(b"v1".as_ref()));
        assert!(resp.values[1].is_none());
    }

    #[test]
    fn test_batch() {
        let svc = make_service();

        let resp = svc
            .batch(BatchRequest {
                operations: vec![
                    Operation::Set(SetRequest {
                        key: "bk".into(),
                        value: b"bv".to_vec(),
                        database: 0,
                        ttl_ms: None,
                        nx: false,
                        xx: false,
                    }),
                    Operation::Get(GetRequest {
                        key: "bk".into(),
                        database: 0,
                    }),
                    Operation::Delete(DeleteRequest {
                        keys: vec!["bk".into()],
                        database: 0,
                    }),
                ],
            })
            .unwrap();

        assert_eq!(resp.results.len(), 3);
        assert!(resp.results[0].success); // set
        assert!(resp.results[1].success); // get
        assert_eq!(resp.results[1].data.as_deref(), Some(b"bv".as_ref()));
        assert!(resp.results[2].success); // delete
    }

    #[test]
    fn test_info() {
        let svc = make_service();
        let resp = svc.info(InfoRequest { sections: vec![] }).unwrap();
        assert!(resp.info.contains("ferrite_version"));
    }

    #[test]
    fn test_service_registry() {
        let store = Arc::new(Store::new(16));
        let svc = Arc::new(FerriteServiceImpl::new(store));

        let mut registry = ServiceRegistry::new();
        registry.register("ferrite.v1.Ferrite", svc.clone());

        assert!(registry.get("ferrite.v1.Ferrite").is_some());
        assert!(registry.get("unknown").is_none());
        assert_eq!(registry.list().len(), 1);
    }

    #[test]
    fn test_simple_pattern_match() {
        assert!(simple_pattern_match("*", "anything"));
        assert!(simple_pattern_match("user:*", "user:123"));
        assert!(!simple_pattern_match("user:*", "session:123"));
        assert!(simple_pattern_match("h?llo", "hello"));
        assert!(!simple_pattern_match("h?llo", "heello"));
        assert!(simple_pattern_match("*key*", "my_key_name"));
    }
}
