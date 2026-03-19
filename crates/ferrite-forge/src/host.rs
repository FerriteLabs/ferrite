//! Host API surface exposed to Forge modules.
//!
//! The host API is the seam between the wasm sandbox and the rest of Ferrite.
//! Modules call into it via Wasmtime imports (currently the `ferrite_kv`
//! module).  This crate defines:
//!
//! - [`HostContext`] — trait the embedder implements to plug in real KV ops.
//! - [`InMemoryHostContext`] — reference impl used by tests and the
//!   eval harness.
//! - [`link_host_api`] — registers the imports on a Wasmtime [`Linker`] so a
//!   module that imports `ferrite_kv.get` and `ferrite_kv.set` can call them.
//!
//! Wire format: keys and values are passed as `(ptr, len)` pairs into the
//! module's linear memory, identical to the convention used by the
//! function-export contract in [`super::exec`].

use std::sync::Arc;

/// Trait every embedder must implement.  Concrete impls live outside this
/// crate so Forge stays storage-agnostic per ADR-019.
pub trait HostContext: Send + Sync + 'static {
    /// Read `key`.  Returns `None` if absent.  Errors propagate as wasm traps.
    fn kv_get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, HostError>;

    /// Write `value` to `key`.
    fn kv_set(&self, key: &[u8], value: &[u8]) -> Result<(), HostError>;

    /// Delete `key`.  Returns whether it existed.
    fn kv_del(&self, key: &[u8]) -> Result<bool, HostError>;

    /// Enumerate keys whose names start with `prefix`, returning at most
    /// `limit` keys.  Order is unspecified.  The default impl returns an
    /// empty list so existing embedders continue to compile after the
    /// trait was extended; production impls should provide a real scan.
    #[allow(unused_variables)]
    fn kv_scan(&self, prefix: &[u8], limit: usize) -> Result<Vec<Vec<u8>>, HostError> {
        Ok(Vec::new())
    }

    /// Atomically increment the integer value stored at `key` by `delta`
    /// and return the new value.  Treats a missing key as `0`.  Returns
    /// [`HostError::NotInteger`] if the existing value isn't a parseable
    /// signed integer, or [`HostError::Overflow`] on i64 overflow.
    ///
    /// The default impl is **not** atomic — it composes get/set and is
    /// only suitable for single-threaded reference embedders.
    /// Production impls (e.g. backed by a real Store) should provide an
    /// atomic implementation.
    fn kv_incr(&self, key: &[u8], delta: i64) -> Result<i64, HostError> {
        let current: i64 = match self.kv_get(key)? {
            Some(bytes) => std::str::from_utf8(&bytes)
                .map_err(|_| HostError::NotInteger)?
                .parse()
                .map_err(|_| HostError::NotInteger)?,
            None => 0,
        };
        let new = current.checked_add(delta).ok_or(HostError::Overflow)?;
        self.kv_set(key, new.to_string().as_bytes())?;
        Ok(new)
    }

    /// Set a millisecond-precision expiration on `key`.  Returns
    /// `true` if the key existed and the TTL was applied, `false` if
    /// the key did not exist.  The default impl returns `false`
    /// without touching state — production embedders must override.
    #[allow(unused_variables)]
    fn kv_expire(&self, key: &[u8], ttl_ms: u64) -> Result<bool, HostError> {
        Ok(false)
    }

    /// Return the milliseconds-until-expiry for `key`, mirroring
    /// Redis's `PTTL` semantics with one extension: callers that
    /// only have second-resolution backends should convert by
    /// multiplying by 1000.  Special values: `-1` = key has no
    /// expiry, `-2` = key does not exist.  The default impl
    /// returns `-2` so unbacked embedders see "missing".
    #[allow(unused_variables)]
    fn kv_ttl(&self, key: &[u8]) -> Result<i64, HostError> {
        Ok(-2)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum HostError {
    #[error("acl violation: key '{0}' not in declared keyspace")]
    Acl(String),
    #[error("backend error: {0}")]
    Backend(String),
    #[error("value is not a parseable integer")]
    NotInteger,
    #[error("integer overflow")]
    Overflow,
}

// ---------------------------------------------------------------------------
// Reference in-memory implementation
// ---------------------------------------------------------------------------

/// Reference `HostContext` backed by a parking_lot RwLock + HashMap.  Used
/// by tests and the eval harness.  Production embedders implement the trait
/// against the real engine.
#[derive(Debug, Default, Clone)]
pub struct InMemoryHostContext {
    inner: Arc<parking_lot::RwLock<std::collections::HashMap<Vec<u8>, Vec<u8>>>>,
    /// Optional per-key expiration deadlines.  `kv_expire` writes here;
    /// `kv_ttl` reads from it.  This is a separate map (rather than
    /// folded into the value tuple) so `snapshot()` and existing
    /// callers remain backward-compatible.
    ttls: Arc<parking_lot::RwLock<std::collections::HashMap<Vec<u8>, std::time::Instant>>>,
}

impl InMemoryHostContext {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn snapshot(&self) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.inner
            .read()
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }
}

impl HostContext for InMemoryHostContext {
    fn kv_get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, HostError> {
        Ok(self.inner.read().get(key).cloned())
    }
    fn kv_set(&self, key: &[u8], value: &[u8]) -> Result<(), HostError> {
        self.inner.write().insert(key.to_vec(), value.to_vec());
        Ok(())
    }
    fn kv_del(&self, key: &[u8]) -> Result<bool, HostError> {
        Ok(self.inner.write().remove(key).is_some())
    }
    fn kv_scan(&self, prefix: &[u8], limit: usize) -> Result<Vec<Vec<u8>>, HostError> {
        let guard = self.inner.read();
        // Collect all matching keys, sort for deterministic order, then
        // take the first `limit`.
        let mut out: Vec<Vec<u8>> = guard
            .keys()
            .filter(|k| k.starts_with(prefix))
            .cloned()
            .collect();
        out.sort();
        out.truncate(limit);
        Ok(out)
    }
    fn kv_incr(&self, key: &[u8], delta: i64) -> Result<i64, HostError> {
        // Atomic against other in-memory impls — single write-lock
        // critical section spans both the read and the write.
        let mut guard = self.inner.write();
        let current: i64 = match guard.get(key) {
            Some(bytes) => std::str::from_utf8(bytes)
                .map_err(|_| HostError::NotInteger)?
                .parse()
                .map_err(|_| HostError::NotInteger)?,
            None => 0,
        };
        let new = current.checked_add(delta).ok_or(HostError::Overflow)?;
        guard.insert(key.to_vec(), new.to_string().into_bytes());
        Ok(new)
    }
    fn kv_expire(&self, key: &[u8], ttl_ms: u64) -> Result<bool, HostError> {
        if !self.inner.read().contains_key(key) {
            return Ok(false);
        }
        let deadline = std::time::Instant::now() + std::time::Duration::from_millis(ttl_ms);
        self.ttls.write().insert(key.to_vec(), deadline);
        Ok(true)
    }
    fn kv_ttl(&self, key: &[u8]) -> Result<i64, HostError> {
        if !self.inner.read().contains_key(key) {
            return Ok(-2);
        }
        let Some(deadline) = self.ttls.read().get(key).copied() else {
            return Ok(-1);
        };
        let now = std::time::Instant::now();
        if now >= deadline {
            // Past the deadline — report missing.  We don't auto-evict
            // here; production impls (Store) handle that elsewhere.
            return Ok(-2);
        }
        Ok(deadline.duration_since(now).as_millis() as i64)
    }
}

// ---------------------------------------------------------------------------
// Linker integration
// ---------------------------------------------------------------------------

#[cfg(feature = "runtime")]
pub use runtime_link::{link_host_api, HostState};

#[cfg(feature = "runtime")]
mod runtime_link {
    use super::{HostContext, HostError};
    use std::sync::Arc;
    use wasmtime::{AsContextMut, Caller, Linker, Memory};

    /// Per-call state stored in `wasmtime::Store::data`.  Carries the host
    /// context plus a "scratch" region inside the module's linear memory
    /// where return values are written before the module reads them.
    pub struct HostState {
        pub ctx: Arc<dyn HostContext>,
        /// Offset in the module's memory where the host writes return blobs.
        /// Modules must advertise this as their own scratch area; for the
        /// reference contract we use page 1 (offset 65_536).
        pub scratch_offset: u32,
    }

    impl HostState {
        pub fn new(ctx: Arc<dyn HostContext>) -> Self {
            Self {
                ctx,
                scratch_offset: 65_536,
            }
        }
    }

    /// Wire the `ferrite_kv` import module into `linker`.
    ///
    /// Imports exposed:
    /// - `kv_get(kptr: i32, klen: i32) -> i64` — packed `(ptr, len)`; len `< 0` if missing.
    /// - `kv_set(kptr: i32, klen: i32, vptr: i32, vlen: i32) -> i32` — 0 ok, 1 acl, 2 backend.
    /// - `kv_del(kptr: i32, klen: i32) -> i32` — 0 missing, 1 deleted, negative on error.
    /// - `kv_scan(pptr: i32, plen: i32, limit: i32) -> i64` — packed
    ///   `(scratch_ptr, total_bytes)`; buffer encodes a sequence of
    ///   `[u32 LE key_len][key bytes...]` records.  `total_bytes < 0`
    ///   on error; `total_bytes == 0` means no matches.
    /// - `kv_incr(kptr: i32, klen: i32, delta: i64, out_ptr: i32) -> i32`
    ///   — atomic increment.  Returns status: 0 ok (new value written
    ///   as i64 LE at `out_ptr`), 1 acl, 2 not-integer, 3 overflow,
    ///   4 backend.  Treats missing key as `0`.
    /// - `kv_expire(kptr: i32, klen: i32, ttl_ms: i64) -> i32` — set
    ///   ms-precision TTL.  Returns 0 if the key did not exist, 1 if
    ///   the TTL was applied, -1 on ACL denial, -2 on backend error,
    ///   -3 if `ttl_ms` is negative.
    /// - `kv_ttl(kptr: i32, klen: i32) -> i64` — returns ms-until-
    ///   expiry, or one of the special values `-1` (no TTL), `-2`
    ///   (key missing), `-3` (ACL denied), `-4` (backend error).
    pub fn link_host_api(linker: &mut Linker<HostState>) -> Result<(), wasmtime::Error> {
        linker.func_wrap(
            "ferrite_kv",
            "kv_get",
            |mut caller: Caller<'_, HostState>, kptr: i32, klen: i32| -> i64 {
                let Some(mem) = caller.get_export("memory").and_then(|e| e.into_memory()) else {
                    return pack(0, -1);
                };
                let Ok(key) = read_bytes(&mut caller, mem, kptr, klen) else {
                    return pack(0, -1);
                };
                let ctx = caller.data().ctx.clone();
                let scratch = caller.data().scratch_offset;
                match ctx.kv_get(&key) {
                    Ok(Some(value)) => match write_bytes(&mut caller, mem, scratch, &value) {
                        Ok(()) => pack(scratch, value.len() as i32),
                        Err(_) => pack(0, -1),
                    },
                    Ok(None) | Err(_) => pack(0, -1),
                }
            },
        )?;

        linker.func_wrap(
            "ferrite_kv",
            "kv_set",
            |mut caller: Caller<'_, HostState>,
             kptr: i32,
             klen: i32,
             vptr: i32,
             vlen: i32|
             -> i32 {
                let Some(mem) = caller.get_export("memory").and_then(|e| e.into_memory()) else {
                    return 2;
                };
                let Ok(key) = read_bytes(&mut caller, mem, kptr, klen) else {
                    return 2;
                };
                let Ok(value) = read_bytes(&mut caller, mem, vptr, vlen) else {
                    return 2;
                };
                let ctx = caller.data().ctx.clone();
                match ctx.kv_set(&key, &value) {
                    Ok(()) => 0,
                    Err(HostError::Acl(_)) => 1,
                    Err(_) => 2,
                }
            },
        )?;

        linker.func_wrap(
            "ferrite_kv",
            "kv_del",
            |mut caller: Caller<'_, HostState>, kptr: i32, klen: i32| -> i32 {
                let Some(mem) = caller.get_export("memory").and_then(|e| e.into_memory()) else {
                    return -1;
                };
                let Ok(key) = read_bytes(&mut caller, mem, kptr, klen) else {
                    return -1;
                };
                let ctx = caller.data().ctx.clone();
                match ctx.kv_del(&key) {
                    Ok(true) => 1,
                    Ok(false) => 0,
                    Err(_) => -1,
                }
            },
        )?;

        linker.func_wrap(
            "ferrite_kv",
            "kv_scan",
            |mut caller: Caller<'_, HostState>, pptr: i32, plen: i32, limit: i32| -> i64 {
                let Some(mem) = caller.get_export("memory").and_then(|e| e.into_memory()) else {
                    return pack(0, -1);
                };
                let Ok(prefix) = read_bytes(&mut caller, mem, pptr, plen) else {
                    return pack(0, -1);
                };
                if limit < 0 {
                    return pack(0, -1);
                }
                let ctx = caller.data().ctx.clone();
                let scratch = caller.data().scratch_offset;
                let Ok(keys) = ctx.kv_scan(&prefix, limit as usize) else {
                    return pack(0, -1);
                };
                // Encode as a sequence of [u32 LE key_len][key bytes...].
                let total: usize = keys.iter().map(|k| 4 + k.len()).sum();
                let mut buf = Vec::with_capacity(total);
                for k in &keys {
                    buf.extend_from_slice(&(k.len() as u32).to_le_bytes());
                    buf.extend_from_slice(k);
                }
                match write_bytes(&mut caller, mem, scratch, &buf) {
                    Ok(()) => pack(scratch, buf.len() as i32),
                    Err(_) => pack(0, -1),
                }
            },
        )?;

        linker.func_wrap(
            "ferrite_kv",
            "kv_incr",
            |mut caller: Caller<'_, HostState>,
             kptr: i32,
             klen: i32,
             delta: i64,
             out_ptr: i32|
             -> i32 {
                let Some(mem) = caller.get_export("memory").and_then(|e| e.into_memory()) else {
                    return 4;
                };
                let Ok(key) = read_bytes(&mut caller, mem, kptr, klen) else {
                    return 4;
                };
                let ctx = caller.data().ctx.clone();
                let new = match ctx.kv_incr(&key, delta) {
                    Ok(v) => v,
                    Err(HostError::Acl(_)) => return 1,
                    Err(HostError::NotInteger) => return 2,
                    Err(HostError::Overflow) => return 3,
                    Err(_) => return 4,
                };
                if out_ptr < 0 {
                    return 4;
                }
                if write_bytes(&mut caller, mem, out_ptr as u32, &new.to_le_bytes()).is_err() {
                    return 4;
                }
                0
            },
        )?;

        linker.func_wrap(
            "ferrite_kv",
            "kv_expire",
            |mut caller: Caller<'_, HostState>, kptr: i32, klen: i32, ttl_ms: i64| -> i32 {
                if ttl_ms < 0 {
                    return -3;
                }
                let Some(mem) = caller.get_export("memory").and_then(|e| e.into_memory()) else {
                    return -2;
                };
                let Ok(key) = read_bytes(&mut caller, mem, kptr, klen) else {
                    return -2;
                };
                let ctx = caller.data().ctx.clone();
                match ctx.kv_expire(&key, ttl_ms as u64) {
                    Ok(true) => 1,
                    Ok(false) => 0,
                    Err(HostError::Acl(_)) => -1,
                    Err(_) => -2,
                }
            },
        )?;

        linker.func_wrap(
            "ferrite_kv",
            "kv_ttl",
            |mut caller: Caller<'_, HostState>, kptr: i32, klen: i32| -> i64 {
                let Some(mem) = caller.get_export("memory").and_then(|e| e.into_memory()) else {
                    return -4;
                };
                let Ok(key) = read_bytes(&mut caller, mem, kptr, klen) else {
                    return -4;
                };
                let ctx = caller.data().ctx.clone();
                match ctx.kv_ttl(&key) {
                    Ok(v) => v,
                    Err(HostError::Acl(_)) => -3,
                    Err(_) => -4,
                }
            },
        )?;

        Ok(())
    }

    fn pack(ptr: u32, len: i32) -> i64 {
        ((ptr as u64) << 32) as i64 | (len as i64 & 0xffff_ffff)
    }

    fn read_bytes(
        caller: &mut Caller<'_, HostState>,
        mem: Memory,
        ptr: i32,
        len: i32,
    ) -> Result<Vec<u8>, ()> {
        if ptr < 0 || len < 0 {
            return Err(());
        }
        let mut buf = vec![0u8; len as usize];
        mem.read(&caller.as_context_mut(), ptr as usize, &mut buf)
            .map_err(|_| ())?;
        Ok(buf)
    }

    fn write_bytes(
        caller: &mut Caller<'_, HostState>,
        mem: Memory,
        offset: u32,
        data: &[u8],
    ) -> Result<(), ()> {
        let needed_pages = (offset as u64 + data.len() as u64).div_ceil(65_536);
        let current_pages = mem.size(&caller.as_context_mut());
        if needed_pages > current_pages {
            mem.grow(&mut caller.as_context_mut(), needed_pages - current_pages)
                .map_err(|_| ())?;
        }
        mem.write(&mut caller.as_context_mut(), offset as usize, data)
            .map_err(|_| ())?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// ACL-enforcing wrapper
// ---------------------------------------------------------------------------

/// Wraps any [`HostContext`] with key-pattern ACL checks.
///
/// Patterns derive from a [`super::module::ModuleAcl`].  Every `kv_get`
/// checks `read_keys`, every `kv_set`/`kv_del` checks `write_keys`.
/// Patterns are simple globs:
///
/// - `*` at the end matches any suffix (`user:*` matches `user:42`).
/// - exact match otherwise.
///
/// Empty pattern lists mean "deny all" — the safer default per ADR-019 §Risks.
pub struct AclHostContext<C: HostContext> {
    inner: C,
    read_patterns: Vec<String>,
    write_patterns: Vec<String>,
}

impl<C: HostContext> AclHostContext<C> {
    pub fn new(inner: C, acl: &super::module::ModuleAcl) -> Self {
        Self {
            inner,
            read_patterns: acl.read_keys.clone(),
            write_patterns: acl.write_keys.clone(),
        }
    }

    fn allowed(patterns: &[String], key: &[u8]) -> bool {
        let Ok(key_str) = std::str::from_utf8(key) else {
            return false;
        };
        patterns.iter().any(|p| match_glob(p, key_str))
    }
}

fn match_glob(pattern: &str, key: &str) -> bool {
    if let Some(prefix) = pattern.strip_suffix('*') {
        key.starts_with(prefix)
    } else {
        pattern == key
    }
}

impl<C: HostContext> HostContext for AclHostContext<C> {
    fn kv_get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, HostError> {
        if !Self::allowed(&self.read_patterns, key) {
            return Err(HostError::Acl(String::from_utf8_lossy(key).into_owned()));
        }
        self.inner.kv_get(key)
    }
    fn kv_set(&self, key: &[u8], value: &[u8]) -> Result<(), HostError> {
        if !Self::allowed(&self.write_patterns, key) {
            return Err(HostError::Acl(String::from_utf8_lossy(key).into_owned()));
        }
        self.inner.kv_set(key, value)
    }
    fn kv_del(&self, key: &[u8]) -> Result<bool, HostError> {
        if !Self::allowed(&self.write_patterns, key) {
            return Err(HostError::Acl(String::from_utf8_lossy(key).into_owned()));
        }
        self.inner.kv_del(key)
    }
    fn kv_scan(&self, prefix: &[u8], limit: usize) -> Result<Vec<Vec<u8>>, HostError> {
        // Pull from the inner store, then filter by read-glob.  The prefix
        // itself isn't required to match a glob — we only enforce that
        // each *returned* key passes the read ACL.  This keeps semantics
        // intuitive ("scan returns the subset of matching keys you may
        // read") while still gating data exfiltration.
        let raw = self.inner.kv_scan(prefix, limit.saturating_mul(4))?;
        let mut out: Vec<Vec<u8>> = raw
            .into_iter()
            .filter(|k| Self::allowed(&self.read_patterns, k))
            .take(limit)
            .collect();
        out.sort();
        Ok(out)
    }
    fn kv_incr(&self, key: &[u8], delta: i64) -> Result<i64, HostError> {
        // INCR mutates the value, so it requires write permission;
        // we also check read because conceptually incr is read-modify-
        // write and a write-only ACL shouldn't be able to learn the
        // post-increment value.
        if !Self::allowed(&self.write_patterns, key) || !Self::allowed(&self.read_patterns, key) {
            return Err(HostError::Acl(String::from_utf8_lossy(key).into_owned()));
        }
        // Delegate to the inner impl so its atomicity guarantees are
        // preserved (otherwise we'd compose get + set non-atomically).
        self.inner.kv_incr(key, delta)
    }
    fn kv_expire(&self, key: &[u8], ttl_ms: u64) -> Result<bool, HostError> {
        // Setting a TTL mutates key metadata → write permission.
        if !Self::allowed(&self.write_patterns, key) {
            return Err(HostError::Acl(String::from_utf8_lossy(key).into_owned()));
        }
        self.inner.kv_expire(key, ttl_ms)
    }
    fn kv_ttl(&self, key: &[u8]) -> Result<i64, HostError> {
        // Reading the TTL exposes liveness/state info → read permission.
        if !Self::allowed(&self.read_patterns, key) {
            return Err(HostError::Acl(String::from_utf8_lossy(key).into_owned()));
        }
        self.inner.kv_ttl(key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::module::ModuleAcl;

    #[test]
    fn in_memory_host_roundtrip() {
        let h = InMemoryHostContext::new();
        assert_eq!(h.kv_get(b"missing").unwrap(), None);
        h.kv_set(b"k", b"v").unwrap();
        assert_eq!(h.kv_get(b"k").unwrap().as_deref(), Some(b"v".as_ref()));
        assert!(h.kv_del(b"k").unwrap());
        assert!(!h.kv_del(b"k").unwrap());
        assert_eq!(h.kv_get(b"k").unwrap(), None);
    }

    #[test]
    fn snapshot_returns_all_entries() {
        let h = InMemoryHostContext::new();
        h.kv_set(b"a", b"1").unwrap();
        h.kv_set(b"b", b"2").unwrap();
        let mut snap = h.snapshot();
        snap.sort();
        assert_eq!(
            snap,
            vec![
                (b"a".to_vec(), b"1".to_vec()),
                (b"b".to_vec(), b"2".to_vec())
            ]
        );
    }

    #[test]
    fn acl_default_denies_all() {
        let inner = InMemoryHostContext::new();
        inner.kv_set(b"user:1", b"v").unwrap();
        let acl = ModuleAcl::default();
        let guarded = AclHostContext::new(inner, &acl);
        assert!(matches!(guarded.kv_get(b"user:1"), Err(HostError::Acl(_))));
        assert!(matches!(
            guarded.kv_set(b"user:1", b"x"),
            Err(HostError::Acl(_))
        ));
    }

    #[test]
    fn acl_allows_matching_glob() {
        let inner = InMemoryHostContext::new();
        let acl = ModuleAcl {
            read_keys: vec!["user:*".into()],
            write_keys: vec!["scratch:*".into()],
        };
        let guarded = AclHostContext::new(inner, &acl);
        guarded.kv_set(b"scratch:42", b"ok").unwrap();
        assert_eq!(guarded.kv_get(b"user:1").unwrap(), None);
        assert!(matches!(guarded.kv_get(b"other:1"), Err(HostError::Acl(_))));
        assert!(matches!(
            guarded.kv_set(b"user:1", b"x"),
            Err(HostError::Acl(_))
        ));
    }

    #[test]
    fn acl_exact_match_when_no_wildcard() {
        let inner = InMemoryHostContext::new();
        let acl = ModuleAcl {
            read_keys: vec!["only-this".into()],
            write_keys: vec![],
        };
        let guarded = AclHostContext::new(inner, &acl);
        assert_eq!(guarded.kv_get(b"only-this").unwrap(), None);
        assert!(matches!(
            guarded.kv_get(b"only-this-too"),
            Err(HostError::Acl(_))
        ));
    }

    #[test]
    fn in_memory_scan_filters_by_prefix_and_limit() {
        let h = InMemoryHostContext::new();
        for k in ["user:1", "user:2", "user:3", "other:1"] {
            h.kv_set(k.as_bytes(), b"v").unwrap();
        }
        let all = h.kv_scan(b"user:", 10).unwrap();
        assert_eq!(
            all,
            vec![b"user:1".to_vec(), b"user:2".to_vec(), b"user:3".to_vec()]
        );
        let limited = h.kv_scan(b"user:", 2).unwrap();
        assert_eq!(limited, vec![b"user:1".to_vec(), b"user:2".to_vec()]);
        assert!(h.kv_scan(b"missing:", 10).unwrap().is_empty());
    }

    #[test]
    fn acl_scan_filters_results_to_read_glob() {
        let inner = InMemoryHostContext::new();
        for k in ["user:1", "user:2", "secret:1"] {
            inner.kv_set(k.as_bytes(), b"v").unwrap();
        }
        let acl = ModuleAcl {
            read_keys: vec!["user:*".into()],
            write_keys: vec![],
        };
        let guarded = AclHostContext::new(inner, &acl);
        // Scan with empty prefix returns only keys matching the read glob.
        let out = guarded.kv_scan(b"", 10).unwrap();
        assert_eq!(out, vec![b"user:1".to_vec(), b"user:2".to_vec()]);
        // Scan with non-permitted prefix returns empty (filtered).
        assert!(guarded.kv_scan(b"secret:", 10).unwrap().is_empty());
    }

    #[test]
    fn in_memory_incr_creates_and_increments() {
        let h = InMemoryHostContext::new();
        // Missing key seeds at 0.
        assert_eq!(h.kv_incr(b"counter", 1).unwrap(), 1);
        assert_eq!(h.kv_incr(b"counter", 4).unwrap(), 5);
        assert_eq!(h.kv_incr(b"counter", -2).unwrap(), 3);
        assert_eq!(
            h.kv_get(b"counter").unwrap().as_deref(),
            Some(b"3".as_ref())
        );
    }

    #[test]
    fn in_memory_incr_rejects_non_integer_value() {
        let h = InMemoryHostContext::new();
        h.kv_set(b"k", b"hello").unwrap();
        assert!(matches!(h.kv_incr(b"k", 1), Err(HostError::NotInteger)));
    }

    #[test]
    fn in_memory_incr_detects_overflow() {
        let h = InMemoryHostContext::new();
        h.kv_set(b"k", i64::MAX.to_string().as_bytes()).unwrap();
        assert!(matches!(h.kv_incr(b"k", 1), Err(HostError::Overflow)));
    }

    #[test]
    fn acl_incr_requires_both_read_and_write() {
        let inner = InMemoryHostContext::new();
        let acl = ModuleAcl {
            read_keys: vec!["counter:*".into()],
            write_keys: vec!["counter:*".into()],
        };
        let guarded = AclHostContext::new(inner, &acl);
        assert_eq!(guarded.kv_incr(b"counter:hits", 1).unwrap(), 1);
        // Out-of-glob keys must be denied.
        assert!(matches!(
            guarded.kv_incr(b"other:hits", 1),
            Err(HostError::Acl(_))
        ));

        // Read-only ACL cannot incr.
        let inner2 = InMemoryHostContext::new();
        let acl_ro = ModuleAcl {
            read_keys: vec!["counter:*".into()],
            write_keys: vec![],
        };
        let guarded_ro = AclHostContext::new(inner2, &acl_ro);
        assert!(matches!(
            guarded_ro.kv_incr(b"counter:hits", 1),
            Err(HostError::Acl(_))
        ));
    }

    #[test]
    fn in_memory_expire_and_ttl_roundtrip() {
        let h = InMemoryHostContext::new();
        // Missing key — expire is a no-op, ttl reports -2.
        assert!(!h.kv_expire(b"missing", 1_000).unwrap());
        assert_eq!(h.kv_ttl(b"missing").unwrap(), -2);

        h.kv_set(b"sess:1", b"hello").unwrap();
        // No expire set yet — ttl reports -1.
        assert_eq!(h.kv_ttl(b"sess:1").unwrap(), -1);

        assert!(h.kv_expire(b"sess:1", 60_000).unwrap());
        let remaining = h.kv_ttl(b"sess:1").unwrap();
        assert!((1..=60_000).contains(&remaining), "remaining={remaining}");
    }

    #[test]
    fn in_memory_ttl_reports_missing_after_deadline() {
        let h = InMemoryHostContext::new();
        h.kv_set(b"k", b"v").unwrap();
        assert!(h.kv_expire(b"k", 1).unwrap());
        // Sleep just past the 1 ms deadline.
        std::thread::sleep(std::time::Duration::from_millis(10));
        assert_eq!(h.kv_ttl(b"k").unwrap(), -2);
    }

    #[test]
    fn acl_expire_requires_write_glob() {
        let inner = InMemoryHostContext::new();
        inner.kv_set(b"sess:1", b"x").unwrap();
        inner.kv_set(b"other:1", b"x").unwrap();
        let acl = ModuleAcl {
            read_keys: vec!["sess:*".into()],
            write_keys: vec!["sess:*".into()],
        };
        let guarded = AclHostContext::new(inner, &acl);
        assert!(guarded.kv_expire(b"sess:1", 1_000).unwrap());
        assert!(matches!(
            guarded.kv_expire(b"other:1", 1_000),
            Err(HostError::Acl(_))
        ));
    }

    #[test]
    fn acl_ttl_requires_read_glob() {
        let inner = InMemoryHostContext::new();
        inner.kv_set(b"sess:1", b"x").unwrap();
        inner.kv_expire(b"sess:1", 60_000).unwrap();
        let acl = ModuleAcl {
            read_keys: vec!["sess:*".into()],
            write_keys: vec![],
        };
        let guarded = AclHostContext::new(inner, &acl);
        assert!(guarded.kv_ttl(b"sess:1").unwrap() > 0);
        assert!(matches!(guarded.kv_ttl(b"other:1"), Err(HostError::Acl(_))));
    }
}
