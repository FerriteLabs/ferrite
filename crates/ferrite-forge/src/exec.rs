//! Forge per-call executor.
//!
//! Loads a registered module into a per-call `Store`, sets fuel + epoch
//! deadline per the [`ResourceBudget`], invokes a named export with input
//! bytes, and returns the output bytes (or an `ExecError` describing which
//! resource was exhausted).
//!
//! This is the P1 baseline: it supports modules that export a function with
//! signature `(i32, i32) -> i64` where the two i32s are `(input_ptr,
//! input_len)` and the i64 is `(output_ptr << 32) | output_len`.  The host
//! API surface (KV ops, logging, etc.) lands in P2 — see
//! `docs/phases/m2-forge-roadmap.md`.

use crate::budget::ResourceBudget;
use crate::host::{link_host_api, HostContext, HostState};
use crate::module::Module;
use std::sync::Arc;
use std::thread;
use std::time::Instant;
use wasmtime::{Engine, Linker, Memory, Module as WasmModule, Store, TypedFunc};

/// Errors returned by [`Executor::call`].
#[derive(Debug, thiserror::Error)]
pub enum ExecError {
    #[error("module compilation failed: {0}")]
    Compile(String),
    #[error("module instantiation failed: {0}")]
    Instantiate(String),
    #[error("export '{0}' not found or has wrong signature")]
    BadExport(String),
    #[error("module did not export a 'memory'")]
    NoMemory,
    #[error("invalid budget: {0}")]
    Budget(#[from] crate::budget::BudgetError),
    #[error("fuel exhausted")]
    FuelExhausted,
    #[error("wall-time exceeded")]
    WallTimeExceeded,
    #[error("module trapped: {0}")]
    Trap(String),
    #[error("invalid output pointer/length returned by module")]
    BadOutput,
}

/// Single-shot executor.  In production each worker thread holds one and
/// reuses it across calls; for the P1 baseline a fresh `Store` is built per
/// call so the test surface is small.
pub struct Executor {
    engine: Engine,
}

impl Executor {
    pub fn new(engine: Engine) -> Self {
        Self { engine }
    }

    /// Compile + invoke `export_name(input)` on `module` with the given budget.
    /// Returns the bytes the module wrote to its memory and signalled via the
    /// packed `(ptr, len)` return value.
    pub fn call(
        &self,
        module: &Module,
        export_name: &str,
        input: &[u8],
        budget: ResourceBudget,
    ) -> Result<Vec<u8>, ExecError> {
        budget.validate()?;

        let wasm = WasmModule::new(&self.engine, &module.bytes)
            .map_err(|e| ExecError::Compile(e.to_string()))?;
        let mut store: Store<()> = Store::new(&self.engine, ());

        // Wasmtime requires fuel to be set when `consume_fuel(true)` (engine
        // default for Forge); `None` in the budget means "unbounded" so we
        // set u64::MAX which is functionally infinite for any real call.
        store
            .set_fuel(budget.fuel.unwrap_or(u64::MAX))
            .map_err(|e| ExecError::Compile(e.to_string()))?;
        // Each tick of the epoch will be a yield-or-trap point.
        store.set_epoch_deadline(1);

        let linker: Linker<()> = Linker::new(&self.engine);
        let instance = linker
            .instantiate(&mut store, &wasm)
            .map_err(|e| ExecError::Instantiate(e.to_string()))?;

        let memory: Memory = instance
            .get_memory(&mut store, "memory")
            .ok_or(ExecError::NoMemory)?;
        let func: TypedFunc<(i32, i32), i64> = instance
            .get_typed_func(&mut store, export_name)
            .map_err(|_| ExecError::BadExport(export_name.into()))?;

        // Grow memory if needed, then write input at offset 0.
        let needed_pages = (input.len() / 65_536) as u64 + 1;
        let current_pages = memory.size(&mut store);
        if needed_pages > current_pages {
            memory
                .grow(&mut store, needed_pages - current_pages)
                .map_err(|e| ExecError::Instantiate(e.to_string()))?;
        }
        memory
            .write(&mut store, 0, input)
            .map_err(|e| ExecError::Instantiate(e.to_string()))?;

        // Spawn a watchdog thread that bumps the engine epoch when wall-time
        // expires.  This is the standard wasmtime cooperative-cancel pattern.
        let engine_clone = self.engine.clone();
        let deadline = budget.wall_time;
        let cancelled = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let cancelled_watch = Arc::clone(&cancelled);
        let watchdog = thread::spawn(move || {
            thread::sleep(deadline);
            if !cancelled_watch.load(std::sync::atomic::Ordering::Acquire) {
                engine_clone.increment_epoch();
            }
        });

        let started = Instant::now();
        let call_result = func.call(&mut store, (0, input.len() as i32));
        let elapsed = started.elapsed();
        cancelled.store(true, std::sync::atomic::Ordering::Release);
        // Best-effort join; if the watchdog already fired that's fine.
        let _ = watchdog.join();

        let packed = match call_result {
            Ok(v) => v,
            Err(e) => {
                if let Some(trap) = e.downcast_ref::<wasmtime::Trap>() {
                    match trap {
                        wasmtime::Trap::OutOfFuel => return Err(ExecError::FuelExhausted),
                        wasmtime::Trap::Interrupt => return Err(ExecError::WallTimeExceeded),
                        _ => {}
                    }
                }
                if elapsed >= deadline {
                    return Err(ExecError::WallTimeExceeded);
                }
                return Err(ExecError::Trap(e.to_string()));
            }
        };

        let out_ptr = (packed >> 32) as u32 as usize;
        let out_len = (packed & 0xffff_ffff) as u32 as usize;
        let mem_size = memory.data_size(&store);
        if out_ptr
            .checked_add(out_len)
            .map_or(true, |end| end > mem_size)
        {
            return Err(ExecError::BadOutput);
        }
        let mut out = vec![0u8; out_len];
        memory
            .read(&store, out_ptr, &mut out)
            .map_err(|e| ExecError::Trap(e.to_string()))?;
        Ok(out)
    }

    /// Like [`Executor::call`], but exposes the `ferrite_kv` host API to
    /// the module via the supplied [`HostContext`].
    ///
    /// The host context is wrapped in `Arc` and stashed in a per-call
    /// `Store<HostState>` so the wasm imports `ferrite_kv::kv_get`,
    /// `kv_set`, and `kv_del` resolve to the operator's backend.
    /// Callers should pre-wrap the context in [`crate::AclHostContext`]
    /// when ACL enforcement is required.
    pub fn call_with_host(
        &self,
        module: &Module,
        export_name: &str,
        input: &[u8],
        budget: ResourceBudget,
        ctx: Arc<dyn HostContext>,
    ) -> Result<Vec<u8>, ExecError> {
        budget.validate()?;

        let wasm = WasmModule::new(&self.engine, &module.bytes)
            .map_err(|e| ExecError::Compile(e.to_string()))?;
        let mut store: Store<HostState> = Store::new(&self.engine, HostState::new(ctx));

        store
            .set_fuel(budget.fuel.unwrap_or(u64::MAX))
            .map_err(|e| ExecError::Compile(e.to_string()))?;
        store.set_epoch_deadline(1);

        let mut linker: Linker<HostState> = Linker::new(&self.engine);
        link_host_api(&mut linker).map_err(|e| ExecError::Instantiate(e.to_string()))?;
        let instance = linker
            .instantiate(&mut store, &wasm)
            .map_err(|e| ExecError::Instantiate(e.to_string()))?;

        let memory: Memory = instance
            .get_memory(&mut store, "memory")
            .ok_or(ExecError::NoMemory)?;
        let func: TypedFunc<(i32, i32), i64> = instance
            .get_typed_func(&mut store, export_name)
            .map_err(|_| ExecError::BadExport(export_name.into()))?;

        // Need at least 2 pages: page 0 for input/output, page 1 for host scratch.
        let needed_pages = ((input.len() / 65_536) as u64 + 1).max(2);
        let current_pages = memory.size(&mut store);
        if needed_pages > current_pages {
            memory
                .grow(&mut store, needed_pages - current_pages)
                .map_err(|e| ExecError::Instantiate(e.to_string()))?;
        }
        memory
            .write(&mut store, 0, input)
            .map_err(|e| ExecError::Instantiate(e.to_string()))?;

        let engine_clone = self.engine.clone();
        let deadline = budget.wall_time;
        let cancelled = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let cancelled_watch = Arc::clone(&cancelled);
        let watchdog = thread::spawn(move || {
            thread::sleep(deadline);
            if !cancelled_watch.load(std::sync::atomic::Ordering::Acquire) {
                engine_clone.increment_epoch();
            }
        });

        let started = Instant::now();
        let call_result = func.call(&mut store, (0, input.len() as i32));
        let elapsed = started.elapsed();
        cancelled.store(true, std::sync::atomic::Ordering::Release);
        let _ = watchdog.join();

        let packed = match call_result {
            Ok(v) => v,
            Err(e) => {
                if let Some(trap) = e.downcast_ref::<wasmtime::Trap>() {
                    match trap {
                        wasmtime::Trap::OutOfFuel => return Err(ExecError::FuelExhausted),
                        wasmtime::Trap::Interrupt => return Err(ExecError::WallTimeExceeded),
                        _ => {}
                    }
                }
                if elapsed >= deadline {
                    return Err(ExecError::WallTimeExceeded);
                }
                return Err(ExecError::Trap(e.to_string()));
            }
        };

        let out_ptr = (packed >> 32) as u32 as usize;
        let out_len = (packed & 0xffff_ffff) as u32 as usize;
        let mem_size = memory.data_size(&store);
        if out_ptr
            .checked_add(out_len)
            .map_or(true, |end| end > mem_size)
        {
            return Err(ExecError::BadOutput);
        }
        let mut out = vec![0u8; out_len];
        memory
            .read(&store, out_ptr, &mut out)
            .map_err(|e| ExecError::Trap(e.to_string()))?;
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::EngineFactory;
    use crate::module::ModuleAcl;
    use std::time::Duration;

    /// Tiny WAT module that copies its input to output and returns
    /// the packed `(ptr, len)` result.  Memory page 0 is reused for both
    /// input (offset 0) and output (offset 65_000).
    fn echo_module_bytes() -> Vec<u8> {
        let wat = r#"
            (module
              (memory (export "memory") 1)
              (func (export "echo") (param $iptr i32) (param $ilen i32) (result i64)
                (local $optr i32)
                (local.set $optr (i32.const 65000))
                (memory.copy (local.get $optr) (local.get $iptr) (local.get $ilen))
                (i64.or
                  (i64.shl (i64.extend_i32_u (local.get $optr)) (i64.const 32))
                  (i64.extend_i32_u (local.get $ilen)))))
        "#;
        wat::parse_str(wat).expect("parse wat")
    }

    /// WAT module with an infinite loop, used to test fuel + wall time.
    fn looper_module_bytes() -> Vec<u8> {
        let wat = r#"
            (module
              (memory (export "memory") 1)
              (func (export "spin") (param i32) (param i32) (result i64)
                (loop $l (br $l))
                (i64.const 0)))
        "#;
        wat::parse_str(wat).expect("parse wat")
    }

    fn exec() -> Executor {
        Executor::new(EngineFactory::build().expect("engine"))
    }

    #[test]
    fn echo_module_roundtrip() {
        let m = Module::new("echo", echo_module_bytes(), ModuleAcl::default());
        let out = exec()
            .call(&m, "echo", b"hello forge", ResourceBudget::default())
            .unwrap();
        assert_eq!(out, b"hello forge");
    }

    #[test]
    fn missing_export_is_named() {
        let m = Module::new("echo", echo_module_bytes(), ModuleAcl::default());
        let err = exec()
            .call(&m, "no_such_fn", b"", ResourceBudget::default())
            .unwrap_err();
        match err {
            ExecError::BadExport(name) => assert_eq!(name, "no_such_fn"),
            other => panic!("wrong variant: {other:?}"),
        }
    }

    #[test]
    fn fuel_exhaustion_is_reported() {
        let m = Module::new("loop", looper_module_bytes(), ModuleAcl::default());
        let budget = ResourceBudget::default()
            .with_fuel(10_000)
            .with_wall_time(Duration::from_secs(10));
        let err = exec().call(&m, "spin", b"", budget).unwrap_err();
        assert!(matches!(err, ExecError::FuelExhausted), "got: {err:?}");
    }

    #[test]
    fn wall_time_exceeded_is_reported() {
        let m = Module::new("loop", looper_module_bytes(), ModuleAcl::default());
        let budget = ResourceBudget {
            fuel: None,
            memory_bytes: 64 * 1024 * 1024,
            wall_time: Duration::from_millis(50),
        };
        let err = exec().call(&m, "spin", b"", budget).unwrap_err();
        assert!(matches!(err, ExecError::WallTimeExceeded), "got: {err:?}");
    }

    #[test]
    fn invalid_budget_rejected_before_compile() {
        let m = Module::new("echo", echo_module_bytes(), ModuleAcl::default());
        let bad = ResourceBudget::default().with_memory_bytes(0);
        let err = exec().call(&m, "echo", b"", bad).unwrap_err();
        assert!(matches!(err, ExecError::Budget(_)));
    }

    /// A WAT module that imports `ferrite_kv::kv_set` and writes the
    /// input under the key `"hello"`, returning the input back as output.
    fn host_writer_module_bytes() -> Vec<u8> {
        let wat = r#"
            (module
              (import "ferrite_kv" "kv_set"
                (func $kv_set (param i32 i32 i32 i32) (result i32)))
              (memory (export "memory") 1)
              ;; "hello" lives at offset 100.
              (data (i32.const 100) "hello")
              (func (export "store_input") (param $iptr i32) (param $ilen i32) (result i64)
                (drop (call $kv_set (i32.const 100) (i32.const 5)
                                    (local.get $iptr) (local.get $ilen)))
                (i64.or
                  (i64.shl (i64.extend_i32_u (local.get $iptr)) (i64.const 32))
                  (i64.extend_i32_u (local.get $ilen)))))
        "#;
        wat::parse_str(wat).expect("parse wat")
    }

    #[test]
    fn host_kv_set_visible_to_caller() {
        let acl = crate::module::ModuleAcl {
            read_keys: vec!["*".to_string()],
            write_keys: vec!["*".to_string()],
        };
        let m = Module::new("writer", host_writer_module_bytes(), acl.clone());
        let inner = crate::host::InMemoryHostContext::new();
        let host: Arc<dyn HostContext> = Arc::new(crate::AclHostContext::new(inner.clone(), &acl));
        let out = exec()
            .call_with_host(&m, "store_input", b"world", ResourceBudget::default(), host)
            .unwrap();
        assert_eq!(out, b"world");
        let snapshot = inner.snapshot();
        assert!(
            snapshot.iter().any(|(k, v)| k == b"hello" && v == b"world"),
            "snapshot was {snapshot:?}"
        );
    }
}
