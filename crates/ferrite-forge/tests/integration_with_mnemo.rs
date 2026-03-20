//! Integration test: a Forge module computes a "summary" string from input
//! and writes the result into Mnemo via a host-side adapter.  This proves
//! the trait surface and the wasm host bindings cooperate end-to-end.
//!
//! The wasm module here does NOT call into the host KV API — instead the
//! Rust caller invokes the module to transform the input and then writes
//! the result into Mnemo.  This mirrors the "phase 1" wiring where Forge
//! is invoked by command handlers that themselves talk to the storage layer
//! (rather than letting modules write to storage directly, which is a P2
//! capability).

#![cfg(feature = "runtime")]

use ferrite_forge::{engine::EngineFactory, module::ModuleAcl, Executor, Module, ResourceBudget};
use ferrite_mnemo::{InMemoryMnemoStore, MemoryKind, MemoryRecordBuilder, RecallFilter, Scope};

fn uppercase_module() -> Vec<u8> {
    // Reads input bytes, ASCII-uppercases them in place, and returns
    // the packed (ptr, len) pointing at the same buffer.
    let wat = r#"
        (module
          (memory (export "memory") 1)
          (func (export "upcase") (param $iptr i32) (param $ilen i32) (result i64)
            (local $i i32)
            (local $b i32)
            (local.set $i (i32.const 0))
            (block $done
              (loop $l
                (br_if $done (i32.ge_s (local.get $i) (local.get $ilen)))
                (local.set $b
                  (i32.load8_u (i32.add (local.get $iptr) (local.get $i))))
                (if (i32.and
                      (i32.ge_u (local.get $b) (i32.const 97))
                      (i32.le_u (local.get $b) (i32.const 122)))
                  (then
                    (i32.store8
                      (i32.add (local.get $iptr) (local.get $i))
                      (i32.sub (local.get $b) (i32.const 32)))))
                (local.set $i (i32.add (local.get $i) (i32.const 1)))
                (br $l)))
            (i64.or
              (i64.shl (i64.extend_i32_u (local.get $iptr)) (i64.const 32))
              (i64.extend_i32_u (local.get $ilen)))))
    "#;
    wat::parse_str(wat).expect("parse wat")
}

#[test]
fn forge_transforms_input_then_mnemo_persists_it() {
    // 1. Forge: compile + run a module that uppercases input.
    let exec = Executor::new(EngineFactory::build().expect("engine"));
    let module = Module::new("upcase", uppercase_module(), ModuleAcl::default());
    let summarized = exec
        .call(
            &module,
            "upcase",
            b"the user prefers tabs",
            ResourceBudget::default(),
        )
        .expect("forge call");
    assert_eq!(summarized, b"THE USER PREFERS TABS");

    // 2. Mnemo: persist the Forge-produced string as an episodic memory.
    let store = InMemoryMnemoStore::new();
    let scope = Scope::new("acme", "agent-1");
    let record = MemoryRecordBuilder::new()
        .id("rec-from-forge")
        .tenant("acme")
        .agent("agent-1")
        .kind(MemoryKind::Episodic)
        .content(String::from_utf8(summarized).expect("utf8"))
        .importance(0.8)
        .created_at(1)
        .build()
        .expect("build");
    store.put(&scope, record).expect("mnemo put");

    // 3. Recall: confirm Mnemo round-trips the Forge output.
    let result = store.recall(
        &scope,
        100,
        &RecallFilter {
            limit: 10,
            ..Default::default()
        },
    );
    assert_eq!(result.records.len(), 1);
    assert_eq!(result.records[0].content, "THE USER PREFERS TABS");
    assert_eq!(result.records[0].kind, MemoryKind::Episodic);
}

#[test]
fn forge_module_metadata_is_stable_for_replication() {
    // Two embedders compiling the same wasm bytes must derive the same
    // sha256 — that's how replicas decide they don't need to refetch a
    // module they already have.
    let m1 = Module::new("m", uppercase_module(), ModuleAcl::default());
    let m2 = Module::new("m", uppercase_module(), ModuleAcl::default());
    assert_eq!(m1.meta.sha256, m2.meta.sha256);
    assert_eq!(m1.meta.size_bytes, m2.meta.size_bytes);
}
