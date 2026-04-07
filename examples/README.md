# Ferrite Examples

Runnable demonstrations of Ferrite's APIs. Most examples are self-contained
embedded-mode programs; a few spin up a temporary in-process server.

| Example | What it shows | Mode | Run |
|---|---|---|---|
| [`basic_operations.rs`](basic_operations.rs) | GET/SET, INCR/DECR, hashes, lists | embedded | `cargo run --example basic_operations` |
| [`embedded_basic.rs`](embedded_basic.rs) | High-level `Ferrite` builder API for strings, counters, hashes, lists, sets | embedded | `cargo run --example embedded_basic` |
| [`embedded_iot.rs`](embedded_iot.rs) | IoT/edge sensor data caching with bounded memory | embedded | `cargo run --example embedded_iot` |
| [`embedded_edge_cache.rs`](embedded_edge_cache.rs) | Edge cache with periodic cloud sync via change-log | embedded | `cargo run --example embedded_edge_cache` |
| [`edge_sync.rs`](edge_sync.rs) | Sensor capture, time-bucket aggregation, offline sync queue | embedded | `cargo run --example edge_sync` |
| [`persistence_config.rs`](persistence_config.rs) | In-memory vs persistent, sync modes, WAL settings | embedded | `cargo run --example persistence_config` |
| [`transactions.rs`](transactions.rs) | Atomic multi-key transactions and rollback | embedded | `cargo run --example transactions` |
| [`crdt_operations.rs`](crdt_operations.rs) | GCounter/PNCounter/LwwRegister/MvRegister/OrSet merges | embedded | `cargo run --example crdt_operations` |
| [`ferriteql_demo.rs`](ferriteql_demo.rs) | FerriteQL: SELECT/WHERE/JOIN/GROUP BY over key-value data | embedded | `cargo run --example ferriteql_demo` |
| [`vector_search.rs`](vector_search.rs) | HNSW index, KNN similarity search, distance metrics | embedded | `cargo run --example vector_search` |
| [`ai_features.rs`](ai_features.rs) | Vector search + semantic caching demo | embedded | `cargo run --example ai_features` |
| [`semantic_caching_demo.rs`](semantic_caching_demo.rs) | LLM response caching via cosine similarity | embedded | `cargo run --example semantic_caching_demo` |
| [`server_mode.rs`](server_mode.rs) | Standalone Redis-compatible server config + bring-up | server | `cargo run --example server_mode` |
| [`client_connection.rs`](client_connection.rs) | RESP client connecting over TCP (like redis-cli) | server | `cargo run --example client_connection` |
| [`pubsub.rs`](pubsub.rs) | Publish/subscribe + pattern subscriptions | server | `cargo run --example pubsub` |
| [`lua_scripting.rs`](lua_scripting.rs) | EVAL/EVALSHA Lua scripts with KEYS/ARGV | server (req. `--features scripting`) | `cargo run --example lua_scripting --features scripting` |
| [`wasm_playground.rs`](wasm_playground.rs) | Interactive REPL for Ferrite commands | embedded | `cargo run --example wasm_playground` |

## Sub-example projects

| Folder | Description |
|---|---|
| [`github-actions/`](github-actions/) | Reusable workflow snippets that consume Ferrite |
| [`plugins/`](plugins/) | Native plugin example |
| [`wasm_udf/`](wasm_udf/) | WebAssembly user-defined function example |
| [`langchain_cache.py`](langchain_cache.py) | Python: Ferrite as a LangChain semantic cache |
| [`llamaindex_cache.py`](llamaindex_cache.py) | Python: Ferrite as a LlamaIndex semantic cache |

## Running everything

The `Makefile` provides shortcuts:

```bash
make test-examples      # compile-check every example (fast)
make test-examples-run  # actually run the embedded examples (no server needed)
make run-example EXAMPLE=basic_operations
```

## Adding a new example

1. Add `examples/<name>.rs` with a `//!` doc comment explaining what it shows.
2. Add a row to the table above.
3. If it can run without a server, add it to the `EMBEDDED_EXAMPLES` list in `Makefile`.
4. Reference it from the relevant feature doc in `docs/`.

See [`CONTRIBUTING.md`](../CONTRIBUTING.md#examples) for the full guidelines.
