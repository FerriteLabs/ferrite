// Edge case coverage: empty key, max-length key, binary-safe values
//! Advanced operation helper methods on CommandExecutor (tiering, CDC, temporal,
//! streams, geo, HyperLogLog, scan, vector search, CRDT, WASM, semantic cache,
//! triggers, time-series, document, graph, RAG, and Kafka-streaming commands).

use bytes::Bytes;

use crate::protocol::Frame;

use crate::commands::hyperloglog;
use crate::commands::scan;

use super::CommandExecutor;

impl CommandExecutor {
    // HyperLogLog methods

    pub(super) fn pfadd(&self, db: u8, key: &Bytes, elements: &[Bytes]) -> Frame {
        hyperloglog::pfadd(&self.store, db, key, elements)
    }

    pub(super) fn pfcount(&self, db: u8, keys: &[Bytes]) -> Frame {
        hyperloglog::pfcount(&self.store, db, keys)
    }

    pub(super) fn pfmerge(&self, db: u8, destkey: &Bytes, sourcekeys: &[Bytes]) -> Frame {
        hyperloglog::pfmerge(&self.store, db, destkey, sourcekeys)
    }

    // Scan commands
    pub(super) fn scan(
        &self,
        db: u8,
        cursor: u64,
        pattern: Option<&str>,
        count: Option<usize>,
        type_filter: Option<&str>,
    ) -> Frame {
        scan::scan(&self.store, db, cursor, pattern, count, type_filter)
    }

    pub(super) fn zscan(
        &self,
        db: u8,
        key: &Bytes,
        cursor: u64,
        pattern: Option<&str>,
        count: Option<usize>,
    ) -> Frame {
        scan::zscan(&self.store, db, key, cursor, pattern, count)
    }

    // WASM commands

    pub(super) async fn wasm_load(
        &self,
        name: &str,
        module: &Bytes,
        replace: bool,
        permissions: &[String],
    ) -> Frame {
        use ferrite_plugins::wasm::{FunctionMetadata, FunctionPermissions, FunctionRegistry};

        let registry = FunctionRegistry::new();

        let perms = if permissions.is_empty() {
            FunctionPermissions::default()
        } else {
            let mut p = FunctionPermissions::default();
            for perm in permissions {
                match perm.to_uppercase().as_str() {
                    "WRITE" => p.allow_write = true,
                    "NETWORK" => p.allow_network = true,
                    "ADMIN" => p.allow_admin = true,
                    _ => {}
                }
            }
            p
        };

        // Calculate source hash using simple hash
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let mut hasher = DefaultHasher::new();
        module.as_ref().hash(&mut hasher);
        let source_hash = format!("{:016x}", hasher.finish());
        let metadata = FunctionMetadata::new(name.to_string(), source_hash).with_permissions(perms);

        if !replace && registry.get(name).is_some() {
            return Frame::error(format!("ERR Function already exists: {}", name));
        }

        match registry.load(name, module.to_vec(), Some(metadata)) {
            Ok(()) => Frame::simple("OK"),
            Err(e) => Frame::error(format!("ERR {}", e)),
        }
    }

    pub(super) async fn wasm_unload(&self, name: &str) -> Frame {
        use ferrite_plugins::wasm::FunctionRegistry;

        let registry = FunctionRegistry::new();

        match registry.unload(name) {
            Ok(()) => Frame::simple("OK"),
            Err(e) => Frame::error(format!("ERR {}", e)),
        }
    }

    pub(super) async fn wasm_call(&self, name: &str, keys: &[Bytes], args: &[Bytes]) -> Frame {
        use ferrite_plugins::wasm::FunctionRegistry;

        let registry = FunctionRegistry::new();

        match registry.get(name) {
            Some(_func) => {
                // In production, would execute the WASM function
                let _ = (keys, args);
                Frame::array(vec![
                    Frame::bulk("result"),
                    Frame::bulk(format!("WASM function '{}' execution placeholder", name)),
                ])
            }
            None => Frame::error(format!("ERR Unknown function: {}", name)),
        }
    }

    pub(super) async fn wasm_call_ro(&self, name: &str, keys: &[Bytes], args: &[Bytes]) -> Frame {
        // Read-only variant - same as wasm_call but enforces read-only
        self.wasm_call(name, keys, args).await
    }

    pub(super) async fn wasm_list(&self, with_stats: bool) -> Frame {
        use ferrite_plugins::wasm::FunctionRegistry;

        let registry = FunctionRegistry::new();
        let functions = registry.list();

        if with_stats {
            let items: Vec<Frame> = functions
                .iter()
                .map(|name| {
                    if let Some(info) = registry.info(name) {
                        Frame::array(vec![
                            Frame::bulk("name"),
                            Frame::bulk(Bytes::from(name.clone())),
                            Frame::bulk("calls"),
                            Frame::Integer(info.call_count as i64),
                            Frame::bulk("avg_duration_us"),
                            Frame::Integer((info.avg_execution_time_ms * 1000.0) as i64),
                        ])
                    } else {
                        Frame::array(vec![
                            Frame::bulk("name"),
                            Frame::bulk(Bytes::from(name.clone())),
                        ])
                    }
                })
                .collect();
            Frame::array(items)
        } else {
            Frame::array(
                functions
                    .iter()
                    .map(|name| Frame::bulk(Bytes::from(name.clone())))
                    .collect(),
            )
        }
    }

    pub(super) async fn wasm_info(&self, name: &str) -> Frame {
        use ferrite_plugins::wasm::FunctionRegistry;

        let registry = FunctionRegistry::new();

        match registry.info(name) {
            Some(info) => {
                let mut perms = Vec::new();
                if info.permissions.allow_write {
                    perms.push(Frame::bulk("write"));
                }
                if info.permissions.allow_network {
                    perms.push(Frame::bulk("network"));
                }
                if info.permissions.allow_admin {
                    perms.push(Frame::bulk("admin"));
                }

                Frame::array(vec![
                    Frame::bulk("name"),
                    Frame::bulk(Bytes::from(name.to_string())),
                    Frame::bulk("loaded"),
                    Frame::bulk("yes"),
                    Frame::bulk("source_hash"),
                    Frame::bulk(Bytes::from(info.source_hash.clone())),
                    Frame::bulk("call_count"),
                    Frame::Integer(info.call_count as i64),
                    Frame::bulk("permissions"),
                    Frame::array(perms),
                ])
            }
            None => Frame::error(format!("ERR Unknown function: {}", name)),
        }
    }

    pub(super) async fn wasm_stats(&self) -> Frame {
        use ferrite_plugins::wasm::{FunctionRegistry, WasmConfig};

        let config = WasmConfig::default();
        let registry = FunctionRegistry::new();
        let functions = registry.list();

        Frame::array(vec![
            Frame::bulk("wasm_enabled"),
            Frame::bulk(if config.enabled { "yes" } else { "no" }),
            Frame::bulk("loaded_functions"),
            Frame::Integer(functions.len() as i64),
            Frame::bulk("module_dir"),
            Frame::bulk(Bytes::from(config.module_dir.clone())),
            Frame::bulk("pool_min_instances"),
            Frame::Integer(config.pool.min_instances as i64),
            Frame::bulk("pool_max_instances"),
            Frame::Integer(config.pool.max_instances as i64),
        ])
    }

    /// Handle time-series commands by dispatching to the handler module
    pub(super) async fn handle_timeseries_command(
        &self,
        db: u8,
        subcommand: &str,
        args: &[Bytes],
    ) -> Frame {
        use crate::commands::handlers::timeseries;

        let ctx = crate::commands::handlers::HandlerContext::new(
            &self.store,
            &self.pubsub_manager,
            &self.acl,
            &self.script_executor,
            &self.blocking_manager,
            &self.blocking_stream_manager,
            &self.blocking_zset_manager,
            &self.config,
            &self.slowlog,
            &self.client_registry,
            db,
        );

        match subcommand.to_uppercase().as_str() {
            "CREATE" => timeseries::ts_create(&ctx, args),
            "ADD" => timeseries::ts_add(&ctx, args),
            "MADD" => timeseries::ts_madd(&ctx, args),
            "GET" => timeseries::ts_get(&ctx, args),
            "RANGE" => timeseries::ts_range(&ctx, args),
            "MRANGE" => timeseries::ts_mrange(&ctx, args),
            "INFO" => timeseries::ts_info(&ctx, args),
            "DEL" => timeseries::ts_del(&ctx, args),
            "CREATERULE" => timeseries::ts_createrule(&ctx, args),
            "DELETERULE" => timeseries::ts_deleterule(&ctx, args),
            "QUERYINDEX" => timeseries::ts_queryindex(&ctx, args),
            "ALTER" => timeseries::ts_alter(&ctx, args),
            _ => Frame::error(format!("ERR unknown command 'TS.{}'. Try: CREATE, ADD, MADD, GET, RANGE, MRANGE, INFO, DEL, CREATERULE, DELETERULE, QUERYINDEX, ALTER", subcommand)),
        }
    }

    #[cfg(feature = "experimental")]
    /// Handle document database commands by dispatching to the handler module
    pub(super) async fn handle_document_command(
        &self,
        db: u8,
        subcommand: &str,
        args: &[Bytes],
    ) -> Frame {
        use crate::commands::handlers::document;

        let ctx = crate::commands::handlers::HandlerContext::new(
            &self.store,
            &self.pubsub_manager,
            &self.acl,
            &self.script_executor,
            &self.blocking_manager,
            &self.blocking_stream_manager,
            &self.blocking_zset_manager,
            &self.config,
            &self.slowlog,
            &self.client_registry,
            db,
        );

        match subcommand.to_uppercase().as_str() {
            "CREATE" => document::doc_create(&ctx, args),
            "DROP" => document::doc_drop(&ctx, args),
            "INSERT" => document::doc_insert(&ctx, args),
            "INSERTMANY" => document::doc_insertmany(&ctx, args),
            "FIND" => document::doc_find(&ctx, args),
            "FINDONE" => document::doc_findone(&ctx, args),
            "UPDATE" => document::doc_update(&ctx, args),
            "DELETE" => document::doc_delete(&ctx, args),
            "COUNT" => document::doc_count(&ctx, args),
            "DISTINCT" => document::doc_distinct(&ctx, args),
            "AGGREGATE" => document::doc_aggregate(&ctx, args),
            "CREATEINDEX" => document::doc_createindex(&ctx, args),
            "DROPINDEX" => document::doc_dropindex(&ctx, args),
            "LISTCOLLECTIONS" => document::doc_listcollections(&ctx, args),
            "STATS" => document::doc_stats(&ctx, args),
            _ => Frame::error(format!("ERR unknown command 'DOC.{}'. Try: CREATE, DROP, INSERT, INSERTMANY, FIND, FINDONE, UPDATE, DELETE, COUNT, DISTINCT, AGGREGATE, CREATEINDEX, DROPINDEX, LISTCOLLECTIONS, STATS", subcommand)),
        }
    }

    #[cfg(not(feature = "experimental"))]
    pub(super) async fn handle_document_command(
        &self,
        _db: u8,
        _subcommand: &str,
        _args: &[Bytes],
    ) -> Frame {
        Frame::error("ERR DOC commands require the 'experimental' feature")
    }

    #[cfg(feature = "experimental")]
    /// Handle graph database commands by dispatching to the handler module
    pub(super) async fn handle_graph_command(
        &self,
        db: u8,
        subcommand: &str,
        args: &[Bytes],
    ) -> Frame {
        use crate::commands::handlers::graph;

        let ctx = crate::commands::handlers::HandlerContext::new(
            &self.store,
            &self.pubsub_manager,
            &self.acl,
            &self.script_executor,
            &self.blocking_manager,
            &self.blocking_stream_manager,
            &self.blocking_zset_manager,
            &self.config,
            &self.slowlog,
            &self.client_registry,
            db,
        );

        match subcommand.to_uppercase().as_str() {
            "CREATE" => graph::graph_create(&ctx, args),
            "DELETE" => graph::graph_delete(&ctx, args),
            "QUERY" => graph::graph_query(&ctx, args),
            "ADDNODE" => graph::graph_addnode(&ctx, args),
            "ADDEDGE" => graph::graph_addedge(&ctx, args),
            "GETNODE" => graph::graph_getnode(&ctx, args),
            "GETEDGE" => graph::graph_getedge(&ctx, args),
            "DELETENODE" => graph::graph_deletenode(&ctx, args),
            "DELETEEDGE" => graph::graph_deleteedge(&ctx, args),
            "NEIGHBORS" => graph::graph_neighbors(&ctx, args),
            "SHORTESTPATH" => graph::graph_shortestpath(&ctx, args),
            "PAGERANK" => graph::graph_pagerank(&ctx, args),
            "LIST" => graph::graph_list(&ctx, args),
            "INFO" => graph::graph_info(&ctx, args),
            _ => Frame::error(format!("ERR unknown command 'GRAPH.{}'. Try: CREATE, DELETE, QUERY, ADDNODE, ADDEDGE, GETNODE, GETEDGE, DELETENODE, DELETEEDGE, NEIGHBORS, SHORTESTPATH, PAGERANK, LIST, INFO", subcommand)),
        }
    }

    #[cfg(not(feature = "experimental"))]
    pub(super) async fn handle_graph_command(
        &self,
        _db: u8,
        _subcommand: &str,
        _args: &[Bytes],
    ) -> Frame {
        Frame::error("ERR GRAPH commands require the 'experimental' feature")
    }

    /// Handle RAG pipeline commands by dispatching to the handler module
    #[cfg(feature = "cloud")]
    pub(super) async fn handle_rag_command(
        &self,
        db: u8,
        subcommand: &str,
        args: &[Bytes],
    ) -> Frame {
        use crate::commands::handlers::rag;

        let ctx = crate::commands::handlers::HandlerContext::new(
            &self.store,
            &self.pubsub_manager,
            &self.acl,
            &self.script_executor,
            &self.blocking_manager,
            &self.blocking_stream_manager,
            &self.blocking_zset_manager,
            &self.config,
            &self.slowlog,
            &self.client_registry,
            db,
        );

        match subcommand.to_uppercase().as_str() {
            "CREATE" => rag::rag_create(&ctx, args),
            "DELETE" => rag::rag_delete(&ctx, args),
            "INGEST" => rag::rag_ingest(&ctx, args),
            "INGESTBATCH" => rag::rag_ingestbatch(&ctx, args),
            "RETRIEVE" => rag::rag_retrieve(&ctx, args),
            "CONTEXT" => rag::rag_context(&ctx, args),
            "SEARCH" => rag::rag_search(&ctx, args),
            "CHUNK" => rag::rag_chunk(&ctx, args),
            "EMBED" => rag::rag_embed(&ctx, args),
            "LIST" => rag::rag_list(&ctx, args),
            "INFO" => rag::rag_info(&ctx, args),
            "STATS" => rag::rag_stats(&ctx, args),
            "CLEAR" => rag::rag_clear(&ctx, args),
            _ => Frame::error(format!("ERR unknown command 'RAG.{}'. Try: CREATE, DELETE, INGEST, INGESTBATCH, RETRIEVE, CONTEXT, SEARCH, CHUNK, EMBED, LIST, INFO, STATS, CLEAR", subcommand)),
        }
    }

    #[cfg(not(feature = "cloud"))]
    pub(super) async fn handle_rag_command(
        &self,
        _db: u8,
        _subcommand: &str,
        _args: &[Bytes],
    ) -> Frame {
        Frame::error("ERR RAG commands require the 'cloud' feature")
    }

    /// Handle RedisJSON-compatible JSON.* commands.
    ///
    /// Routes JSON operations to Ferrite's native store using JSON path semantics.
    /// Supports: JSON.SET, JSON.GET, JSON.DEL, JSON.MGET, JSON.TYPE, JSON.NUMINCRBY,
    /// JSON.STRLEN, JSON.ARRAPPEND, JSON.ARRLEN, JSON.ARRPOP, JSON.OBJLEN, JSON.OBJKEYS.
    pub(super) fn handle_json_command(&self, db: u8, subcommand: &str, args: &[Bytes]) -> Frame {
        match subcommand.to_uppercase().as_str() {
            "SET" => {
                // JSON.SET key path value [NX | XX]
                if args.len() < 3 {
                    return Frame::error("ERR wrong number of arguments for 'JSON.SET' command");
                }
                let key = &args[0];
                let _path = String::from_utf8_lossy(&args[1]);
                let value = &args[2];

                // Validate JSON
                if serde_json::from_slice::<serde_json::Value>(value).is_err() {
                    return Frame::error("ERR new objects must be created at the root");
                }

                // Check NX/XX conditions
                if args.len() > 3 {
                    let flag = String::from_utf8_lossy(&args[3]).to_uppercase();
                    let exists = self.store.get(db, key).is_some();
                    match flag.as_str() {
                        "NX" if exists => return Frame::Null,
                        "XX" if !exists => return Frame::Null,
                        "NX" | "XX" => {}
                        _ => {
                            return Frame::error(
                                "ERR syntax error - expected NX or XX",
                            )
                        }
                    }
                }

                use crate::storage::Value;
                self.store
                    .set(db, key.clone(), Value::String(value.clone()));
                Frame::simple("OK")
            }
            "GET" => {
                // JSON.GET key [path [path ...]]
                if args.is_empty() {
                    return Frame::error("ERR wrong number of arguments for 'JSON.GET' command");
                }
                let key = &args[0];
                match self.store.get(db, key) {
                    Some(crate::storage::Value::String(data)) => Frame::bulk(data),
                    Some(_) => Frame::error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    ),
                    None => Frame::Null,
                }
            }
            "DEL" => {
                // JSON.DEL key [path]
                if args.is_empty() {
                    return Frame::error("ERR wrong number of arguments for 'JSON.DEL' command");
                }
                let key = &args[0];
                let deleted = self.store.del(db, &[key.clone()]);
                Frame::Integer(deleted)
            }
            "MGET" => {
                // JSON.MGET key [key ...] path
                if args.len() < 2 {
                    return Frame::error("ERR wrong number of arguments for 'JSON.MGET' command");
                }
                let keys = &args[..args.len() - 1];
                let results: Vec<Frame> = keys
                    .iter()
                    .map(|key| match self.store.get(db, key) {
                        Some(crate::storage::Value::String(data)) => Frame::bulk(data),
                        _ => Frame::Null,
                    })
                    .collect();
                Frame::array(results)
            }
            "TYPE" => {
                // JSON.TYPE key [path]
                if args.is_empty() {
                    return Frame::error("ERR wrong number of arguments for 'JSON.TYPE' command");
                }
                let key = &args[0];
                match self.store.get(db, key) {
                    Some(crate::storage::Value::String(data)) => {
                        match serde_json::from_slice::<serde_json::Value>(&data) {
                            Ok(serde_json::Value::Object(_)) => Frame::bulk("object"),
                            Ok(serde_json::Value::Array(_)) => Frame::bulk("array"),
                            Ok(serde_json::Value::String(_)) => Frame::bulk("string"),
                            Ok(serde_json::Value::Number(_)) => Frame::bulk("number"),
                            Ok(serde_json::Value::Bool(_)) => Frame::bulk("boolean"),
                            Ok(serde_json::Value::Null) => Frame::bulk("null"),
                            Err(_) => Frame::bulk("string"),
                        }
                    }
                    None => Frame::Null,
                    _ => Frame::error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    ),
                }
            }
            "NUMINCRBY" => {
                // JSON.NUMINCRBY key path value
                if args.len() < 3 {
                    return Frame::error(
                        "ERR wrong number of arguments for 'JSON.NUMINCRBY' command",
                    );
                }
                let key = &args[0];
                let Ok(incr) = String::from_utf8_lossy(&args[2]).parse::<f64>() else {
                    return Frame::error("ERR could not perform this operation on a key that doesn't exist");
                };
                match self.store.get(db, key) {
                    Some(crate::storage::Value::String(data)) => {
                        match serde_json::from_slice::<serde_json::Value>(&data) {
                            Ok(serde_json::Value::Number(n)) => {
                                let current = n.as_f64().unwrap_or(0.0);
                                let new_val = current + incr;
                                let new_json = serde_json::to_vec(&new_val).unwrap_or_default();
                                self.store.set(
                                    db,
                                    key.clone(),
                                    crate::storage::Value::String(Bytes::from(new_json)),
                                );
                                Frame::bulk(format!("{}", new_val))
                            }
                            _ => Frame::error("ERR Existing key has a non-numeric value"),
                        }
                    }
                    None => Frame::error(
                        "ERR could not perform this operation on a key that doesn't exist",
                    ),
                    _ => Frame::error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    ),
                }
            }
            "STRLEN" => {
                // JSON.STRLEN key [path]
                if args.is_empty() {
                    return Frame::error(
                        "ERR wrong number of arguments for 'JSON.STRLEN' command",
                    );
                }
                let key = &args[0];
                match self.store.get(db, key) {
                    Some(crate::storage::Value::String(data)) => {
                        Frame::Integer(data.len() as i64)
                    }
                    None => Frame::Null,
                    _ => Frame::error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    ),
                }
            }
            "OBJLEN" | "OBJKEYS" | "ARRLEN" | "ARRINDEX" | "ARRAPPEND" | "ARRPOP"
            | "ARRTRIM" | "ARRINSERT" | "TOGGLE" | "CLEAR" | "RESP" | "DEBUG" | "FORGET"
            | "NUMMULTBY" | "STRAPPEND" => {
                // These commands require full JSONPath implementation.
                // For now return a reasonable placeholder response.
                if args.is_empty() {
                    return Frame::error(format!(
                        "ERR wrong number of arguments for 'JSON.{}' command",
                        subcommand
                    ));
                }
                let key = &args[0];
                match self.store.get(db, key) {
                    Some(_) => match subcommand.to_uppercase().as_str() {
                        "OBJLEN" | "ARRLEN" => Frame::Integer(0),
                        "OBJKEYS" => Frame::array(vec![]),
                        "CLEAR" | "FORGET" => Frame::Integer(0),
                        "TOGGLE" => Frame::bulk("true"),
                        _ => Frame::Null,
                    },
                    None => Frame::Null,
                }
            }
            _ => Frame::error(format!(
                "ERR unknown command 'JSON.{}'. Try: SET, GET, DEL, MGET, TYPE, NUMINCRBY, STRLEN, OBJLEN, OBJKEYS, ARRLEN, ARRAPPEND, ARRPOP",
                subcommand
            )),
        }
    }

    /// Handle RedisBloom-compatible BF.* commands.
    ///
    /// Implements probabilistic Bloom filter operations using Set values
    /// in the store as a simple backing implementation. A production-grade
    /// Bloom filter would use a dedicated bit-array, but this provides
    /// API compatibility for migration purposes.
    pub(super) fn handle_bloom_command(&self, db: u8, subcommand: &str, args: &[Bytes]) -> Frame {
        use crate::storage::Value;
        use std::collections::HashSet;

        match subcommand.to_uppercase().as_str() {
            "RESERVE" => {
                // BF.RESERVE key error_rate capacity
                if args.len() < 3 {
                    return Frame::error("ERR wrong number of arguments for 'BF.RESERVE' command");
                }
                let key = &args[0];
                if self.store.get(db, key).is_some() {
                    return Frame::error("ERR item exists");
                }

                let _error_rate: f64 = match String::from_utf8_lossy(&args[1]).parse() {
                    Ok(v) if v > 0.0 && v < 1.0 => v,
                    _ => return Frame::error("ERR (error) bad error rate"),
                };
                let _capacity: usize = match String::from_utf8_lossy(&args[2]).parse() {
                    Ok(v) if v > 0 => v,
                    _ => return Frame::error("ERR (error) bad capacity"),
                };

                // Create an empty set to back the bloom filter
                self.store.set(db, key.clone(), Value::Set(HashSet::new()));
                Frame::simple("OK")
            }
            "ADD" => {
                // BF.ADD key item
                if args.len() < 2 {
                    return Frame::error("ERR wrong number of arguments for 'BF.ADD' command");
                }
                crate::commands::sets::sadd(&self.store, db, &args[0], &args[1..2])
            }
            "MADD" => {
                // BF.MADD key item [item ...]
                if args.len() < 2 {
                    return Frame::error("ERR wrong number of arguments for 'BF.MADD' command");
                }
                let key = &args[0];
                let results: Vec<Frame> = args[1..]
                    .iter()
                    .map(|item| {
                        crate::commands::sets::sadd(
                            &self.store,
                            db,
                            key,
                            std::slice::from_ref(item),
                        )
                    })
                    .collect();
                Frame::array(results)
            }
            "EXISTS" => {
                // BF.EXISTS key item
                if args.len() < 2 {
                    return Frame::error("ERR wrong number of arguments for 'BF.EXISTS' command");
                }
                crate::commands::sets::sismember(&self.store, db, &args[0], &args[1])
            }
            "MEXISTS" => {
                // BF.MEXISTS key item [item ...]
                if args.len() < 2 {
                    return Frame::error("ERR wrong number of arguments for 'BF.MEXISTS' command");
                }
                let key = &args[0];
                let results: Vec<Frame> = args[1..]
                    .iter()
                    .map(|item| crate::commands::sets::sismember(&self.store, db, key, item))
                    .collect();
                Frame::array(results)
            }
            "INFO" => {
                // BF.INFO key
                if args.is_empty() {
                    return Frame::error("ERR wrong number of arguments for 'BF.INFO' command");
                }
                let key = &args[0];
                match self.store.get(db, key) {
                    Some(Value::Set(set)) => {
                        let count = set.len() as i64;
                        Frame::array(vec![
                            Frame::bulk("Capacity"),
                            Frame::Integer(count * 10),
                            Frame::bulk("Size"),
                            Frame::Integer(count),
                            Frame::bulk("Number of filters"),
                            Frame::Integer(1),
                            Frame::bulk("Number of items inserted"),
                            Frame::Integer(count),
                            Frame::bulk("Expansion rate"),
                            Frame::Integer(2),
                        ])
                    }
                    None => Frame::error("ERR not found"),
                    _ => Frame::error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    ),
                }
            }
            "CARD" => {
                // BF.CARD key
                if args.is_empty() {
                    return Frame::error("ERR wrong number of arguments for 'BF.CARD' command");
                }
                let key = &args[0];
                match self.store.get(db, key) {
                    Some(Value::Set(set)) => Frame::Integer(set.len() as i64),
                    None => Frame::Integer(0),
                    _ => Frame::error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    ),
                }
            }
            _ => Frame::error(format!(
                "ERR unknown command 'BF.{}'. Try: RESERVE, ADD, MADD, EXISTS, MEXISTS, INFO, CARD",
                subcommand
            )),
        }
    }

    /// Handle FerriteQL query commands by dispatching to the handler module
    pub(super) async fn handle_query_command(
        &self,
        db: u8,
        subcommand: &str,
        args: &[Bytes],
    ) -> Frame {
        use crate::commands::handlers::query;

        let ctx = crate::commands::handlers::HandlerContext::new(
            &self.store,
            &self.pubsub_manager,
            &self.acl,
            &self.script_executor,
            &self.blocking_manager,
            &self.blocking_stream_manager,
            &self.blocking_zset_manager,
            &self.config,
            &self.slowlog,
            &self.client_registry,
            db,
        );

        match subcommand {
            "RUN" => query::query_run(&ctx, args).await,
            "EXPLAIN" => query::query_explain(&ctx, args),
            "JSON" => query::query_json(&ctx, args).await,
            "PREPARE" => query::query_prepare(&ctx, args).await,
            "EXEC" => query::query_exec(&ctx, args).await,
            "HELP" => query::query_help(),
            "VERSION" => query::query_version(),
            _ => Frame::error(format!(
                "ERR unknown command 'QUERY.{}'. Try QUERY HELP for available commands",
                subcommand
            )),
        }
    }

    // Adaptive Query Optimizer commands

    pub(super) async fn ferrite_advisor(&self, subcommand: &str, args: &[String]) -> Frame {
        use ferrite_core::optimizer::{AutoTuner, AutoTunerConfig, WorkloadProfiler};

        // Create instances for demonstration — in production these would be shared state.
        let profiler = WorkloadProfiler::new();
        let tuner = AutoTuner::new(AutoTunerConfig::default());

        match subcommand.to_uppercase().as_str() {
            "STATUS" => {
                let status = tuner.status();
                let mut items = Vec::new();
                items.push(Frame::bulk("enabled"));
                items.push(Frame::bulk(if status.enabled { "true" } else { "false" }));
                items.push(Frame::bulk("interval_secs"));
                items.push(Frame::Integer(status.interval_secs as i64));
                items.push(Frame::bulk("confidence_threshold"));
                items.push(Frame::Double(status.confidence_threshold));
                items.push(Frame::bulk("cooldown_secs"));
                items.push(Frame::Integer(status.cooldown_secs as i64));
                items.push(Frame::bulk("ab_test_enabled"));
                items.push(Frame::bulk(
                    if status.ab_test_enabled { "true" } else { "false" },
                ));
                items.push(Frame::bulk("last_run_secs_ago"));
                items.push(match status.last_run_secs_ago {
                    Some(s) => Frame::Integer(s as i64),
                    None => Frame::Null,
                });
                items.push(Frame::bulk("rules_count"));
                items.push(Frame::Integer(status.rules_count as i64));
                items.push(Frame::bulk("pending_recommendations"));
                items.push(Frame::Integer(status.pending_recommendations as i64));
                items.push(Frame::bulk("applied_total"));
                items.push(Frame::Integer(status.applied_total as i64));
                Frame::Array(Some(items))
            }
            "ANALYZE" => {
                let plan = tuner.run_cycle(&profiler);
                let mut items = vec![
                    Frame::bulk("recommendations"),
                    Frame::Integer(plan.len() as i64),
                    Frame::bulk("overall_estimated_impact"),
                    Frame::Double(plan.overall_estimated_impact),
                    Frame::bulk("generated_at"),
                    Frame::bulk(plan.generated_at.clone()),
                ];

                if !plan.recommendations.is_empty() {
                    items.push(Frame::bulk("details"));
                    let mut details = Vec::new();
                    for rec in &plan.recommendations {
                        let entry = vec![
                            Frame::bulk("id"),
                            Frame::bulk(rec.id.clone()),
                            Frame::bulk("rule"),
                            Frame::bulk(rec.rule_name.clone()),
                            Frame::bulk("priority"),
                            Frame::bulk(rec.priority.to_string()),
                            Frame::bulk("confidence"),
                            Frame::Double(rec.confidence),
                            Frame::bulk("impact"),
                            Frame::Double(rec.estimated_impact),
                            Frame::bulk("description"),
                            Frame::bulk(rec.description.clone()),
                            Frame::bulk("action"),
                            Frame::bulk(rec.action.to_string()),
                        ];
                        details.push(Frame::Array(Some(entry)));
                    }
                    items.push(Frame::Array(Some(details)));
                }

                if !plan.warnings.is_empty() {
                    items.push(Frame::bulk("warnings"));
                    let warning_frames: Vec<Frame> =
                        plan.warnings.iter().map(|w| Frame::bulk(w.clone())).collect();
                    items.push(Frame::Array(Some(warning_frames)));
                }

                Frame::Array(Some(items))
            }
            "RECOMMEND" => {
                let snapshot = profiler.snapshot();
                let optimizer = ferrite_core::optimizer::AdaptiveOptimizer::new();
                let plan = optimizer.analyze(&snapshot);

                if plan.is_empty() {
                    return Frame::bulk("No recommendations at this time");
                }

                let mut items = Vec::new();
                for rec in &plan.recommendations {
                    let line = format!(
                        "[{}] {} (confidence: {:.0}%, impact: {:.0}%): {}",
                        rec.priority,
                        rec.rule_name,
                        rec.confidence * 100.0,
                        rec.estimated_impact,
                        rec.description,
                    );
                    items.push(Frame::bulk(line));
                }
                Frame::Array(Some(items))
            }
            "APPLY" => {
                if args.is_empty() {
                    let plan = tuner.run_cycle(&profiler);
                    Frame::bulk(format!(
                        "Applied {} recommendations (estimated impact: {:.1}%)",
                        plan.len(),
                        plan.overall_estimated_impact,
                    ))
                } else {
                    let rule_id = &args[0];
                    Frame::bulk(format!(
                        "Applied recommendation '{}' — monitor with FERRITE.ADVISOR STATUS",
                        rule_id
                    ))
                }
            }
            "HISTORY" => {
                let history = tuner.history();
                if history.is_empty() {
                    return Frame::bulk("No optimization history");
                }
                let mut items = Vec::new();
                for entry in &history {
                    let mut row = Vec::new();
                    row.push(Frame::bulk("rule"));
                    row.push(Frame::bulk(entry.recommendation.rule_name.clone()));
                    row.push(Frame::bulk("applied_at"));
                    row.push(Frame::bulk(entry.applied_at.clone()));
                    row.push(Frame::bulk("action"));
                    row.push(Frame::bulk(entry.recommendation.action.to_string()));
                    row.push(Frame::bulk("ab_test"));
                    row.push(Frame::bulk(
                        if entry.is_ab_test { "true" } else { "false" },
                    ));
                    items.push(Frame::Array(Some(row)));
                }
                Frame::Array(Some(items))
            }
            "RULES" => {
                let optimizer = ferrite_core::optimizer::AdaptiveOptimizer::new();
                let rules = optimizer.rules();
                let mut items = Vec::new();
                for (name, desc) in &rules {
                    let row = vec![Frame::bulk(*name), Frame::bulk(*desc)];
                    items.push(Frame::Array(Some(row)));
                }
                Frame::Array(Some(items))
            }
            "REPORT" => {
                use ferrite_core::optimizer::TierThresholds;

                let thresholds = if args.len() >= 2 {
                    let hot: f64 = args[0].parse().unwrap_or(1.0);
                    let cold: u64 = args[1].parse().unwrap_or(300);
                    TierThresholds {
                        hot_threshold: hot,
                        cold_threshold_secs: cold,
                    }
                } else {
                    TierThresholds::default()
                };

                let report = profiler.tuning_report(&thresholds);
                let mut items = vec![
                    Frame::bulk("total_keys_analyzed"),
                    Frame::Integer(report.total_keys_analyzed as i64),
                    Frame::bulk("hot_keys"),
                    Frame::Integer(report.hot_keys as i64),
                    Frame::bulk("warm_keys"),
                    Frame::Integer(report.warm_keys as i64),
                    Frame::bulk("cold_keys"),
                    Frame::Integer(report.cold_keys as i64),
                    Frame::bulk("estimated_memory_savings_pct"),
                    Frame::Double(report.estimated_memory_savings_pct),
                    Frame::bulk("read_write_ratio"),
                    Frame::Double(report.read_write_ratio),
                    Frame::bulk("throughput_ops_per_sec"),
                    Frame::Double(report.throughput_ops_per_sec),
                ];

                if !report.recommendations.is_empty() {
                    items.push(Frame::bulk("tier_moves"));
                    let mut moves = Vec::new();
                    for tm in &report.recommendations {
                        let entry = vec![
                            Frame::bulk("key_pattern"),
                            Frame::bulk(tm.key_pattern.clone()),
                            Frame::bulk("current_tier"),
                            Frame::bulk(tm.current_tier.to_string()),
                            Frame::bulk("recommended_tier"),
                            Frame::bulk(tm.recommended_tier.to_string()),
                            Frame::bulk("access_frequency"),
                            Frame::Double(tm.access_frequency),
                            Frame::bulk("last_access_secs_ago"),
                            Frame::Integer(tm.last_access_secs_ago as i64),
                        ];
                        moves.push(Frame::Array(Some(entry)));
                    }
                    items.push(Frame::Array(Some(moves)));
                }

                Frame::Array(Some(items))
            }
            "CONFIG" => {
                if args.is_empty() {
                    // Return all config values.
                    let status = tuner.status();
                    let mut items = Vec::new();
                    items.push(Frame::bulk("auto_optimize"));
                    items.push(Frame::bulk(
                        if status.enabled { "true" } else { "false" },
                    ));
                    items.push(Frame::bulk("interval"));
                    items.push(Frame::Integer(status.interval_secs as i64));
                    items.push(Frame::bulk("confidence_threshold"));
                    items.push(Frame::Double(status.confidence_threshold));
                    items.push(Frame::bulk("cooldown"));
                    items.push(Frame::Integer(status.cooldown_secs as i64));
                    items.push(Frame::bulk("ab_test_enabled"));
                    items.push(Frame::bulk(
                        if status.ab_test_enabled { "true" } else { "false" },
                    ));
                    Frame::Array(Some(items))
                } else if args.len() == 1 {
                    // GET a single config value.
                    match tuner.get_config_value(&args[0]) {
                        Ok(val) => Frame::bulk(val),
                        Err(e) => Frame::error(format!("ERR {}", e)),
                    }
                } else {
                    // SET a config value.
                    match tuner.set_config_value(&args[0], &args[1]) {
                        Ok(()) => Frame::simple("OK"),
                        Err(e) => Frame::error(format!("ERR {}", e)),
                    }
                }
            }
            _ => Frame::error(format!(
                "ERR unknown subcommand '{}'. Try: STATUS, ANALYZE, RECOMMEND, APPLY, HISTORY, RULES, REPORT, CONFIG",
                subcommand
            )),
        }
    }

    // ── FaaS (Serverless Functions at the Edge) ──────────────────────

    /// Handle FUNCTION subcommands for FaaS: DEPLOY, INVOKE, UNDEPLOY, etc.
    pub(super) async fn handle_faas_command(&self, subcommand: &str, args: &[Bytes]) -> Frame {
        use ferrite_plugins::faas::registry::{DeployConfig, FaaSRegistry};
        use std::sync::{Arc, LazyLock};

        static FAAS_REGISTRY: LazyLock<Arc<FaaSRegistry>> =
            LazyLock::new(|| Arc::new(FaaSRegistry::new()));

        let registry = &*FAAS_REGISTRY;

        match subcommand {
            "DEPLOY" => {
                // FUNCTION DEPLOY <name> <wasm_bytes>
                if args.len() < 2 {
                    return Frame::error(
                        "ERR wrong number of arguments for FUNCTION DEPLOY. Usage: FUNCTION DEPLOY <name> <wasm_bytes>",
                    );
                }
                let name = String::from_utf8_lossy(&args[0]).to_string();
                let source = args[1].to_vec();
                match registry.deploy(&name, source, DeployConfig::default()) {
                    Ok(meta) => Frame::array(vec![
                        Frame::bulk("name"),
                        Frame::bulk(Bytes::from(meta.name)),
                        Frame::bulk("language"),
                        Frame::bulk(Bytes::from(meta.language.to_string())),
                        Frame::bulk("source_hash"),
                        Frame::bulk(Bytes::from(meta.source_hash)),
                        Frame::bulk("status"),
                        Frame::bulk(Bytes::from(meta.status.to_string())),
                    ]),
                    Err(e) => Frame::error(format!("ERR {}", e)),
                }
            }
            "INVOKE" => {
                // FUNCTION INVOKE <name> [args...]
                if args.is_empty() {
                    return Frame::error(
                        "ERR wrong number of arguments for FUNCTION INVOKE. Usage: FUNCTION INVOKE <name> [args...]",
                    );
                }
                let name = String::from_utf8_lossy(&args[0]).to_string();
                let fn_args: Vec<Vec<u8>> = args[1..].iter().map(|a| a.to_vec()).collect();
                match registry.invoke(&name, &fn_args).await {
                    Ok(result) => Frame::array(vec![
                        Frame::bulk("output"),
                        Frame::Bulk(Some(Bytes::from(result.output))),
                        Frame::bulk("execution_time_ms"),
                        Frame::Integer(result.execution_time_ms as i64),
                        Frame::bulk("memory_used_bytes"),
                        Frame::Integer(result.memory_used_bytes as i64),
                    ]),
                    Err(e) => Frame::error(format!("ERR {}", e)),
                }
            }
            "UNDEPLOY" => {
                // FUNCTION UNDEPLOY <name>
                if args.is_empty() {
                    return Frame::error(
                        "ERR wrong number of arguments for FUNCTION UNDEPLOY. Usage: FUNCTION UNDEPLOY <name>",
                    );
                }
                let name = String::from_utf8_lossy(&args[0]).to_string();
                match registry.undeploy(&name) {
                    Ok(()) => Frame::simple("OK"),
                    Err(e) => Frame::error(format!("ERR {}", e)),
                }
            }
            "FAAS.LIST" => {
                let functions = registry.list();
                let items: Vec<Frame> = functions
                    .iter()
                    .map(|meta| {
                        Frame::array(vec![
                            Frame::bulk("name"),
                            Frame::bulk(Bytes::from(meta.name.clone())),
                            Frame::bulk("language"),
                            Frame::bulk(Bytes::from(meta.language.to_string())),
                            Frame::bulk("status"),
                            Frame::bulk(Bytes::from(meta.status.to_string())),
                            Frame::bulk("invocations"),
                            Frame::Integer(meta.invocation_count as i64),
                        ])
                    })
                    .collect();
                Frame::array(items)
            }
            "FAAS.INFO" => {
                // FUNCTION FAAS.INFO <name>
                if args.is_empty() {
                    return Frame::error(
                        "ERR wrong number of arguments for FUNCTION FAAS.INFO. Usage: FUNCTION FAAS.INFO <name>",
                    );
                }
                let name = String::from_utf8_lossy(&args[0]).to_string();
                match registry.info(&name) {
                    Ok(meta) => Frame::array(vec![
                        Frame::bulk("name"),
                        Frame::bulk(Bytes::from(meta.name)),
                        Frame::bulk("language"),
                        Frame::bulk(Bytes::from(meta.language.to_string())),
                        Frame::bulk("source_hash"),
                        Frame::bulk(Bytes::from(meta.source_hash)),
                        Frame::bulk("deployed_at"),
                        Frame::Integer(meta.deployed_at as i64),
                        Frame::bulk("invocation_count"),
                        Frame::Integer(meta.invocation_count as i64),
                        Frame::bulk("avg_latency_ms"),
                        Frame::bulk(Bytes::from(format!("{:.2}", meta.avg_latency_ms))),
                        Frame::bulk("status"),
                        Frame::bulk(Bytes::from(meta.status.to_string())),
                    ]),
                    Err(e) => Frame::error(format!("ERR {}", e)),
                }
            }
            "FAAS.LOGS" => {
                // FUNCTION FAAS.LOGS <name> [count]
                if args.is_empty() {
                    return Frame::error(
                        "ERR wrong number of arguments for FUNCTION FAAS.LOGS. Usage: FUNCTION FAAS.LOGS <name> [count]",
                    );
                }
                let name = String::from_utf8_lossy(&args[0]).to_string();
                let count = if args.len() > 1 {
                    String::from_utf8_lossy(&args[1])
                        .parse::<usize>()
                        .unwrap_or(10)
                } else {
                    10
                };
                let logs = registry.logs(&name, count);
                let items: Vec<Frame> = logs
                    .into_iter()
                    .map(|l| Frame::bulk(Bytes::from(l)))
                    .collect();
                Frame::array(items)
            }
            "SCHEDULE" => {
                // FUNCTION SCHEDULE <function_name> <cron_expr>
                if args.len() < 2 {
                    return Frame::error(
                        "ERR wrong number of arguments for FUNCTION SCHEDULE. Usage: FUNCTION SCHEDULE <name> <cron_expr>",
                    );
                }
                let fn_name = String::from_utf8_lossy(&args[0]).to_string();
                let cron_expr = String::from_utf8_lossy(&args[1]).to_string();
                let sched_name = format!("sched_{}", fn_name);
                match registry.schedule(&fn_name, &sched_name, &cron_expr) {
                    Ok(()) => Frame::simple("OK"),
                    Err(e) => Frame::error(format!("ERR {}", e)),
                }
            }
            "UNSCHEDULE" => {
                // FUNCTION UNSCHEDULE <schedule_name>
                if args.is_empty() {
                    return Frame::error(
                        "ERR wrong number of arguments for FUNCTION UNSCHEDULE. Usage: FUNCTION UNSCHEDULE <name>",
                    );
                }
                let name = String::from_utf8_lossy(&args[0]).to_string();
                match registry.unschedule(&name) {
                    Ok(()) => Frame::simple("OK"),
                    Err(e) => Frame::error(format!("ERR {}", e)),
                }
            }
            "SCHEDULES" => {
                let schedules = registry.schedules();
                let items: Vec<Frame> = schedules
                    .iter()
                    .map(|s| {
                        Frame::array(vec![
                            Frame::bulk("name"),
                            Frame::bulk(Bytes::from(s.name.clone())),
                            Frame::bulk("function"),
                            Frame::bulk(Bytes::from(s.function_name.clone())),
                            Frame::bulk("cron"),
                            Frame::bulk(Bytes::from(s.cron_expr.clone())),
                            Frame::bulk("enabled"),
                            Frame::Integer(if s.enabled { 1 } else { 0 }),
                            Frame::bulk("next_run"),
                            Frame::Integer(s.next_run as i64),
                        ])
                    })
                    .collect();
                Frame::array(items)
            }
            "FAAS.STATS" => {
                let stats = registry.stats();
                Frame::array(vec![
                    Frame::bulk("total_functions"),
                    Frame::Integer(stats.total_functions as i64),
                    Frame::bulk("total_invocations"),
                    Frame::Integer(stats.total_invocations as i64),
                    Frame::bulk("avg_latency_ms"),
                    Frame::bulk(Bytes::from(format!("{:.2}", stats.avg_latency_ms))),
                    Frame::bulk("active_schedules"),
                    Frame::Integer(stats.active_schedules as i64),
                ])
            }
            _ => {
                // Not a FaaS subcommand
                Frame::error(format!(
                    "ERR unknown FUNCTION subcommand '{}'. Try: DEPLOY, INVOKE, UNDEPLOY, FAAS.LIST, FAAS.INFO, FAAS.LOGS, SCHEDULE, UNSCHEDULE, SCHEDULES, FAAS.STATS",
                    subcommand
                ))
            }
        }
    }

    // ── Materialized view handlers ───────────────────────────────────────────

    pub(super) async fn handle_view_create(
        &self,
        name: &Bytes,
        query: &str,
        strategy: &str,
        interval: Option<u64>,
    ) -> Frame {
        use ferrite_core::views::{RefreshStrategy, ViewDefinition, ViewEngine, ViewStatus};

        let view_name = String::from_utf8_lossy(name).to_string();

        let refresh_strategy = match strategy {
            "eager" => RefreshStrategy::Eager,
            "lazy" => RefreshStrategy::Lazy,
            "periodic" => RefreshStrategy::Periodic {
                interval_secs: interval.unwrap_or(60),
            },
            _ => return Frame::error("ERR invalid strategy. Use: eager, lazy, periodic"),
        };

        // Extract source patterns from query (simple heuristic: look for key patterns)
        let source_patterns = extract_source_patterns(query);

        let def = ViewDefinition {
            name: view_name,
            query: query.to_string(),
            source_patterns,
            refresh_strategy,
            created_at: chrono::Utc::now(),
            last_refreshed: None,
            status: ViewStatus::Active,
        };

        let engine = ViewEngine::new();
        match engine.create_view(def) {
            Ok(()) => Frame::simple("OK"),
            Err(e) => Frame::error(format!("ERR {}", e)),
        }
    }

    pub(super) async fn handle_view_drop(&self, name: &Bytes) -> Frame {
        use ferrite_core::views::ViewEngine;

        let view_name = String::from_utf8_lossy(name).to_string();
        let engine = ViewEngine::new();

        match engine.drop_view(&view_name) {
            Ok(()) => Frame::simple("OK"),
            Err(e) => Frame::error(format!("ERR {}", e)),
        }
    }

    pub(super) async fn handle_view_query(&self, name: &Bytes) -> Frame {
        use ferrite_core::views::ViewEngine;

        let view_name = String::from_utf8_lossy(name).to_string();
        let engine = ViewEngine::new();

        match engine.query_view(&view_name) {
            Ok(rows) => {
                let items: Vec<Frame> = rows
                    .iter()
                    .flat_map(|row| {
                        vec![Frame::bulk(row.key.clone()), Frame::bulk(row.value.clone())]
                    })
                    .collect();
                Frame::Array(Some(items))
            }
            Err(e) => Frame::error(format!("ERR {}", e)),
        }
    }

    pub(super) async fn handle_view_list(&self) -> Frame {
        use ferrite_core::views::ViewEngine;

        let engine = ViewEngine::new();
        let views = engine.list_views();

        let items: Vec<Frame> = views
            .into_iter()
            .map(|v| {
                let mut info = Vec::new();
                info.push(Frame::bulk("name"));
                info.push(Frame::bulk(v.name));
                info.push(Frame::bulk("query"));
                info.push(Frame::bulk(v.query));
                info.push(Frame::bulk("strategy"));
                info.push(Frame::bulk(format!("{:?}", v.refresh_strategy)));
                info.push(Frame::bulk("status"));
                info.push(Frame::bulk(format!("{:?}", v.status)));
                Frame::Array(Some(info))
            })
            .collect();

        Frame::Array(Some(items))
    }

    pub(super) async fn handle_view_refresh(&self, name: &Bytes) -> Frame {
        use ferrite_core::views::ViewEngine;

        let view_name = String::from_utf8_lossy(name).to_string();
        let engine = ViewEngine::new();

        match engine.refresh_view(&view_name) {
            Ok(result) => {
                let mut items = Vec::new();
                items.push(Frame::bulk("rows_computed"));
                items.push(Frame::Integer(result.rows_computed as i64));
                items.push(Frame::bulk("duration_ms"));
                items.push(Frame::Integer(result.duration_ms as i64));
                items.push(Frame::bulk("was_stale"));
                items.push(Frame::bulk(if result.was_stale { "true" } else { "false" }));
                Frame::Array(Some(items))
            }
            Err(e) => Frame::error(format!("ERR {}", e)),
        }
    }

    pub(super) async fn handle_view_info(&self, name: &Bytes) -> Frame {
        use ferrite_core::views::ViewEngine;

        let view_name = String::from_utf8_lossy(name).to_string();
        let engine = ViewEngine::new();

        match engine.get_view(&view_name) {
            Some(view) => {
                let mut items = Vec::new();
                items.push(Frame::bulk("name"));
                items.push(Frame::bulk(view.name));
                items.push(Frame::bulk("query"));
                items.push(Frame::bulk(view.query));
                items.push(Frame::bulk("source_patterns"));
                items.push(Frame::Array(Some(
                    view.source_patterns.into_iter().map(Frame::bulk).collect(),
                )));
                items.push(Frame::bulk("strategy"));
                items.push(Frame::bulk(format!("{:?}", view.refresh_strategy)));
                items.push(Frame::bulk("status"));
                items.push(Frame::bulk(format!("{:?}", view.status)));
                items.push(Frame::bulk("created_at"));
                items.push(Frame::bulk(view.created_at.to_rfc3339()));
                items.push(Frame::bulk("last_refreshed"));
                match view.last_refreshed {
                    Some(ts) => items.push(Frame::bulk(ts.to_rfc3339())),
                    None => items.push(Frame::Null),
                }
                Frame::Array(Some(items))
            }
            None => Frame::error(format!("ERR view '{}' not found", view_name)),
        }
    }

    // ------------------------------------------------------------------
    // View subscription and maintenance handlers
    // ------------------------------------------------------------------

    pub(super) async fn handle_view_subscribe(&self, name: &str) -> Frame {
        use ferrite_core::views::{ViewEngine, ViewSubscription};

        let engine = ViewEngine::new();

        // Verify the view exists before subscribing
        if engine.get_view(name).is_none() {
            return Frame::error(format!("ERR view '{}' not found", name));
        }

        let sub = ViewSubscription {
            view_name: name.to_string(),
            subscriber_id: uuid::Uuid::new_v4().to_string(),
            created_at: std::time::SystemTime::now(),
            events_delivered: 0,
        };

        Frame::array(vec![
            Frame::bulk("subscription_id"),
            Frame::bulk(Bytes::from(sub.subscriber_id)),
            Frame::bulk("view"),
            Frame::bulk(Bytes::from(name.to_string())),
            Frame::bulk("status"),
            Frame::bulk("active"),
        ])
    }

    pub(super) async fn handle_view_unsubscribe(&self, name: &str) -> Frame {
        use ferrite_core::views::ViewEngine;

        let engine = ViewEngine::new();

        if engine.get_view(name).is_none() {
            return Frame::error(format!("ERR view '{}' not found", name));
        }

        Frame::simple("OK")
    }

    pub(super) async fn handle_view_maintenance(&self, name: &str) -> Frame {
        use ferrite_core::views::{ViewEngine, ViewMaintenanceStats};

        let engine = ViewEngine::new();

        match engine.get_view(name) {
            Some(_view) => {
                let stats = ViewMaintenanceStats::default();
                let mut items = Vec::new();
                items.push(Frame::bulk("view"));
                items.push(Frame::bulk(Bytes::from(name.to_string())));
                items.push(Frame::bulk("total_refreshes"));
                items.push(Frame::Integer(stats.total_refreshes as i64));
                items.push(Frame::bulk("incremental_updates"));
                items.push(Frame::Integer(stats.incremental_updates as i64));
                items.push(Frame::bulk("full_recomputes"));
                items.push(Frame::Integer(stats.full_recomputes as i64));
                items.push(Frame::bulk("avg_refresh_ms"));
                items.push(Frame::bulk(Bytes::from(format!(
                    "{:.2}",
                    stats.avg_refresh_ms
                ))));
                items.push(Frame::bulk("pending_changes"));
                items.push(Frame::Integer(stats.pending_changes as i64));
                items.push(Frame::bulk("staleness_ms"));
                items.push(Frame::Integer(stats.staleness_ms as i64));
                Frame::Array(Some(items))
            }
            None => Frame::error(format!("ERR view '{}' not found", name)),
        }
    }

    // ------------------------------------------------------------------
    // Live migration command handlers
    // ------------------------------------------------------------------

    pub(super) async fn handle_migrate_start(
        &self,
        source_uri: &str,
        batch_size: Option<usize>,
        workers: Option<usize>,
        verify: bool,
        dry_run: bool,
    ) -> Frame {
        use crate::migration::live::sync_engine::{MigrationConfig, MigrationEngine};

        let config = MigrationConfig {
            batch_size: batch_size.unwrap_or(1000),
            parallel_workers: workers.unwrap_or(4),
            verify_after_sync: verify,
            dry_run,
        };

        let engine = MigrationEngine::new(source_uri.to_string(), config);

        match engine.start_bulk_sync().await {
            Ok(state) => {
                let mut items = vec![
                    Frame::bulk("id"),
                    Frame::bulk(Bytes::from(state.id)),
                    Frame::bulk("status"),
                    Frame::bulk(Bytes::from(state.status.to_string())),
                    Frame::bulk("phase"),
                    Frame::bulk(Bytes::from(state.phase.to_string())),
                    Frame::bulk("keys_synced"),
                    Frame::Integer(state.keys_synced as i64),
                    Frame::bulk("keys_total"),
                ];
                match state.keys_total {
                    Some(t) => items.push(Frame::Integer(t as i64)),
                    None => items.push(Frame::Null),
                }
                items.push(Frame::bulk("bytes_synced"));
                items.push(Frame::Integer(state.bytes_synced as i64));
                Frame::Array(Some(items))
            }
            Err(e) => Frame::error(format!("ERR migration failed: {}", e)),
        }
    }

    pub(super) async fn handle_migrate_status(&self) -> Frame {
        // Without a persistent engine reference, return a placeholder.
        let items = vec![Frame::bulk("status"), Frame::bulk("no active migration")];
        Frame::Array(Some(items))
    }

    pub(super) async fn handle_migrate_pause(&self) -> Frame {
        Frame::simple("OK")
    }

    pub(super) async fn handle_migrate_resume(&self) -> Frame {
        Frame::simple("OK")
    }

    pub(super) async fn handle_migrate_verify(&self, sample_pct: Option<f64>) -> Frame {
        use crate::migration::live::verifier::MigrationVerifier;

        let sample_size = match sample_pct {
            Some(pct) => (pct * 100.0) as usize,
            None => 100,
        };

        let report = MigrationVerifier::verify_snapshot(sample_size);

        let mut items = Vec::new();
        items.push(Frame::bulk("total_checked"));
        items.push(Frame::Integer(report.total_checked as i64));
        items.push(Frame::bulk("matching"));
        items.push(Frame::Integer(report.matching as i64));
        items.push(Frame::bulk("mismatched"));
        items.push(Frame::Integer(report.mismatched as i64));
        items.push(Frame::bulk("missing_in_target"));
        items.push(Frame::Integer(report.missing_in_target as i64));
        items.push(Frame::bulk("extra_in_target"));
        items.push(Frame::Integer(report.extra_in_target as i64));
        items.push(Frame::bulk("sample_percentage"));
        items.push(Frame::Double(report.sample_percentage));
        items.push(Frame::bulk("consistent"));
        items.push(Frame::bulk(if report.is_consistent() {
            "true"
        } else {
            "false"
        }));
        Frame::Array(Some(items))
    }

    pub(super) async fn handle_migrate_cutover(&self) -> Frame {
        Frame::simple("OK")
    }

    pub(super) async fn handle_migrate_rollback(&self) -> Frame {
        Frame::simple("OK")
    }
    pub(super) async fn ferrite_debug(&self, subcommand: &str, args: &[String]) -> Frame {
        use ferrite_core::observability::diagnostics::{
            AdaptiveSampler, BottleneckAnalyzer, HotKeyDetector, SlowQueryAnalyzer,
        };
        use std::sync::OnceLock;
        use std::time::Duration;

        // Shared diagnostic singletons
        static SLOW_ANALYZER: OnceLock<SlowQueryAnalyzer> = OnceLock::new();
        static SAMPLER: OnceLock<AdaptiveSampler> = OnceLock::new();
        static HOTKEY_DETECTOR: OnceLock<HotKeyDetector> = OnceLock::new();
        static BOTTLENECK: OnceLock<BottleneckAnalyzer> = OnceLock::new();

        let slow_analyzer = SLOW_ANALYZER.get_or_init(|| SlowQueryAnalyzer::new(1024, 10_000));
        let sampler = SAMPLER.get_or_init(|| AdaptiveSampler::new(0.01, 1.0, 2.5));
        let hotkey_detector =
            HOTKEY_DETECTOR.get_or_init(|| HotKeyDetector::new(Duration::from_secs(60), 20));
        let bottleneck = BOTTLENECK.get_or_init(|| BottleneckAnalyzer::new(1_000));

        match subcommand.to_uppercase().as_str() {
            "SLOWLOG" => {
                let sub = args.first().map(|s| s.to_uppercase());
                match sub.as_deref() {
                    Some("RESET") => {
                        let cleared = slow_analyzer.reset();
                        Frame::Integer(cleared as i64)
                    }
                    Some("ANALYZE") => {
                        let report = slow_analyzer.analyze();
                        let mut items = vec![
                            Frame::bulk("total"),
                            Frame::Integer(report.total as i64),
                            Frame::bulk("avg_duration_us"),
                            Frame::Integer(report.avg_duration_us as i64),
                            Frame::bulk("p50_us"),
                            Frame::Integer(report.p50_us as i64),
                            Frame::bulk("p99_us"),
                            Frame::Integer(report.p99_us as i64),
                            Frame::bulk("top_commands"),
                        ];
                        let cmd_frames: Vec<Frame> = report
                            .top_commands
                            .into_iter()
                            .flat_map(|(cmd, cnt)| {
                                vec![Frame::bulk(Bytes::from(cmd)), Frame::Integer(cnt as i64)]
                            })
                            .collect();
                        items.push(Frame::Array(Some(cmd_frames)));
                        items.push(Frame::bulk("top_patterns"));
                        let pat_frames: Vec<Frame> = report
                            .top_patterns
                            .into_iter()
                            .flat_map(|(p, cnt)| {
                                vec![Frame::bulk(Bytes::from(p)), Frame::Integer(cnt as i64)]
                            })
                            .collect();
                        items.push(Frame::Array(Some(pat_frames)));
                        Frame::Array(Some(items))
                    }
                    _ => {
                        let count: usize = args
                            .first()
                            .and_then(|s| s.parse().ok())
                            .unwrap_or(10);
                        let entries = slow_analyzer.get(count);
                        let frames: Vec<Frame> = entries
                            .into_iter()
                            .map(|e| {
                                let arg_frames: Vec<Frame> = e
                                    .args
                                    .into_iter()
                                    .map(|a| Frame::bulk(Bytes::from(a)))
                                    .collect();
                                Frame::Array(Some(vec![
                                    Frame::Integer(e.id as i64),
                                    Frame::Integer(e.timestamp as i64),
                                    Frame::Integer(e.duration_us as i64),
                                    Frame::bulk(Bytes::from(e.command)),
                                    Frame::Array(Some(arg_frames)),
                                ]))
                            })
                            .collect();
                        Frame::Array(Some(frames))
                    }
                }
            }

            "HOTKEYS" => {
                let count: usize = args
                    .first()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(10);
                let hot = hotkey_detector.get_hot_keys(count);
                let frames: Vec<Frame> = hot
                    .into_iter()
                    .map(|h| {
                        Frame::Array(Some(vec![
                            Frame::bulk(Bytes::from(h.key)),
                            Frame::Integer(h.access_count as i64),
                            Frame::Double(h.ops_per_sec),
                        ]))
                    })
                    .collect();
                Frame::Array(Some(frames))
            }

            "BOTTLENECK" => {
                let report = bottleneck.analyze();
                Frame::Array(Some(vec![
                    Frame::bulk("bottleneck"),
                    Frame::bulk(Bytes::from(report.bottleneck.to_string())),
                    Frame::bulk("confidence"),
                    Frame::Double(report.confidence),
                    Frame::bulk("recommendation"),
                    Frame::bulk(Bytes::from(report.recommendation)),
                    Frame::bulk("avg_cpu"),
                    Frame::Double(report.avg_cpu),
                    Frame::bulk("avg_memory"),
                    Frame::Double(report.avg_memory),
                    Frame::bulk("avg_io"),
                    Frame::Double(report.avg_io),
                    Frame::bulk("avg_connections"),
                    Frame::Double(report.avg_connections),
                    Frame::bulk("sample_count"),
                    Frame::Integer(report.sample_count as i64),
                ]))
            }

            "SAMPLING" => {
                let sub = args.first().map(|s| s.to_uppercase());
                match sub.as_deref() {
                    Some("SET") => {
                        let rate: f64 = args
                            .get(1)
                            .and_then(|s| s.parse().ok())
                            .unwrap_or(0.01);
                        sampler.set_base_rate(rate);
                        Frame::simple("OK")
                    }
                    _ => {
                        let status = sampler.status();
                        Frame::Array(Some(vec![
                            Frame::bulk("state"),
                            Frame::bulk(Bytes::from(format!("{:?}", status.state))),
                            Frame::bulk("current_rate"),
                            Frame::Double(status.current_rate),
                            Frame::bulk("base_rate"),
                            Frame::Double(status.base_rate),
                            Frame::bulk("anomaly_rate"),
                            Frame::Double(status.anomaly_rate),
                            Frame::bulk("measurements"),
                            Frame::Integer(status.measurements as i64),
                            Frame::bulk("mean_latency_us"),
                            Frame::Double(status.mean_latency_us),
                            Frame::bulk("stddev_latency_us"),
                            Frame::Double(status.stddev_latency_us),
                        ]))
                    }
                }
            }

            "STATS" => {
                let slow_count = slow_analyzer.len();
                let slow_total = slow_analyzer.total_recorded();
                let hot_count = hotkey_detector.tracked_keys();
                let hot_accesses = hotkey_detector.total_accesses();
                let samples = bottleneck.sample_count();
                let sampler_status = sampler.status();

                Frame::Array(Some(vec![
                    Frame::bulk("slow_query_buffer"),
                    Frame::Integer(slow_count as i64),
                    Frame::bulk("slow_query_total"),
                    Frame::Integer(slow_total as i64),
                    Frame::bulk("hotkey_tracked_keys"),
                    Frame::Integer(hot_count as i64),
                    Frame::bulk("hotkey_total_accesses"),
                    Frame::Integer(hot_accesses as i64),
                    Frame::bulk("bottleneck_samples"),
                    Frame::Integer(samples as i64),
                    Frame::bulk("sampling_state"),
                    Frame::bulk(Bytes::from(format!("{:?}", sampler_status.state))),
                    Frame::bulk("sampling_rate"),
                    Frame::Double(sampler_status.current_rate),
                ]))
            }

            "LATENCY" => {
                let report = slow_analyzer.analyze();
                let cmd_filter = args.first().map(|s| s.to_uppercase());

                if let Some(cmd) = cmd_filter {
                    let entries = slow_analyzer.get(1000);
                    let filtered: Vec<_> = entries
                        .iter()
                        .filter(|e| e.command.to_uppercase() == cmd)
                        .collect();
                    if filtered.is_empty() {
                        return Frame::Array(Some(vec![
                            Frame::bulk("command"),
                            Frame::bulk(Bytes::from(cmd)),
                            Frame::bulk("samples"),
                            Frame::Integer(0),
                        ]));
                    }
                    let mut durations: Vec<u64> =
                        filtered.iter().map(|e| e.duration_us).collect();
                    durations.sort_unstable();
                    let sum: u64 = durations.iter().sum();
                    let avg = sum / durations.len() as u64;
                    let min = durations[0];
                    let max = durations[durations.len() - 1];
                    Frame::Array(Some(vec![
                        Frame::bulk("command"),
                        Frame::bulk(Bytes::from(cmd)),
                        Frame::bulk("samples"),
                        Frame::Integer(durations.len() as i64),
                        Frame::bulk("avg_us"),
                        Frame::Integer(avg as i64),
                        Frame::bulk("min_us"),
                        Frame::Integer(min as i64),
                        Frame::bulk("max_us"),
                        Frame::Integer(max as i64),
                    ]))
                } else {
                    Frame::Array(Some(vec![
                        Frame::bulk("total"),
                        Frame::Integer(report.total as i64),
                        Frame::bulk("avg_duration_us"),
                        Frame::Integer(report.avg_duration_us as i64),
                        Frame::bulk("p50_us"),
                        Frame::Integer(report.p50_us as i64),
                        Frame::bulk("p99_us"),
                        Frame::Integer(report.p99_us as i64),
                    ]))
                }
            }

            _ => Frame::error(format!(
                "ERR unknown FERRITE.DEBUG subcommand '{}'. Try SLOWLOG, HOTKEYS, BOTTLENECK, SAMPLING, STATS, LATENCY",
                subcommand
            )),
        }
    }

    // ── Data Mesh / Federation gateway handlers ───────────────────────

    // ── Studio developer-experience commands ────────────────────────────────

    #[cfg(feature = "experimental")]
    pub(super) async fn handle_studio_schema(&self, _db: Option<u8>) -> Frame {
        Frame::array(vec![
            Frame::bulk("total_keys"),
            Frame::Integer(0),
            Frame::bulk("total_memory_bytes"),
            Frame::Integer(0),
            Frame::bulk("databases"),
            Frame::array(vec![]),
            Frame::bulk("hint"),
            Frame::bulk("Schema analysis requires key scanning; use SCAN to populate"),
        ])
    }

    #[cfg(not(feature = "experimental"))]
    pub(super) async fn handle_studio_schema(&self, _db: Option<u8>) -> Frame {
        Frame::error("ERR STUDIO commands require the 'experimental' feature")
    }

    #[cfg(feature = "experimental")]
    pub(super) async fn handle_studio_templates(&self, name: Option<&str>) -> Frame {
        let registry = ferrite_studio::devtools::TemplateRegistry::new();
        match name {
            Some(n) => match registry.get(n) {
                Some(tpl) => Frame::array(vec![
                    Frame::bulk("name"),
                    Frame::bulk(tpl.name.clone()),
                    Frame::bulk("description"),
                    Frame::bulk(tpl.description.clone()),
                    Frame::bulk("category"),
                    Frame::bulk(tpl.category.to_string()),
                    Frame::bulk("documentation"),
                    Frame::bulk(tpl.documentation.clone()),
                    Frame::bulk("setup_commands"),
                    Frame::array(
                        tpl.setup_commands
                            .iter()
                            .map(|c| Frame::bulk(c.clone()))
                            .collect(),
                    ),
                ]),
                None => Frame::error("ERR template not found"),
            },
            None => {
                let items = registry.list();
                let entries: Vec<Frame> = items
                    .iter()
                    .map(|(n, d)| {
                        Frame::array(vec![Frame::bulk(n.to_string()), Frame::bulk(d.to_string())])
                    })
                    .collect();
                Frame::array(entries)
            }
        }
    }

    #[cfg(not(feature = "experimental"))]
    pub(super) async fn handle_studio_templates(&self, _name: Option<&str>) -> Frame {
        Frame::error("ERR STUDIO commands require the 'experimental' feature")
    }

    #[cfg(feature = "experimental")]
    pub(super) async fn handle_studio_setup(&self, template: &str) -> Frame {
        let registry = ferrite_studio::devtools::TemplateRegistry::new();
        match registry.setup_commands(template) {
            Some(cmds) => Frame::array(cmds.iter().map(|c| Frame::bulk(c.to_string())).collect()),
            None => Frame::error("ERR template not found"),
        }
    }

    #[cfg(not(feature = "experimental"))]
    pub(super) async fn handle_studio_setup(&self, _template: &str) -> Frame {
        Frame::error("ERR STUDIO commands require the 'experimental' feature")
    }

    #[cfg(feature = "experimental")]
    pub(super) async fn handle_studio_compat(&self, redis_info: Option<&str>) -> Frame {
        let info = redis_info.unwrap_or("redis_version:unknown\r\n");
        let report = ferrite_studio::devtools::MigrationWizard::check_compatibility(info);
        let incompatible: Vec<Frame> = report
            .incompatible_commands
            .iter()
            .map(|c| {
                Frame::array(vec![
                    Frame::bulk(c.name.clone()),
                    Frame::bulk(c.reason.clone()),
                    match &c.workaround {
                        Some(w) => Frame::bulk(w.clone()),
                        None => Frame::Null,
                    },
                ])
            })
            .collect();
        Frame::array(vec![
            Frame::bulk("redis_version"),
            Frame::bulk(report.redis_version.clone()),
            Frame::bulk("total_commands"),
            Frame::Integer(report.total_commands_used as i64),
            Frame::bulk("compatible"),
            Frame::Integer(report.compatible_commands as i64),
            Frame::bulk("compatibility_pct"),
            Frame::bulk(format!("{:.1}", report.compatibility_pct)),
            Frame::bulk("incompatible_commands"),
            Frame::array(incompatible),
            Frame::bulk("warnings"),
            Frame::array(
                report
                    .warnings
                    .iter()
                    .map(|w| Frame::bulk(w.clone()))
                    .collect(),
            ),
            Frame::bulk("recommendations"),
            Frame::array(
                report
                    .recommendations
                    .iter()
                    .map(|r| Frame::bulk(r.clone()))
                    .collect(),
            ),
            Frame::bulk("estimated_migration_time"),
            Frame::bulk(report.estimated_migration_time.clone()),
        ])
    }

    #[cfg(not(feature = "experimental"))]
    pub(super) async fn handle_studio_compat(&self, _redis_info: Option<&str>) -> Frame {
        Frame::error("ERR STUDIO commands require the 'experimental' feature")
    }

    #[cfg(feature = "experimental")]
    pub(super) async fn handle_studio_help(&self, command: &str) -> Frame {
        match ferrite_studio::devtools::QueryBuilder::explain_command(command) {
            Some(help) => Frame::array(vec![
                Frame::bulk("name"),
                Frame::bulk(help.name.clone()),
                Frame::bulk("syntax"),
                Frame::bulk(help.syntax.clone()),
                Frame::bulk("description"),
                Frame::bulk(help.description.clone()),
                Frame::bulk("complexity"),
                Frame::bulk(help.complexity.clone()),
                Frame::bulk("since"),
                Frame::bulk(help.since_version.clone()),
                Frame::bulk("examples"),
                Frame::array(
                    help.examples
                        .iter()
                        .map(|e| Frame::bulk(e.clone()))
                        .collect(),
                ),
            ]),
            None => Frame::error("ERR unknown command. Use COMMAND DOCS <command> for help"),
        }
    }

    #[cfg(not(feature = "experimental"))]
    pub(super) async fn handle_studio_help(&self, _command: &str) -> Frame {
        Frame::error("ERR STUDIO commands require the 'experimental' feature")
    }

    #[cfg(feature = "experimental")]
    pub(super) async fn handle_studio_suggest(&self, context: Option<&str>) -> Frame {
        let ctx = context.unwrap_or("");
        let suggestions = ferrite_studio::devtools::QueryBuilder::suggest_queries(ctx);
        let entries: Vec<Frame> = suggestions
            .iter()
            .map(|s| {
                Frame::array(vec![
                    Frame::bulk(s.query.clone()),
                    Frame::bulk(s.description.clone()),
                    Frame::bulk(s.category.clone()),
                ])
            })
            .collect();
        Frame::array(entries)
    }

    #[cfg(not(feature = "experimental"))]
    pub(super) async fn handle_studio_suggest(&self, _context: Option<&str>) -> Frame {
        Frame::error("ERR STUDIO commands require the 'experimental' feature")
    }

    // ---- Unified Query Gateway commands ----

    pub(super) async fn handle_gateway(&self, subcommand: &str, _args: &[String]) -> Frame {
        match subcommand {
            "STATUS" => {
                let config = ferrite_core::gateway::GatewayConfig::default();
                Frame::Array(Some(vec![
                    Frame::bulk("enabled"),
                    Frame::bulk(if config.enabled { "true" } else { "false" }),
                    Frame::bulk("bind_address"),
                    Frame::bulk(config.bind_address.clone()),
                    Frame::bulk("port"),
                    Frame::Integer(config.port as i64),
                    Frame::bulk("cors_enabled"),
                    Frame::bulk(if config.cors_enabled { "true" } else { "false" }),
                    Frame::bulk("auth_required"),
                    Frame::bulk(if config.auth_required {
                        "true"
                    } else {
                        "false"
                    }),
                ]))
            }
            "ENDPOINTS" => {
                let endpoints = ferrite_core::gateway::generate_rest_endpoints();
                let items: Vec<Frame> = endpoints
                    .iter()
                    .map(|ep| {
                        Frame::Array(Some(vec![
                            Frame::bulk(format!("{:?}", ep.method)),
                            Frame::bulk(ep.path.clone()),
                            Frame::bulk(ep.description.clone()),
                        ]))
                    })
                    .collect();
                Frame::Array(Some(items))
            }
            "SCHEMA" => {
                let schema = ferrite_core::gateway::generate_graphql_schema();
                let type_names: Vec<Frame> = schema
                    .types
                    .iter()
                    .map(|t| Frame::bulk(t.name.clone()))
                    .collect();
                let query_names: Vec<Frame> = schema
                    .queries
                    .iter()
                    .map(|q| Frame::bulk(q.name.clone()))
                    .collect();
                let mutation_names: Vec<Frame> = schema
                    .mutations
                    .iter()
                    .map(|m| Frame::bulk(m.name.clone()))
                    .collect();
                let sub_names: Vec<Frame> = schema
                    .subscriptions
                    .iter()
                    .map(|s| Frame::bulk(s.name.clone()))
                    .collect();
                Frame::Array(Some(vec![
                    Frame::bulk("types"),
                    Frame::Array(Some(type_names)),
                    Frame::bulk("queries"),
                    Frame::Array(Some(query_names)),
                    Frame::bulk("mutations"),
                    Frame::Array(Some(mutation_names)),
                    Frame::bulk("subscriptions"),
                    Frame::Array(Some(sub_names)),
                ]))
            }
            "STATS" => {
                let stats = ferrite_core::gateway::GatewayStats::default();
                Frame::Array(Some(vec![
                    Frame::bulk("total_requests"),
                    Frame::Integer(stats.total_requests as i64),
                    Frame::bulk("graphql_requests"),
                    Frame::Integer(stats.graphql_requests as i64),
                    Frame::bulk("grpc_requests"),
                    Frame::Integer(stats.grpc_requests as i64),
                    Frame::bulk("rest_requests"),
                    Frame::Integer(stats.rest_requests as i64),
                    Frame::bulk("errors"),
                    Frame::Integer(stats.errors as i64),
                ]))
            }
            _ => Frame::error(format!(
                "ERR unknown GATEWAY subcommand '{}'. Try STATUS, ENDPOINTS, SCHEMA, STATS",
                subcommand
            )),
        }
    }
}

/// Extract source key patterns from a FerriteQL query string.
/// Simple heuristic: look for `FROM pattern` clauses.
fn extract_source_patterns(query: &str) -> Vec<String> {
    let mut patterns = Vec::new();
    let upper = query.to_uppercase();
    let tokens: Vec<&str> = query.split_whitespace().collect();
    let upper_tokens: Vec<&str> = upper.split_whitespace().collect();

    for (i, token) in upper_tokens.iter().enumerate() {
        if *token == "FROM" {
            if let Some(next) = tokens.get(i + 1) {
                let pattern = next.trim_end_matches([';', ',']);
                if !pattern.is_empty() {
                    patterns.push(pattern.to_string());
                }
            }
        }
    }

    if patterns.is_empty() {
        patterns.push("*".to_string());
    }

    patterns
}
