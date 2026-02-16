//! Schema command handlers for Redis-compatible schema management
//!
//! Migration target: `ferrite-search` crate (crates/ferrite-search)
//!
//! Commands:
//! - SCHEMA.CREATE - Create a new schema
//! - SCHEMA.GET - Get schema definition
//! - SCHEMA.LIST - List all schemas
//! - SCHEMA.VERSION - Get/set current version
//! - SCHEMA.EVOLVE - Define schema evolution
//! - SCHEMA.MIGRATE - Execute migration
//! - SCHEMA.MIGRATION.STATUS - Get migration status
//! - SCHEMA.INFER - Infer schema from data
//! - SCHEMA.VALIDATE - Validate key against schema
//! - SCHEMA.DIFF - Compare schemas

use bytes::Bytes;
use std::sync::LazyLock;

use crate::protocol::Frame;
use ferrite_search::schema::{
    FieldDefault, FieldType, MigrationStateType, Schema, SchemaEvolution, SchemaInferrer,
    SchemaMigrator, SchemaRegistry, SchemaValidator,
};

use super::{err_frame, ok_frame};

// Global schema registry (lazy initialized)
static SCHEMA_REGISTRY: LazyLock<SchemaRegistry> =
    LazyLock::new(SchemaRegistry::with_defaults);

static SCHEMA_MIGRATOR: LazyLock<SchemaMigrator> =
    LazyLock::new(SchemaMigrator::with_defaults);

/// Handle SCHEMA.CREATE command
///
/// SCHEMA.CREATE name VERSION version FIELDS field1:type[:required] [field2:type[:required] ...]
/// SCHEMA.CREATE name VERSION version PATTERN key_pattern FIELDS ...
pub fn create(args: &[Bytes]) -> Frame {
    if args.len() < 4 {
        return err_frame(
            "wrong number of arguments for 'SCHEMA.CREATE' command. \
             Usage: SCHEMA.CREATE name VERSION version FIELDS field:type[:required] ...",
        );
    }

    let name = String::from_utf8_lossy(&args[0]).to_string();
    let mut version: u32 = 1;
    let mut key_pattern: Option<String> = None;
    let mut fields_start = 1;

    // Parse options
    let mut i = 1;
    while i < args.len() {
        let arg = String::from_utf8_lossy(&args[i]).to_uppercase();
        match arg.as_str() {
            "VERSION" => {
                if i + 1 >= args.len() {
                    return err_frame("VERSION requires a value");
                }
                version = match String::from_utf8_lossy(&args[i + 1]).parse() {
                    Ok(v) => v,
                    Err(_) => return err_frame("Invalid version number"),
                };
                i += 2;
            }
            "PATTERN" => {
                if i + 1 >= args.len() {
                    return err_frame("PATTERN requires a value");
                }
                key_pattern = Some(String::from_utf8_lossy(&args[i + 1]).to_string());
                i += 2;
            }
            "FIELDS" => {
                fields_start = i + 1;
                break;
            }
            _ => {
                fields_start = i;
                break;
            }
        }
    }

    // Parse fields
    let mut builder = Schema::builder(&name).version(version);

    if let Some(pattern) = key_pattern {
        builder = builder.key_pattern(&pattern);
    }

    for field_spec in &args[fields_start..] {
        let spec = String::from_utf8_lossy(field_spec);
        let parts: Vec<&str> = spec.split(':').collect();

        if parts.len() < 2 {
            return err_frame(&format!(
                "Invalid field specification '{}'. Expected: name:type[:required]",
                spec
            ));
        }

        let field_name = parts[0];
        let field_type = match FieldType::parse(parts[1]) {
            Some(t) => t,
            None => return err_frame(&format!("Unknown type '{}'", parts[1])),
        };

        let required = parts.len() > 2 && parts[2].eq_ignore_ascii_case("required");

        builder = builder.field(field_name, field_type, required);
    }

    let schema = builder.build();

    match SCHEMA_REGISTRY.register(schema) {
        Ok(()) => ok_frame(),
        Err(e) => err_frame(&e.to_string()),
    }
}

/// Handle SCHEMA.GET command
///
/// SCHEMA.GET name [VERSION version]
pub fn get(args: &[Bytes]) -> Frame {
    if args.is_empty() {
        return err_frame("wrong number of arguments for 'SCHEMA.GET' command");
    }

    let name = String::from_utf8_lossy(&args[0]).to_string();
    let version: Option<u32> = if args.len() > 2 {
        let arg = String::from_utf8_lossy(&args[1]).to_uppercase();
        if arg == "VERSION" {
            String::from_utf8_lossy(&args[2]).parse().ok()
        } else {
            None
        }
    } else {
        None
    };

    match SCHEMA_REGISTRY.get(&name, version) {
        Some(schema) => {
            let mut items = vec![
                Frame::bulk("name"),
                Frame::bulk(schema.name.clone()),
                Frame::bulk("version"),
                Frame::Integer(schema.version as i64),
                Frame::bulk("fields"),
            ];
            let fields: Vec<Frame> = schema
                .fields
                .iter()
                .map(|f| {
                    Frame::array(vec![
                        Frame::bulk(f.name.clone()),
                        Frame::bulk(f.field_type.to_type_string()),
                        Frame::bulk(if f.required { "required" } else { "optional" }),
                    ])
                })
                .collect();
            items.push(Frame::array(fields));

            if let Some(pattern) = &schema.key_pattern {
                items.push(Frame::bulk("pattern"));
                items.push(Frame::bulk(pattern.clone()));
            }

            items.push(Frame::bulk("deprecated"));
            items.push(Frame::bulk(if schema.deprecated {
                "true"
            } else {
                "false"
            }));

            Frame::array(items)
        }
        None => Frame::Null,
    }
}

/// Handle SCHEMA.LIST command
pub fn list(_args: &[Bytes]) -> Frame {
    let schemas = SCHEMA_REGISTRY.list();

    let items: Vec<Frame> = schemas
        .iter()
        .map(|s| {
            Frame::array(vec![
                Frame::bulk(s.name.clone()),
                Frame::Integer(s.version as i64),
                Frame::Integer(s.field_count as i64),
                Frame::bulk(if s.deprecated { "deprecated" } else { "active" }),
            ])
        })
        .collect();

    Frame::array(items)
}

/// Handle SCHEMA.VERSION command
///
/// SCHEMA.VERSION name [SET version]
pub fn version(args: &[Bytes]) -> Frame {
    if args.is_empty() {
        return err_frame("wrong number of arguments for 'SCHEMA.VERSION' command");
    }

    let name = String::from_utf8_lossy(&args[0]).to_string();

    if args.len() >= 3 {
        let arg = String::from_utf8_lossy(&args[1]).to_uppercase();
        if arg == "SET" {
            let new_version: u32 = match String::from_utf8_lossy(&args[2]).parse() {
                Ok(v) => v,
                Err(_) => return err_frame("Invalid version number"),
            };

            match SCHEMA_REGISTRY.set_current_version(&name, new_version) {
                Ok(()) => return ok_frame(),
                Err(e) => return err_frame(&e.to_string()),
            }
        }
    }

    match SCHEMA_REGISTRY.current_version(&name) {
        Some(v) => Frame::Integer(v as i64),
        None => Frame::Null,
    }
}

/// Handle SCHEMA.DELETE command
///
/// SCHEMA.DELETE name [VERSION version]
pub fn delete(args: &[Bytes]) -> Frame {
    if args.is_empty() {
        return err_frame("wrong number of arguments for 'SCHEMA.DELETE' command");
    }

    let name = String::from_utf8_lossy(&args[0]).to_string();

    if args.len() >= 3 {
        let arg = String::from_utf8_lossy(&args[1]).to_uppercase();
        if arg == "VERSION" {
            let version: u32 = match String::from_utf8_lossy(&args[2]).parse() {
                Ok(v) => v,
                Err(_) => return err_frame("Invalid version number"),
            };

            match SCHEMA_REGISTRY.delete_version(&name, version) {
                Ok(()) => return ok_frame(),
                Err(e) => return err_frame(&e.to_string()),
            }
        }
    }

    match SCHEMA_REGISTRY.delete(&name) {
        Ok(()) => ok_frame(),
        Err(e) => err_frame(&e.to_string()),
    }
}

/// Handle SCHEMA.EVOLVE command
///
/// SCHEMA.EVOLVE name TO version ADD field:type [RENAME old new] [REMOVE field] [DEPRECATE field]
pub fn evolve(args: &[Bytes]) -> Frame {
    if args.len() < 3 {
        return err_frame(
            "wrong number of arguments for 'SCHEMA.EVOLVE' command. \
             Usage: SCHEMA.EVOLVE name TO version [ADD field:type] [RENAME old new] ...",
        );
    }

    let name = String::from_utf8_lossy(&args[0]).to_string();

    // Get current version
    let from_version = match SCHEMA_REGISTRY.current_version(&name) {
        Some(v) => v,
        None => return err_frame(&format!("Schema '{}' not found", name)),
    };

    // Parse TO version
    let to_idx = args
        .iter()
        .position(|a| String::from_utf8_lossy(a).to_uppercase() == "TO");

    let to_version: u32 = match to_idx {
        Some(idx) if idx + 1 < args.len() => {
            match String::from_utf8_lossy(&args[idx + 1]).parse() {
                Ok(v) => v,
                Err(_) => return err_frame("Invalid target version"),
            }
        }
        _ => from_version + 1,
    };

    let mut builder = SchemaEvolution::new(&name, from_version, to_version);

    // Parse changes
    let mut i = to_idx.map(|idx| idx + 2).unwrap_or(1);
    while i < args.len() {
        let cmd = String::from_utf8_lossy(&args[i]).to_uppercase();

        match cmd.as_str() {
            "ADD" => {
                if i + 1 >= args.len() {
                    return err_frame("ADD requires field:type");
                }
                let spec = String::from_utf8_lossy(&args[i + 1]);
                let parts: Vec<&str> = spec.split(':').collect();
                if parts.len() < 2 {
                    return err_frame("ADD field must be name:type");
                }
                let field_name = parts[0];
                let field_type = match FieldType::parse(parts[1]) {
                    Some(t) => t,
                    None => return err_frame(&format!("Unknown type '{}'", parts[1])),
                };
                builder = builder.add_field(field_name, field_type, None);
                i += 2;
            }
            "RENAME" => {
                if i + 2 >= args.len() {
                    return err_frame("RENAME requires old_name new_name");
                }
                let from = String::from_utf8_lossy(&args[i + 1]).to_string();
                let to = String::from_utf8_lossy(&args[i + 2]).to_string();
                builder = builder.rename_field(&from, &to);
                i += 3;
            }
            "REMOVE" => {
                if i + 1 >= args.len() {
                    return err_frame("REMOVE requires field name");
                }
                let field_name = String::from_utf8_lossy(&args[i + 1]).to_string();
                builder = builder.remove_field(&field_name);
                i += 2;
            }
            "DEPRECATE" => {
                if i + 1 >= args.len() {
                    return err_frame("DEPRECATE requires field name");
                }
                let field_name = String::from_utf8_lossy(&args[i + 1]).to_string();
                builder = builder.deprecate_field(&field_name);
                i += 2;
            }
            "DEFAULT" => {
                if i + 2 >= args.len() {
                    return err_frame("DEFAULT requires field_name value");
                }
                let field_name = String::from_utf8_lossy(&args[i + 1]).to_string();
                let value = String::from_utf8_lossy(&args[i + 2]).to_string();
                builder = builder.set_default(&field_name, FieldDefault::String(value));
                i += 3;
            }
            _ => {
                return err_frame(&format!("Unknown evolution command: {}", cmd));
            }
        }
    }

    let evolution = builder.build();

    // Apply evolution to get new schema
    let current_schema = match SCHEMA_REGISTRY.get(&name, Some(from_version)) {
        Some(s) => s,
        None => return err_frame("Current schema version not found"),
    };

    match evolution.apply(&current_schema) {
        Ok(new_schema) => {
            // Register new version
            match SCHEMA_REGISTRY.register(new_schema) {
                Ok(()) => Frame::array(vec![
                    Frame::bulk("ok"),
                    Frame::bulk("from_version"),
                    Frame::Integer(from_version as i64),
                    Frame::bulk("to_version"),
                    Frame::Integer(to_version as i64),
                    Frame::bulk("breaking"),
                    Frame::bulk(if evolution.breaking { "true" } else { "false" }),
                ]),
                Err(e) => err_frame(&e.to_string()),
            }
        }
        Err(e) => err_frame(&e.to_string()),
    }
}

/// Handle SCHEMA.MIGRATE command
///
/// SCHEMA.MIGRATE name FROM from_version TO to_version [PATTERN key_pattern] [BATCH size] [DRY-RUN]
pub fn migrate(args: &[Bytes]) -> Frame {
    if args.is_empty() {
        return err_frame("wrong number of arguments for 'SCHEMA.MIGRATE' command");
    }

    let name = String::from_utf8_lossy(&args[0]).to_string();
    let mut from_version: Option<u32> = None;
    let mut to_version: Option<u32> = None;
    let mut pattern = format!("{}:*", name);
    let mut dry_run = false;

    // Parse options
    let mut i = 1;
    while i < args.len() {
        let arg = String::from_utf8_lossy(&args[i]).to_uppercase();
        match arg.as_str() {
            "FROM" => {
                if i + 1 < args.len() {
                    from_version = String::from_utf8_lossy(&args[i + 1]).parse().ok();
                    i += 2;
                } else {
                    i += 1;
                }
            }
            "TO" => {
                if i + 1 < args.len() {
                    to_version = String::from_utf8_lossy(&args[i + 1]).parse().ok();
                    i += 2;
                } else {
                    i += 1;
                }
            }
            "PATTERN" => {
                if i + 1 < args.len() {
                    pattern = String::from_utf8_lossy(&args[i + 1]).to_string();
                    i += 2;
                } else {
                    i += 1;
                }
            }
            "DRY-RUN" => {
                dry_run = true;
                i += 1;
            }
            _ => i += 1,
        }
    }

    let from = from_version.unwrap_or_else(|| SCHEMA_REGISTRY.current_version(&name).unwrap_or(1));
    let to = to_version.unwrap_or(from + 1);

    // Build evolution from registered versions
    let from_schema = match SCHEMA_REGISTRY.get(&name, Some(from)) {
        Some(s) => s,
        None => return err_frame(&format!("Schema version {} not found", from)),
    };

    let to_schema = match SCHEMA_REGISTRY.get(&name, Some(to)) {
        Some(s) => s,
        None => return err_frame(&format!("Schema version {} not found", to)),
    };

    // Create a simple evolution (field additions/removals)
    let mut builder = SchemaEvolution::new(&name, from, to);

    // Detect changes
    for field in &to_schema.fields {
        if !from_schema.has_field(&field.name) {
            builder =
                builder.add_field(&field.name, field.field_type.clone(), field.default.clone());
        }
    }

    for field in &from_schema.fields {
        if !to_schema.has_field(&field.name) {
            builder = builder.remove_field(&field.name);
        }
    }

    let evolution = builder.build();

    // Create migration plan
    let mut plan = SCHEMA_MIGRATOR.create_plan(&evolution, &pattern, 0);
    plan.dry_run = dry_run;

    // Start migration
    match SCHEMA_MIGRATOR.start_migration(plan) {
        Ok(migration_id) => Frame::array(vec![
            Frame::bulk("migration_id"),
            Frame::bulk(migration_id.clone()),
            Frame::bulk("status"),
            Frame::bulk("started"),
            Frame::bulk("dry_run"),
            Frame::bulk(if dry_run { "true" } else { "false" }),
        ]),
        Err(e) => err_frame(&e.to_string()),
    }
}

/// Handle SCHEMA.MIGRATION.STATUS command
pub fn migration_status(args: &[Bytes]) -> Frame {
    if args.is_empty() {
        // List all migrations
        let migrations = SCHEMA_MIGRATOR.list_migrations();
        let items: Vec<Frame> = migrations
            .iter()
            .map(|m| {
                let status_str = m.status.to_string();
                let progress_str = format!("{:.1}%", m.progress * 100.0);
                Frame::array(vec![
                    Frame::bulk(m.id.clone()),
                    Frame::bulk(m.schema_name.clone()),
                    Frame::bulk(status_str),
                    Frame::bulk(progress_str),
                ])
            })
            .collect();
        return Frame::array(items);
    }

    let migration_id = String::from_utf8_lossy(&args[0]).to_string();

    match SCHEMA_MIGRATOR.get_status(&migration_id) {
        Some(status) => {
            let status_str = status.status.to_string();
            let progress_str = format!("{:.2}%", status.progress * 100.0);
            Frame::array(vec![
                Frame::bulk("id"),
                Frame::bulk(status.id.clone()),
                Frame::bulk("schema"),
                Frame::bulk(status.schema_name.clone()),
                Frame::bulk("from_version"),
                Frame::Integer(status.from_version as i64),
                Frame::bulk("to_version"),
                Frame::Integer(status.to_version as i64),
                Frame::bulk("status"),
                Frame::bulk(status_str),
                Frame::bulk("progress"),
                Frame::bulk(progress_str),
                Frame::bulk("keys_processed"),
                Frame::Integer(status.keys_processed as i64),
                Frame::bulk("keys_migrated"),
                Frame::Integer(status.keys_migrated as i64),
                Frame::bulk("keys_failed"),
                Frame::Integer(status.keys_failed as i64),
                Frame::bulk("elapsed_secs"),
                Frame::Integer(status.elapsed_secs as i64),
            ])
        }
        None => Frame::Null,
    }
}

/// Handle SCHEMA.INFER command
///
/// SCHEMA.INFER pattern [SAMPLE size] [MIN_PRESENCE ratio]
pub fn infer(args: &[Bytes], store: &crate::storage::Store) -> Frame {
    if args.is_empty() {
        return err_frame("wrong number of arguments for 'SCHEMA.INFER' command");
    }

    let pattern = String::from_utf8_lossy(&args[0]).to_string();
    let mut sample_size = 1000usize;
    let mut min_presence = 0.1f64;

    // Parse options
    let mut i = 1;
    while i < args.len() {
        let arg = String::from_utf8_lossy(&args[i]).to_uppercase();
        match arg.as_str() {
            "SAMPLE" => {
                if i + 1 < args.len() {
                    sample_size = String::from_utf8_lossy(&args[i + 1])
                        .parse()
                        .unwrap_or(1000);
                    i += 2;
                } else {
                    i += 1;
                }
            }
            "MIN_PRESENCE" => {
                if i + 1 < args.len() {
                    min_presence = String::from_utf8_lossy(&args[i + 1]).parse().unwrap_or(0.1);
                    i += 2;
                } else {
                    i += 1;
                }
            }
            _ => i += 1,
        }
    }

    // Get sample of values matching pattern
    let mut values: Vec<serde_json::Value> = Vec::new();
    for key in store.keys(0) {
        if values.len() >= sample_size {
            break;
        }
        let key_str = match std::str::from_utf8(&key) {
            Ok(s) => s,
            Err(_) => continue,
        };
        if !key_str.contains(&pattern) {
            continue;
        }
        if let Some(crate::storage::Value::String(raw)) = store.get(0, &key) {
            if let Ok(json) = serde_json::from_slice(&raw) {
                values.push(json);
            }
        }
    }

    if values.is_empty() {
        return err_frame("No data found matching pattern");
    }

    let inferrer = SchemaInferrer::new()
        .with_sample_size(sample_size)
        .with_min_presence(min_presence);

    let inferred = inferrer.infer_from_values(&values);

    let fields: Vec<Frame> = inferred
        .fields()
        .iter()
        .map(|f| {
            let type_str = f.field_type.to_type_string();
            let presence_str = format!("{:.1}%", f.presence_ratio * 100.0);
            Frame::array(vec![
                Frame::bulk(f.name.clone()),
                Frame::bulk(type_str),
                Frame::bulk(presence_str),
                Frame::bulk(if f.likely_required {
                    "required"
                } else {
                    "optional"
                }),
            ])
        })
        .collect();

    Frame::array(vec![
        Frame::bulk("sample_count"),
        Frame::Integer(inferred.sample_count as i64),
        Frame::bulk("fields"),
        Frame::array(fields),
    ])
}

/// Handle SCHEMA.VALIDATE command
///
/// SCHEMA.VALIDATE schema_name KEY key [STRICT] [COERCE]
pub fn validate(args: &[Bytes], store: &crate::storage::Store) -> Frame {
    if args.len() < 3 {
        return err_frame("wrong number of arguments for 'SCHEMA.VALIDATE' command");
    }

    let schema_name = String::from_utf8_lossy(&args[0]).to_string();

    // Find KEY argument
    let key_idx = args
        .iter()
        .position(|a| String::from_utf8_lossy(a).to_uppercase() == "KEY");

    let key = match key_idx {
        Some(idx) if idx + 1 < args.len() => String::from_utf8_lossy(&args[idx + 1]).to_string(),
        _ => return err_frame("KEY argument required"),
    };

    let strict = args
        .iter()
        .any(|a| String::from_utf8_lossy(a).to_uppercase() == "STRICT");
    let coerce = args
        .iter()
        .any(|a| String::from_utf8_lossy(a).to_uppercase() == "COERCE");

    // Get schema
    let schema = match SCHEMA_REGISTRY.get_current(&schema_name) {
        Some(s) => s,
        None => return err_frame(&format!("Schema '{}' not found", schema_name)),
    };

    // Get value
    let value_bytes = match store.get(0, &Bytes::from(key)) {
        Some(v) => v,
        None => return err_frame("Key not found"),
    };

    let value: serde_json::Value = match value_bytes {
        crate::storage::Value::String(raw) => match serde_json::from_slice(&raw) {
            Ok(v) => v,
            Err(e) => return err_frame(&format!("Invalid JSON: {}", e)),
        },
        _ => return err_frame("Key is not a string value"),
    };

    // Build validator
    let mut validator = SchemaValidator::new();
    if strict {
        validator = validator.strict();
    }
    if coerce {
        validator = validator.with_coercion();
    }

    // Validate
    let result = if coerce {
        validator.validate_and_coerce(&value, &schema)
    } else {
        validator.validate(&value, &schema)
    };

    if result.valid {
        Frame::array(vec![Frame::bulk("valid"), Frame::bulk("true")])
    } else {
        let errors: Vec<Frame> = result
            .errors
            .iter()
            .map(|e| {
                let err_str = e.to_string();
                Frame::bulk(err_str)
            })
            .collect();

        Frame::array(vec![
            Frame::bulk("valid"),
            Frame::bulk("false"),
            Frame::bulk("errors"),
            Frame::array(errors),
        ])
    }
}

/// Handle SCHEMA.DIFF command
///
/// SCHEMA.DIFF schema_name [VERSION v1] [TO v2]
pub fn diff(args: &[Bytes]) -> Frame {
    if args.is_empty() {
        return err_frame("wrong number of arguments for 'SCHEMA.DIFF' command");
    }

    let name = String::from_utf8_lossy(&args[0]).to_string();
    let mut v1: Option<u32> = None;
    let mut v2: Option<u32> = None;

    // Parse options
    let mut i = 1;
    while i < args.len() {
        let arg = String::from_utf8_lossy(&args[i]).to_uppercase();
        match arg.as_str() {
            "VERSION" => {
                if i + 1 < args.len() {
                    v1 = String::from_utf8_lossy(&args[i + 1]).parse().ok();
                    i += 2;
                } else {
                    i += 1;
                }
            }
            "TO" => {
                if i + 1 < args.len() {
                    v2 = String::from_utf8_lossy(&args[i + 1]).parse().ok();
                    i += 2;
                } else {
                    i += 1;
                }
            }
            _ => i += 1,
        }
    }

    let current = SCHEMA_REGISTRY.current_version(&name).unwrap_or(1);
    let from_v = v1.unwrap_or(current.saturating_sub(1).max(1));
    let to_v = v2.unwrap_or(current);

    let from_schema = match SCHEMA_REGISTRY.get(&name, Some(from_v)) {
        Some(s) => s,
        None => return err_frame(&format!("Version {} not found", from_v)),
    };

    let to_schema = match SCHEMA_REGISTRY.get(&name, Some(to_v)) {
        Some(s) => s,
        None => return err_frame(&format!("Version {} not found", to_v)),
    };

    // Calculate differences
    let mut added: Vec<String> = Vec::new();
    let mut removed: Vec<String> = Vec::new();
    let mut changed: Vec<String> = Vec::new();

    for field in &to_schema.fields {
        match from_schema.field(&field.name) {
            None => added.push(field.name.clone()),
            Some(old_field) => {
                if old_field.field_type != field.field_type {
                    changed.push(format!(
                        "{}: {} -> {}",
                        field.name, old_field.field_type, field.field_type
                    ));
                }
                if old_field.required != field.required {
                    changed.push(format!(
                        "{}: {} -> {}",
                        field.name,
                        if old_field.required {
                            "required"
                        } else {
                            "optional"
                        },
                        if field.required {
                            "required"
                        } else {
                            "optional"
                        }
                    ));
                }
            }
        }
    }

    for field in &from_schema.fields {
        if !to_schema.has_field(&field.name) {
            removed.push(field.name.clone());
        }
    }

    Frame::array(vec![
        Frame::bulk("from_version"),
        Frame::Integer(from_v as i64),
        Frame::bulk("to_version"),
        Frame::Integer(to_v as i64),
        Frame::bulk("added"),
        Frame::array(
            added
                .iter()
                .map(|s| Frame::bulk(s.clone()))
                .collect::<Vec<Frame>>(),
        ),
        Frame::bulk("removed"),
        Frame::array(
            removed
                .iter()
                .map(|s| Frame::bulk(s.clone()))
                .collect::<Vec<Frame>>(),
        ),
        Frame::bulk("changed"),
        Frame::array(
            changed
                .iter()
                .map(|s| Frame::bulk(s.clone()))
                .collect::<Vec<Frame>>(),
        ),
    ])
}

/// Handle SCHEMA.INFO command
pub fn info(_args: &[Bytes]) -> Frame {
    let migrations = SCHEMA_MIGRATOR.list_migrations();
    let active_migrations = migrations
        .iter()
        .filter(|m| m.status == MigrationStateType::Running)
        .count();

    Frame::array(vec![
        Frame::bulk("enabled"),
        Frame::bulk("true"),
        Frame::bulk("schema_count"),
        Frame::Integer(SCHEMA_REGISTRY.count() as i64),
        Frame::bulk("version_count"),
        Frame::Integer(SCHEMA_REGISTRY.version_count() as i64),
        Frame::bulk("active_migrations"),
        Frame::Integer(active_migrations as i64),
    ])
}
