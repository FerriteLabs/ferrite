//! CONTRACT.* command handlers
//!
//! Data contract registry commands.
//! CONTRACT.DEFINE, CONTRACT.UNDEFINE, CONTRACT.VALIDATE, CONTRACT.EVOLVE,
//! CONTRACT.INFO, CONTRACT.LIST, CONTRACT.FIND, CONTRACT.VIOLATIONS,
//! CONTRACT.COMPAT, CONTRACT.STATS, CONTRACT.HELP
#![allow(dead_code)]

use std::sync::OnceLock;

use crate::protocol::Frame;
use ferrite_core::audit::contracts::{
    ContractConfig, ContractRegistry, DataContract, DataEncoding, FieldDef, FieldType,
    SchemaDefinition, SchemaType,
};

use super::{err_frame, ok_frame};

/// Global contract registry singleton.
static CONTRACT_REGISTRY: OnceLock<ContractRegistry> = OnceLock::new();

fn get_registry() -> &'static ContractRegistry {
    CONTRACT_REGISTRY.get_or_init(|| ContractRegistry::new(ContractConfig::default()))
}

/// Dispatch a `CONTRACT` subcommand.
pub fn contract_command(subcommand: &str, args: &[String]) -> Frame {
    match subcommand.to_uppercase().as_str() {
        "DEFINE" => handle_define(args),
        "UNDEFINE" => handle_undefine(args),
        "VALIDATE" => handle_validate(args),
        "EVOLVE" => handle_evolve(args),
        "INFO" => handle_info(args),
        "LIST" => handle_list(),
        "FIND" => handle_find(args),
        "VIOLATIONS" => handle_violations(args),
        "COMPAT" => handle_compat(args),
        "STATS" => handle_stats(),
        "HELP" | "" => handle_help(),
        other => err_frame(&format!("unknown CONTRACT subcommand '{}'", other)),
    }
}

/// CONTRACT.DEFINE name key_pattern [TYPE schema_type] [MAXSIZE bytes]
/// [FIELD name type [REQUIRED]] [OWNER owner] [DESCRIPTION desc]
fn handle_define(args: &[String]) -> Frame {
    if args.len() < 2 {
        return err_frame(
            "CONTRACT.DEFINE requires: name key_pattern [TYPE type] [FIELD name type [REQUIRED]] ...",
        );
    }

    let name = args[0].clone();
    let key_pattern = args[1].clone();
    let mut schema_type = SchemaType::Any;
    let mut max_size: Option<u64> = None;
    let mut fields = Vec::new();
    let mut required_fields = Vec::new();
    let mut owner = String::new();
    let mut description = String::new();

    let mut i = 2;
    while i < args.len() {
        match args[i].to_uppercase().as_str() {
            "TYPE" if i + 1 < args.len() => {
                i += 1;
                schema_type = SchemaType::from_str_loose(&args[i]).unwrap_or(SchemaType::Any);
            }
            "MAXSIZE" if i + 1 < args.len() => {
                i += 1;
                max_size = args[i].parse().ok();
            }
            "FIELD" if i + 2 < args.len() => {
                let field_name = args[i + 1].clone();
                let ft_str = &args[i + 2];
                let field_type = match ft_str.to_uppercase().as_str() {
                    "STRING" | "STR" => FieldType::String,
                    "INTEGER" | "INT" => FieldType::Integer,
                    "FLOAT" | "DOUBLE" => FieldType::Float,
                    "BOOLEAN" | "BOOL" => FieldType::Boolean,
                    "ARRAY" => FieldType::Array,
                    "OBJECT" => FieldType::Object,
                    _ => FieldType::Any,
                };
                i += 2;

                let required = if i + 1 < args.len() && args[i + 1].to_uppercase() == "REQUIRED" {
                    i += 1;
                    required_fields.push(field_name.clone());
                    true
                } else {
                    false
                };

                fields.push(FieldDef {
                    name: field_name,
                    field_type,
                    required,
                    description: String::new(),
                    constraints: vec![],
                });
            }
            "OWNER" if i + 1 < args.len() => {
                i += 1;
                owner = args[i].clone();
            }
            "DESCRIPTION" if i + 1 < args.len() => {
                i += 1;
                description = args[i].clone();
            }
            _ => {}
        }
        i += 1;
    }

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    let contract = DataContract {
        name: name.clone(),
        key_pattern,
        schema: SchemaDefinition {
            schema_type,
            fields,
            required_fields,
            max_size_bytes: max_size,
            encoding: DataEncoding::Utf8,
        },
        version: 1,
        created_at: now,
        updated_at: now,
        description,
        owner,
        breaking_changes: vec![],
    };

    match get_registry().register(contract) {
        Ok(()) => ok_frame(),
        Err(e) => err_frame(&e.to_string()),
    }
}

/// CONTRACT.UNDEFINE name
fn handle_undefine(args: &[String]) -> Frame {
    if args.is_empty() {
        return err_frame("CONTRACT.UNDEFINE requires contract name");
    }
    match get_registry().unregister(&args[0]) {
        Ok(()) => ok_frame(),
        Err(e) => err_frame(&e.to_string()),
    }
}

/// CONTRACT.VALIDATE key value
fn handle_validate(args: &[String]) -> Frame {
    if args.len() < 2 {
        return err_frame("CONTRACT.VALIDATE requires key and value");
    }
    let key = &args[0];
    let value = args[1].as_bytes();
    let result = get_registry().validate(key, value);

    let error_frames: Vec<Frame> = result
        .errors
        .iter()
        .map(|e| Frame::bulk(format!("{}: {}", e.field, e.message)))
        .collect();
    let warning_frames: Vec<Frame> = result.warnings.iter().map(|w| Frame::bulk(w.clone())).collect();

    Frame::array(vec![
        Frame::bulk("valid"),
        Frame::bulk(if result.valid { "true" } else { "false" }),
        Frame::bulk("contract"),
        Frame::bulk(
            result
                .contract_name
                .unwrap_or_else(|| "(none)".to_string()),
        ),
        Frame::bulk("errors"),
        Frame::array(error_frames),
        Frame::bulk("warnings"),
        Frame::array(warning_frames),
    ])
}

/// CONTRACT.EVOLVE name [FIELD name type [REQUIRED]] [DESCRIPTION desc]
fn handle_evolve(args: &[String]) -> Frame {
    if args.is_empty() {
        return err_frame("CONTRACT.EVOLVE requires contract name");
    }
    let name = &args[0];
    let mut fields = Vec::new();
    let mut required_fields = Vec::new();
    let mut desc = "schema evolution".to_string();

    // Start from the existing schema if possible.
    let existing = get_registry().get_contract(name);
    let base_schema = existing
        .as_ref()
        .map(|c| c.schema.clone())
        .unwrap_or_default();

    let mut i = 1;
    while i < args.len() {
        match args[i].to_uppercase().as_str() {
            "FIELD" if i + 2 < args.len() => {
                let field_name = args[i + 1].clone();
                let ft_str = &args[i + 2];
                let field_type = match ft_str.to_uppercase().as_str() {
                    "STRING" | "STR" => FieldType::String,
                    "INTEGER" | "INT" => FieldType::Integer,
                    "FLOAT" | "DOUBLE" => FieldType::Float,
                    "BOOLEAN" | "BOOL" => FieldType::Boolean,
                    "ARRAY" => FieldType::Array,
                    "OBJECT" => FieldType::Object,
                    _ => FieldType::Any,
                };
                i += 2;
                let required = if i + 1 < args.len() && args[i + 1].to_uppercase() == "REQUIRED" {
                    i += 1;
                    required_fields.push(field_name.clone());
                    true
                } else {
                    false
                };
                fields.push(FieldDef {
                    name: field_name,
                    field_type,
                    required,
                    description: String::new(),
                    constraints: vec![],
                });
            }
            "DESCRIPTION" if i + 1 < args.len() => {
                i += 1;
                desc = args[i].clone();
            }
            _ => {}
        }
        i += 1;
    }

    let new_schema = SchemaDefinition {
        schema_type: base_schema.schema_type,
        fields: if fields.is_empty() {
            base_schema.fields
        } else {
            fields
        },
        required_fields: if required_fields.is_empty() {
            base_schema.required_fields
        } else {
            required_fields
        },
        max_size_bytes: base_schema.max_size_bytes,
        encoding: base_schema.encoding,
    };

    match get_registry().evolve(name, new_schema, &desc) {
        Ok(result) => Frame::array(vec![
            Frame::bulk("old_version"),
            Frame::Integer(result.old_version as i64),
            Frame::bulk("new_version"),
            Frame::Integer(result.new_version as i64),
            Frame::bulk("is_breaking"),
            Frame::bulk(if result.is_breaking { "true" } else { "false" }),
            Frame::bulk("changes"),
            Frame::array(result.changes.into_iter().map(Frame::bulk).collect()),
        ]),
        Err(e) => err_frame(&e.to_string()),
    }
}

/// CONTRACT.INFO name
fn handle_info(args: &[String]) -> Frame {
    if args.is_empty() {
        return err_frame("CONTRACT.INFO requires contract name");
    }
    match get_registry().get_contract(&args[0]) {
        Some(c) => Frame::array(vec![
            Frame::bulk("name"),
            Frame::bulk(c.name.clone()),
            Frame::bulk("key_pattern"),
            Frame::bulk(c.key_pattern.clone()),
            Frame::bulk("schema_type"),
            Frame::bulk(c.schema.schema_type.as_str()),
            Frame::bulk("version"),
            Frame::Integer(c.version as i64),
            Frame::bulk("fields"),
            Frame::Integer(c.schema.fields.len() as i64),
            Frame::bulk("owner"),
            Frame::bulk(c.owner.clone()),
            Frame::bulk("description"),
            Frame::bulk(c.description.clone()),
        ]),
        None => Frame::Null,
    }
}

/// CONTRACT.LIST
fn handle_list() -> Frame {
    let contracts = get_registry().list_contracts();
    let items: Vec<Frame> = contracts
        .into_iter()
        .map(|c| {
            Frame::array(vec![
                Frame::bulk(c.name.clone()),
                Frame::bulk(c.key_pattern.clone()),
                Frame::Integer(c.version as i64),
                Frame::Integer(c.fields_count as i64),
                Frame::Integer(c.violations as i64),
            ])
        })
        .collect();
    Frame::array(items)
}

/// CONTRACT.FIND key
fn handle_find(args: &[String]) -> Frame {
    if args.is_empty() {
        return err_frame("CONTRACT.FIND requires a key");
    }
    match get_registry().find_contract(&args[0]) {
        Some(c) => Frame::array(vec![
            Frame::bulk("name"),
            Frame::bulk(c.name.clone()),
            Frame::bulk("key_pattern"),
            Frame::bulk(c.key_pattern.clone()),
            Frame::bulk("version"),
            Frame::Integer(c.version as i64),
        ]),
        None => Frame::Null,
    }
}

/// CONTRACT.VIOLATIONS [LIMIT n]
fn handle_violations(args: &[String]) -> Frame {
    let limit = if args.len() >= 2 && args[0].to_uppercase() == "LIMIT" {
        args[1].parse().unwrap_or(100)
    } else {
        100
    };
    let violations = get_registry().violations(limit);
    let items: Vec<Frame> = violations
        .into_iter()
        .map(|v| {
            Frame::array(vec![
                Frame::Integer(v.timestamp as i64),
                Frame::bulk(v.key.clone()),
                Frame::bulk(v.contract.clone()),
                Frame::array(v.errors.into_iter().map(Frame::bulk).collect()),
            ])
        })
        .collect();
    Frame::array(items)
}

/// CONTRACT.COMPAT name
fn handle_compat(args: &[String]) -> Frame {
    if args.is_empty() {
        return err_frame("CONTRACT.COMPAT requires contract name");
    }
    let report = get_registry().compatibility_report(&args[0]);
    Frame::array(vec![
        Frame::bulk("contract"),
        Frame::bulk(report.contract.clone()),
        Frame::bulk("versions"),
        Frame::array(
            report
                .versions
                .into_iter()
                .map(|v| Frame::Integer(v as i64))
                .collect(),
        ),
        Frame::bulk("breaking_changes"),
        Frame::Integer(report.breaking_changes.len() as i64),
        Frame::bulk("total_validations"),
        Frame::Integer(report.total_validations as i64),
        Frame::bulk("violation_rate"),
        Frame::bulk(format!("{:.4}", report.violation_rate)),
    ])
}

/// CONTRACT.STATS
fn handle_stats() -> Frame {
    let stats = get_registry().stats();
    Frame::array(vec![
        Frame::bulk("contracts"),
        Frame::Integer(stats.contracts as i64),
        Frame::bulk("validations"),
        Frame::Integer(stats.validations as i64),
        Frame::bulk("violations"),
        Frame::Integer(stats.violations as i64),
        Frame::bulk("violation_rate"),
        Frame::bulk(format!("{:.4}", stats.violation_rate)),
    ])
}

/// CONTRACT.HELP
fn handle_help() -> Frame {
    Frame::array(vec![
        Frame::bulk("CONTRACT.DEFINE name key_pattern [TYPE type] [MAXSIZE bytes] [FIELD name type [REQUIRED]] [OWNER owner] [DESCRIPTION desc]"),
        Frame::bulk("CONTRACT.UNDEFINE name"),
        Frame::bulk("CONTRACT.VALIDATE key value"),
        Frame::bulk("CONTRACT.EVOLVE name [FIELD name type [REQUIRED]] [DESCRIPTION desc]"),
        Frame::bulk("CONTRACT.INFO name"),
        Frame::bulk("CONTRACT.LIST"),
        Frame::bulk("CONTRACT.FIND key"),
        Frame::bulk("CONTRACT.VIOLATIONS [LIMIT n]"),
        Frame::bulk("CONTRACT.COMPAT name"),
        Frame::bulk("CONTRACT.STATS"),
        Frame::bulk("CONTRACT.HELP"),
    ])
}
