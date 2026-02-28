//! AUDIT.* command handlers
//!
//! AUDIT.LOG, AUDIT.VERIFY, AUDIT.EXPORT, AUDIT.RETENTION, AUDIT.GDPR.*,
//! AUDIT.REPORT, AUDIT.STATS, AUDIT.HELP
#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::OnceLock;
use std::time::Duration;

use bytes::Bytes;

use crate::protocol::Frame;
use ferrite_core::audit::compliance::{
    AuditConfig, AuditEventType, AuditFilter, AuditResult, ComplianceAuditEntry,
    ComplianceAuditLog, ComplianceReport, ExportFormat, GdprHandler, RetentionPolicy,
};

use super::err_frame;

/// Global audit log singleton.
static AUDIT_LOG: OnceLock<ComplianceAuditLog> = OnceLock::new();

/// Global retention policy singleton.
static RETENTION_POLICY: OnceLock<RetentionPolicy> = OnceLock::new();

/// Global GDPR handler singleton.
static GDPR_HANDLER: OnceLock<GdprHandler> = OnceLock::new();

fn get_audit_log() -> &'static ComplianceAuditLog {
    AUDIT_LOG.get_or_init(|| ComplianceAuditLog::new(AuditConfig::default()))
}

fn get_retention_policy() -> &'static RetentionPolicy {
    RETENTION_POLICY.get_or_init(|| RetentionPolicy::new(Duration::from_secs(86400 * 90)))
}

fn get_gdpr_handler() -> &'static GdprHandler {
    GDPR_HANDLER.get_or_init(GdprHandler::new)
}

/// Dispatch an `AUDIT` subcommand.
pub fn audit_command(subcommand: &str, args: &[String]) -> Frame {
    match subcommand.to_uppercase().as_str() {
        "LOG" => audit_log(args),
        "VERIFY" => audit_verify(),
        "EXPORT" => audit_export(args),
        "RETENTION" => audit_retention(args),
        "GDPR.DELETE" => audit_gdpr_delete(args),
        "GDPR.EXPORT" => audit_gdpr_export(args),
        "REPORT" => audit_report(args),
        "STATS" => audit_stats(),
        "HELP" => audit_help(),
        _ => err_frame(&format!("unknown AUDIT subcommand '{}'", subcommand)),
    }
}

/// AUDIT.LOG [LIMIT n] [USER user] [TYPE type] [KEY pattern] [SINCE ts] [UNTIL ts]
fn audit_log(args: &[String]) -> Frame {
    let mut filter = AuditFilter {
        limit: 100,
        ..Default::default()
    };

    let mut i = 0;
    while i < args.len() {
        match args[i].to_uppercase().as_str() {
            "LIMIT" => {
                if i + 1 < args.len() {
                    filter.limit = args[i + 1].parse().unwrap_or(100);
                    i += 2;
                } else {
                    i += 1;
                }
            }
            "USER" => {
                if i + 1 < args.len() {
                    filter.user = Some(args[i + 1].clone());
                    i += 2;
                } else {
                    i += 1;
                }
            }
            "TYPE" => {
                if i + 1 < args.len() {
                    filter.event_type = AuditEventType::from_str_loose(&args[i + 1]);
                    i += 2;
                } else {
                    i += 1;
                }
            }
            "KEY" => {
                if i + 1 < args.len() {
                    filter.key_pattern = Some(args[i + 1].clone());
                    i += 2;
                } else {
                    i += 1;
                }
            }
            "SINCE" => {
                if i + 1 < args.len() {
                    filter.start_time = args[i + 1].parse().ok();
                    i += 2;
                } else {
                    i += 1;
                }
            }
            "UNTIL" => {
                if i + 1 < args.len() {
                    filter.end_time = args[i + 1].parse().ok();
                    i += 2;
                } else {
                    i += 1;
                }
            }
            _ => i += 1,
        }
    }

    let log = get_audit_log();
    let entries = log.query(filter);
    let frames: Vec<Frame> = entries.into_iter().map(audit_entry_to_frame).collect();
    Frame::Array(Some(frames))
}

/// AUDIT.VERIFY
fn audit_verify() -> Frame {
    let log = get_audit_log();
    let result = log.verify_integrity();

    let mut map = HashMap::new();
    map.insert(Bytes::from("valid"), Frame::Boolean(result.valid));
    map.insert(
        Bytes::from("entries_checked"),
        Frame::Integer(result.entries_checked as i64),
    );
    map.insert(
        Bytes::from("chain_start"),
        Frame::Integer(result.chain_start as i64),
    );
    map.insert(
        Bytes::from("chain_end"),
        Frame::Integer(result.chain_end as i64),
    );
    if let Some(invalid) = result.first_invalid {
        map.insert(Bytes::from("first_invalid"), Frame::Integer(invalid as i64));
    }

    Frame::Map(map)
}

/// AUDIT.EXPORT [JSON|CSV]
fn audit_export(args: &[String]) -> Frame {
    let format = if args.is_empty() {
        ExportFormat::Json
    } else {
        ExportFormat::from_str_loose(&args[0]).unwrap_or(ExportFormat::Json)
    };

    let log = get_audit_log();
    match log.export(format) {
        Ok(data) => Frame::Bulk(Some(Bytes::from(data))),
        Err(e) => err_frame(&e.to_string()),
    }
}

/// AUDIT.RETENTION GET pattern | AUDIT.RETENTION SET pattern ttl_days
fn audit_retention(args: &[String]) -> Frame {
    if args.is_empty() {
        return err_frame("AUDIT.RETENTION requires GET or SET subcommand");
    }

    let policy = get_retention_policy();

    match args[0].to_uppercase().as_str() {
        "GET" => {
            if args.len() < 2 {
                return err_frame("AUDIT.RETENTION GET requires a pattern");
            }
            let ttl = policy.get_ttl(&args[1]);
            Frame::Integer(ttl.as_secs() as i64 / 86400)
        }
        "SET" => {
            if args.len() < 3 {
                return err_frame("AUDIT.RETENTION SET requires: pattern ttl_days");
            }
            let days: u64 = match args[2].parse() {
                Ok(v) => v,
                Err(_) => return err_frame("ttl_days must be an integer"),
            };
            policy.set_key_pattern_ttl(&args[1], Duration::from_secs(days * 86400));
            Frame::Simple(Bytes::from("OK"))
        }
        _ => err_frame("AUDIT.RETENTION requires GET or SET"),
    }
}

/// AUDIT.GDPR.DELETE subject_id
fn audit_gdpr_delete(args: &[String]) -> Frame {
    if args.is_empty() {
        return err_frame("AUDIT.GDPR.DELETE requires: subject_id");
    }

    let handler = get_gdpr_handler();
    let result = handler.right_to_deletion(&args[0]);

    let mut map = HashMap::new();
    map.insert(
        Bytes::from("keys_found"),
        Frame::Integer(result.keys_found as i64),
    );
    map.insert(
        Bytes::from("keys_deleted"),
        Frame::Integer(result.keys_deleted as i64),
    );
    map.insert(
        Bytes::from("duration_us"),
        Frame::Integer(result.duration.as_micros() as i64),
    );

    Frame::Map(map)
}

/// AUDIT.GDPR.EXPORT subject_id
fn audit_gdpr_export(args: &[String]) -> Frame {
    if args.is_empty() {
        return err_frame("AUDIT.GDPR.EXPORT requires: subject_id");
    }

    let handler = get_gdpr_handler();
    let result = handler.data_export(&args[0]);

    let json = serde_json::to_string(&result).unwrap_or_default();
    Frame::Bulk(Some(Bytes::from(json)))
}

/// AUDIT.REPORT [SOC2|HIPAA|GDPR]
fn audit_report(args: &[String]) -> Frame {
    let framework = if args.is_empty() { "SOC2" } else { &args[0] };

    let log = get_audit_log();
    let policy = get_retention_policy();
    let report = ComplianceReport::generate(log, policy, framework);

    let json = serde_json::to_string_pretty(&report).unwrap_or_default();
    Frame::Bulk(Some(Bytes::from(json)))
}

/// AUDIT.STATS
fn audit_stats() -> Frame {
    let log = get_audit_log();
    let integrity = log.verify_integrity();

    let mut map = HashMap::new();
    map.insert(
        Bytes::from("total_entries"),
        Frame::Integer(log.count() as i64),
    );
    map.insert(
        Bytes::from("integrity_valid"),
        Frame::Boolean(integrity.valid),
    );

    Frame::Map(map)
}

/// AUDIT.HELP
fn audit_help() -> Frame {
    let help = vec![
        "AUDIT.LOG [LIMIT n] [USER user] [TYPE type] [KEY pattern] [SINCE ts] [UNTIL ts] - Query audit log",
        "AUDIT.VERIFY - Verify hash chain integrity",
        "AUDIT.EXPORT [JSON|CSV] - Export audit log",
        "AUDIT.RETENTION GET pattern - Get retention TTL (days)",
        "AUDIT.RETENTION SET pattern ttl_days - Set retention TTL",
        "AUDIT.GDPR.DELETE subject_id - Right to deletion",
        "AUDIT.GDPR.EXPORT subject_id - Export subject data",
        "AUDIT.REPORT [SOC2|HIPAA|GDPR] - Generate compliance report",
        "AUDIT.STATS - Audit log statistics",
        "AUDIT.HELP - This help text",
    ];

    Frame::Array(Some(
        help.into_iter()
            .map(|s| Frame::Bulk(Some(Bytes::from(s.to_string()))))
            .collect(),
    ))
}

/// Convert an audit entry to a Frame.
fn audit_entry_to_frame(entry: ComplianceAuditEntry) -> Frame {
    let result_str = match &entry.result {
        AuditResult::Success => "success".to_string(),
        AuditResult::Error(msg) => format!("error:{}", msg),
        AuditResult::Denied => "denied".to_string(),
    };

    let fields = vec![
        Frame::Bulk(Some(Bytes::from("id"))),
        Frame::Integer(entry.id as i64),
        Frame::Bulk(Some(Bytes::from("timestamp"))),
        Frame::Integer(entry.timestamp as i64),
        Frame::Bulk(Some(Bytes::from("event_type"))),
        Frame::Bulk(Some(Bytes::from(entry.event_type.to_string()))),
        Frame::Bulk(Some(Bytes::from("user"))),
        Frame::Bulk(Some(Bytes::from(entry.user))),
        Frame::Bulk(Some(Bytes::from("command"))),
        Frame::Bulk(Some(Bytes::from(entry.command))),
        Frame::Bulk(Some(Bytes::from("key"))),
        Frame::Bulk(Some(Bytes::from(entry.key.unwrap_or_default()))),
        Frame::Bulk(Some(Bytes::from("result"))),
        Frame::Bulk(Some(Bytes::from(result_str))),
        Frame::Bulk(Some(Bytes::from("db"))),
        Frame::Integer(entry.db as i64),
    ];
    Frame::Array(Some(fields))
}
