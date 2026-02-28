//! POLICYENGINE.* command handlers
//!
//! Programmable access control policy commands.
//! POLICYENGINE.CREATE, POLICYENGINE.DELETE, POLICYENGINE.TEST, POLICYENGINE.LIST,
//! POLICYENGINE.INFO, POLICYENGINE.EVALUATE, POLICYENGINE.STATS, POLICYENGINE.HELP
#![allow(dead_code)]

use std::sync::OnceLock;

use crate::protocol::Frame;
use ferrite_core::auth::policy_engine::{
    EffectScope, EvaluationContext, Policy, PolicyAction, PolicyCondition, PolicyConfig,
    PolicyEngine,
};

use super::{err_frame, ok_frame};

/// Global policy engine singleton.
static POLICY_ENGINE: OnceLock<PolicyEngine> = OnceLock::new();

fn get_engine() -> &'static PolicyEngine {
    POLICY_ENGINE.get_or_init(|| PolicyEngine::new(PolicyConfig::default()))
}

/// Dispatch a `POLICYENGINE` subcommand.
pub fn policy_engine_command(subcommand: &str, args: &[String]) -> Frame {
    match subcommand.to_uppercase().as_str() {
        "CREATE" => handle_create(args),
        "DELETE" => handle_delete(args),
        "TEST" => handle_test(args),
        "LIST" => handle_list(args),
        "INFO" => handle_info(args),
        "EVALUATE" => handle_evaluate(args),
        "STATS" => handle_stats(),
        "HELP" | "" => handle_help(),
        other => err_frame(&format!("unknown POLICYENGINE subcommand '{}'", other)),
    }
}

/// POLICYENGINE.CREATE name ALLOW|DENY ON command_pattern FOR key_pattern
/// [WHEN field op value] [PRIORITY n] [DESCRIPTION "..."]
fn handle_create(args: &[String]) -> Frame {
    if args.len() < 2 {
        return err_frame(
            "POLICYENGINE.CREATE requires: name action [ON cmd_pattern] [FOR key_pattern] [WHEN field op value] [PRIORITY n] [DESCRIPTION desc]",
        );
    }

    let name = args[0].clone();
    let action = match PolicyAction::from_str_loose(&args[1]) {
        Some(a) => a,
        None => return err_frame("invalid action; use ALLOW, DENY, ALLOW_WITH_AUDIT, or DENY_WITH_WARNING"),
    };

    let mut scope = EffectScope::default();
    let mut conditions = Vec::new();
    let mut priority: i32 = 0;
    let mut description = String::new();

    let mut i = 2;
    while i < args.len() {
        match args[i].to_uppercase().as_str() {
            "ON" if i + 1 < args.len() => {
                i += 1;
                scope.commands.push(args[i].clone());
            }
            "FOR" if i + 1 < args.len() => {
                i += 1;
                scope.key_patterns.push(args[i].clone());
            }
            "WHEN" if i + 3 < args.len() => {
                let field_str = &args[i + 1];
                let op_str = &args[i + 2];
                let val_str = &args[i + 3];
                if let (Some(field), Some(op)) = (
                    ferrite_core::auth::policy_engine::ConditionField::from_str_loose(field_str),
                    ferrite_core::auth::policy_engine::ConditionOp::from_str_loose(op_str),
                ) {
                    let value = serde_json::from_str(val_str)
                        .unwrap_or_else(|_| serde_json::Value::String(val_str.clone()));
                    conditions.push(PolicyCondition {
                        field,
                        operator: op,
                        value,
                    });
                }
                i += 3;
            }
            "PRIORITY" if i + 1 < args.len() => {
                i += 1;
                priority = args[i].parse().unwrap_or(0);
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

    let policy = Policy {
        name: name.clone(),
        description,
        priority,
        action,
        conditions,
        effect_scope: scope,
        enabled: true,
        created_at: now,
        version: 1,
    };

    match get_engine().add_policy(policy) {
        Ok(()) => ok_frame(),
        Err(e) => err_frame(&e.to_string()),
    }
}

/// POLICYENGINE.DELETE name
fn handle_delete(args: &[String]) -> Frame {
    if args.is_empty() {
        return err_frame("POLICYENGINE.DELETE requires policy name");
    }
    match get_engine().remove_policy(&args[0]) {
        Ok(()) => ok_frame(),
        Err(e) => err_frame(&e.to_string()),
    }
}

/// POLICYENGINE.TEST name USER user COMMAND cmd [KEY key] [TENANT tenant]
fn handle_test(args: &[String]) -> Frame {
    if args.is_empty() {
        return err_frame("POLICYENGINE.TEST requires policy name and context params");
    }
    let policy_name = &args[0];
    let mut user = String::new();
    let mut command = String::new();
    let mut key: Option<String> = None;
    let mut tenant: Option<String> = None;

    let mut i = 1;
    while i < args.len() {
        match args[i].to_uppercase().as_str() {
            "USER" if i + 1 < args.len() => {
                i += 1;
                user = args[i].clone();
            }
            "COMMAND" if i + 1 < args.len() => {
                i += 1;
                command = args[i].clone();
            }
            "KEY" if i + 1 < args.len() => {
                i += 1;
                key = Some(args[i].clone());
            }
            "TENANT" if i + 1 < args.len() => {
                i += 1;
                tenant = Some(args[i].clone());
            }
            _ => {}
        }
        i += 1;
    }

    let mut ctx = EvaluationContext::new(&user, &command);
    ctx.key = key;
    ctx.tenant = tenant;

    let decision = get_engine().test_policy(policy_name, &ctx);
    Frame::array(vec![
        Frame::bulk("action"),
        Frame::bulk(decision.action.as_str()),
        Frame::bulk("matched_policy"),
        Frame::bulk(
            decision
                .matched_policy
                .unwrap_or_else(|| "(none)".to_string()),
        ),
        Frame::bulk("reason"),
        Frame::bulk(decision.reason),
        Frame::bulk("evaluation_us"),
        Frame::Integer(decision.evaluation_us as i64),
    ])
}

/// POLICYENGINE.LIST [ACTIVE|ALL]
fn handle_list(args: &[String]) -> Frame {
    let filter_active = args
        .first()
        .map(|s| s.to_uppercase() == "ACTIVE")
        .unwrap_or(false);

    let policies = get_engine().list_policies();
    let items: Vec<Frame> = policies
        .into_iter()
        .filter(|p| !filter_active || p.enabled)
        .map(|p| {
            Frame::array(vec![
                Frame::bulk(p.name.clone()),
                Frame::Integer(p.priority as i64),
                Frame::bulk(p.action.as_str()),
                Frame::bulk(if p.enabled { "enabled" } else { "disabled" }),
                Frame::Integer(p.evaluations as i64),
                Frame::Integer(p.denials as i64),
            ])
        })
        .collect();
    Frame::array(items)
}

/// POLICYENGINE.INFO name
fn handle_info(args: &[String]) -> Frame {
    if args.is_empty() {
        return err_frame("POLICYENGINE.INFO requires policy name");
    }
    match get_engine().get_policy(&args[0]) {
        Some(p) => Frame::array(vec![
            Frame::bulk("name"),
            Frame::bulk(p.name.clone()),
            Frame::bulk("description"),
            Frame::bulk(p.description.clone()),
            Frame::bulk("priority"),
            Frame::Integer(p.priority as i64),
            Frame::bulk("action"),
            Frame::bulk(p.action.as_str()),
            Frame::bulk("enabled"),
            Frame::bulk(if p.enabled { "true" } else { "false" }),
            Frame::bulk("conditions"),
            Frame::Integer(p.conditions.len() as i64),
            Frame::bulk("version"),
            Frame::Integer(p.version as i64),
        ]),
        None => Frame::Null,
    }
}

/// POLICYENGINE.EVALUATE USER user COMMAND cmd [KEY key]
fn handle_evaluate(args: &[String]) -> Frame {
    let mut user = String::new();
    let mut command = String::new();
    let mut key: Option<String> = None;

    let mut i = 0;
    while i < args.len() {
        match args[i].to_uppercase().as_str() {
            "USER" if i + 1 < args.len() => {
                i += 1;
                user = args[i].clone();
            }
            "COMMAND" if i + 1 < args.len() => {
                i += 1;
                command = args[i].clone();
            }
            "KEY" if i + 1 < args.len() => {
                i += 1;
                key = Some(args[i].clone());
            }
            _ => {}
        }
        i += 1;
    }

    let mut ctx = EvaluationContext::new(&user, &command);
    ctx.key = key;

    let decision = get_engine().evaluate(&ctx);
    Frame::array(vec![
        Frame::bulk("action"),
        Frame::bulk(decision.action.as_str()),
        Frame::bulk("matched_policy"),
        Frame::bulk(
            decision
                .matched_policy
                .unwrap_or_else(|| "(none)".to_string()),
        ),
        Frame::bulk("reason"),
        Frame::bulk(decision.reason),
        Frame::bulk("evaluation_us"),
        Frame::Integer(decision.evaluation_us as i64),
    ])
}

/// POLICYENGINE.STATS
fn handle_stats() -> Frame {
    let stats = get_engine().stats();
    Frame::array(vec![
        Frame::bulk("total_policies"),
        Frame::Integer(stats.total_policies as i64),
        Frame::bulk("active"),
        Frame::Integer(stats.active as i64),
        Frame::bulk("evaluations"),
        Frame::Integer(stats.evaluations as i64),
        Frame::bulk("denials"),
        Frame::Integer(stats.denials as i64),
        Frame::bulk("cache_hits"),
        Frame::Integer(stats.cache_hits as i64),
        Frame::bulk("avg_eval_us"),
        Frame::Integer(stats.avg_eval_us as i64),
    ])
}

/// POLICYENGINE.HELP
fn handle_help() -> Frame {
    Frame::array(vec![
        Frame::bulk("POLICYENGINE.CREATE name ALLOW|DENY [ON cmd_pattern] [FOR key_pattern] [WHEN field op value] [PRIORITY n] [DESCRIPTION desc]"),
        Frame::bulk("POLICYENGINE.DELETE name"),
        Frame::bulk("POLICYENGINE.TEST name USER user COMMAND cmd [KEY key] [TENANT tenant]"),
        Frame::bulk("POLICYENGINE.LIST [ACTIVE|ALL]"),
        Frame::bulk("POLICYENGINE.INFO name"),
        Frame::bulk("POLICYENGINE.EVALUATE USER user COMMAND cmd [KEY key]"),
        Frame::bulk("POLICYENGINE.STATS"),
        Frame::bulk("POLICYENGINE.HELP"),
    ])
}
