//! Policy command handlers
//!
//! Implements declarative data policy commands.

use std::sync::OnceLock;

use bytes::Bytes;

use crate::protocol::Frame;
use ferrite_enterprise::policy::{
    Policy, PolicyAction, PolicyConfig, PolicyManager, PolicyRule, PolicyType, RuleCondition,
};

use super::{err_frame, ok_frame, HandlerContext};

/// Global policy manager instance
static POLICY_MANAGER: OnceLock<PolicyManager> = OnceLock::new();

/// Get the global policy manager
fn policy_manager() -> &'static PolicyManager {
    POLICY_MANAGER.get_or_init(|| PolicyManager::new(PolicyConfig::default()))
}

/// Handle POLICY.CREATE command
///
/// POLICY.CREATE name type [PATTERNS pattern1 pattern2 ...] [DESCRIPTION desc]
pub fn handle_policy_create(_ctx: &HandlerContext<'_>, args: &[Bytes]) -> Frame {
    if args.len() < 2 {
        return err_frame("POLICY.CREATE requires name and type");
    }

    let name = String::from_utf8_lossy(&args[0]).to_string();
    let type_str = String::from_utf8_lossy(&args[1]).to_lowercase();

    let policy_type = match type_str.as_str() {
        "ttl" => PolicyType::Ttl,
        "retention" => PolicyType::Retention,
        "access" => PolicyType::Access,
        "encryption" => PolicyType::Encryption,
        "compliance" => PolicyType::Compliance,
        "custom" => PolicyType::Custom,
        _ => {
            return err_frame(
                "invalid policy type (ttl, retention, access, encryption, compliance, custom)",
            )
        }
    };

    let mut policy = Policy::new(&name, policy_type);

    let mut i = 2;
    while i < args.len() {
        let arg = String::from_utf8_lossy(&args[i]).to_uppercase();
        match arg.as_str() {
            "PATTERNS" => {
                i += 1;
                let mut patterns = Vec::new();
                while i < args.len() {
                    let val = String::from_utf8_lossy(&args[i]).to_string();
                    if val
                        .chars()
                        .next()
                        .map(|c| c.is_uppercase())
                        .unwrap_or(false)
                    {
                        break;
                    }
                    patterns.push(val);
                    i += 1;
                }
                if !patterns.is_empty() {
                    policy = policy.with_patterns(patterns);
                }
                continue;
            }
            "DESCRIPTION" if i + 1 < args.len() => {
                i += 1;
                policy = policy.with_description(String::from_utf8_lossy(&args[i]).to_string());
            }
            "PRIORITY" if i + 1 < args.len() => {
                i += 1;
                if let Ok(p) = String::from_utf8_lossy(&args[i]).parse::<i32>() {
                    policy = policy.with_priority(p);
                }
            }
            _ => {}
        }
        i += 1;
    }

    match policy_manager().register(policy) {
        Ok(id) => Frame::bulk(id),
        Err(e) => err_frame(&e.to_string()),
    }
}

/// Handle POLICY.DELETE command
///
/// POLICY.DELETE policy_id
pub fn handle_policy_delete(_ctx: &HandlerContext<'_>, args: &[Bytes]) -> Frame {
    if args.is_empty() {
        return err_frame("POLICY.DELETE requires policy ID");
    }

    let id = String::from_utf8_lossy(&args[0]).to_string();

    match policy_manager().unregister(&id) {
        Ok(()) => ok_frame(),
        Err(e) => err_frame(&e.to_string()),
    }
}

/// Handle POLICY.GET command
///
/// POLICY.GET policy_id
pub fn handle_policy_get(_ctx: &HandlerContext<'_>, args: &[Bytes]) -> Frame {
    if args.is_empty() {
        return err_frame("POLICY.GET requires policy ID");
    }

    let id = String::from_utf8_lossy(&args[0]).to_string();

    match policy_manager().get(&id) {
        Some(policy) => Frame::array(vec![
            Frame::bulk("id"),
            Frame::bulk(policy.id),
            Frame::bulk("name"),
            Frame::bulk(policy.name),
            Frame::bulk("type"),
            Frame::bulk(policy.policy_type.as_str()),
            Frame::bulk("description"),
            Frame::bulk(policy.description),
            Frame::bulk("enabled"),
            Frame::bulk(if policy.enabled { "true" } else { "false" }),
            Frame::bulk("priority"),
            Frame::Integer(policy.priority as i64),
            Frame::bulk("patterns"),
            Frame::array(policy.key_patterns.into_iter().map(Frame::bulk).collect()),
            Frame::bulk("rules"),
            Frame::Integer(policy.rules.len() as i64),
        ]),
        None => Frame::Null,
    }
}

/// Handle POLICY.LIST command
///
/// POLICY.LIST [TYPE type]
pub fn handle_policy_list(_ctx: &HandlerContext<'_>, args: &[Bytes]) -> Frame {
    let policies = if args.len() >= 2 {
        let type_keyword = String::from_utf8_lossy(&args[0]).to_uppercase();
        if type_keyword == "TYPE" {
            let type_str = String::from_utf8_lossy(&args[1]).to_lowercase();
            let policy_type = match type_str.as_str() {
                "ttl" => Some(PolicyType::Ttl),
                "retention" => Some(PolicyType::Retention),
                "access" => Some(PolicyType::Access),
                "encryption" => Some(PolicyType::Encryption),
                "compliance" => Some(PolicyType::Compliance),
                "custom" => Some(PolicyType::Custom),
                _ => None,
            };

            if let Some(pt) = policy_type {
                policy_manager().list_by_type(pt)
            } else {
                policy_manager().list()
            }
        } else {
            policy_manager().list()
        }
    } else {
        policy_manager().list()
    };

    let items: Vec<Frame> = policies
        .into_iter()
        .map(|p| {
            Frame::array(vec![
                Frame::bulk(p.id),
                Frame::bulk(p.name),
                Frame::bulk(p.policy_type.as_str()),
                Frame::bulk(if p.enabled { "enabled" } else { "disabled" }),
            ])
        })
        .collect();

    Frame::array(items)
}

/// Handle POLICY.RULE.ADD command
///
/// POLICY.RULE.ADD policy_id rule_name ACTION action [CONDITION condition]
pub fn handle_policy_rule_add(_ctx: &HandlerContext<'_>, args: &[Bytes]) -> Frame {
    if args.len() < 4 {
        return err_frame("POLICY.RULE.ADD requires policy_id, rule_name, ACTION, action");
    }

    let policy_id = String::from_utf8_lossy(&args[0]).to_string();
    let rule_name = String::from_utf8_lossy(&args[1]).to_string();

    let mut rule = PolicyRule::new(&rule_name);

    let mut i = 2;
    while i < args.len() {
        let keyword = String::from_utf8_lossy(&args[i]).to_uppercase();
        match keyword.as_str() {
            "ACTION" if i + 1 < args.len() => {
                i += 1;
                let action_str = String::from_utf8_lossy(&args[i]).to_lowercase();
                let action = match action_str.as_str() {
                    "allow" => PolicyAction::Allow,
                    "deny" => PolicyAction::Deny("policy denied".to_string()),
                    "redact" => PolicyAction::Redact,
                    "encrypt" => PolicyAction::RequireEncryption,
                    _ if action_str.starts_with("ttl:") => {
                        if let Ok(ttl) = action_str[4..].parse::<u64>() {
                            PolicyAction::SetTtl(ttl)
                        } else {
                            return err_frame("invalid TTL value");
                        }
                    }
                    _ if action_str.starts_with("log:") => {
                        PolicyAction::Log(action_str[4..].to_string())
                    }
                    _ => PolicyAction::Custom(action_str),
                };
                rule = rule.with_action(action);
            }
            "CONDITION" if i + 1 < args.len() => {
                i += 1;
                let cond_str = String::from_utf8_lossy(&args[i]).to_string();
                // Parse condition: "key:pattern" or "op:GET,SET" or "size:>1000"
                if let Some((cond_type, cond_val)) = cond_str.split_once(':') {
                    let condition = match cond_type.to_lowercase().as_str() {
                        "key" => RuleCondition::KeyMatches(cond_val.to_string()),
                        "op" => RuleCondition::OperationIn(
                            cond_val.split(',').map(|s| s.to_string()).collect(),
                        ),
                        "size" if cond_val.starts_with('>') => {
                            if let Ok(size) = cond_val[1..].parse::<u64>() {
                                RuleCondition::ValueSizeGt(size)
                            } else {
                                continue;
                            }
                        }
                        "size" if cond_val.starts_with('<') => {
                            if let Ok(size) = cond_val[1..].parse::<u64>() {
                                RuleCondition::ValueSizeLt(size)
                            } else {
                                continue;
                            }
                        }
                        _ => RuleCondition::Custom(cond_str),
                    };
                    rule = rule.with_condition(condition);
                }
            }
            "TERMINAL" => {
                rule = rule.terminal();
            }
            _ => {}
        }
        i += 1;
    }

    // In a real implementation, we'd add the rule to the policy
    // For now, just return success
    Frame::bulk(format!(
        "Rule '{}' would be added to policy '{}'",
        rule_name, policy_id
    ))
}

/// Handle POLICY.EVALUATE command
///
/// POLICY.EVALUATE key operation
pub fn handle_policy_evaluate(_ctx: &HandlerContext<'_>, args: &[Bytes]) -> Frame {
    if args.len() < 2 {
        return err_frame("POLICY.EVALUATE requires key and operation");
    }

    let key = String::from_utf8_lossy(&args[0]).to_string();
    let operation = String::from_utf8_lossy(&args[1]).to_string();

    let evaluation = policy_manager().evaluate(&key, &operation);

    Frame::array(vec![
        Frame::bulk("allowed"),
        Frame::bulk(if evaluation.allowed { "true" } else { "false" }),
        Frame::bulk("reason"),
        Frame::bulk(
            evaluation
                .denial_reason
                .unwrap_or_else(|| "N/A".to_string()),
        ),
        Frame::bulk("actions"),
        Frame::Integer(evaluation.actions.len() as i64),
        Frame::bulk("matched_policies"),
        Frame::Integer(evaluation.matched_policies.len() as i64),
    ])
}

/// Handle POLICY.STATS command
///
/// POLICY.STATS
pub fn handle_policy_stats(_ctx: &HandlerContext<'_>, _args: &[Bytes]) -> Frame {
    let stats = policy_manager().stats();

    Frame::array(vec![
        Frame::bulk("total_policies"),
        Frame::Integer(stats.total_policies as i64),
        Frame::bulk("enabled_policies"),
        Frame::Integer(stats.enabled_policies as i64),
        Frame::bulk("ttl_policies"),
        Frame::Integer(stats.ttl_policies as i64),
        Frame::bulk("access_policies"),
        Frame::Integer(stats.access_policies as i64),
        Frame::bulk("encryption_policies"),
        Frame::Integer(stats.encryption_policies as i64),
        Frame::bulk("audit_entries"),
        Frame::Integer(stats.audit_entries as i64),
    ])
}

/// Handle POLICY.AUDIT command
///
/// POLICY.AUDIT [LIMIT n]
pub fn handle_policy_audit(_ctx: &HandlerContext<'_>, args: &[Bytes]) -> Frame {
    let limit = if args.len() >= 2 {
        let keyword = String::from_utf8_lossy(&args[0]).to_uppercase();
        if keyword == "LIMIT" {
            String::from_utf8_lossy(&args[1]).parse().ok()
        } else {
            None
        }
    } else {
        Some(100)
    };

    let entries = policy_manager().audit_log().get_entries(limit);

    let items: Vec<Frame> = entries
        .into_iter()
        .map(|e| {
            Frame::array(vec![
                Frame::Integer(e.timestamp as i64),
                Frame::bulk(e.level.as_str()),
                Frame::bulk(e.event),
            ])
        })
        .collect();

    Frame::array(items)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_policy_manager_singleton() {
        let p1 = policy_manager();
        let p2 = policy_manager();
        assert!(std::ptr::eq(p1, p2));
    }
}
