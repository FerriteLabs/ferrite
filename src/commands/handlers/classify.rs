//! CLASSIFY.* command handlers
//!
//! CLASSIFY.KEY, CLASSIFY.SCAN, CLASSIFY.TAG, CLASSIFY.TAGS, CLASSIFY.FIND,
//! CLASSIFY.RULES, CLASSIFY.SUMMARY, CLASSIFY.REPORT, CLASSIFY.STATS, CLASSIFY.HELP
#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::OnceLock;

use bytes::Bytes;

use crate::protocol::Frame;
use ferrite_core::audit::classifier::{
    ClassCategory, ClassificationRule, ClassifierConfig, DataClassifier, PatternRule,
    PatternTarget,
};

use super::err_frame;

/// Global data classifier singleton.
static DATA_CLASSIFIER: OnceLock<DataClassifier> = OnceLock::new();

fn get_classifier() -> &'static DataClassifier {
    DATA_CLASSIFIER.get_or_init(|| DataClassifier::new(ClassifierConfig::default()))
}

/// Dispatch a `CLASSIFY` subcommand.
pub fn classify_command(subcommand: &str, args: &[String]) -> Frame {
    match subcommand.to_uppercase().as_str() {
        "KEY" => handle_key(args),
        "SCAN" => handle_scan(args),
        "TAG" => handle_tag(args),
        "TAGS" => handle_tags(args),
        "FIND" => handle_find(args),
        "RULES" => handle_rules(args),
        "SUMMARY" => handle_summary(),
        "REPORT" => handle_report(args),
        "STATS" => handle_stats(),
        "HELP" | "" => handle_help(),
        other => err_frame(&format!("unknown CLASSIFY subcommand '{}'", other)),
    }
}

/// CLASSIFY.KEY key [value_hint] — classify a single key.
fn handle_key(args: &[String]) -> Frame {
    let key = match args.first() {
        Some(k) => k,
        None => return err_frame("CLASSIFY.KEY requires a key"),
    };
    let value_hint = args.get(1).map(|s| s.as_bytes()).unwrap_or(b"");
    let classes = get_classifier().classify_key(key, value_hint);
    if classes.is_empty() {
        return Frame::Array(Some(vec![Frame::Bulk(Some(Bytes::from("no classifications")))]));
    }
    let frames: Vec<Frame> = classes
        .iter()
        .map(|c| {
            let mut map = HashMap::new();
            map.insert(
                Bytes::from_static(b"category"),
                Frame::Bulk(Some(Bytes::from(c.category.to_string()))),
            );
            if let Some(ref sub) = c.subcategory {
                map.insert(
                    Bytes::from_static(b"subcategory"),
                    Frame::Bulk(Some(Bytes::from(sub.clone()))),
                );
            }
            map.insert(
                Bytes::from_static(b"confidence"),
                Frame::Double(c.confidence),
            );
            map.insert(
                Bytes::from_static(b"source"),
                Frame::Bulk(Some(Bytes::from(format!("{:?}", c.source)))),
            );
            Frame::Map(map)
        })
        .collect();
    Frame::Array(Some(frames))
}

/// CLASSIFY.SCAN pattern [SAMPLE rate] [LIMIT n] — batch scan.
fn handle_scan(args: &[String]) -> Frame {
    // In a real implementation, we would scan the store. Here we return
    // a report based on any key-value pairs passed.
    let limit: usize = args
        .iter()
        .position(|a| a.to_uppercase() == "LIMIT")
        .and_then(|i| args.get(i + 1))
        .and_then(|s| s.parse().ok())
        .unwrap_or(100);

    // Simulate an empty scan (no store access in handler-only mode).
    let report = get_classifier().scan(&[]);
    let mut map = HashMap::new();
    map.insert(
        Bytes::from_static(b"total_scanned"),
        Frame::Integer(report.total_scanned as i64),
    );
    map.insert(
        Bytes::from_static(b"classified"),
        Frame::Integer(report.classified as i64),
    );
    map.insert(
        Bytes::from_static(b"unclassified"),
        Frame::Integer(report.unclassified as i64),
    );
    map.insert(
        Bytes::from_static(b"scan_duration_ms"),
        Frame::Integer(report.scan_duration_ms as i64),
    );
    map.insert(
        Bytes::from_static(b"limit"),
        Frame::Integer(limit as i64),
    );
    Frame::Map(map)
}

/// CLASSIFY.TAG key category — manual tag.
fn handle_tag(args: &[String]) -> Frame {
    if args.len() < 2 {
        return err_frame("CLASSIFY.TAG requires key and category");
    }
    let key = &args[0];
    let category: ClassCategory = match args[1].parse() {
        Ok(c) => c,
        Err(e) => return err_frame(&format!("invalid category: {}", e)),
    };
    match get_classifier().tag_key(key, category) {
        Ok(()) => Frame::Simple(Bytes::from("OK")),
        Err(e) => err_frame(&e.to_string()),
    }
}

/// CLASSIFY.TAGS key — get classifications for a key.
fn handle_tags(args: &[String]) -> Frame {
    let key = match args.first() {
        Some(k) => k,
        None => return err_frame("CLASSIFY.TAGS requires a key"),
    };
    let tags = get_classifier().get_tags(key);
    if tags.is_empty() {
        return Frame::Array(Some(vec![]));
    }
    let frames: Vec<Frame> = tags
        .iter()
        .map(|c| {
            Frame::Bulk(Some(Bytes::from(format!(
                "{} ({:.0}% via {:?})",
                c.category,
                c.confidence * 100.0,
                c.source,
            ))))
        })
        .collect();
    Frame::Array(Some(frames))
}

/// CLASSIFY.FIND category — find keys by class.
fn handle_find(args: &[String]) -> Frame {
    let category: ClassCategory = match args.first().map(|s| s.parse()) {
        Some(Ok(c)) => c,
        _ => return err_frame("CLASSIFY.FIND requires a valid category"),
    };
    let keys = get_classifier().find_by_class(category);
    let frames: Vec<Frame> = keys
        .into_iter()
        .map(|k| Frame::Bulk(Some(Bytes::from(k))))
        .collect();
    Frame::Array(Some(frames))
}

/// CLASSIFY.RULES [ADD name category pattern target] [REMOVE name] — manage rules.
fn handle_rules(args: &[String]) -> Frame {
    if args.is_empty() {
        let stats = get_classifier().stats();
        return Frame::Bulk(Some(Bytes::from(format!(
            "{} rules loaded",
            stats.rules_count
        ))));
    }
    match args[0].to_uppercase().as_str() {
        "ADD" => {
            if args.len() < 5 {
                return err_frame("CLASSIFY.RULES ADD requires name category pattern target");
            }
            let name = args[1].clone();
            let category: ClassCategory = match args[2].parse() {
                Ok(c) => c,
                Err(e) => return err_frame(&format!("invalid category: {}", e)),
            };
            let pattern = args[3].clone();
            let target = match args[4].to_uppercase().as_str() {
                "KEYNAME" | "KEY" => PatternTarget::KeyName,
                "VALUE" | "VALUECONTENT" => PatternTarget::ValueContent,
                "FIELD" | "FIELDNAME" => PatternTarget::FieldName,
                "FORMAT" | "VALUEFORMAT" => PatternTarget::ValueFormat,
                _ => return err_frame("invalid target: use KEYNAME, VALUE, FIELD, or FORMAT"),
            };
            let rule = ClassificationRule {
                name,
                description: "User-defined rule".into(),
                category,
                patterns: vec![PatternRule {
                    target,
                    pattern,
                    confidence: 0.85,
                }],
                priority: 10,
                enabled: true,
            };
            match get_classifier().add_rule(rule) {
                Ok(()) => Frame::Simple(Bytes::from("OK")),
                Err(e) => err_frame(&e.to_string()),
            }
        }
        "REMOVE" => {
            if args.len() < 2 {
                return err_frame("CLASSIFY.RULES REMOVE requires a rule name");
            }
            match get_classifier().remove_rule(&args[1]) {
                Ok(()) => Frame::Simple(Bytes::from("OK")),
                Err(e) => err_frame(&e.to_string()),
            }
        }
        _ => err_frame("CLASSIFY.RULES expects ADD or REMOVE"),
    }
}

/// CLASSIFY.SUMMARY — classification overview.
fn handle_summary() -> Frame {
    let summary = get_classifier().classification_summary();
    let mut map = HashMap::new();
    map.insert(
        Bytes::from_static(b"total_tagged"),
        Frame::Integer(summary.total_tagged as i64),
    );
    map.insert(
        Bytes::from_static(b"unclassified"),
        Frame::Integer(summary.unclassified as i64),
    );
    let cat_frames: Vec<Frame> = summary
        .by_category
        .iter()
        .map(|(k, v)| Frame::Bulk(Some(Bytes::from(format!("{}: {}", k, v)))))
        .collect();
    map.insert(
        Bytes::from_static(b"by_category"),
        Frame::Array(Some(cat_frames)),
    );
    let risk_frames: Vec<Frame> = summary
        .highest_risk_keys
        .iter()
        .map(|k| Frame::Bulk(Some(Bytes::from(k.clone()))))
        .collect();
    map.insert(
        Bytes::from_static(b"highest_risk_keys"),
        Frame::Array(Some(risk_frames)),
    );
    Frame::Map(map)
}

/// CLASSIFY.REPORT [GDPR|HIPAA|SOC2|PCI] — compliance report.
fn handle_report(args: &[String]) -> Frame {
    let framework = args
        .first()
        .map(|s| s.as_str())
        .unwrap_or("GDPR");
    let report = get_classifier().compliance_report(framework);
    let mut map = HashMap::new();
    map.insert(
        Bytes::from_static(b"framework"),
        Frame::Bulk(Some(Bytes::from(report.framework))),
    );
    map.insert(
        Bytes::from_static(b"classified_keys"),
        Frame::Integer(report.classified_keys as i64),
    );
    map.insert(
        Bytes::from_static(b"compliant"),
        Frame::Bulk(Some(Bytes::from(if report.compliant {
            "yes"
        } else {
            "no"
        }))),
    );
    let issue_frames: Vec<Frame> = report
        .issues
        .iter()
        .map(|i| {
            Frame::Bulk(Some(Bytes::from(format!(
                "[{}] {} - {} ({})",
                i.severity, i.key_pattern, i.issue, i.recommendation
            ))))
        })
        .collect();
    map.insert(
        Bytes::from_static(b"issues"),
        Frame::Array(Some(issue_frames)),
    );
    Frame::Map(map)
}

/// CLASSIFY.STATS — operational statistics.
fn handle_stats() -> Frame {
    let stats = get_classifier().stats();
    let mut map = HashMap::new();
    map.insert(
        Bytes::from_static(b"total_scans"),
        Frame::Integer(stats.total_scans as i64),
    );
    map.insert(
        Bytes::from_static(b"total_classified"),
        Frame::Integer(stats.total_classified as i64),
    );
    map.insert(
        Bytes::from_static(b"rules_count"),
        Frame::Integer(stats.rules_count as i64),
    );
    map.insert(
        Bytes::from_static(b"manual_tags"),
        Frame::Integer(stats.manual_tags as i64),
    );
    map.insert(
        Bytes::from_static(b"propagations"),
        Frame::Integer(stats.propagations as i64),
    );
    Frame::Map(map)
}

/// CLASSIFY.HELP — usage information.
fn handle_help() -> Frame {
    let lines = vec![
        "CLASSIFY.KEY key [value_hint]                    - Classify a single key",
        "CLASSIFY.SCAN pattern [SAMPLE rate] [LIMIT n]    - Batch scan keys",
        "CLASSIFY.TAG key category                        - Manually tag a key",
        "CLASSIFY.TAGS key                                - Get classifications for a key",
        "CLASSIFY.FIND category                           - Find keys by classification",
        "CLASSIFY.RULES [ADD name cat pat tgt] [REMOVE n] - Manage classification rules",
        "CLASSIFY.SUMMARY                                 - Classification overview",
        "CLASSIFY.REPORT [GDPR|HIPAA|SOC2|PCI]            - Compliance report",
        "CLASSIFY.STATS                                   - Classifier statistics",
        "CLASSIFY.HELP                                    - Show this help",
    ];
    let frames: Vec<Frame> = lines
        .into_iter()
        .map(|l| Frame::Bulk(Some(Bytes::from(l))))
        .collect();
    Frame::Array(Some(frames))
}
