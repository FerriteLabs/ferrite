//! Materialized view command handlers
#![allow(dead_code)]

use std::sync::OnceLock;
use std::time::{Duration, Instant};

use bytes::Bytes;

use crate::protocol::Frame;
use ferrite_core::views::materialized::{
    FilterOp, MaterializedViewManager, ViewDefinition, ViewFilter, ViewOrderBy,
};

use super::err_frame;

/// Global materialized view manager singleton.
static VIEW_MANAGER: OnceLock<MaterializedViewManager> = OnceLock::new();

/// Return the global view manager.
pub fn global_view_manager() -> &'static MaterializedViewManager {
    VIEW_MANAGER.get_or_init(MaterializedViewManager::new)
}

/// Dispatch a `VIEW` subcommand.
pub fn view_command(subcommand: &str, args: &[String]) -> Frame {
    match subcommand.to_uppercase().as_str() {
        "CREATE" => view_create(args),
        "GET" => view_get(args),
        "REFRESH" => view_refresh(args),
        "DROP" => view_drop(args),
        "LIST" => view_list(),
        "INFO" => view_info(args),
        "HELP" => view_help(),
        _ => err_frame(&format!("unknown VIEW subcommand '{}'", subcommand)),
    }
}

/// VIEW.CREATE name FROM pattern [WHERE field op value] [ORDER BY field ASC|DESC] [LIMIT n]
///   [REFRESH interval_secs] [AUTO]
fn view_create(args: &[String]) -> Frame {
    // Minimum: name FROM pattern
    if args.len() < 3 {
        return err_frame(
            "VIEW CREATE requires: name FROM pattern [WHERE field op value] \
             [ORDER BY field ASC|DESC] [LIMIT n] [REFRESH secs] [AUTO]",
        );
    }

    let name = args[0].clone();

    if args[1].to_uppercase() != "FROM" {
        return err_frame("expected FROM after view name");
    }

    let source_pattern = args[2].clone();

    let mut filter: Option<ViewFilter> = None;
    let mut order_by: Option<ViewOrderBy> = None;
    let mut limit: Option<usize> = None;
    let mut refresh_interval: Option<Duration> = None;
    let mut auto_refresh = false;

    let mut i = 3;
    while i < args.len() {
        match args[i].to_uppercase().as_str() {
            "WHERE" => {
                if i + 3 >= args.len() {
                    return err_frame("WHERE requires: field op value");
                }
                let field = args[i + 1].clone();
                let op = match parse_filter_op(&args[i + 2]) {
                    Some(op) => op,
                    None => return err_frame(&format!("unknown filter op '{}'", args[i + 2])),
                };
                let value = parse_json_value(&args[i + 3]);
                filter = Some(ViewFilter { field, op, value });
                i += 4;
            }
            "ORDER" => {
                if i + 2 >= args.len() || args[i + 1].to_uppercase() != "BY" {
                    return err_frame("expected ORDER BY field [ASC|DESC]");
                }
                if i + 3 >= args.len() {
                    return err_frame("ORDER BY requires a field name");
                }
                let field = args[i + 3].clone();
                let descending = if i + 4 < args.len() {
                    match args[i + 4].to_uppercase().as_str() {
                        "DESC" => {
                            i += 1; // consume extra token
                            true
                        }
                        "ASC" => {
                            i += 1;
                            false
                        }
                        _ => false,
                    }
                } else {
                    false
                };
                order_by = Some(ViewOrderBy { field, descending });
                i += 4;
            }
            "LIMIT" => {
                if i + 1 >= args.len() {
                    return err_frame("LIMIT requires a number");
                }
                limit = match args[i + 1].parse::<usize>() {
                    Ok(n) => Some(n),
                    Err(_) => return err_frame("LIMIT must be a positive integer"),
                };
                i += 2;
            }
            "REFRESH" => {
                if i + 1 >= args.len() {
                    return err_frame("REFRESH requires interval in seconds");
                }
                refresh_interval = match args[i + 1].parse::<u64>() {
                    Ok(s) => Some(Duration::from_secs(s)),
                    Err(_) => return err_frame("REFRESH interval must be a positive integer"),
                };
                i += 2;
            }
            "AUTO" => {
                auto_refresh = true;
                i += 1;
            }
            _ => {
                i += 1;
            }
        }
    }

    let def = ViewDefinition {
        name,
        source_pattern,
        filter,
        order_by,
        limit,
        created_at: Some(Instant::now()),
        last_refreshed: None,
        refresh_interval,
        auto_refresh,
    };

    match global_view_manager().create_view(def) {
        Ok(()) => Frame::simple("OK"),
        Err(e) => err_frame(&e.to_string()),
    }
}

/// VIEW.GET name — return materialized data as array of bulk strings (JSON rows).
fn view_get(args: &[String]) -> Frame {
    if args.is_empty() {
        return err_frame("VIEW GET requires a view name");
    }

    match global_view_manager().get_view(&args[0]) {
        Some(result) => {
            let frames: Vec<Frame> = result
                .rows
                .iter()
                .map(|row| Frame::bulk(Bytes::from(row.to_string())))
                .collect();
            Frame::array(frames)
        }
        None => Frame::Null,
    }
}

/// VIEW.REFRESH name — force refresh, return row count.
fn view_refresh(args: &[String]) -> Frame {
    if args.is_empty() {
        return err_frame("VIEW REFRESH requires a view name");
    }

    match global_view_manager().refresh_view(&args[0]) {
        Ok(result) => Frame::integer(result.total_count as i64),
        Err(e) => err_frame(&e.to_string()),
    }
}

/// VIEW.DROP name — delete view.
fn view_drop(args: &[String]) -> Frame {
    if args.is_empty() {
        return err_frame("VIEW DROP requires a view name");
    }

    match global_view_manager().drop_view(&args[0]) {
        Ok(()) => Frame::simple("OK"),
        Err(e) => err_frame(&e.to_string()),
    }
}

/// VIEW.LIST — return all views with metadata.
fn view_list() -> Frame {
    let views = global_view_manager().list_views();
    let frames: Vec<Frame> = views
        .into_iter()
        .map(|v| {
            Frame::array(vec![
                Frame::bulk("name"),
                Frame::bulk(Bytes::from(v.name)),
                Frame::bulk("source"),
                Frame::bulk(Bytes::from(v.source_pattern)),
                Frame::bulk("rows"),
                Frame::integer(v.row_count as i64),
                Frame::bulk("auto_refresh"),
                Frame::integer(i64::from(v.auto_refresh)),
                Frame::bulk("stale"),
                Frame::integer(i64::from(v.stale)),
            ])
        })
        .collect();
    Frame::array(frames)
}

/// VIEW.INFO name — return view definition and stats.
fn view_info(args: &[String]) -> Frame {
    if args.is_empty() {
        return err_frame("VIEW INFO requires a view name");
    }

    let views = global_view_manager().list_views();
    match views.into_iter().find(|v| v.name == args[0]) {
        Some(v) => Frame::array(vec![
            Frame::bulk("name"),
            Frame::bulk(Bytes::from(v.name)),
            Frame::bulk("source_pattern"),
            Frame::bulk(Bytes::from(v.source_pattern)),
            Frame::bulk("row_count"),
            Frame::integer(v.row_count as i64),
            Frame::bulk("auto_refresh"),
            Frame::integer(i64::from(v.auto_refresh)),
            Frame::bulk("stale"),
            Frame::integer(i64::from(v.stale)),
        ]),
        None => err_frame(&format!("view '{}' not found", args[0])),
    }
}

/// VIEW.HELP — return help text.
fn view_help() -> Frame {
    Frame::array(vec![
        Frame::bulk(
            "VIEW.CREATE name FROM pattern [WHERE field op value] [ORDER BY field ASC|DESC] [LIMIT n] [REFRESH secs] [AUTO]",
        ),
        Frame::bulk("  Create a materialized view over a key pattern."),
        Frame::bulk("VIEW.GET name"),
        Frame::bulk("  Return cached rows as JSON bulk strings."),
        Frame::bulk("VIEW.REFRESH name"),
        Frame::bulk("  Force recomputation; returns row count."),
        Frame::bulk("VIEW.DROP name"),
        Frame::bulk("  Delete a materialized view."),
        Frame::bulk("VIEW.LIST"),
        Frame::bulk("  List all views with metadata."),
        Frame::bulk("VIEW.INFO name"),
        Frame::bulk("  Return view definition and statistics."),
        Frame::bulk("VIEW.HELP"),
        Frame::bulk("  Show this help text."),
    ])
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn parse_filter_op(s: &str) -> Option<FilterOp> {
    match s.to_uppercase().as_str() {
        "=" | "EQ" => Some(FilterOp::Eq),
        "!=" | "NE" => Some(FilterOp::Ne),
        ">" | "GT" => Some(FilterOp::Gt),
        "<" | "LT" => Some(FilterOp::Lt),
        ">=" | "GTE" => Some(FilterOp::Gte),
        "<=" | "LTE" => Some(FilterOp::Lte),
        "CONTAINS" => Some(FilterOp::Contains),
        "EXISTS" => Some(FilterOp::Exists),
        _ => None,
    }
}

fn parse_json_value(s: &str) -> serde_json::Value {
    serde_json::from_str(s).unwrap_or_else(|_| serde_json::Value::String(s.to_string()))
}
