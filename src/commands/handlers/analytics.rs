//! ANALYTICS.* command handlers
//!
//! ANALYTICS.QUERY, ANALYTICS.COUNT, ANALYTICS.SUM, ANALYTICS.AVG,
//! ANALYTICS.MIN, ANALYTICS.MAX, ANALYTICS.TOP, ANALYTICS.HISTOGRAM,
//! ANALYTICS.EXPLAIN, ANALYTICS.STATS, ANALYTICS.HELP
#![allow(dead_code)]

use std::sync::OnceLock;

use crate::protocol::Frame;
use ferrite_core::query::analytics::{
    AggFunction, AnalyticsConfig, AnalyticsEngine, AnalyticsQuery, FilterExpr, FilterOp,
    OrderExpr, SelectExpr,
};

use super::err_frame;

/// Global analytics engine singleton.
static ANALYTICS_ENGINE: OnceLock<AnalyticsEngine> = OnceLock::new();

fn get_engine() -> &'static AnalyticsEngine {
    ANALYTICS_ENGINE.get_or_init(|| AnalyticsEngine::new(AnalyticsConfig::default()))
}

/// Dispatch an `ANALYTICS` subcommand.
pub fn analytics_command(subcommand: &str, args: &[String]) -> Frame {
    match subcommand.to_uppercase().as_str() {
        "QUERY" => handle_query(args),
        "COUNT" => handle_count(args),
        "SUM" => handle_agg(args, AggFunction::Sum),
        "AVG" => handle_agg(args, AggFunction::Avg),
        "MIN" => handle_agg(args, AggFunction::Min),
        "MAX" => handle_agg(args, AggFunction::Max),
        "TOP" => handle_top(args),
        "HISTOGRAM" => handle_histogram(args),
        "EXPLAIN" => handle_explain(args),
        "STATS" => handle_stats(),
        "HELP" => handle_help(),
        other => err_frame(&format!("unknown ANALYTICS subcommand: {}", other)),
    }
}

// ---------------------------------------------------------------------------
// Subcommand handlers
// ---------------------------------------------------------------------------

fn handle_query(args: &[String]) -> Frame {
    // ANALYTICS.QUERY FROM <pattern> SELECT <fields> [WHERE <cond>] [GROUP BY <fields>]
    //                [ORDER BY <field> [DESC]] [LIMIT <n>]
    if args.is_empty() {
        return err_frame("ANALYTICS.QUERY requires arguments. Usage: ANALYTICS.QUERY FROM <pattern> SELECT <fields> ...");
    }

    let args_upper: Vec<String> = args.iter().map(|a| a.to_uppercase()).collect();

    // Find FROM
    let from_idx = args_upper.iter().position(|a| a == "FROM");
    let select_idx = args_upper.iter().position(|a| a == "SELECT");

    let source = match from_idx {
        Some(i) if i + 1 < args.len() => args[i + 1].clone(),
        _ => return err_frame("ANALYTICS.QUERY: missing FROM <pattern>"),
    };

    // Parse SELECT fields
    let select_exprs = match select_idx {
        Some(si) => {
            let end = find_keyword_after(&args_upper, si + 1);
            parse_select_fields(&args[si + 1..end])
        }
        None => return err_frame("ANALYTICS.QUERY: missing SELECT <fields>"),
    };

    // Parse WHERE
    let filter = {
        let where_idx = args_upper.iter().position(|a| a == "WHERE");
        match where_idx {
            Some(wi) => {
                let end = find_keyword_after(&args_upper, wi + 1);
                parse_filter(&args[wi + 1..end])
            }
            None => None,
        }
    };

    // Parse GROUP BY
    let group_by = {
        let gb_idx = args_upper
            .windows(2)
            .position(|w| w[0] == "GROUP" && w[1] == "BY")
            .map(|i| i + 2);
        match gb_idx {
            Some(i) => {
                let end = find_keyword_after(&args_upper, i);
                args[i..end]
                    .iter()
                    .flat_map(|s| s.split(','))
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect()
            }
            None => vec![],
        }
    };

    // Parse ORDER BY
    let order_by = {
        let ob_idx = args_upper
            .windows(2)
            .position(|w| w[0] == "ORDER" && w[1] == "BY")
            .map(|i| i + 2);
        match ob_idx {
            Some(i) => {
                let end = find_keyword_after(&args_upper, i);
                parse_order_by(&args[i..end])
            }
            None => vec![],
        }
    };

    // Parse LIMIT
    let limit = {
        let lim_idx = args_upper.iter().position(|a| a == "LIMIT");
        lim_idx.and_then(|i| args.get(i + 1).and_then(|v| v.parse().ok()))
    };

    let query = AnalyticsQuery {
        source,
        select: select_exprs,
        filter,
        group_by,
        having: None,
        order_by,
        limit,
        window: None,
    };

    match get_engine().execute(query) {
        Ok(result) => {
            let json = serde_json::to_string(&result).unwrap_or_else(|_| "{}".to_string());
            Frame::Bulk(Some(bytes::Bytes::from(json)))
        }
        Err(e) => err_frame(&e.to_string()),
    }
}

fn handle_count(args: &[String]) -> Frame {
    // ANALYTICS.COUNT <pattern> [WHERE <cond>]
    if args.is_empty() {
        return err_frame("ANALYTICS.COUNT <pattern> [WHERE <cond>]");
    }
    let source = &args[0];
    let filter = if args.len() > 2 && args[1].to_uppercase() == "WHERE" {
        parse_filter(&args[2..])
    } else {
        None
    };

    match get_engine().count(source, filter) {
        Ok(count) => Frame::Integer(count as i64),
        Err(e) => err_frame(&e.to_string()),
    }
}

fn handle_agg(args: &[String], func: AggFunction) -> Frame {
    // ANALYTICS.<AGG> <pattern> <field> [WHERE <cond>]
    if args.len() < 2 {
        return err_frame("Usage: ANALYTICS.<AGG> <pattern> <field> [WHERE <cond>]");
    }
    let source = &args[0];
    let field = &args[1];
    let filter = if args.len() > 3 && args[2].to_uppercase() == "WHERE" {
        parse_filter(&args[3..])
    } else {
        None
    };

    match get_engine().aggregate(source, func, field, filter) {
        Ok(val) => {
            let s = val.to_string();
            Frame::Bulk(Some(bytes::Bytes::from(s)))
        }
        Err(e) => err_frame(&e.to_string()),
    }
}

fn handle_top(args: &[String]) -> Frame {
    // ANALYTICS.TOP <pattern> <field> <k>
    if args.len() < 3 {
        return err_frame("Usage: ANALYTICS.TOP <pattern> <field> <k>");
    }
    let source = &args[0];
    let field = &args[1];
    let k: usize = match args[2].parse() {
        Ok(v) => v,
        Err(_) => return err_frame("ANALYTICS.TOP: k must be a positive integer"),
    };

    match get_engine().top_k(source, field, k) {
        Ok(entries) => {
            let frames: Vec<Frame> = entries
                .into_iter()
                .map(|(key, val)| {
                    Frame::Array(Some(vec![
                        Frame::Bulk(Some(bytes::Bytes::from(key))),
                        Frame::Bulk(Some(bytes::Bytes::from(val.to_string()))),
                    ]))
                })
                .collect();
            Frame::Array(Some(frames))
        }
        Err(e) => err_frame(&e.to_string()),
    }
}

fn handle_histogram(args: &[String]) -> Frame {
    // ANALYTICS.HISTOGRAM <pattern> <field> [BUCKETS <n>]
    if args.len() < 2 {
        return err_frame("Usage: ANALYTICS.HISTOGRAM <pattern> <field> [BUCKETS <n>]");
    }
    let source = &args[0];
    let field = &args[1];
    let buckets: usize = if args.len() >= 4 && args[2].to_uppercase() == "BUCKETS" {
        args[3].parse().unwrap_or(10)
    } else {
        10
    };

    match get_engine().histogram(source, field, buckets) {
        Ok(hist) => {
            let json =
                serde_json::to_string(&hist).unwrap_or_else(|_| "[]".to_string());
            Frame::Bulk(Some(bytes::Bytes::from(json)))
        }
        Err(e) => err_frame(&e.to_string()),
    }
}

fn handle_explain(args: &[String]) -> Frame {
    if args.is_empty() {
        return err_frame("Usage: ANALYTICS.EXPLAIN FROM <pattern> SELECT <fields> ...");
    }

    let args_upper: Vec<String> = args.iter().map(|a| a.to_uppercase()).collect();
    let from_idx = args_upper.iter().position(|a| a == "FROM");
    let select_idx = args_upper.iter().position(|a| a == "SELECT");

    let source = match from_idx {
        Some(i) if i + 1 < args.len() => args[i + 1].clone(),
        _ => return err_frame("ANALYTICS.EXPLAIN: missing FROM <pattern>"),
    };

    let select_exprs = match select_idx {
        Some(si) => {
            let end = find_keyword_after(&args_upper, si + 1);
            parse_select_fields(&args[si + 1..end])
        }
        None => vec![SelectExpr {
            field: "*".to_string(),
            alias: None,
            function: None,
        }],
    };

    let query = AnalyticsQuery {
        source,
        select: select_exprs,
        filter: None,
        group_by: vec![],
        having: None,
        order_by: vec![],
        limit: None,
        window: None,
    };

    let plan = get_engine().explain(&query);
    let json =
        serde_json::to_string(&plan).unwrap_or_else(|_| "{}".to_string());
    Frame::Bulk(Some(bytes::Bytes::from(json)))
}

fn handle_stats() -> Frame {
    let stats = get_engine().stats();
    let json =
        serde_json::to_string(&stats).unwrap_or_else(|_| "{}".to_string());
    Frame::Bulk(Some(bytes::Bytes::from(json)))
}

fn handle_help() -> Frame {
    let help = vec![
        "ANALYTICS.QUERY FROM <pattern> SELECT <fields> [WHERE <cond>] [GROUP BY <fields>] [ORDER BY <field> [DESC]] [LIMIT <n>]",
        "ANALYTICS.COUNT <pattern> [WHERE <cond>]",
        "ANALYTICS.SUM <pattern> <field> [WHERE <cond>]",
        "ANALYTICS.AVG <pattern> <field> [WHERE <cond>]",
        "ANALYTICS.MIN <pattern> <field> [WHERE <cond>]",
        "ANALYTICS.MAX <pattern> <field> [WHERE <cond>]",
        "ANALYTICS.TOP <pattern> <field> <k>",
        "ANALYTICS.HISTOGRAM <pattern> <field> [BUCKETS <n>]",
        "ANALYTICS.EXPLAIN FROM <pattern> SELECT <fields> ...",
        "ANALYTICS.STATS",
        "ANALYTICS.HELP",
    ];
    let frames: Vec<Frame> = help
        .into_iter()
        .map(|s| Frame::Bulk(Some(bytes::Bytes::from(s.to_string()))))
        .collect();
    Frame::Array(Some(frames))
}

// ---------------------------------------------------------------------------
// Parsing helpers
// ---------------------------------------------------------------------------

static KEYWORDS: &[&str] = &[
    "FROM", "SELECT", "WHERE", "GROUP", "ORDER", "LIMIT", "HAVING",
];

fn find_keyword_after(args: &[String], start: usize) -> usize {
    for i in start..args.len() {
        if KEYWORDS.contains(&args[i].as_str()) {
            return i;
        }
    }
    args.len()
}

fn parse_select_fields(tokens: &[String]) -> Vec<SelectExpr> {
    let joined = tokens.join(" ");
    joined
        .split(',')
        .map(|part| {
            let part = part.trim();
            // Check for AGG(field) pattern
            if let Some(open) = part.find('(') {
                if let Some(close) = part.find(')') {
                    let func_name = part[..open].trim().to_uppercase();
                    let field = part[open + 1..close].trim().to_string();
                    let function = match func_name.as_str() {
                        "COUNT" => Some(AggFunction::Count),
                        "SUM" => Some(AggFunction::Sum),
                        "AVG" => Some(AggFunction::Avg),
                        "MIN" => Some(AggFunction::Min),
                        "MAX" => Some(AggFunction::Max),
                        "STDDEV" => Some(AggFunction::StdDev),
                        "VARIANCE" => Some(AggFunction::Variance),
                        "FIRST" => Some(AggFunction::First),
                        "LAST" => Some(AggFunction::Last),
                        _ => None,
                    };
                    return SelectExpr {
                        field,
                        alias: None,
                        function,
                    };
                }
            }
            SelectExpr {
                field: part.to_string(),
                alias: None,
                function: None,
            }
        })
        .collect()
}

fn parse_filter(tokens: &[String]) -> Option<FilterExpr> {
    // Simple: <field> <op> <value>
    if tokens.len() < 3 {
        return None;
    }
    let field = tokens[0].clone();
    let op_str = tokens[1].to_uppercase();
    let value_str = tokens[2..].join(" ");

    let op = match op_str.as_str() {
        "=" | "==" => FilterOp::Eq,
        "!=" | "<>" => FilterOp::Ne,
        ">" => FilterOp::Gt,
        "<" => FilterOp::Lt,
        ">=" => FilterOp::Gte,
        "<=" => FilterOp::Lte,
        "LIKE" => FilterOp::Like,
        _ => return None,
    };

    let value = serde_json::from_str(&value_str)
        .unwrap_or_else(|_| serde_json::Value::String(value_str));

    Some(FilterExpr { field, op, value })
}

fn parse_order_by(tokens: &[String]) -> Vec<OrderExpr> {
    let mut result = Vec::new();
    let mut i = 0;
    while i < tokens.len() {
        let field = tokens[i].clone();
        if KEYWORDS.contains(&field.to_uppercase().as_str()) {
            break;
        }
        let descending = if i + 1 < tokens.len() && tokens[i + 1].to_uppercase() == "DESC" {
            i += 1;
            true
        } else {
            if i + 1 < tokens.len() && tokens[i + 1].to_uppercase() == "ASC" {
                i += 1;
            }
            false
        };
        result.push(OrderExpr {
            field: field.trim_end_matches(',').to_string(),
            descending,
        });
        i += 1;
    }
    result
}
