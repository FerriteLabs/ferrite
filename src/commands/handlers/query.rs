//! FerriteQL query command handlers
//!
//! This module handles the QUERY.* command family, bridging the RESP protocol
//! layer to the FerriteQL query engine in `ferrite-core`.
//!
//! ## Commands
//!
//! - `QUERY "<sql>"` — Execute a FerriteQL query
//! - `QUERY.JSON "<sql>"` — Execute and return results as JSON
//! - `QUERY.EXPLAIN "<sql>"` — Show query execution plan
//! - `QUERY.PREPARE <name> "<sql>"` — Create a prepared statement
//! - `QUERY.EXEC <name> [params...]` — Execute a prepared statement
//! - `QUERY.HELP` — Show available commands
//! - `QUERY.VERSION` — Show FerriteQL version

use bytes::Bytes;

use crate::protocol::Frame;
use crate::query::{QueryConfig, QueryEngine};

use super::HandlerContext;

/// FerriteQL version
const FERRITEQL_VERSION: &str = "0.1.0";

/// Generate helpful error messages for FerriteQL syntax errors
fn ferriteql_syntax_help(query: &str) -> String {
    let query_upper = query.trim().to_uppercase();

    if query_upper.starts_with("SELECT") && !query_upper.contains("FROM") {
        return "Syntax error: SELECT requires FROM clause. Example: SELECT * FROM keys WHERE pattern = 'user:*'".to_string();
    }

    if query_upper.starts_with("INSERT") {
        return "FerriteQL uses SET for writes. Example: SET key = 'value' TTL 3600".to_string();
    }

    if query_upper.starts_with("UPDATE") {
        return "FerriteQL uses SET for updates. Example: SET key = 'new_value'".to_string();
    }

    if query_upper.starts_with("DELETE") && !query_upper.starts_with("DELETE FROM") {
        return "Syntax: DELETE FROM keys WHERE key = 'mykey'".to_string();
    }

    "Unrecognized FerriteQL syntax. Try: QUERY HELP for available commands".to_string()
}

/// QUERY "<sql>" — Execute a FerriteQL query
pub async fn query_run(ctx: &HandlerContext<'_>, args: &[Bytes]) -> Frame {
    if args.is_empty() {
        return Frame::error(
            "ERR wrong number of arguments for 'QUERY' command. Usage: QUERY \"<sql>\"",
        );
    }

    let sql = match std::str::from_utf8(&args[0]) {
        Ok(s) => s,
        Err(_) => return Frame::error("ERR query must be a valid UTF-8 string"),
    };

    if sql.trim().is_empty() {
        return Frame::error("ERR empty query string");
    }

    let engine = QueryEngine::new(ctx.store.clone(), QueryConfig::default());
    match engine.execute(sql, ctx.db).await {
        Ok(result) => result.to_frame(),
        Err(e) => {
            let help = ferriteql_syntax_help(sql);
            Frame::error(format!("ERR FerriteQL: {}. Hint: {}", e, help))
        }
    }
}

/// QUERY.JSON "<sql>" — Execute and return results as a JSON-encoded bulk string
pub async fn query_json(ctx: &HandlerContext<'_>, args: &[Bytes]) -> Frame {
    if args.is_empty() {
        return Frame::error(
            "ERR wrong number of arguments for 'QUERY.JSON' command. Usage: QUERY.JSON \"<sql>\"",
        );
    }

    let sql = match std::str::from_utf8(&args[0]) {
        Ok(s) => s,
        Err(_) => return Frame::error("ERR query must be a valid UTF-8 string"),
    };

    if sql.trim().is_empty() {
        return Frame::error("ERR empty query string");
    }

    let engine = QueryEngine::new(ctx.store.clone(), QueryConfig::default());
    match engine.execute(sql, ctx.db).await {
        Ok(result) => {
            let mut rows_json = Vec::new();
            for row in &result.rows {
                let mut obj = Vec::new();
                for (i, col) in result.columns.iter().enumerate() {
                    let val = row
                        .values
                        .get(i)
                        .map(|v| format!("{}", v))
                        .unwrap_or_else(|| "null".to_string());
                    obj.push(format!("\"{}\":\"{}\"", col, val));
                }
                rows_json.push(format!("{{{}}}", obj.join(",")));
            }
            let json = format!(
                "{{\"columns\":{:?},\"rows\":[{}],\"stats\":{{\"keys_scanned\":{},\"rows_returned\":{},\"execution_time_us\":{}}}}}",
                result.columns,
                rows_json.join(","),
                result.stats.keys_scanned,
                result.stats.rows_returned,
                result.stats.execution_time_us
            );
            Frame::Bulk(Some(Bytes::from(json)))
        }
        Err(e) => {
            let help = ferriteql_syntax_help(sql);
            Frame::error(format!("ERR FerriteQL: {}. Hint: {}", e, help))
        }
    }
}

/// QUERY.EXPLAIN "<sql>" — Show query execution plan
pub fn query_explain(ctx: &HandlerContext<'_>, args: &[Bytes]) -> Frame {
    if args.is_empty() {
        return Frame::error(
            "ERR wrong number of arguments for 'QUERY.EXPLAIN' command. Usage: QUERY.EXPLAIN \"<sql>\"",
        );
    }

    let sql = match std::str::from_utf8(&args[0]) {
        Ok(s) => s,
        Err(_) => return Frame::error("ERR query must be a valid UTF-8 string"),
    };

    if sql.trim().is_empty() {
        return Frame::error("ERR empty query string");
    }

    let engine = QueryEngine::new(ctx.store.clone(), QueryConfig::default());
    match engine.explain(sql) {
        Ok(plan) => Frame::Bulk(Some(Bytes::from(plan))),
        Err(e) => {
            let help = ferriteql_syntax_help(sql);
            Frame::error(format!("ERR FerriteQL: {}. Hint: {}", e, help))
        }
    }
}

/// QUERY.PREPARE <name> "<sql>" — Create a prepared statement
pub async fn query_prepare(ctx: &HandlerContext<'_>, args: &[Bytes]) -> Frame {
    if args.len() < 2 {
        return Frame::error(
            "ERR wrong number of arguments for 'QUERY.PREPARE' command. Usage: QUERY.PREPARE <name> \"<sql>\"",
        );
    }

    let name = match std::str::from_utf8(&args[0]) {
        Ok(s) => s,
        Err(_) => return Frame::error("ERR statement name must be a valid UTF-8 string"),
    };

    let sql = match std::str::from_utf8(&args[1]) {
        Ok(s) => s,
        Err(_) => return Frame::error("ERR query must be a valid UTF-8 string"),
    };

    if name.is_empty() {
        return Frame::error("ERR statement name cannot be empty");
    }

    if sql.trim().is_empty() {
        return Frame::error("ERR empty query string");
    }

    let engine = QueryEngine::new(ctx.store.clone(), QueryConfig::default());
    match engine.prepare(name, sql).await {
        Ok(()) => Frame::Simple(Bytes::from("OK")),
        Err(e) => Frame::error(format!("ERR FerriteQL prepare: {}", e)),
    }
}

/// QUERY.EXEC <name> [params...] — Execute a prepared statement
pub async fn query_exec(ctx: &HandlerContext<'_>, args: &[Bytes]) -> Frame {
    if args.is_empty() {
        return Frame::error(
            "ERR wrong number of arguments for 'QUERY.EXEC' command. Usage: QUERY.EXEC <name> [params...]",
        );
    }

    let name = match std::str::from_utf8(&args[0]) {
        Ok(s) => s,
        Err(_) => return Frame::error("ERR statement name must be a valid UTF-8 string"),
    };

    let params: Vec<crate::query::Value> = args[1..]
        .iter()
        .map(|a| {
            let s = String::from_utf8_lossy(a).to_string();
            if let Ok(n) = s.parse::<i64>() {
                crate::query::Value::Int(n)
            } else if let Ok(f) = s.parse::<f64>() {
                crate::query::Value::Float(f)
            } else {
                crate::query::Value::String(s)
            }
        })
        .collect();

    let engine = QueryEngine::new(ctx.store.clone(), QueryConfig::default());
    match engine.execute_prepared(name, params, ctx.db).await {
        Ok(result) => result.to_frame(),
        Err(e) => Frame::error(format!("ERR FerriteQL exec: {}", e)),
    }
}

/// QUERY HELP — Show available FerriteQL commands
pub fn query_help() -> Frame {
    let help_lines = vec![
        Frame::bulk("FerriteQL — SQL-like query language for Ferrite"),
        Frame::bulk(""),
        Frame::bulk("Commands:"),
        Frame::bulk("  QUERY \"<sql>\"              Execute a FerriteQL query"),
        Frame::bulk("  QUERY.JSON \"<sql>\"         Execute and return JSON results"),
        Frame::bulk("  QUERY.EXPLAIN \"<sql>\"      Show query execution plan"),
        Frame::bulk("  QUERY.PREPARE <name> <sql> Create a prepared statement"),
        Frame::bulk("  QUERY.EXEC <name> [args]   Execute a prepared statement"),
        Frame::bulk("  QUERY.HELP                 Show this help"),
        Frame::bulk("  QUERY.VERSION              Show FerriteQL version"),
        Frame::bulk(""),
        Frame::bulk("SQL Syntax:"),
        Frame::bulk("  SELECT [DISTINCT] <cols> FROM <pattern>"),
        Frame::bulk("    [JOIN <pattern> ON <cond>]"),
        Frame::bulk("    [WHERE <condition>]"),
        Frame::bulk("    [GROUP BY <cols>] [HAVING <cond>]"),
        Frame::bulk("    [ORDER BY <col> [ASC|DESC]]"),
        Frame::bulk("    [LIMIT n] [OFFSET n]"),
        Frame::bulk(""),
        Frame::bulk("Examples:"),
        Frame::bulk("  QUERY \"SELECT * FROM users:* WHERE status = 'active' LIMIT 100\""),
        Frame::bulk("  QUERY \"SELECT type, COUNT(*) FROM keys GROUP BY type\""),
        Frame::bulk("  QUERY.EXPLAIN \"SELECT * FROM orders:* WHERE total > 100\""),
    ];
    Frame::Array(Some(help_lines))
}

/// QUERY VERSION — Show FerriteQL version
pub fn query_version() -> Frame {
    Frame::Bulk(Some(Bytes::from(format!(
        "FerriteQL v{} (experimental)",
        FERRITEQL_VERSION
    ))))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_syntax_help_select_without_from() {
        let help = ferriteql_syntax_help("SELECT * WHERE x = 1");
        assert!(help.contains("SELECT requires FROM clause"));
    }

    #[test]
    fn test_syntax_help_insert() {
        let help = ferriteql_syntax_help("INSERT INTO keys VALUES ('a', 'b')");
        assert!(help.contains("uses SET for writes"));
    }

    #[test]
    fn test_syntax_help_update() {
        let help = ferriteql_syntax_help("UPDATE keys SET val = 'x'");
        assert!(help.contains("uses SET for updates"));
    }

    #[test]
    fn test_syntax_help_delete_without_from() {
        let help = ferriteql_syntax_help("DELETE key1");
        assert!(help.contains("DELETE FROM keys"));
    }

    #[test]
    fn test_syntax_help_unrecognized() {
        let help = ferriteql_syntax_help("FOOBAR something");
        assert!(help.contains("QUERY HELP"));
    }

    #[test]
    fn test_query_help_returns_array() {
        let frame = query_help();
        match frame {
            Frame::Array(Some(items)) => assert!(!items.is_empty()),
            _ => panic!("expected array frame"),
        }
    }

    #[test]
    fn test_query_version_returns_bulk() {
        let frame = query_version();
        match frame {
            Frame::Bulk(Some(b)) => {
                let s = String::from_utf8_lossy(&b);
                assert!(s.contains("FerriteQL v"));
                assert!(s.contains("experimental"));
            }
            _ => panic!("expected bulk frame"),
        }
    }
}
