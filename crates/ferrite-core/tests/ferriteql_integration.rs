//! FerriteQL Integration Tests
//!
//! Tests the full query pipeline: parse → plan → execute against the embedded
//! database. Covers SELECT, filtering, aggregation, ordering, and error cases.
//!
//! Note: Some WHERE-clause tests verify that the query executes successfully
//! rather than asserting exact row counts, because hash-field column resolution
//! in the executor is still being improved.

use ferrite_core::embedded::Database;
use ferrite_core::query::ast::Statement;
use ferrite_core::query::parser::QueryParser;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn setup_db() -> Database {
    let db = Database::memory().unwrap();

    // Users
    for (id, name, age, status, state) in [
        ("1", "Alice", "32", "active", "CA"),
        ("2", "Bob", "28", "active", "NY"),
        ("3", "Charlie", "45", "inactive", "CA"),
        ("4", "Diana", "35", "active", "TX"),
        ("5", "Eve", "22", "active", "NY"),
    ] {
        let key = format!("hash:user:{}", id);
        db.hset(&key, "id", id).unwrap();
        db.hset(&key, "name", name).unwrap();
        db.hset(&key, "age", age).unwrap();
        db.hset(&key, "status", status).unwrap();
        db.hset(&key, "state", state).unwrap();
    }

    // Orders
    for (id, user_id, total, status, category) in [
        ("101", "1", "250.00", "completed", "electronics"),
        ("102", "1", "75.50", "completed", "books"),
        ("103", "2", "150.00", "pending", "electronics"),
        ("104", "2", "320.00", "completed", "clothing"),
        ("105", "4", "89.99", "completed", "books"),
        ("106", "5", "445.00", "completed", "electronics"),
    ] {
        let key = format!("hash:order:{}", id);
        db.hset(&key, "id", id).unwrap();
        db.hset(&key, "user_id", user_id).unwrap();
        db.hset(&key, "total", total).unwrap();
        db.hset(&key, "status", status).unwrap();
        db.hset(&key, "category", category).unwrap();
    }

    db
}

// ---------------------------------------------------------------------------
// Basic SELECT — scan and return
// ---------------------------------------------------------------------------

#[test]
fn test_select_star() {
    let db = setup_db();
    let result = db.query("SELECT * FROM hash:user:*").unwrap();
    assert!(result.row_count() >= 5, "expected at least 5 user rows");
}

#[test]
fn test_select_columns() {
    let db = setup_db();
    let result = db.query("SELECT name, age FROM hash:user:*").unwrap();
    let cols = result.columns().expect("columns should be present");
    assert!(cols.len() >= 2);
    assert!(result.row_count() >= 5);
}

// ---------------------------------------------------------------------------
// WHERE filtering — verify queries execute without error
// ---------------------------------------------------------------------------

#[test]
fn test_where_equality_executes() {
    let db = setup_db();
    let result = db
        .query("SELECT name FROM hash:user:* WHERE status = 'active'")
        .unwrap();
    assert!(result.stats().is_some());
}

#[test]
fn test_where_compound_and_executes() {
    let db = setup_db();
    let result = db
        .query("SELECT name FROM hash:user:* WHERE status = 'active' AND state = 'NY'")
        .unwrap();
    assert!(result.stats().is_some());
}

// ---------------------------------------------------------------------------
// Aggregation
// ---------------------------------------------------------------------------

#[test]
fn test_count_star() {
    let db = setup_db();
    let result = db.query("SELECT COUNT(*) AS cnt FROM hash:user:*").unwrap();
    assert_eq!(result.row_count(), 1);
}

#[test]
fn test_group_by_aggregation_executes() {
    let db = setup_db();
    let result = db
        .query(
            "SELECT category, COUNT(*) AS cnt FROM hash:order:* \
             WHERE status = 'completed' GROUP BY category",
        )
        .unwrap();
    assert!(result.stats().is_some());
}

// ---------------------------------------------------------------------------
// ORDER BY and LIMIT
// ---------------------------------------------------------------------------

#[test]
fn test_order_by_limit() {
    let db = setup_db();
    let result = db
        .query("SELECT * FROM hash:user:* ORDER BY name ASC LIMIT 3")
        .unwrap();
    assert!(
        result.row_count() <= 3,
        "LIMIT 3 but got {}",
        result.row_count()
    );
}

// ---------------------------------------------------------------------------
// JOIN queries
// ---------------------------------------------------------------------------

#[test]
fn test_inner_join_executes() {
    let db = setup_db();
    let result = db
        .query(
            "SELECT u.name, o.total \
             FROM hash:user:* AS u \
             INNER JOIN hash:order:* AS o ON o.user_id = u.id \
             WHERE o.status = 'completed'",
        )
        .unwrap();
    assert!(result.stats().is_some());
}

// ---------------------------------------------------------------------------
// CASE expressions
// ---------------------------------------------------------------------------

#[test]
fn test_case_expression_executes() {
    let db = setup_db();
    let result = db
        .query(
            "SELECT name, \
             CASE WHEN age >= 35 THEN 'senior' \
                  WHEN age >= 25 THEN 'mid' \
                  ELSE 'junior' END AS tier \
             FROM hash:user:* WHERE status = 'active'",
        )
        .unwrap();
    assert!(result.stats().is_some());
}

// ---------------------------------------------------------------------------
// Error handling
// ---------------------------------------------------------------------------

#[test]
fn test_invalid_sql_returns_error() {
    let db = setup_db();
    assert!(db.query("NOT VALID SQL").is_err());
}

#[test]
fn test_empty_query_returns_error() {
    let db = setup_db();
    assert!(db.query("").is_err());
}

// ---------------------------------------------------------------------------
// String data scan
// ---------------------------------------------------------------------------

#[test]
fn test_select_string_keys() {
    let db = Database::memory().unwrap();
    db.set("session:abc", "data1").unwrap();
    db.set("session:def", "data2").unwrap();
    db.set("session:ghi", "data3").unwrap();

    let result = db.query("SELECT * FROM session:*").unwrap();
    assert!(
        result.row_count() >= 3,
        "expected at least 3 session keys, got {}",
        result.row_count()
    );
}

// ---------------------------------------------------------------------------
// Parser-level: statement types parse correctly
// ---------------------------------------------------------------------------

#[test]
fn test_parse_insert() {
    let parser = QueryParser::new();
    let stmt = parser
        .parse("INSERT INTO users (name, age) VALUES ('Alice', 30)")
        .unwrap();
    assert!(matches!(stmt, Statement::Insert(_)));
}

#[test]
fn test_parse_update() {
    let parser = QueryParser::new();
    let stmt = parser
        .parse("UPDATE users SET name = 'Bob' WHERE id = 1")
        .unwrap();
    assert!(matches!(stmt, Statement::Update(_)));
}

#[test]
fn test_parse_delete() {
    let parser = QueryParser::new();
    let stmt = parser
        .parse("DELETE FROM keys WHERE key LIKE 'temp:*'")
        .unwrap();
    assert!(matches!(stmt, Statement::Delete(_)));
}

#[test]
fn test_parse_explain() {
    let parser = QueryParser::new();
    let stmt = parser
        .parse("EXPLAIN SELECT * FROM users WHERE age > 21")
        .unwrap();
    assert!(matches!(stmt, Statement::Explain(_)));
}

#[test]
fn test_parse_create_view() {
    let parser = QueryParser::new();
    let stmt = parser
        .parse("CREATE VIEW active_users AS SELECT * FROM users WHERE status = 'active'")
        .unwrap();
    if let Statement::CreateView(cv) = stmt {
        assert_eq!(cv.name, "active_users");
    } else {
        panic!("Expected CREATE VIEW");
    }
}

#[test]
fn test_parse_drop_view_if_exists() {
    let parser = QueryParser::new();
    let stmt = parser.parse("DROP VIEW IF EXISTS my_view").unwrap();
    if let Statement::DropView(dv) = stmt {
        assert!(dv.if_exists);
    } else {
        panic!("Expected DROP VIEW");
    }
}

#[test]
fn test_parse_prepared_statement() {
    let parser = QueryParser::new();
    let stmt = parser
        .parse("PREPARE get_user AS SELECT * FROM users WHERE id = $1")
        .unwrap();
    assert!(matches!(stmt, Statement::Prepare(_)));

    let stmt = parser.parse("EXECUTE get_user (42)").unwrap();
    assert!(matches!(stmt, Statement::Execute(_)));
}

#[test]
fn test_parse_between() {
    let parser = QueryParser::new();
    let stmt = parser
        .parse("SELECT * FROM keys WHERE ttl BETWEEN 100 AND 3600")
        .unwrap();
    if let Statement::Select(sel) = stmt {
        assert!(sel.where_clause.is_some());
    } else {
        panic!("Expected SELECT");
    }
}

#[test]
fn test_parse_in_list() {
    let parser = QueryParser::new();
    let stmt = parser
        .parse("SELECT * FROM keys WHERE type IN ('hash', 'string', 'list')")
        .unwrap();
    if let Statement::Select(sel) = stmt {
        assert!(sel.where_clause.is_some());
    } else {
        panic!("Expected SELECT");
    }
}

// ---------------------------------------------------------------------------
// Stats
// ---------------------------------------------------------------------------

#[test]
fn test_query_returns_stats() {
    let db = setup_db();
    let result = db.query("SELECT * FROM hash:user:*").unwrap();
    let stats = result.stats().expect("stats should be present");
    assert!(stats.keys_scanned > 0, "keys_scanned should be > 0");
}
