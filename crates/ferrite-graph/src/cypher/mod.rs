//! Cypher query language support
//!
//! Implements a subset of the Cypher graph query language including:
//! - `MATCH` patterns (nodes, relationships, variable-length paths)
//! - `OPTIONAL MATCH` for left-join semantics
//! - `WHERE` clauses with comparison operators
//! - `RETURN` with property access and aggregation (count, sum, avg, min, max)
//! - `ORDER BY`, `LIMIT`, `SKIP`
//! - `CREATE` for nodes and relationships
//! - `DELETE` / `DETACH DELETE` for removing nodes and edges
//! - `SET` for updating properties and adding labels
//! - `REMOVE` for removing properties and labels
//!
//! # Examples
//!
//! ```ignore
//! // Find all people
//! MATCH (n:Person) RETURN n
//!
//! // Filter by property
//! MATCH (n:Person {name: 'Alice'}) RETURN n
//!
//! // Traverse relationships
//! MATCH (a)-[:KNOWS]->(b) RETURN a, b
//!
//! // Variable-length paths
//! MATCH (a)-[:KNOWS*1..3]->(b) RETURN b
//!
//! // WHERE clause
//! MATCH (n:Person) WHERE n.age > 10 RETURN n
//!
//! // Aggregation
//! MATCH (n) RETURN count(n)
//!
//! // Ordering and limits
//! MATCH (n) RETURN n ORDER BY n.name LIMIT 10
//!
//! // Create nodes
//! CREATE (n:Person {name: 'Diana', age: 28})
//!
//! // Create relationships
//! MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:FRIENDS]->(b)
//! ```

pub mod ast;
pub mod executor;
pub mod parser;
pub mod planner;

pub use ast::{
    AggregateFunc, CreateClause, CreateElement, CypherOp, CypherQuery, CypherStatement,
    DeleteClause, Expr, MatchPattern, NodePatternAst, OrderByItem, PropertyAccess, RelDirection,
    RelPatternAst, RemoveClause, RemoveItem, ReturnClause, ReturnItem, SetClause, SetItem,
    WhereExpr,
};
pub use executor::{execute, execute_read_only};
pub use parser::CypherParser;
pub use planner::{plan, PlanStep, QueryPlan};

/// Errors produced by the Cypher parser and executor.
#[derive(Debug, Clone, thiserror::Error)]
pub enum CypherError {
    /// Lexer encountered an invalid token.
    #[error("parse error: {0}")]
    Parse(String),

    /// Expected a specific token but found something else.
    #[error("syntax error: {0}")]
    Syntax(String),

    /// A variable referenced in the query was not bound.
    #[error("unbound variable: {0}")]
    UnboundVariable(String),

    /// Type mismatch during execution (e.g. non-numeric aggregation).
    #[error("type error: {0}")]
    Type(String),

    /// An unsupported Cypher feature was used.
    #[error("unsupported: {0}")]
    Unsupported(String),

    /// Generic execution error.
    #[error("execution error: {0}")]
    Execution(String),
}

impl From<String> for CypherError {
    fn from(s: String) -> Self {
        Self::Execution(s)
    }
}

/// Result alias for Cypher operations.
pub type CypherResult<T> = std::result::Result<T, CypherError>;
