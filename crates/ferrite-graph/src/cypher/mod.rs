//! Cypher query language support
//!
//! Implements a subset of the Cypher graph query language including:
//! - `MATCH` patterns (nodes, relationships, variable-length paths)
//! - `WHERE` clauses with comparison operators
//! - `RETURN` with property access and aggregation (count, sum, avg, min, max)
//! - `ORDER BY`, `LIMIT`, `SKIP`
//! - `CREATE` for nodes and relationships
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
    AggregateFunc, CreateClause, CreateElement, CypherOp, CypherQuery, CypherStatement, Expr,
    MatchPattern, NodePatternAst, OrderByItem, PropertyAccess, RelDirection, RelPatternAst,
    ReturnClause, ReturnItem, WhereExpr,
};
pub use executor::{execute, execute_read_only};
pub use parser::CypherParser;
pub use planner::{plan, PlanStep, QueryPlan};
