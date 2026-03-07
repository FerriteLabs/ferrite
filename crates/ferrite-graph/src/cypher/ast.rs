//! Cypher query AST (Abstract Syntax Tree)
//!
//! Defines the AST node types for a subset of the Cypher query language.

use std::collections::HashMap;

use crate::graph::PropertyValue;

/// A complete Cypher statement.
#[derive(Debug, Clone)]
pub enum CypherStatement {
    /// A read query: MATCH ... WHERE ... RETURN ...
    Query(CypherQuery),
    /// A CREATE statement
    Create(CreateClause),
    /// A combined MATCH + CREATE statement
    MatchCreate {
        /// Match clause
        match_clause: MatchPattern,
        /// Create clause
        create_clause: CreateClause,
    },
    /// A MATCH + DELETE statement
    MatchDelete {
        /// Match clause
        match_clause: MatchPattern,
        /// Optional WHERE filter
        where_clause: Option<WhereExpr>,
        /// Delete clause
        delete_clause: DeleteClause,
    },
    /// A MATCH + SET statement (update properties/labels)
    MatchSet {
        /// Match clause
        match_clause: MatchPattern,
        /// Optional WHERE filter
        where_clause: Option<WhereExpr>,
        /// Set clause
        set_clause: SetClause,
    },
    /// A MATCH + REMOVE statement (remove properties/labels)
    MatchRemove {
        /// Match clause
        match_clause: MatchPattern,
        /// Optional WHERE filter
        where_clause: Option<WhereExpr>,
        /// Remove clause
        remove_clause: RemoveClause,
    },
}

/// A read-only Cypher query.
#[derive(Debug, Clone)]
pub struct CypherQuery {
    /// MATCH pattern
    pub match_clause: MatchPattern,
    /// Optional OPTIONAL MATCH pattern
    pub optional_match: Option<MatchPattern>,
    /// Optional WHERE clause
    pub where_clause: Option<WhereExpr>,
    /// RETURN clause
    pub return_clause: ReturnClause,
    /// Optional ORDER BY
    pub order_by: Option<Vec<OrderByItem>>,
    /// Optional LIMIT
    pub limit: Option<usize>,
    /// Optional SKIP
    pub skip: Option<usize>,
}

/// MATCH clause containing one or more patterns separated by commas.
#[derive(Debug, Clone)]
pub struct MatchPattern {
    /// The pattern elements
    pub patterns: Vec<PatternPart>,
}

/// A single pattern part, e.g. `(a:Person)-[:KNOWS]->(b)`.
#[derive(Debug, Clone)]
pub struct PatternPart {
    /// Starting node
    pub start: NodePatternAst,
    /// Chain of relationship + node pairs
    pub chain: Vec<(RelPatternAst, NodePatternAst)>,
}

/// A node pattern like `(n:Person {name: "Alice"})`.
#[derive(Debug, Clone)]
pub struct NodePatternAst {
    /// Optional variable name
    pub variable: Option<String>,
    /// Optional labels
    pub labels: Vec<String>,
    /// Optional inline properties
    pub properties: Option<HashMap<String, PropertyValue>>,
}

/// A relationship pattern like `[:KNOWS*1..3]`.
#[derive(Debug, Clone)]
pub struct RelPatternAst {
    /// Optional variable name
    pub variable: Option<String>,
    /// Optional relationship type
    pub rel_type: Option<String>,
    /// Direction
    pub direction: RelDirection,
    /// Variable-length path bounds (None = single hop)
    pub var_length: Option<(usize, usize)>,
    /// Optional inline properties
    pub properties: Option<HashMap<String, PropertyValue>>,
}

/// Relationship direction in a pattern.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelDirection {
    /// `(a)-[]->(b)`
    Out,
    /// `(a)<-[]-(b)`
    In,
    /// `(a)-[]-(b)`
    Both,
}

/// WHERE expression tree.
#[derive(Debug, Clone)]
pub enum WhereExpr {
    /// `variable.property op value`
    Comparison {
        /// Left side: variable.property
        left: PropertyAccess,
        /// Comparison operator
        op: CypherOp,
        /// Right side value
        right: Expr,
    },
    /// AND of expressions
    And(Box<WhereExpr>, Box<WhereExpr>),
    /// OR of expressions
    Or(Box<WhereExpr>, Box<WhereExpr>),
    /// NOT expression
    Not(Box<WhereExpr>),
}

/// A property access like `n.age`.
#[derive(Debug, Clone)]
pub struct PropertyAccess {
    /// Variable name
    pub variable: String,
    /// Property name
    pub property: String,
}

/// Comparison operator in WHERE clause.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CypherOp {
    /// `=`
    Eq,
    /// `<>` or `!=`
    Ne,
    /// `<`
    Lt,
    /// `<=`
    Le,
    /// `>`
    Gt,
    /// `>=`
    Ge,
    /// `CONTAINS`
    Contains,
    /// `STARTS WITH`
    StartsWith,
    /// `ENDS WITH`
    EndsWith,
}

/// An expression (right-hand side of comparison or return item).
#[derive(Debug, Clone)]
pub enum Expr {
    /// Literal value
    Literal(PropertyValue),
    /// Property access
    Property(PropertyAccess),
    /// A variable reference (whole node/edge)
    Variable(String),
    /// Aggregation function
    Aggregate(AggregateFunc, Box<Expr>),
}

/// Aggregate functions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateFunc {
    /// COUNT
    Count,
    /// SUM
    Sum,
    /// AVG
    Avg,
    /// MIN
    Min,
    /// MAX
    Max,
}

/// RETURN clause.
#[derive(Debug, Clone)]
pub struct ReturnClause {
    /// Items to return
    pub items: Vec<ReturnItem>,
    /// DISTINCT flag
    pub distinct: bool,
}

/// A single return item.
#[derive(Debug, Clone)]
pub struct ReturnItem {
    /// The expression
    pub expr: Expr,
    /// Optional alias (`AS name`)
    pub alias: Option<String>,
}

/// ORDER BY item.
#[derive(Debug, Clone)]
pub struct OrderByItem {
    /// Expression to sort by
    pub expr: Expr,
    /// True = ascending, false = descending
    pub ascending: bool,
}

/// CREATE clause.
#[derive(Debug, Clone)]
pub struct CreateClause {
    /// Elements to create
    pub elements: Vec<CreateElement>,
}

/// An element to create.
#[derive(Debug, Clone)]
pub enum CreateElement {
    /// Create a node
    Node(NodePatternAst),
    /// Create a relationship between two existing variables
    Relationship {
        /// From variable
        from: String,
        /// Relationship type
        rel_type: String,
        /// To variable
        to: String,
        /// Optional properties
        properties: Option<HashMap<String, PropertyValue>>,
    },
}

/// DELETE clause.
#[derive(Debug, Clone)]
pub struct DeleteClause {
    /// Variables to delete
    pub variables: Vec<String>,
    /// Whether to use DETACH DELETE (also removes connected edges)
    pub detach: bool,
}

/// SET clause for updating properties and labels.
#[derive(Debug, Clone)]
pub struct SetClause {
    /// Items to set
    pub items: Vec<SetItem>,
}

/// A single SET item.
#[derive(Debug, Clone)]
pub enum SetItem {
    /// Set a property: `n.name = 'value'`
    Property {
        /// Variable name
        variable: String,
        /// Property name
        property: String,
        /// Value to set
        value: Expr,
    },
    /// Add a label: `n:NewLabel`
    Label {
        /// Variable name
        variable: String,
        /// Label to add
        label: String,
    },
}

/// REMOVE clause for removing properties and labels.
#[derive(Debug, Clone)]
pub struct RemoveClause {
    /// Items to remove
    pub items: Vec<RemoveItem>,
}

/// A single REMOVE item.
#[derive(Debug, Clone)]
pub enum RemoveItem {
    /// Remove a property: `n.name`
    Property {
        /// Variable name
        variable: String,
        /// Property name
        property: String,
    },
    /// Remove a label: `n:Label`
    Label {
        /// Variable name
        variable: String,
        /// Label to remove
        label: String,
    },
}
