//! FerriteQL Abstract Syntax Tree
//!
//! Defines the AST nodes for the query language.

use serde::{Deserialize, Serialize};

/// A complete SQL statement
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Statement {
    /// SELECT query
    Select(SelectStatement),
    /// INSERT statement (for views)
    Insert(InsertStatement),
    /// UPDATE statement
    Update(UpdateStatement),
    /// DELETE statement
    Delete(DeleteStatement),
    /// CREATE VIEW
    CreateView(CreateViewStatement),
    /// DROP VIEW
    DropView(DropViewStatement),
    /// EXPLAIN query
    Explain(Box<Statement>),
    /// PREPARE statement
    Prepare(PrepareStatement),
    /// EXECUTE prepared statement
    Execute(ExecuteStatement),
}

/// SELECT statement
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct SelectStatement {
    /// Columns to select
    pub columns: Vec<SelectColumn>,
    /// FROM clause (key patterns)
    pub from: Vec<FromClause>,
    /// JOIN clauses
    pub joins: Vec<JoinClause>,
    /// WHERE clause
    pub where_clause: Option<Expr>,
    /// GROUP BY clause
    pub group_by: Vec<Expr>,
    /// HAVING clause
    pub having: Option<Expr>,
    /// ORDER BY clause
    pub order_by: Vec<OrderByItem>,
    /// LIMIT
    pub limit: Option<u64>,
    /// OFFSET
    pub offset: Option<u64>,
    /// DISTINCT
    pub distinct: bool,
}

/// Column in SELECT clause
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum SelectColumn {
    /// All columns (*)
    All,
    /// All columns from a specific table (table.*)
    AllFrom(String),
    /// Expression with optional alias
    Expr {
        /// The expression
        expr: Expr,
        /// Optional alias name
        alias: Option<String>,
    },
}

/// FROM clause - represents a key pattern source
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FromClause {
    /// Key pattern (e.g., "users:*")
    pub pattern: String,
    /// Alias for the source
    pub alias: Option<String>,
}

/// JOIN clause
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct JoinClause {
    /// Type of join
    pub join_type: JoinType,
    /// Source to join
    pub source: FromClause,
    /// Join condition
    pub on: Option<Expr>,
}

/// Type of join
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum JoinType {
    /// Inner join
    Inner,
    /// Left outer join
    Left,
    /// Right outer join
    Right,
    /// Full outer join
    Full,
    /// Cross join (cartesian product)
    Cross,
}

/// ORDER BY item
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OrderByItem {
    /// Expression to order by
    pub expr: Expr,
    /// Sort direction
    pub direction: SortDirection,
    /// Nulls first or last
    pub nulls: Option<NullsOrder>,
}

/// Sort direction
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SortDirection {
    /// Ascending order
    Asc,
    /// Descending order
    Desc,
}

/// Nulls ordering
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum NullsOrder {
    /// Nulls sort before all other values
    First,
    /// Nulls sort after all other values
    Last,
}

/// Expression node
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Expr {
    /// Literal value
    Literal(Literal),
    /// Column reference (optionally qualified)
    Column(ColumnRef),
    /// Binary operation
    BinaryOp {
        /// Left-hand side expression
        left: Box<Expr>,
        /// The binary operator
        op: BinaryOperator,
        /// Right-hand side expression
        right: Box<Expr>,
    },
    /// Unary operation
    UnaryOp {
        /// The unary operator
        op: UnaryOperator,
        /// The operand expression
        expr: Box<Expr>,
    },
    /// Function call
    Function(FunctionCall),
    /// CASE expression
    Case {
        /// Optional operand for simple CASE
        operand: Option<Box<Expr>>,
        /// WHEN condition THEN result pairs
        when_clauses: Vec<(Expr, Expr)>,
        /// ELSE result
        else_clause: Option<Box<Expr>>,
    },
    /// Subquery
    Subquery(Box<SelectStatement>),
    /// IN expression with a list of values
    In {
        /// Expression to test
        expr: Box<Expr>,
        /// List of values
        list: Vec<Expr>,
        /// True if NOT IN
        negated: bool,
    },
    /// IN expression with a subquery
    InSubquery {
        /// Expression to test
        expr: Box<Expr>,
        /// Subquery that produces the value list
        subquery: Box<SelectStatement>,
        /// True if NOT IN
        negated: bool,
    },
    /// BETWEEN expression
    Between {
        /// Expression to test
        expr: Box<Expr>,
        /// Lower bound
        low: Box<Expr>,
        /// Upper bound
        high: Box<Expr>,
        /// True if NOT BETWEEN
        negated: bool,
    },
    /// LIKE expression
    Like {
        /// Expression to test
        expr: Box<Expr>,
        /// Pattern to match against
        pattern: Box<Expr>,
        /// True if NOT LIKE
        negated: bool,
    },
    /// IS NULL expression
    IsNull {
        /// Expression to test
        expr: Box<Expr>,
        /// True if IS NOT NULL
        negated: bool,
    },
    /// EXISTS subquery
    Exists(Box<SelectStatement>),
    /// Parameter placeholder ($1, $2, etc.)
    Parameter(usize),
    /// Cast expression
    Cast {
        /// Expression to cast
        expr: Box<Expr>,
        /// Target data type
        data_type: String,
    },
    /// Nested expression (parentheses)
    Nested(Box<Expr>),
}

/// Literal value
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Literal {
    /// SQL NULL
    Null,
    /// Boolean value
    Boolean(bool),
    /// Integer value
    Integer(i64),
    /// Floating-point value
    Float(f64),
    /// String value
    String(String),
}

/// Column reference
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ColumnRef {
    /// Table/alias name (optional)
    pub table: Option<String>,
    /// Column name
    pub column: String,
}

impl ColumnRef {
    /// Creates an unqualified column reference.
    pub fn new(column: impl Into<String>) -> Self {
        Self {
            table: None,
            column: column.into(),
        }
    }

    /// Creates a table-qualified column reference.
    pub fn qualified(table: impl Into<String>, column: impl Into<String>) -> Self {
        Self {
            table: Some(table.into()),
            column: column.into(),
        }
    }
}

/// Binary operators
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum BinaryOperator {
    // Arithmetic
    /// Addition (`+`)
    Add,
    /// Subtraction (`-`)
    Subtract,
    /// Multiplication (`*`)
    Multiply,
    /// Division (`/`)
    Divide,
    /// Modulo (`%`)
    Modulo,
    // Comparison
    /// Equal (`=`)
    Equal,
    /// Not equal (`!=`)
    NotEqual,
    /// Less than (`<`)
    LessThan,
    /// Less than or equal (`<=`)
    LessThanOrEqual,
    /// Greater than (`>`)
    GreaterThan,
    /// Greater than or equal (`>=`)
    GreaterThanOrEqual,
    // Logical
    /// Logical AND
    And,
    /// Logical OR
    Or,
    // String
    /// String concatenation (`||`)
    Concat,
}

impl std::fmt::Display for BinaryOperator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BinaryOperator::Add => write!(f, "+"),
            BinaryOperator::Subtract => write!(f, "-"),
            BinaryOperator::Multiply => write!(f, "*"),
            BinaryOperator::Divide => write!(f, "/"),
            BinaryOperator::Modulo => write!(f, "%"),
            BinaryOperator::Equal => write!(f, "="),
            BinaryOperator::NotEqual => write!(f, "!="),
            BinaryOperator::LessThan => write!(f, "<"),
            BinaryOperator::LessThanOrEqual => write!(f, "<="),
            BinaryOperator::GreaterThan => write!(f, ">"),
            BinaryOperator::GreaterThanOrEqual => write!(f, ">="),
            BinaryOperator::And => write!(f, "AND"),
            BinaryOperator::Or => write!(f, "OR"),
            BinaryOperator::Concat => write!(f, "||"),
        }
    }
}

/// Unary operators
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum UnaryOperator {
    /// Logical NOT
    Not,
    /// Numeric negation
    Minus,
    /// Numeric positive (no-op)
    Plus,
}

/// Function call
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FunctionCall {
    /// Function name
    pub name: String,
    /// Arguments
    pub args: Vec<Expr>,
    /// DISTINCT modifier (for aggregates)
    pub distinct: bool,
    /// Filter clause (for aggregates)
    pub filter: Option<Box<Expr>>,
    /// OVER clause (for window functions)
    pub over: Option<WindowSpec>,
}

/// Window specification
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WindowSpec {
    /// PARTITION BY
    pub partition_by: Vec<Expr>,
    /// ORDER BY
    pub order_by: Vec<OrderByItem>,
    /// Window frame
    pub frame: Option<WindowFrame>,
}

/// Window frame
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WindowFrame {
    /// Frame type
    pub frame_type: FrameType,
    /// Start bound
    pub start: FrameBound,
    /// End bound
    pub end: Option<FrameBound>,
}

/// Frame type
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum FrameType {
    /// Row-based frame
    Rows,
    /// Range-based frame
    Range,
    /// Groups-based frame
    Groups,
}

/// Frame bound
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum FrameBound {
    /// Current row position
    CurrentRow,
    /// N rows/range before current row (unbounded if `None`)
    Preceding(Option<u64>),
    /// N rows/range after current row (unbounded if `None`)
    Following(Option<u64>),
}

/// INSERT statement
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InsertStatement {
    /// Target pattern
    pub target: String,
    /// Column names
    pub columns: Vec<String>,
    /// Values to insert
    pub values: Vec<Vec<Expr>>,
}

/// UPDATE statement
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct UpdateStatement {
    /// Target pattern
    pub target: String,
    /// SET assignments
    pub assignments: Vec<(String, Expr)>,
    /// WHERE clause
    pub where_clause: Option<Expr>,
}

/// DELETE statement
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DeleteStatement {
    /// Target pattern
    pub target: String,
    /// WHERE clause
    pub where_clause: Option<Expr>,
}

/// CREATE VIEW statement
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CreateViewStatement {
    /// View name
    pub name: String,
    /// Underlying query
    pub query: SelectStatement,
    /// Whether to replace if exists
    pub or_replace: bool,
    /// Materialized view options
    pub materialized: Option<MaterializedOptions>,
}

/// Materialized view options
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MaterializedOptions {
    /// Refresh type
    pub refresh: RefreshType,
    /// Refresh interval in milliseconds
    pub refresh_interval_ms: Option<u64>,
}

/// Refresh type for materialized views
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RefreshType {
    /// Full refresh
    Complete,
    /// Incremental refresh
    Incremental,
    /// On-demand refresh
    OnDemand,
}

/// DROP VIEW statement
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DropViewStatement {
    /// View name
    pub name: String,
    /// IF EXISTS
    pub if_exists: bool,
}

/// PREPARE statement
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PrepareStatement {
    /// Statement name
    pub name: String,
    /// The query to prepare
    pub query: Box<Statement>,
}

/// EXECUTE statement
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExecuteStatement {
    /// Statement name
    pub name: String,
    /// Parameter values
    pub params: Vec<Expr>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_column_ref() {
        let simple = ColumnRef::new("name");
        assert!(simple.table.is_none());
        assert_eq!(simple.column, "name");

        let qualified = ColumnRef::qualified("users", "name");
        assert_eq!(qualified.table, Some("users".to_string()));
        assert_eq!(qualified.column, "name");
    }

    #[test]
    fn test_select_default() {
        let select = SelectStatement::default();
        assert!(select.columns.is_empty());
        assert!(select.from.is_empty());
        assert!(select.where_clause.is_none());
        assert!(!select.distinct);
    }
}
