//! JSONPath AST types (RFC 9535 subset)

use std::fmt;

/// A parsed JSONPath expression
#[derive(Debug, Clone, PartialEq)]
pub struct JsonPath {
    /// Path segments following the root `$`
    pub segments: Vec<Segment>,
}

impl JsonPath {
    /// Create a new JSONPath from segments
    pub fn new(segments: Vec<Segment>) -> Self {
        Self { segments }
    }

    /// Check if this is the root path `$`
    pub fn is_root(&self) -> bool {
        self.segments.is_empty()
    }
}

impl fmt::Display for JsonPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "$")?;
        for seg in &self.segments {
            write!(f, "{}", seg)?;
        }
        Ok(())
    }
}

/// A segment in a JSONPath expression
#[derive(Debug, Clone, PartialEq)]
pub enum Segment {
    /// Direct child access: `.key` or `['key']`
    Child(String),
    /// Array index: `[0]`, `[-1]`
    Index(i64),
    /// Array slice: `[start:end]` or `[start:end:step]`
    Slice {
        start: Option<i64>,
        end: Option<i64>,
        step: Option<i64>,
    },
    /// Wildcard: `[*]` or `.*`
    Wildcard,
    /// Recursive descent: `..key`
    RecursiveDescent(String),
    /// Recursive descent wildcard: `..*`
    RecursiveWildcard,
    /// Filter expression: `[?expr]`
    Filter(FilterExpr),
}

impl fmt::Display for Segment {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Segment::Child(key) => write!(f, ".{}", key),
            Segment::Index(i) => write!(f, "[{}]", i),
            Segment::Slice { start, end, step } => {
                write!(f, "[")?;
                if let Some(s) = start {
                    write!(f, "{}", s)?;
                }
                write!(f, ":")?;
                if let Some(e) = end {
                    write!(f, "{}", e)?;
                }
                if let Some(st) = step {
                    write!(f, ":{}", st)?;
                }
                write!(f, "]")
            }
            Segment::Wildcard => write!(f, "[*]"),
            Segment::RecursiveDescent(key) => write!(f, "..{}", key),
            Segment::RecursiveWildcard => write!(f, "..*"),
            Segment::Filter(expr) => write!(f, "[?{}]", expr),
        }
    }
}

/// Filter expression in a JSONPath query
#[derive(Debug, Clone, PartialEq)]
pub enum FilterExpr {
    /// Comparison: `@.field op value`
    Comparison {
        left: FilterOperand,
        op: ComparisonOp,
        right: FilterOperand,
    },
    /// Logical AND
    And(Box<FilterExpr>, Box<FilterExpr>),
    /// Logical OR
    Or(Box<FilterExpr>, Box<FilterExpr>),
    /// Logical NOT
    Not(Box<FilterExpr>),
    /// Existence check: `@.field`
    Exists(FilterPath),
}

impl fmt::Display for FilterExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FilterExpr::Comparison { left, op, right } => {
                write!(f, "{} {} {}", left, op, right)
            }
            FilterExpr::And(a, b) => write!(f, "{} && {}", a, b),
            FilterExpr::Or(a, b) => write!(f, "{} || {}", a, b),
            FilterExpr::Not(e) => write!(f, "!{}", e),
            FilterExpr::Exists(p) => write!(f, "{}", p),
        }
    }
}

/// An operand in a filter comparison
#[derive(Debug, Clone, PartialEq)]
pub enum FilterOperand {
    /// Path reference: `@.field`
    Path(FilterPath),
    /// Literal value
    Literal(LiteralValue),
}

impl fmt::Display for FilterOperand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FilterOperand::Path(p) => write!(f, "{}", p),
            FilterOperand::Literal(v) => write!(f, "{}", v),
        }
    }
}

/// A path in a filter expression (relative to current element `@`)
#[derive(Debug, Clone, PartialEq)]
pub struct FilterPath {
    /// Field segments from `@`
    pub segments: Vec<String>,
}

impl fmt::Display for FilterPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "@")?;
        for seg in &self.segments {
            write!(f, ".{}", seg)?;
        }
        Ok(())
    }
}

/// A literal value in a filter expression
#[derive(Debug, Clone, PartialEq)]
pub enum LiteralValue {
    /// String literal
    String(String),
    /// Integer literal
    Integer(i64),
    /// Float literal
    Float(f64),
    /// Boolean literal
    Bool(bool),
    /// Null literal
    Null,
}

impl fmt::Display for LiteralValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LiteralValue::String(s) => write!(f, "\"{}\"", s),
            LiteralValue::Integer(i) => write!(f, "{}", i),
            LiteralValue::Float(v) => write!(f, "{}", v),
            LiteralValue::Bool(b) => write!(f, "{}", b),
            LiteralValue::Null => write!(f, "null"),
        }
    }
}

/// Comparison operators for filter expressions
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ComparisonOp {
    /// Equal `==`
    Eq,
    /// Not equal `!=`
    Ne,
    /// Less than `<`
    Lt,
    /// Less than or equal `<=`
    Le,
    /// Greater than `>`
    Gt,
    /// Greater than or equal `>=`
    Ge,
}

impl fmt::Display for ComparisonOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ComparisonOp::Eq => write!(f, "=="),
            ComparisonOp::Ne => write!(f, "!="),
            ComparisonOp::Lt => write!(f, "<"),
            ComparisonOp::Le => write!(f, "<="),
            ComparisonOp::Gt => write!(f, ">"),
            ComparisonOp::Ge => write!(f, ">="),
        }
    }
}
