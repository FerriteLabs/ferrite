//! FerriteQL Type System
//!
//! Defines the data types used in the query language.

use serde::{Deserialize, Serialize};

/// Data types supported by FerriteQL
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DataType {
    /// Null type
    Null,
    /// Boolean type
    Boolean,
    /// 64-bit signed integer
    Integer,
    /// 64-bit floating point
    Float,
    /// UTF-8 string
    String,
    /// Binary data
    Bytes,
    /// Array of values
    Array(Box<DataType>),
    /// Map/Object type
    Map,
    /// Any type (for dynamic typing)
    Any,
}

impl DataType {
    /// Check if a type is numeric
    pub fn is_numeric(&self) -> bool {
        matches!(self, DataType::Integer | DataType::Float)
    }

    /// Check if a type is comparable
    pub fn is_comparable(&self) -> bool {
        matches!(
            self,
            DataType::Integer | DataType::Float | DataType::String | DataType::Boolean
        )
    }

    /// Check if this type can be coerced to another
    pub fn can_coerce_to(&self, target: &DataType) -> bool {
        if self == target || *target == DataType::Any || *self == DataType::Any {
            return true;
        }

        match (self, target) {
            // Numeric coercions
            (DataType::Integer, DataType::Float) => true,
            // String coercions
            (DataType::Integer, DataType::String) => true,
            (DataType::Float, DataType::String) => true,
            (DataType::Boolean, DataType::String) => true,
            // Null can coerce to anything
            (DataType::Null, _) => true,
            _ => false,
        }
    }

    /// Get the result type of a binary operation
    pub fn binary_result(&self, other: &DataType) -> Option<DataType> {
        match (self, other) {
            // Same types
            (a, b) if a == b => Some(a.clone()),
            // Numeric promotion
            (DataType::Integer, DataType::Float) | (DataType::Float, DataType::Integer) => {
                Some(DataType::Float)
            }
            // String concatenation
            (DataType::String, _) | (_, DataType::String) => Some(DataType::String),
            // Any type
            (DataType::Any, other) | (other, DataType::Any) => Some(other.clone()),
            // Null propagation
            (DataType::Null, _) | (_, DataType::Null) => Some(DataType::Null),
            _ => None,
        }
    }
}

impl std::fmt::Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DataType::Null => write!(f, "NULL"),
            DataType::Boolean => write!(f, "BOOLEAN"),
            DataType::Integer => write!(f, "INTEGER"),
            DataType::Float => write!(f, "FLOAT"),
            DataType::String => write!(f, "STRING"),
            DataType::Bytes => write!(f, "BYTES"),
            DataType::Array(inner) => write!(f, "ARRAY<{}>", inner),
            DataType::Map => write!(f, "MAP"),
            DataType::Any => write!(f, "ANY"),
        }
    }
}

/// Type information for a column
#[derive(Clone, Debug)]
pub struct ColumnType {
    /// Column name
    pub name: String,
    /// Data type
    pub data_type: DataType,
    /// Whether the column can be null
    pub nullable: bool,
}

impl ColumnType {
    /// Creates a new nullable column with the given name and data type.
    pub fn new(name: impl Into<String>, data_type: DataType) -> Self {
        Self {
            name: name.into(),
            data_type,
            nullable: true,
        }
    }

    /// Sets this column as non-nullable (builder pattern).
    pub fn not_null(mut self) -> Self {
        self.nullable = false;
        self
    }
}

/// Schema for a result set
#[derive(Clone, Debug, Default)]
pub struct Schema {
    /// Columns in the schema
    pub columns: Vec<ColumnType>,
}

impl Schema {
    /// Creates a new schema with the given columns.
    pub fn new(columns: Vec<ColumnType>) -> Self {
        Self { columns }
    }

    /// Creates an empty schema with no columns.
    pub fn empty() -> Self {
        Self {
            columns: Vec::new(),
        }
    }

    /// Appends a column to the schema.
    pub fn add_column(&mut self, column: ColumnType) {
        self.columns.push(column);
    }

    /// Returns the column with the given name, if it exists.
    pub fn get_column(&self, name: &str) -> Option<&ColumnType> {
        self.columns.iter().find(|c| c.name == name)
    }

    /// Returns the zero-based index of the named column, if it exists.
    pub fn get_column_index(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|c| c.name == name)
    }

    /// Returns a list of all column names in order.
    pub fn column_names(&self) -> Vec<&str> {
        self.columns.iter().map(|c| c.name.as_str()).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_type_coercion() {
        assert!(DataType::Integer.can_coerce_to(&DataType::Float));
        assert!(DataType::Integer.can_coerce_to(&DataType::String));
        assert!(DataType::Null.can_coerce_to(&DataType::Integer));
        assert!(!DataType::String.can_coerce_to(&DataType::Integer));
    }

    #[test]
    fn test_binary_result() {
        assert_eq!(
            DataType::Integer.binary_result(&DataType::Float),
            Some(DataType::Float)
        );
        assert_eq!(
            DataType::Integer.binary_result(&DataType::Integer),
            Some(DataType::Integer)
        );
    }

    #[test]
    fn test_schema() {
        let mut schema = Schema::empty();
        schema.add_column(ColumnType::new("id", DataType::Integer));
        schema.add_column(ColumnType::new("name", DataType::String));

        assert_eq!(schema.columns.len(), 2);
        assert_eq!(schema.get_column_index("name"), Some(1));
        assert!(schema.get_column("id").is_some());
    }
}
