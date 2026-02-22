//! MongoDB-compatible query language implementation

use std::collections::HashSet;

use super::document::Document;
use super::DocumentStoreError;

/// Query operator types
#[derive(Debug, Clone)]
pub enum QueryOperator {
    // Comparison operators
    /// Equal
    Eq(serde_json::Value),
    /// Not equal
    Ne(serde_json::Value),
    /// Greater than
    Gt(serde_json::Value),
    /// Greater than or equal
    Gte(serde_json::Value),
    /// Less than
    Lt(serde_json::Value),
    /// Less than or equal
    Lte(serde_json::Value),
    /// In array
    In(Vec<serde_json::Value>),
    /// Not in array
    Nin(Vec<serde_json::Value>),

    // Logical operators
    /// Logical AND
    And(Vec<DocumentQuery>),
    /// Logical OR
    Or(Vec<DocumentQuery>),
    /// Logical NOT
    Not(Box<QueryOperator>),
    /// Logical NOR
    Nor(Vec<DocumentQuery>),

    // Element operators
    /// Field exists
    Exists(bool),
    /// Type check
    Type(BsonType),

    // Evaluation operators
    /// Regular expression match
    Regex(String, String), // pattern, options
    /// Mod operator
    Mod(i64, i64), // divisor, remainder
    /// Where (JavaScript expression - simplified)
    Where(String),

    // Array operators
    /// All elements match
    All(Vec<serde_json::Value>),
    /// Element match
    ElemMatch(Box<DocumentQuery>),
    /// Array size
    Size(usize),

    // Geospatial operators (simplified)
    /// Near point
    Near {
        x: f64,
        y: f64,
        max_distance: Option<f64>,
    },
    /// Within geometry
    GeoWithin(GeoShape),

    // Text search
    /// Text search
    Text {
        search: String,
        language: Option<String>,
        case_sensitive: bool,
    },
}

/// BSON types for $type operator
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BsonType {
    /// Double
    Double = 1,
    /// String
    String = 2,
    /// Object
    Object = 3,
    /// Array
    Array = 4,
    /// Binary
    Binary = 5,
    /// ObjectId
    ObjectId = 7,
    /// Boolean
    Boolean = 8,
    /// Date
    Date = 9,
    /// Null
    Null = 10,
    /// 32-bit integer
    Int32 = 16,
    /// 64-bit integer
    Int64 = 18,
    /// Number (any numeric type)
    Number = -1,
}

impl BsonType {
    fn from_str(s: &str) -> Option<Self> {
        match s {
            "double" => Some(Self::Double),
            "string" => Some(Self::String),
            "object" => Some(Self::Object),
            "array" => Some(Self::Array),
            "binData" => Some(Self::Binary),
            "objectId" => Some(Self::ObjectId),
            "bool" => Some(Self::Boolean),
            "date" => Some(Self::Date),
            "null" => Some(Self::Null),
            "int" => Some(Self::Int32),
            "long" => Some(Self::Int64),
            "number" => Some(Self::Number),
            _ => None,
        }
    }

    fn matches(&self, value: &serde_json::Value) -> bool {
        matches!(
            (self, value),
            (Self::Double | Self::Number, serde_json::Value::Number(_))
                | (Self::String, serde_json::Value::String(_))
                | (Self::Object, serde_json::Value::Object(_))
                | (Self::Array, serde_json::Value::Array(_))
                | (Self::Boolean, serde_json::Value::Bool(_))
                | (Self::Null, serde_json::Value::Null)
        )
    }
}

/// Geo shape for geospatial queries
#[derive(Debug, Clone)]
pub enum GeoShape {
    /// Bounding box
    Box {
        bottom_left: (f64, f64),
        top_right: (f64, f64),
    },
    /// Circle
    Circle { center: (f64, f64), radius: f64 },
    /// Polygon
    Polygon(Vec<(f64, f64)>),
}

/// A field condition in a query
#[derive(Debug, Clone)]
pub struct FieldCondition {
    /// Field path (dot notation supported)
    pub field: String,
    /// Operator to apply
    pub operator: QueryOperator,
}

/// A document query
#[derive(Debug, Clone)]
pub struct DocumentQuery {
    /// Field conditions
    conditions: Vec<FieldCondition>,
    /// Logical operators at root level
    logical_ops: Vec<QueryOperator>,
    /// Sort specification
    #[allow(dead_code)] // Planned for v0.2 — stored for query result sorting
    sort: Option<Vec<(String, SortOrder)>>,
    /// Skip count
    #[allow(dead_code)] // Planned for v0.2 — stored for query result pagination
    skip: Option<usize>,
    /// Limit count
    #[allow(dead_code)] // Planned for v0.2 — stored for query result pagination
    limit: Option<usize>,
}

/// Sort order
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortOrder {
    /// Ascending
    Ascending,
    /// Descending
    Descending,
}

impl DocumentQuery {
    /// Create an empty query (matches all)
    pub fn new() -> Self {
        Self {
            conditions: Vec::new(),
            logical_ops: Vec::new(),
            sort: None,
            skip: None,
            limit: None,
        }
    }

    /// Check if query is empty (matches all)
    pub fn is_empty(&self) -> bool {
        self.conditions.is_empty() && self.logical_ops.is_empty()
    }

    /// Parse query from JSON
    pub fn from_json(value: serde_json::Value) -> Result<Self, DocumentStoreError> {
        let mut query = DocumentQuery::new();

        let obj = match value.as_object() {
            Some(obj) => obj,
            None if value.is_null() => return Ok(query),
            None => {
                return Err(DocumentStoreError::InvalidQuery(
                    "Query must be an object".into(),
                ))
            }
        };

        for (key, value) in obj {
            if key.starts_with('$') {
                // Logical operator
                let op = Self::parse_logical_operator(key, value)?;
                query.logical_ops.push(op);
            } else {
                // Field condition
                let conditions = Self::parse_field_condition(key, value)?;
                query.conditions.extend(conditions);
            }
        }

        Ok(query)
    }

    /// Parse a logical operator
    fn parse_logical_operator(
        op: &str,
        value: &serde_json::Value,
    ) -> Result<QueryOperator, DocumentStoreError> {
        match op {
            "$and" => {
                let arr = value.as_array().ok_or_else(|| {
                    DocumentStoreError::InvalidQuery("$and requires an array".into())
                })?;
                let queries: Result<Vec<_>, _> = arr
                    .iter()
                    .map(|v| DocumentQuery::from_json(v.clone()))
                    .collect();
                Ok(QueryOperator::And(queries?))
            }
            "$or" => {
                let arr = value.as_array().ok_or_else(|| {
                    DocumentStoreError::InvalidQuery("$or requires an array".into())
                })?;
                let queries: Result<Vec<_>, _> = arr
                    .iter()
                    .map(|v| DocumentQuery::from_json(v.clone()))
                    .collect();
                Ok(QueryOperator::Or(queries?))
            }
            "$nor" => {
                let arr = value.as_array().ok_or_else(|| {
                    DocumentStoreError::InvalidQuery("$nor requires an array".into())
                })?;
                let queries: Result<Vec<_>, _> = arr
                    .iter()
                    .map(|v| DocumentQuery::from_json(v.clone()))
                    .collect();
                Ok(QueryOperator::Nor(queries?))
            }
            "$text" => {
                let obj = value.as_object().ok_or_else(|| {
                    DocumentStoreError::InvalidQuery("$text requires an object".into())
                })?;
                let search = obj.get("$search").and_then(|v| v.as_str()).ok_or_else(|| {
                    DocumentStoreError::InvalidQuery("$text requires $search".into())
                })?;
                let language = obj
                    .get("$language")
                    .and_then(|v| v.as_str())
                    .map(String::from);
                let case_sensitive = obj
                    .get("$caseSensitive")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false);
                Ok(QueryOperator::Text {
                    search: search.to_string(),
                    language,
                    case_sensitive,
                })
            }
            "$where" => {
                let expr = value.as_str().ok_or_else(|| {
                    DocumentStoreError::InvalidQuery("$where requires a string".into())
                })?;
                Ok(QueryOperator::Where(expr.to_string()))
            }
            _ => Err(DocumentStoreError::InvalidQuery(format!(
                "Unknown operator: {}",
                op
            ))),
        }
    }

    /// Parse a field condition
    fn parse_field_condition(
        field: &str,
        value: &serde_json::Value,
    ) -> Result<Vec<FieldCondition>, DocumentStoreError> {
        let mut conditions = Vec::new();

        match value {
            // Simple equality
            serde_json::Value::Null
            | serde_json::Value::Bool(_)
            | serde_json::Value::Number(_)
            | serde_json::Value::String(_)
            | serde_json::Value::Array(_) => {
                conditions.push(FieldCondition {
                    field: field.to_string(),
                    operator: QueryOperator::Eq(value.clone()),
                });
            }
            // Operator object
            serde_json::Value::Object(obj) => {
                // Check if it's an operator or a nested document match
                let has_operators = obj.keys().any(|k| k.starts_with('$'));

                if has_operators {
                    for (op, op_value) in obj {
                        let operator = Self::parse_comparison_operator(op, op_value)?;
                        conditions.push(FieldCondition {
                            field: field.to_string(),
                            operator,
                        });
                    }
                } else {
                    // Nested document equality
                    conditions.push(FieldCondition {
                        field: field.to_string(),
                        operator: QueryOperator::Eq(value.clone()),
                    });
                }
            }
        }

        Ok(conditions)
    }

    /// Parse a comparison operator
    fn parse_comparison_operator(
        op: &str,
        value: &serde_json::Value,
    ) -> Result<QueryOperator, DocumentStoreError> {
        match op {
            "$eq" => Ok(QueryOperator::Eq(value.clone())),
            "$ne" => Ok(QueryOperator::Ne(value.clone())),
            "$gt" => Ok(QueryOperator::Gt(value.clone())),
            "$gte" => Ok(QueryOperator::Gte(value.clone())),
            "$lt" => Ok(QueryOperator::Lt(value.clone())),
            "$lte" => Ok(QueryOperator::Lte(value.clone())),
            "$in" => {
                let arr = value.as_array().ok_or_else(|| {
                    DocumentStoreError::InvalidQuery("$in requires an array".into())
                })?;
                Ok(QueryOperator::In(arr.clone()))
            }
            "$nin" => {
                let arr = value.as_array().ok_or_else(|| {
                    DocumentStoreError::InvalidQuery("$nin requires an array".into())
                })?;
                Ok(QueryOperator::Nin(arr.clone()))
            }
            "$exists" => {
                let exists = value.as_bool().ok_or_else(|| {
                    DocumentStoreError::InvalidQuery("$exists requires a boolean".into())
                })?;
                Ok(QueryOperator::Exists(exists))
            }
            "$type" => {
                let type_name = value.as_str().ok_or_else(|| {
                    DocumentStoreError::InvalidQuery("$type requires a string".into())
                })?;
                let bson_type = BsonType::from_str(type_name).ok_or_else(|| {
                    DocumentStoreError::InvalidQuery(format!("Unknown type: {}", type_name))
                })?;
                Ok(QueryOperator::Type(bson_type))
            }
            "$regex" => {
                let pattern = value.as_str().ok_or_else(|| {
                    DocumentStoreError::InvalidQuery("$regex requires a string".into())
                })?;
                Ok(QueryOperator::Regex(pattern.to_string(), String::new()))
            }
            "$options" => {
                // This should be handled together with $regex
                Err(DocumentStoreError::InvalidQuery(
                    "$options must be used with $regex".into(),
                ))
            }
            "$mod" => {
                let arr = value.as_array().ok_or_else(|| {
                    DocumentStoreError::InvalidQuery("$mod requires an array".into())
                })?;
                if arr.len() != 2 {
                    return Err(DocumentStoreError::InvalidQuery(
                        "$mod requires [divisor, remainder]".into(),
                    ));
                }
                let divisor = arr[0].as_i64().ok_or_else(|| {
                    DocumentStoreError::InvalidQuery("$mod divisor must be integer".into())
                })?;
                let remainder = arr[1].as_i64().ok_or_else(|| {
                    DocumentStoreError::InvalidQuery("$mod remainder must be integer".into())
                })?;
                Ok(QueryOperator::Mod(divisor, remainder))
            }
            "$all" => {
                let arr = value.as_array().ok_or_else(|| {
                    DocumentStoreError::InvalidQuery("$all requires an array".into())
                })?;
                Ok(QueryOperator::All(arr.clone()))
            }
            "$elemMatch" => {
                let query = DocumentQuery::from_json(value.clone())?;
                Ok(QueryOperator::ElemMatch(Box::new(query)))
            }
            "$size" => {
                let size = value.as_u64().ok_or_else(|| {
                    DocumentStoreError::InvalidQuery("$size requires an integer".into())
                })? as usize;
                Ok(QueryOperator::Size(size))
            }
            "$not" => {
                let inner = Self::parse_comparison_operator_from_object(value)?;
                Ok(QueryOperator::Not(Box::new(inner)))
            }
            "$near" => {
                let obj = value.as_object().ok_or_else(|| {
                    DocumentStoreError::InvalidQuery("$near requires an object".into())
                })?;
                let geometry = obj.get("$geometry").ok_or_else(|| {
                    DocumentStoreError::InvalidQuery("$near requires $geometry".into())
                })?;
                let coords = geometry
                    .get("coordinates")
                    .and_then(|c| c.as_array())
                    .ok_or_else(|| {
                        DocumentStoreError::InvalidQuery("$near requires coordinates".into())
                    })?;
                let x = coords.first().and_then(|v| v.as_f64()).unwrap_or(0.0);
                let y = coords.get(1).and_then(|v| v.as_f64()).unwrap_or(0.0);
                let max_distance = obj.get("$maxDistance").and_then(|v| v.as_f64());
                Ok(QueryOperator::Near { x, y, max_distance })
            }
            _ => Err(DocumentStoreError::InvalidQuery(format!(
                "Unknown operator: {}",
                op
            ))),
        }
    }

    /// Parse comparison operator from object
    fn parse_comparison_operator_from_object(
        value: &serde_json::Value,
    ) -> Result<QueryOperator, DocumentStoreError> {
        let obj = value
            .as_object()
            .ok_or_else(|| DocumentStoreError::InvalidQuery("Expected operator object".into()))?;

        if let Some((op, val)) = obj.iter().next() {
            Self::parse_comparison_operator(op, val)
        } else {
            Err(DocumentStoreError::InvalidQuery(
                "Empty operator object".into(),
            ))
        }
    }

    /// Check if a document matches this query
    pub fn matches(&self, doc: &Document) -> bool {
        // Check all field conditions
        for condition in &self.conditions {
            if !Self::matches_condition(doc, condition) {
                return false;
            }
        }

        // Check logical operators
        for op in &self.logical_ops {
            if !Self::matches_logical_op(doc, op) {
                return false;
            }
        }

        true
    }

    /// Check if a document matches a field condition
    fn matches_condition(doc: &Document, condition: &FieldCondition) -> bool {
        let field_value = if condition.field == "_id" {
            Some(doc.id.to_json())
        } else {
            doc.get_field(&condition.field).cloned()
        };

        Self::matches_operator(&field_value, &condition.operator)
    }

    /// Check if a value matches an operator
    fn matches_operator(value: &Option<serde_json::Value>, op: &QueryOperator) -> bool {
        match op {
            QueryOperator::Eq(expected) => value
                .as_ref()
                .map(|v| v == expected)
                .unwrap_or(expected.is_null()),
            QueryOperator::Ne(expected) => value
                .as_ref()
                .map(|v| v != expected)
                .unwrap_or(!expected.is_null()),
            QueryOperator::Gt(expected) => {
                if let (Some(v), Some(e)) = (value, expected.as_f64()) {
                    v.as_f64().map(|n| n > e).unwrap_or(false)
                        || v.as_str()
                            .map(|s| s > expected.as_str().unwrap_or(""))
                            .unwrap_or(false)
                } else {
                    false
                }
            }
            QueryOperator::Gte(expected) => {
                if let (Some(v), Some(e)) = (value, expected.as_f64()) {
                    v.as_f64().map(|n| n >= e).unwrap_or(false)
                } else if let (Some(v), Some(e)) =
                    (value.as_ref().and_then(|v| v.as_str()), expected.as_str())
                {
                    v >= e
                } else {
                    false
                }
            }
            QueryOperator::Lt(expected) => {
                if let (Some(v), Some(e)) = (value, expected.as_f64()) {
                    v.as_f64().map(|n| n < e).unwrap_or(false)
                } else if let (Some(v), Some(e)) =
                    (value.as_ref().and_then(|v| v.as_str()), expected.as_str())
                {
                    v < e
                } else {
                    false
                }
            }
            QueryOperator::Lte(expected) => {
                if let (Some(v), Some(e)) = (value, expected.as_f64()) {
                    v.as_f64().map(|n| n <= e).unwrap_or(false)
                } else if let (Some(v), Some(e)) =
                    (value.as_ref().and_then(|v| v.as_str()), expected.as_str())
                {
                    v <= e
                } else {
                    false
                }
            }
            QueryOperator::In(arr) => value.as_ref().map(|v| arr.contains(v)).unwrap_or(false),
            QueryOperator::Nin(arr) => value.as_ref().map(|v| !arr.contains(v)).unwrap_or(true),
            QueryOperator::Exists(should_exist) => {
                let exists = value.is_some();
                exists == *should_exist
            }
            QueryOperator::Type(bson_type) => value
                .as_ref()
                .map(|v| bson_type.matches(v))
                .unwrap_or(false),
            QueryOperator::Regex(pattern, _options) => {
                if let Some(serde_json::Value::String(s)) = value {
                    regex::Regex::new(pattern)
                        .map(|re| re.is_match(s))
                        .unwrap_or(false)
                } else {
                    false
                }
            }
            QueryOperator::Mod(divisor, remainder) => {
                if let Some(v) = value.as_ref().and_then(|v| v.as_i64()) {
                    v % divisor == *remainder
                } else {
                    false
                }
            }
            QueryOperator::All(expected) => {
                if let Some(serde_json::Value::Array(arr)) = value {
                    expected.iter().all(|e| arr.contains(e))
                } else {
                    false
                }
            }
            QueryOperator::ElemMatch(query) => {
                if let Some(serde_json::Value::Array(arr)) = value {
                    arr.iter().any(|elem| {
                        if let Ok(doc) = Document::from_json(elem.clone()) {
                            query.matches(&doc)
                        } else {
                            false
                        }
                    })
                } else {
                    false
                }
            }
            QueryOperator::Size(size) => {
                if let Some(serde_json::Value::Array(arr)) = value {
                    arr.len() == *size
                } else {
                    false
                }
            }
            QueryOperator::Not(inner) => !Self::matches_operator(value, inner),
            QueryOperator::Near { x, y, max_distance } => {
                // Simplified near matching - check if point is within max_distance
                if let Some(v) = value {
                    if let Some(coords) = v.get("coordinates").and_then(|c| c.as_array()) {
                        if let (Some(px), Some(py)) = (
                            coords.first().and_then(|v| v.as_f64()),
                            coords.get(1).and_then(|v| v.as_f64()),
                        ) {
                            let dist = (px - x).hypot(py - y);
                            return max_distance.map(|d| dist <= d).unwrap_or(true);
                        }
                    }
                }
                false
            }
            QueryOperator::GeoWithin(shape) => {
                if let Some(v) = value {
                    if let Some(coords) = v.get("coordinates").and_then(|c| c.as_array()) {
                        if let (Some(x), Some(y)) = (
                            coords.first().and_then(|v| v.as_f64()),
                            coords.get(1).and_then(|v| v.as_f64()),
                        ) {
                            return Self::point_in_shape(x, y, shape);
                        }
                    }
                }
                false
            }
            QueryOperator::Text {
                search,
                case_sensitive,
                ..
            } => {
                // Simple text search - check if any string field contains the search term
                if let Some(v) = value {
                    Self::text_search(v, search, *case_sensitive)
                } else {
                    false
                }
            }
            QueryOperator::Where(_expr) => {
                // JavaScript expressions not supported - always return true
                true
            }
            QueryOperator::And(_) | QueryOperator::Or(_) | QueryOperator::Nor(_) => {
                // These are handled at the query level
                true
            }
        }
    }

    /// Check if point is within a shape
    fn point_in_shape(x: f64, y: f64, shape: &GeoShape) -> bool {
        match shape {
            GeoShape::Box {
                bottom_left,
                top_right,
            } => x >= bottom_left.0 && x <= top_right.0 && y >= bottom_left.1 && y <= top_right.1,
            GeoShape::Circle { center, radius } => {
                let dist = (x - center.0).hypot(y - center.1);
                dist <= *radius
            }
            GeoShape::Polygon(points) => {
                // Ray casting algorithm
                let mut inside = false;
                let n = points.len();
                let mut j = n - 1;

                for i in 0..n {
                    let (xi, yi) = points[i];
                    let (xj, yj) = points[j];

                    if ((yi > y) != (yj > y)) && (x < (xj - xi) * (y - yi) / (yj - yi) + xi) {
                        inside = !inside;
                    }
                    j = i;
                }

                inside
            }
        }
    }

    /// Simple text search
    fn text_search(value: &serde_json::Value, search: &str, case_sensitive: bool) -> bool {
        match value {
            serde_json::Value::String(s) => {
                if case_sensitive {
                    s.contains(search)
                } else {
                    s.to_lowercase().contains(&search.to_lowercase())
                }
            }
            serde_json::Value::Object(map) => map
                .values()
                .any(|v| Self::text_search(v, search, case_sensitive)),
            serde_json::Value::Array(arr) => arr
                .iter()
                .any(|v| Self::text_search(v, search, case_sensitive)),
            _ => false,
        }
    }

    /// Check if a document matches a logical operator
    fn matches_logical_op(doc: &Document, op: &QueryOperator) -> bool {
        match op {
            QueryOperator::And(queries) => queries.iter().all(|q| q.matches(doc)),
            QueryOperator::Or(queries) => queries.iter().any(|q| q.matches(doc)),
            QueryOperator::Nor(queries) => !queries.iter().any(|q| q.matches(doc)),
            QueryOperator::Text {
                search,
                case_sensitive,
                ..
            } => Self::text_search(&doc.data, search, *case_sensitive),
            _ => true,
        }
    }

    /// Get all fields used in the query
    pub fn get_fields(&self) -> HashSet<String> {
        let mut fields = HashSet::new();

        for condition in &self.conditions {
            fields.insert(condition.field.clone());
        }

        for op in &self.logical_ops {
            Self::collect_fields_from_op(op, &mut fields);
        }

        fields
    }

    /// Collect fields from a logical operator
    fn collect_fields_from_op(op: &QueryOperator, fields: &mut HashSet<String>) {
        match op {
            QueryOperator::And(queries)
            | QueryOperator::Or(queries)
            | QueryOperator::Nor(queries) => {
                for q in queries {
                    for condition in &q.conditions {
                        fields.insert(condition.field.clone());
                    }
                    for inner_op in &q.logical_ops {
                        Self::collect_fields_from_op(inner_op, fields);
                    }
                }
            }
            _ => {}
        }
    }
}

impl Default for DocumentQuery {
    fn default() -> Self {
        Self::new()
    }
}

/// Query builder for fluent query construction
pub struct QueryBuilder {
    conditions: Vec<FieldCondition>,
    logical_ops: Vec<QueryOperator>,
    sort: Option<Vec<(String, SortOrder)>>,
    skip: Option<usize>,
    limit: Option<usize>,
}

impl QueryBuilder {
    /// Create a new query builder
    pub fn new() -> Self {
        Self {
            conditions: Vec::new(),
            logical_ops: Vec::new(),
            sort: None,
            skip: None,
            limit: None,
        }
    }

    /// Add equality condition
    pub fn eq(mut self, field: &str, value: serde_json::Value) -> Self {
        self.conditions.push(FieldCondition {
            field: field.to_string(),
            operator: QueryOperator::Eq(value),
        });
        self
    }

    /// Add not equal condition
    pub fn ne(mut self, field: &str, value: serde_json::Value) -> Self {
        self.conditions.push(FieldCondition {
            field: field.to_string(),
            operator: QueryOperator::Ne(value),
        });
        self
    }

    /// Add greater than condition
    pub fn gt(mut self, field: &str, value: serde_json::Value) -> Self {
        self.conditions.push(FieldCondition {
            field: field.to_string(),
            operator: QueryOperator::Gt(value),
        });
        self
    }

    /// Add greater than or equal condition
    pub fn gte(mut self, field: &str, value: serde_json::Value) -> Self {
        self.conditions.push(FieldCondition {
            field: field.to_string(),
            operator: QueryOperator::Gte(value),
        });
        self
    }

    /// Add less than condition
    pub fn lt(mut self, field: &str, value: serde_json::Value) -> Self {
        self.conditions.push(FieldCondition {
            field: field.to_string(),
            operator: QueryOperator::Lt(value),
        });
        self
    }

    /// Add less than or equal condition
    pub fn lte(mut self, field: &str, value: serde_json::Value) -> Self {
        self.conditions.push(FieldCondition {
            field: field.to_string(),
            operator: QueryOperator::Lte(value),
        });
        self
    }

    /// Add in condition
    pub fn in_array(mut self, field: &str, values: Vec<serde_json::Value>) -> Self {
        self.conditions.push(FieldCondition {
            field: field.to_string(),
            operator: QueryOperator::In(values),
        });
        self
    }

    /// Add exists condition
    pub fn exists(mut self, field: &str, should_exist: bool) -> Self {
        self.conditions.push(FieldCondition {
            field: field.to_string(),
            operator: QueryOperator::Exists(should_exist),
        });
        self
    }

    /// Add regex condition
    pub fn regex(mut self, field: &str, pattern: &str) -> Self {
        self.conditions.push(FieldCondition {
            field: field.to_string(),
            operator: QueryOperator::Regex(pattern.to_string(), String::new()),
        });
        self
    }

    /// Add sort
    pub fn sort(mut self, field: &str, order: SortOrder) -> Self {
        let mut sort = self.sort.take().unwrap_or_default();
        sort.push((field.to_string(), order));
        self.sort = Some(sort);
        self
    }

    /// Add skip
    pub fn skip(mut self, n: usize) -> Self {
        self.skip = Some(n);
        self
    }

    /// Add limit
    pub fn limit(mut self, n: usize) -> Self {
        self.limit = Some(n);
        self
    }

    /// Build the query
    pub fn build(self) -> DocumentQuery {
        DocumentQuery {
            conditions: self.conditions,
            logical_ops: self.logical_ops,
            sort: self.sort,
            skip: self.skip,
            limit: self.limit,
        }
    }
}

impl Default for QueryBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_equality_query() {
        let query = DocumentQuery::from_json(json!({ "name": "Alice" })).unwrap();

        let doc = Document::from_json(json!({ "name": "Alice", "age": 30 })).unwrap();
        assert!(query.matches(&doc));

        let doc2 = Document::from_json(json!({ "name": "Bob", "age": 25 })).unwrap();
        assert!(!query.matches(&doc2));
    }

    #[test]
    fn test_comparison_operators() {
        let query = DocumentQuery::from_json(json!({
            "age": { "$gte": 18, "$lt": 65 }
        }))
        .unwrap();

        let doc1 = Document::from_json(json!({ "age": 30 })).unwrap();
        assert!(query.matches(&doc1));

        let doc2 = Document::from_json(json!({ "age": 15 })).unwrap();
        assert!(!query.matches(&doc2));

        let doc3 = Document::from_json(json!({ "age": 70 })).unwrap();
        assert!(!query.matches(&doc3));
    }

    #[test]
    fn test_in_operator() {
        let query = DocumentQuery::from_json(json!({
            "status": { "$in": ["active", "pending"] }
        }))
        .unwrap();

        let doc1 = Document::from_json(json!({ "status": "active" })).unwrap();
        assert!(query.matches(&doc1));

        let doc2 = Document::from_json(json!({ "status": "inactive" })).unwrap();
        assert!(!query.matches(&doc2));
    }

    #[test]
    fn test_and_operator() {
        let query = DocumentQuery::from_json(json!({
            "$and": [
                { "age": { "$gte": 18 } },
                { "active": true }
            ]
        }))
        .unwrap();

        let doc1 = Document::from_json(json!({ "age": 25, "active": true })).unwrap();
        assert!(query.matches(&doc1));

        let doc2 = Document::from_json(json!({ "age": 25, "active": false })).unwrap();
        assert!(!query.matches(&doc2));
    }

    #[test]
    fn test_or_operator() {
        let query = DocumentQuery::from_json(json!({
            "$or": [
                { "role": "admin" },
                { "age": { "$gte": 21 } }
            ]
        }))
        .unwrap();

        let doc1 = Document::from_json(json!({ "role": "admin", "age": 18 })).unwrap();
        assert!(query.matches(&doc1));

        let doc2 = Document::from_json(json!({ "role": "user", "age": 25 })).unwrap();
        assert!(query.matches(&doc2));

        let doc3 = Document::from_json(json!({ "role": "user", "age": 18 })).unwrap();
        assert!(!query.matches(&doc3));
    }

    #[test]
    fn test_exists_operator() {
        let query = DocumentQuery::from_json(json!({
            "email": { "$exists": true }
        }))
        .unwrap();

        let doc1 = Document::from_json(json!({ "email": "test@example.com" })).unwrap();
        assert!(query.matches(&doc1));

        let doc2 = Document::from_json(json!({ "name": "Alice" })).unwrap();
        assert!(!query.matches(&doc2));
    }

    #[test]
    fn test_nested_field() {
        let query = DocumentQuery::from_json(json!({
            "address.city": "NYC"
        }))
        .unwrap();

        let doc = Document::from_json(json!({
            "name": "Alice",
            "address": { "city": "NYC", "zip": "10001" }
        }))
        .unwrap();
        assert!(query.matches(&doc));
    }

    #[test]
    fn test_query_builder() {
        let query = QueryBuilder::new()
            .eq("name", json!("Alice"))
            .gte("age", json!(18))
            .exists("email", true)
            .build();

        let doc = Document::from_json(json!({
            "name": "Alice",
            "age": 30,
            "email": "alice@example.com"
        }))
        .unwrap();
        assert!(query.matches(&doc));
    }
}
