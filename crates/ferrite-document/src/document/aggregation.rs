//! Aggregation pipeline implementation (MongoDB-compatible)

use std::collections::HashMap;

use super::collection::Collection;
use super::document::Document;
use super::query::DocumentQuery;
use super::DocumentStoreError;

/// Aggregation pipeline
#[derive(Debug, Clone)]
pub struct AggregationPipeline {
    /// Pipeline stages
    stages: Vec<PipelineStage>,
}

/// Pipeline stage types
#[derive(Debug, Clone)]
pub enum PipelineStage {
    /// Match documents
    Match(DocumentQuery),
    /// Project fields
    Project(ProjectSpec),
    /// Group documents
    Group(GroupSpec),
    /// Sort documents
    Sort(Vec<(String, SortDirection)>),
    /// Limit results
    Limit(usize),
    /// Skip results
    Skip(usize),
    /// Unwind array field
    Unwind(UnwindSpec),
    /// Lookup (join)
    Lookup(LookupSpec),
    /// Add fields
    AddFields(HashMap<String, Expression>),
    /// Replace root
    ReplaceRoot(Expression),
    /// Count documents
    Count(String),
    /// Sample documents
    Sample(usize),
    /// Facet (multiple pipelines)
    Facet(HashMap<String, Vec<PipelineStage>>),
    /// Bucket
    Bucket(BucketSpec),
    /// Bucket auto
    BucketAuto(BucketAutoSpec),
    /// Out to collection
    Out(String),
    /// Merge to collection
    Merge(MergeSpec),
}

/// Sort direction
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortDirection {
    /// Ascending
    Ascending,
    /// Descending
    Descending,
}

/// Project specification
#[derive(Debug, Clone)]
pub struct ProjectSpec {
    /// Fields to include/exclude
    pub fields: HashMap<String, ProjectValue>,
}

/// Project value
#[derive(Debug, Clone)]
pub enum ProjectValue {
    /// Include field
    Include,
    /// Exclude field
    Exclude,
    /// Computed expression
    Expression(Expression),
}

/// Group specification
#[derive(Debug, Clone)]
pub struct GroupSpec {
    /// Group key
    pub id: Expression,
    /// Accumulators
    pub accumulators: HashMap<String, Accumulator>,
}

/// Accumulator operators
#[derive(Debug, Clone)]
pub enum Accumulator {
    /// Sum
    Sum(Expression),
    /// Average
    Avg(Expression),
    /// Min
    Min(Expression),
    /// Max
    Max(Expression),
    /// First
    First(Expression),
    /// Last
    Last(Expression),
    /// Push to array
    Push(Expression),
    /// Add to set
    AddToSet(Expression),
    /// Standard deviation (population)
    StdDevPop(Expression),
    /// Standard deviation (sample)
    StdDevSamp(Expression),
    /// Count
    Count,
}

/// Expression for computed fields
#[derive(Debug, Clone)]
pub enum Expression {
    /// Literal value
    Literal(serde_json::Value),
    /// Field reference ($field or $$variable)
    Field(String),
    /// Array of expressions
    Array(Vec<Expression>),
    /// Object with expression values
    Object(HashMap<String, Expression>),
    /// Addition
    Add(Vec<Expression>),
    /// Subtraction
    Subtract(Box<Expression>, Box<Expression>),
    /// Multiplication
    Multiply(Vec<Expression>),
    /// Division
    Divide(Box<Expression>, Box<Expression>),
    /// Modulo
    Mod(Box<Expression>, Box<Expression>),
    /// Concatenation
    Concat(Vec<Expression>),
    /// Conditional
    Cond {
        /// Condition
        r#if: Box<Expression>,
        /// Then branch
        then: Box<Expression>,
        /// Else branch
        r#else: Box<Expression>,
    },
    /// Switch
    Switch {
        /// Branches
        branches: Vec<(Expression, Expression)>,
        /// Default
        default: Box<Expression>,
    },
    /// Array element at index
    ArrayElemAt(Box<Expression>, Box<Expression>),
    /// Array size
    Size(Box<Expression>),
    /// String to upper
    ToUpper(Box<Expression>),
    /// String to lower
    ToLower(Box<Expression>),
    /// Substring
    Substr(Box<Expression>, Box<Expression>, Box<Expression>),
    /// Year from date
    Year(Box<Expression>),
    /// Month from date
    Month(Box<Expression>),
    /// Day from date
    DayOfMonth(Box<Expression>),
    /// Comparison
    Eq(Box<Expression>, Box<Expression>),
    /// Not equal
    Ne(Box<Expression>, Box<Expression>),
    /// Greater than
    Gt(Box<Expression>, Box<Expression>),
    /// Greater than or equal
    Gte(Box<Expression>, Box<Expression>),
    /// Less than
    Lt(Box<Expression>, Box<Expression>),
    /// Less than or equal
    Lte(Box<Expression>, Box<Expression>),
    /// Logical and
    And(Vec<Expression>),
    /// Logical or
    Or(Vec<Expression>),
    /// Logical not
    Not(Box<Expression>),
    /// If null
    IfNull(Box<Expression>, Box<Expression>),
    /// Type of value
    Type(Box<Expression>),
}

/// Unwind specification
#[derive(Debug, Clone)]
pub struct UnwindSpec {
    /// Path to array field
    pub path: String,
    /// Include array index
    pub include_array_index: Option<String>,
    /// Preserve null and empty arrays
    pub preserve_null_and_empty: bool,
}

/// Lookup (join) specification
#[derive(Debug, Clone)]
pub struct LookupSpec {
    /// Foreign collection
    pub from: String,
    /// Local field
    pub local_field: String,
    /// Foreign field
    pub foreign_field: String,
    /// Output array field
    pub r#as: String,
}

/// Bucket specification
#[derive(Debug, Clone)]
pub struct BucketSpec {
    /// Group by expression
    pub group_by: Expression,
    /// Bucket boundaries
    pub boundaries: Vec<serde_json::Value>,
    /// Default bucket
    pub default: Option<String>,
    /// Output accumulators
    pub output: HashMap<String, Accumulator>,
}

/// Bucket auto specification
#[derive(Debug, Clone)]
pub struct BucketAutoSpec {
    /// Group by expression
    pub group_by: Expression,
    /// Number of buckets
    pub buckets: usize,
    /// Output accumulators
    pub output: HashMap<String, Accumulator>,
    /// Granularity
    pub granularity: Option<String>,
}

/// Merge specification
#[derive(Debug, Clone)]
pub struct MergeSpec {
    /// Target collection
    pub into: String,
    /// Match fields
    pub on: Vec<String>,
    /// When matched action
    pub when_matched: MergeAction,
    /// When not matched action
    pub when_not_matched: MergeAction,
}

/// Merge action
#[derive(Debug, Clone)]
pub enum MergeAction {
    /// Replace document
    Replace,
    /// Keep existing
    KeepExisting,
    /// Merge documents
    Merge,
    /// Insert document
    Insert,
    /// Discard document
    Discard,
    /// Fail
    Fail,
}

impl AggregationPipeline {
    /// Create a new pipeline
    pub fn new() -> Self {
        Self { stages: Vec::new() }
    }

    /// Parse pipeline from JSON
    pub fn from_json(stages: Vec<serde_json::Value>) -> Result<Self, DocumentStoreError> {
        let mut pipeline = Self::new();

        for stage_value in stages {
            let stage = Self::parse_stage(stage_value)?;
            pipeline.stages.push(stage);
        }

        Ok(pipeline)
    }

    /// Parse a single stage
    fn parse_stage(value: serde_json::Value) -> Result<PipelineStage, DocumentStoreError> {
        let obj = value.as_object().ok_or_else(|| {
            DocumentStoreError::InvalidAggregation("Stage must be an object".into())
        })?;

        if obj.len() != 1 {
            return Err(DocumentStoreError::InvalidAggregation(
                "Stage must have exactly one operator".into(),
            ));
        }

        let (op, val) = obj
            .iter()
            .next()
            .ok_or_else(|| DocumentStoreError::InvalidAggregation("Empty stage operator".into()))?;

        match op.as_str() {
            "$match" => {
                let query = DocumentQuery::from_json(val.clone())?;
                Ok(PipelineStage::Match(query))
            }
            "$project" => {
                let spec = Self::parse_project(val)?;
                Ok(PipelineStage::Project(spec))
            }
            "$group" => {
                let spec = Self::parse_group(val)?;
                Ok(PipelineStage::Group(spec))
            }
            "$sort" => {
                let sort = Self::parse_sort(val)?;
                Ok(PipelineStage::Sort(sort))
            }
            "$limit" => {
                let limit = val.as_u64().ok_or_else(|| {
                    DocumentStoreError::InvalidAggregation("$limit must be an integer".into())
                })? as usize;
                Ok(PipelineStage::Limit(limit))
            }
            "$skip" => {
                let skip = val.as_u64().ok_or_else(|| {
                    DocumentStoreError::InvalidAggregation("$skip must be an integer".into())
                })? as usize;
                Ok(PipelineStage::Skip(skip))
            }
            "$unwind" => {
                let spec = Self::parse_unwind(val)?;
                Ok(PipelineStage::Unwind(spec))
            }
            "$lookup" => {
                let spec = Self::parse_lookup(val)?;
                Ok(PipelineStage::Lookup(spec))
            }
            "$addFields" => {
                let fields = Self::parse_add_fields(val)?;
                Ok(PipelineStage::AddFields(fields))
            }
            "$count" => {
                let field = val.as_str().ok_or_else(|| {
                    DocumentStoreError::InvalidAggregation("$count must be a string".into())
                })?;
                Ok(PipelineStage::Count(field.to_string()))
            }
            "$sample" => {
                let obj = val.as_object().ok_or_else(|| {
                    DocumentStoreError::InvalidAggregation("$sample must be an object".into())
                })?;
                let size = obj.get("size").and_then(|v| v.as_u64()).ok_or_else(|| {
                    DocumentStoreError::InvalidAggregation("$sample requires size".into())
                })? as usize;
                Ok(PipelineStage::Sample(size))
            }
            "$out" => {
                let collection = val.as_str().ok_or_else(|| {
                    DocumentStoreError::InvalidAggregation("$out must be a string".into())
                })?;
                Ok(PipelineStage::Out(collection.to_string()))
            }
            _ => Err(DocumentStoreError::InvalidAggregation(format!(
                "Unknown stage operator: {}",
                op
            ))),
        }
    }

    /// Parse project specification
    fn parse_project(value: &serde_json::Value) -> Result<ProjectSpec, DocumentStoreError> {
        let obj = value.as_object().ok_or_else(|| {
            DocumentStoreError::InvalidAggregation("$project must be an object".into())
        })?;

        let mut fields = HashMap::new();

        for (field, val) in obj {
            let pv = match val {
                serde_json::Value::Number(n) => {
                    if n.as_i64() == Some(0) {
                        ProjectValue::Exclude
                    } else {
                        ProjectValue::Include
                    }
                }
                serde_json::Value::Bool(b) => {
                    if *b {
                        ProjectValue::Include
                    } else {
                        ProjectValue::Exclude
                    }
                }
                _ => {
                    let expr = Self::parse_expression(val)?;
                    ProjectValue::Expression(expr)
                }
            };
            fields.insert(field.clone(), pv);
        }

        Ok(ProjectSpec { fields })
    }

    /// Parse group specification
    fn parse_group(value: &serde_json::Value) -> Result<GroupSpec, DocumentStoreError> {
        let obj = value.as_object().ok_or_else(|| {
            DocumentStoreError::InvalidAggregation("$group must be an object".into())
        })?;

        let id_val = obj
            .get("_id")
            .ok_or_else(|| DocumentStoreError::InvalidAggregation("$group requires _id".into()))?;
        let id = Self::parse_expression(id_val)?;

        let mut accumulators = HashMap::new();

        for (field, val) in obj {
            if field == "_id" {
                continue;
            }

            let acc = Self::parse_accumulator(val)?;
            accumulators.insert(field.clone(), acc);
        }

        Ok(GroupSpec { id, accumulators })
    }

    /// Parse accumulator
    fn parse_accumulator(value: &serde_json::Value) -> Result<Accumulator, DocumentStoreError> {
        let obj = value.as_object().ok_or_else(|| {
            DocumentStoreError::InvalidAggregation("Accumulator must be an object".into())
        })?;

        if obj.len() != 1 {
            return Err(DocumentStoreError::InvalidAggregation(
                "Accumulator must have exactly one operator".into(),
            ));
        }

        let (op, val) = obj.iter().next().ok_or_else(|| {
            DocumentStoreError::InvalidAggregation("Empty accumulator operator".into())
        })?;

        match op.as_str() {
            "$sum" => {
                let expr = Self::parse_expression(val)?;
                Ok(Accumulator::Sum(expr))
            }
            "$avg" => {
                let expr = Self::parse_expression(val)?;
                Ok(Accumulator::Avg(expr))
            }
            "$min" => {
                let expr = Self::parse_expression(val)?;
                Ok(Accumulator::Min(expr))
            }
            "$max" => {
                let expr = Self::parse_expression(val)?;
                Ok(Accumulator::Max(expr))
            }
            "$first" => {
                let expr = Self::parse_expression(val)?;
                Ok(Accumulator::First(expr))
            }
            "$last" => {
                let expr = Self::parse_expression(val)?;
                Ok(Accumulator::Last(expr))
            }
            "$push" => {
                let expr = Self::parse_expression(val)?;
                Ok(Accumulator::Push(expr))
            }
            "$addToSet" => {
                let expr = Self::parse_expression(val)?;
                Ok(Accumulator::AddToSet(expr))
            }
            "$count" => Ok(Accumulator::Count),
            _ => Err(DocumentStoreError::InvalidAggregation(format!(
                "Unknown accumulator: {}",
                op
            ))),
        }
    }

    /// Parse expression
    fn parse_expression(value: &serde_json::Value) -> Result<Expression, DocumentStoreError> {
        match value {
            serde_json::Value::Null | serde_json::Value::Bool(_) | serde_json::Value::Number(_) => {
                Ok(Expression::Literal(value.clone()))
            }
            serde_json::Value::String(s) => {
                if let Some(stripped) = s.strip_prefix('$') {
                    Ok(Expression::Field(stripped.to_string()))
                } else {
                    Ok(Expression::Literal(value.clone()))
                }
            }
            serde_json::Value::Array(arr) => {
                let exprs: Result<Vec<_>, _> = arr.iter().map(Self::parse_expression).collect();
                Ok(Expression::Array(exprs?))
            }
            serde_json::Value::Object(obj) => {
                // Check if it's an operator
                if let Some((op, val)) = obj.iter().next() {
                    if op.starts_with('$') && obj.len() == 1 {
                        return Self::parse_operator_expression(op, val);
                    }
                }

                // Otherwise it's an object expression
                let mut fields = HashMap::new();
                for (k, v) in obj {
                    fields.insert(k.clone(), Self::parse_expression(v)?);
                }
                Ok(Expression::Object(fields))
            }
        }
    }

    /// Parse operator expression
    fn parse_operator_expression(
        op: &str,
        value: &serde_json::Value,
    ) -> Result<Expression, DocumentStoreError> {
        match op {
            "$literal" => Ok(Expression::Literal(value.clone())),
            "$add" => {
                let arr = value.as_array().ok_or_else(|| {
                    DocumentStoreError::InvalidAggregation("$add requires an array".into())
                })?;
                let exprs: Result<Vec<_>, _> = arr.iter().map(Self::parse_expression).collect();
                Ok(Expression::Add(exprs?))
            }
            "$subtract" => {
                let arr = value.as_array().ok_or_else(|| {
                    DocumentStoreError::InvalidAggregation("$subtract requires an array".into())
                })?;
                if arr.len() != 2 {
                    return Err(DocumentStoreError::InvalidAggregation(
                        "$subtract requires exactly 2 arguments".into(),
                    ));
                }
                let left = Self::parse_expression(&arr[0])?;
                let right = Self::parse_expression(&arr[1])?;
                Ok(Expression::Subtract(Box::new(left), Box::new(right)))
            }
            "$multiply" => {
                let arr = value.as_array().ok_or_else(|| {
                    DocumentStoreError::InvalidAggregation("$multiply requires an array".into())
                })?;
                let exprs: Result<Vec<_>, _> = arr.iter().map(Self::parse_expression).collect();
                Ok(Expression::Multiply(exprs?))
            }
            "$divide" => {
                let arr = value.as_array().ok_or_else(|| {
                    DocumentStoreError::InvalidAggregation("$divide requires an array".into())
                })?;
                if arr.len() != 2 {
                    return Err(DocumentStoreError::InvalidAggregation(
                        "$divide requires exactly 2 arguments".into(),
                    ));
                }
                let left = Self::parse_expression(&arr[0])?;
                let right = Self::parse_expression(&arr[1])?;
                Ok(Expression::Divide(Box::new(left), Box::new(right)))
            }
            "$concat" => {
                let arr = value.as_array().ok_or_else(|| {
                    DocumentStoreError::InvalidAggregation("$concat requires an array".into())
                })?;
                let exprs: Result<Vec<_>, _> = arr.iter().map(Self::parse_expression).collect();
                Ok(Expression::Concat(exprs?))
            }
            "$cond" => {
                let obj = value.as_object().ok_or_else(|| {
                    DocumentStoreError::InvalidAggregation("$cond requires an object".into())
                })?;
                let if_expr = Self::parse_expression(obj.get("if").ok_or_else(|| {
                    DocumentStoreError::InvalidAggregation("$cond requires if".into())
                })?)?;
                let then_expr = Self::parse_expression(obj.get("then").ok_or_else(|| {
                    DocumentStoreError::InvalidAggregation("$cond requires then".into())
                })?)?;
                let else_expr = Self::parse_expression(obj.get("else").ok_or_else(|| {
                    DocumentStoreError::InvalidAggregation("$cond requires else".into())
                })?)?;
                Ok(Expression::Cond {
                    r#if: Box::new(if_expr),
                    then: Box::new(then_expr),
                    r#else: Box::new(else_expr),
                })
            }
            "$toUpper" => {
                let expr = Self::parse_expression(value)?;
                Ok(Expression::ToUpper(Box::new(expr)))
            }
            "$toLower" => {
                let expr = Self::parse_expression(value)?;
                Ok(Expression::ToLower(Box::new(expr)))
            }
            "$size" => {
                let expr = Self::parse_expression(value)?;
                Ok(Expression::Size(Box::new(expr)))
            }
            "$type" => {
                let expr = Self::parse_expression(value)?;
                Ok(Expression::Type(Box::new(expr)))
            }
            "$ifNull" => {
                let arr = value.as_array().ok_or_else(|| {
                    DocumentStoreError::InvalidAggregation("$ifNull requires an array".into())
                })?;
                if arr.len() != 2 {
                    return Err(DocumentStoreError::InvalidAggregation(
                        "$ifNull requires exactly 2 arguments".into(),
                    ));
                }
                let expr = Self::parse_expression(&arr[0])?;
                let replacement = Self::parse_expression(&arr[1])?;
                Ok(Expression::IfNull(Box::new(expr), Box::new(replacement)))
            }
            _ => Err(DocumentStoreError::InvalidAggregation(format!(
                "Unknown expression operator: {}",
                op
            ))),
        }
    }

    /// Parse sort specification
    fn parse_sort(
        value: &serde_json::Value,
    ) -> Result<Vec<(String, SortDirection)>, DocumentStoreError> {
        let obj = value.as_object().ok_or_else(|| {
            DocumentStoreError::InvalidAggregation("$sort must be an object".into())
        })?;

        let mut sort = Vec::new();

        for (field, dir) in obj {
            let direction = match dir.as_i64() {
                Some(1) => SortDirection::Ascending,
                Some(-1) => SortDirection::Descending,
                _ => {
                    return Err(DocumentStoreError::InvalidAggregation(
                        "Sort direction must be 1 or -1".into(),
                    ))
                }
            };
            sort.push((field.clone(), direction));
        }

        Ok(sort)
    }

    /// Parse unwind specification
    fn parse_unwind(value: &serde_json::Value) -> Result<UnwindSpec, DocumentStoreError> {
        match value {
            serde_json::Value::String(path) => {
                let path = path.strip_prefix('$').unwrap_or(path).to_string();
                Ok(UnwindSpec {
                    path,
                    include_array_index: None,
                    preserve_null_and_empty: false,
                })
            }
            serde_json::Value::Object(obj) => {
                let path = obj.get("path").and_then(|v| v.as_str()).ok_or_else(|| {
                    DocumentStoreError::InvalidAggregation("$unwind requires path".into())
                })?;
                let path = path.strip_prefix('$').unwrap_or(path).to_string();
                let include_array_index = obj
                    .get("includeArrayIndex")
                    .and_then(|v| v.as_str())
                    .map(String::from);
                let preserve_null_and_empty = obj
                    .get("preserveNullAndEmptyArrays")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false);
                Ok(UnwindSpec {
                    path,
                    include_array_index,
                    preserve_null_and_empty,
                })
            }
            _ => Err(DocumentStoreError::InvalidAggregation(
                "$unwind must be a string or object".into(),
            )),
        }
    }

    /// Parse lookup specification
    fn parse_lookup(value: &serde_json::Value) -> Result<LookupSpec, DocumentStoreError> {
        let obj = value.as_object().ok_or_else(|| {
            DocumentStoreError::InvalidAggregation("$lookup must be an object".into())
        })?;

        let from = obj
            .get("from")
            .and_then(|v| v.as_str())
            .ok_or_else(|| DocumentStoreError::InvalidAggregation("$lookup requires from".into()))?
            .to_string();

        let local_field = obj
            .get("localField")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                DocumentStoreError::InvalidAggregation("$lookup requires localField".into())
            })?
            .to_string();

        let foreign_field = obj
            .get("foreignField")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                DocumentStoreError::InvalidAggregation("$lookup requires foreignField".into())
            })?
            .to_string();

        let r#as = obj
            .get("as")
            .and_then(|v| v.as_str())
            .ok_or_else(|| DocumentStoreError::InvalidAggregation("$lookup requires as".into()))?
            .to_string();

        Ok(LookupSpec {
            from,
            local_field,
            foreign_field,
            r#as,
        })
    }

    /// Parse add fields specification
    fn parse_add_fields(
        value: &serde_json::Value,
    ) -> Result<HashMap<String, Expression>, DocumentStoreError> {
        let obj = value.as_object().ok_or_else(|| {
            DocumentStoreError::InvalidAggregation("$addFields must be an object".into())
        })?;

        let mut fields = HashMap::new();

        for (field, val) in obj {
            fields.insert(field.clone(), Self::parse_expression(val)?);
        }

        Ok(fields)
    }

    /// Execute the pipeline
    pub fn execute(&self, collection: &Collection) -> Result<Vec<Document>, DocumentStoreError> {
        let mut documents: Vec<Document> = collection.documents().cloned().collect();

        for stage in &self.stages {
            documents = self.execute_stage(stage, documents)?;
        }

        Ok(documents)
    }

    /// Execute a single stage
    fn execute_stage(
        &self,
        stage: &PipelineStage,
        documents: Vec<Document>,
    ) -> Result<Vec<Document>, DocumentStoreError> {
        match stage {
            PipelineStage::Match(query) => {
                Ok(documents.into_iter().filter(|d| query.matches(d)).collect())
            }
            PipelineStage::Project(spec) => self.execute_project(documents, spec),
            PipelineStage::Group(spec) => self.execute_group(documents, spec),
            PipelineStage::Sort(sort) => self.execute_sort(documents, sort),
            PipelineStage::Limit(n) => Ok(documents.into_iter().take(*n).collect()),
            PipelineStage::Skip(n) => Ok(documents.into_iter().skip(*n).collect()),
            PipelineStage::Unwind(spec) => self.execute_unwind(documents, spec),
            PipelineStage::AddFields(fields) => self.execute_add_fields(documents, fields),
            PipelineStage::Count(field) => {
                let count = documents.len() as i64;
                let doc = Document::from_json(serde_json::json!({ field: count }))?;
                Ok(vec![doc])
            }
            PipelineStage::Sample(n) => {
                use std::collections::HashSet;
                if documents.len() <= *n {
                    return Ok(documents);
                }
                // Simple random sampling
                let mut sampled = Vec::with_capacity(*n);
                let mut indices: HashSet<usize> = HashSet::new();
                while indices.len() < *n {
                    let idx = (std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_nanos() as usize)
                        % documents.len();
                    if indices.insert(idx) {
                        sampled.push(documents[idx].clone());
                    }
                }
                Ok(sampled)
            }
            PipelineStage::ReplaceRoot(expr) => self.execute_replace_root(documents, expr),
            PipelineStage::Facet(facets) => self.execute_facet(documents, facets),
            PipelineStage::Bucket(spec) => self.execute_bucket(documents, spec),
            PipelineStage::BucketAuto(spec) => self.execute_bucket_auto(documents, spec),
            PipelineStage::Lookup(_) => {
                // Lookup requires access to other collections which is not available in this context
                // This would need to be handled at a higher level (DocumentStore)
                Err(DocumentStoreError::InvalidAggregation(
                    "$lookup must be executed through DocumentStore.aggregate() for cross-collection joins".into()
                ))
            }
            PipelineStage::Out(_) => {
                // Out writes to a collection, requires external access
                Err(DocumentStoreError::InvalidAggregation(
                    "$out must be executed through DocumentStore.aggregate() for collection writes"
                        .into(),
                ))
            }
            PipelineStage::Merge(_) => {
                // Merge writes to a collection, requires external access
                Err(DocumentStoreError::InvalidAggregation(
                    "$merge must be executed through DocumentStore.aggregate() for collection writes".into()
                ))
            }
        }
    }

    /// Execute replace root stage
    fn execute_replace_root(
        &self,
        documents: Vec<Document>,
        expr: &Expression,
    ) -> Result<Vec<Document>, DocumentStoreError> {
        let mut result = Vec::with_capacity(documents.len());

        for doc in documents {
            let new_root = self.evaluate_expression(expr, &doc)?;

            // The new root must be an object
            if !new_root.is_object() {
                return Err(DocumentStoreError::InvalidAggregation(
                    "$replaceRoot expression must evaluate to an object".into(),
                ));
            }

            let new_doc = Document {
                id: doc.id.clone(),
                data: new_root,
                version: doc.version,
                created_at: doc.created_at,
                updated_at: doc.updated_at,
            };
            result.push(new_doc);
        }

        Ok(result)
    }

    /// Execute facet stage (multiple parallel pipelines)
    fn execute_facet(
        &self,
        documents: Vec<Document>,
        facets: &HashMap<String, Vec<PipelineStage>>,
    ) -> Result<Vec<Document>, DocumentStoreError> {
        let mut facet_results = serde_json::Map::new();

        for (name, stages) in facets {
            // Run each sub-pipeline independently
            let mut sub_docs = documents.clone();
            for stage in stages {
                sub_docs = self.execute_stage(stage, sub_docs)?;
            }

            // Convert documents to JSON array
            let results: Vec<serde_json::Value> =
                sub_docs.into_iter().map(|d| d.data.clone()).collect();
            facet_results.insert(name.clone(), serde_json::Value::Array(results));
        }

        // Return single document with all facet results
        let doc = Document::from_json(serde_json::Value::Object(facet_results))?;
        Ok(vec![doc])
    }

    /// Execute bucket stage
    fn execute_bucket(
        &self,
        documents: Vec<Document>,
        spec: &BucketSpec,
    ) -> Result<Vec<Document>, DocumentStoreError> {
        if spec.boundaries.len() < 2 {
            return Err(DocumentStoreError::InvalidAggregation(
                "$bucket requires at least 2 boundaries".into(),
            ));
        }

        // Create buckets
        let mut buckets: HashMap<String, Vec<Document>> = HashMap::new();
        let mut default_bucket: Vec<Document> = Vec::new();

        for doc in documents {
            let value = self.evaluate_expression(&spec.group_by, &doc)?;
            let mut found_bucket = false;

            // Find which bucket this value belongs to
            for i in 0..spec.boundaries.len() - 1 {
                let lower = &spec.boundaries[i];
                let upper = &spec.boundaries[i + 1];

                if Self::value_in_range(&value, lower, upper) {
                    let bucket_key = serde_json::to_string(lower).unwrap_or_default();
                    buckets.entry(bucket_key).or_default().push(doc.clone());
                    found_bucket = true;
                    break;
                }
            }

            if !found_bucket && spec.default.is_some() {
                default_bucket.push(doc);
                // If no default, document is dropped (MongoDB behavior)
            }
        }

        // Build result documents
        let mut result = Vec::new();

        for i in 0..spec.boundaries.len() - 1 {
            let lower = &spec.boundaries[i];
            let bucket_key = serde_json::to_string(lower).unwrap_or_default();
            let docs = buckets.get(&bucket_key).cloned().unwrap_or_default();

            let mut bucket_doc = serde_json::Map::new();
            bucket_doc.insert("_id".to_string(), lower.clone());

            for (field, acc) in &spec.output {
                let value = self.evaluate_accumulator(acc, &docs)?;
                bucket_doc.insert(field.clone(), value);
            }

            result.push(Document::from_json(serde_json::Value::Object(bucket_doc))?);
        }

        // Add default bucket if exists
        if let Some(ref default_name) = spec.default {
            if !default_bucket.is_empty() {
                let mut bucket_doc = serde_json::Map::new();
                bucket_doc.insert(
                    "_id".to_string(),
                    serde_json::Value::String(default_name.clone()),
                );

                for (field, acc) in &spec.output {
                    let value = self.evaluate_accumulator(acc, &default_bucket)?;
                    bucket_doc.insert(field.clone(), value);
                }

                result.push(Document::from_json(serde_json::Value::Object(bucket_doc))?);
            }
        }

        Ok(result)
    }

    /// Check if value is within range [lower, upper)
    fn value_in_range(
        value: &serde_json::Value,
        lower: &serde_json::Value,
        upper: &serde_json::Value,
    ) -> bool {
        match (value.as_f64(), lower.as_f64(), upper.as_f64()) {
            (Some(v), Some(l), Some(u)) => v >= l && v < u,
            _ => {
                // Try string comparison
                match (value.as_str(), lower.as_str(), upper.as_str()) {
                    (Some(v), Some(l), Some(u)) => v >= l && v < u,
                    _ => false,
                }
            }
        }
    }

    /// Execute bucket auto stage
    fn execute_bucket_auto(
        &self,
        documents: Vec<Document>,
        spec: &BucketAutoSpec,
    ) -> Result<Vec<Document>, DocumentStoreError> {
        if spec.buckets == 0 {
            return Err(DocumentStoreError::InvalidAggregation(
                "$bucketAuto requires at least 1 bucket".into(),
            ));
        }

        // Collect all values and sort them
        let mut values: Vec<(f64, Document)> = Vec::new();
        for doc in documents {
            let value = self.evaluate_expression(&spec.group_by, &doc)?;
            if let Some(v) = value.as_f64() {
                values.push((v, doc));
            }
        }

        if values.is_empty() {
            return Ok(Vec::new());
        }

        values.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));

        // Calculate bucket boundaries
        let num_buckets = spec.buckets.min(values.len());
        let docs_per_bucket = values.len().div_ceil(num_buckets);

        let mut result = Vec::new();
        let mut start_idx = 0;

        while start_idx < values.len() {
            let end_idx = (start_idx + docs_per_bucket).min(values.len());
            let bucket_docs: Vec<Document> = values[start_idx..end_idx]
                .iter()
                .map(|(_, d)| d.clone())
                .collect();

            let min_val = values[start_idx].0;
            let max_val = if end_idx < values.len() {
                values[end_idx].0
            } else {
                values[end_idx - 1].0 + 1.0 // Upper bound is exclusive
            };

            let mut bucket_doc = serde_json::Map::new();
            bucket_doc.insert(
                "_id".to_string(),
                serde_json::json!({
                    "min": min_val,
                    "max": max_val
                }),
            );

            for (field, acc) in &spec.output {
                let value = self.evaluate_accumulator(acc, &bucket_docs)?;
                bucket_doc.insert(field.clone(), value);
            }

            result.push(Document::from_json(serde_json::Value::Object(bucket_doc))?);
            start_idx = end_idx;
        }

        Ok(result)
    }

    /// Execute project stage
    fn execute_project(
        &self,
        documents: Vec<Document>,
        spec: &ProjectSpec,
    ) -> Result<Vec<Document>, DocumentStoreError> {
        let mut result = Vec::with_capacity(documents.len());

        for doc in documents {
            let mut new_data = serde_json::Map::new();

            // Determine if we're including or excluding
            let has_includes = spec
                .fields
                .values()
                .any(|v| matches!(v, ProjectValue::Include | ProjectValue::Expression(_)));

            if has_includes {
                // Include mode: only specified fields
                for (field, pv) in &spec.fields {
                    match pv {
                        ProjectValue::Include => {
                            if let Some(value) = doc.get_field(field) {
                                new_data.insert(field.clone(), value.clone());
                            }
                        }
                        ProjectValue::Expression(expr) => {
                            let value = self.evaluate_expression(expr, &doc)?;
                            new_data.insert(field.clone(), value);
                        }
                        ProjectValue::Exclude => {}
                    }
                }
                // Always include _id unless explicitly excluded
                if !spec.fields.contains_key("_id")
                    || !matches!(spec.fields.get("_id"), Some(ProjectValue::Exclude))
                {
                    new_data.insert("_id".to_string(), doc.id.to_json());
                }
            } else {
                // Exclude mode: all fields except specified
                if let serde_json::Value::Object(obj) = &doc.data {
                    for (field, value) in obj {
                        if !spec.fields.contains_key(field) {
                            new_data.insert(field.clone(), value.clone());
                        }
                    }
                }
            }

            let new_doc = Document {
                id: doc.id.clone(),
                data: serde_json::Value::Object(new_data),
                version: doc.version,
                created_at: doc.created_at,
                updated_at: doc.updated_at,
            };
            result.push(new_doc);
        }

        Ok(result)
    }

    /// Execute group stage
    fn execute_group(
        &self,
        documents: Vec<Document>,
        spec: &GroupSpec,
    ) -> Result<Vec<Document>, DocumentStoreError> {
        let mut groups: HashMap<String, Vec<Document>> = HashMap::new();

        for doc in documents {
            let key = self.evaluate_expression(&spec.id, &doc)?;
            let key_str = serde_json::to_string(&key).unwrap_or_default();
            groups.entry(key_str).or_default().push(doc);
        }

        let mut result = Vec::with_capacity(groups.len());

        for (key_str, docs) in groups {
            let key: serde_json::Value = serde_json::from_str(&key_str).unwrap_or_default();
            let mut group_doc = serde_json::Map::new();
            group_doc.insert("_id".to_string(), key);

            for (field, acc) in &spec.accumulators {
                let value = self.evaluate_accumulator(acc, &docs)?;
                group_doc.insert(field.clone(), value);
            }

            let doc = Document::from_json(serde_json::Value::Object(group_doc))?;
            result.push(doc);
        }

        Ok(result)
    }

    /// Execute sort stage
    fn execute_sort(
        &self,
        mut documents: Vec<Document>,
        sort: &[(String, SortDirection)],
    ) -> Result<Vec<Document>, DocumentStoreError> {
        documents.sort_by(|a, b| {
            for (field, direction) in sort {
                let va = a.get_field(field);
                let vb = b.get_field(field);

                let cmp = Self::compare_values(va, vb);
                if cmp != std::cmp::Ordering::Equal {
                    return match direction {
                        SortDirection::Ascending => cmp,
                        SortDirection::Descending => cmp.reverse(),
                    };
                }
            }
            std::cmp::Ordering::Equal
        });

        Ok(documents)
    }

    /// Compare two values for sorting
    fn compare_values(
        a: Option<&serde_json::Value>,
        b: Option<&serde_json::Value>,
    ) -> std::cmp::Ordering {
        match (a, b) {
            (None, None) => std::cmp::Ordering::Equal,
            (None, Some(_)) => std::cmp::Ordering::Less,
            (Some(_), None) => std::cmp::Ordering::Greater,
            (Some(va), Some(vb)) => match (va, vb) {
                (serde_json::Value::Number(na), serde_json::Value::Number(nb)) => {
                    let fa = na.as_f64().unwrap_or(0.0);
                    let fb = nb.as_f64().unwrap_or(0.0);
                    fa.partial_cmp(&fb).unwrap_or(std::cmp::Ordering::Equal)
                }
                (serde_json::Value::String(sa), serde_json::Value::String(sb)) => sa.cmp(sb),
                _ => std::cmp::Ordering::Equal,
            },
        }
    }

    /// Execute unwind stage
    fn execute_unwind(
        &self,
        documents: Vec<Document>,
        spec: &UnwindSpec,
    ) -> Result<Vec<Document>, DocumentStoreError> {
        let mut result = Vec::new();

        for doc in documents {
            let array_value = doc.get_field(&spec.path);

            match array_value {
                Some(serde_json::Value::Array(arr)) if !arr.is_empty() => {
                    for (index, item) in arr.iter().enumerate() {
                        let mut new_data = doc.data.clone();
                        if let Some(obj) = new_data.as_object_mut() {
                            // Replace array with single element
                            obj.insert(spec.path.clone(), item.clone());

                            // Add array index if requested
                            if let Some(ref index_field) = spec.include_array_index {
                                obj.insert(
                                    index_field.clone(),
                                    serde_json::Value::Number((index as i64).into()),
                                );
                            }
                        }
                        let new_doc = Document {
                            id: doc.id.clone(),
                            data: new_data,
                            version: doc.version,
                            created_at: doc.created_at,
                            updated_at: doc.updated_at,
                        };
                        result.push(new_doc);
                    }
                }
                Some(serde_json::Value::Array(_)) | None | Some(serde_json::Value::Null) => {
                    if spec.preserve_null_and_empty {
                        let mut new_data = doc.data.clone();
                        if let Some(obj) = new_data.as_object_mut() {
                            obj.insert(spec.path.clone(), serde_json::Value::Null);
                        }
                        let new_doc = Document {
                            id: doc.id.clone(),
                            data: new_data,
                            version: doc.version,
                            created_at: doc.created_at,
                            updated_at: doc.updated_at,
                        };
                        result.push(new_doc);
                    }
                }
                Some(_) => {
                    // Not an array - include document as-is
                    result.push(doc);
                }
            }
        }

        Ok(result)
    }

    /// Execute add fields stage
    fn execute_add_fields(
        &self,
        documents: Vec<Document>,
        fields: &HashMap<String, Expression>,
    ) -> Result<Vec<Document>, DocumentStoreError> {
        let mut result = Vec::with_capacity(documents.len());

        for doc in documents {
            let mut new_data = doc.data.clone();

            for (field, expr) in fields {
                let value = self.evaluate_expression(expr, &doc)?;
                if let Some(obj) = new_data.as_object_mut() {
                    obj.insert(field.clone(), value);
                }
            }

            let new_doc = Document {
                id: doc.id.clone(),
                data: new_data,
                version: doc.version,
                created_at: doc.created_at,
                updated_at: doc.updated_at,
            };
            result.push(new_doc);
        }

        Ok(result)
    }

    /// Evaluate an expression
    #[allow(clippy::only_used_in_recursion)]
    fn evaluate_expression(
        &self,
        expr: &Expression,
        doc: &Document,
    ) -> Result<serde_json::Value, DocumentStoreError> {
        match expr {
            Expression::Literal(v) => Ok(v.clone()),
            Expression::Field(path) => Ok(doc
                .get_field(path)
                .cloned()
                .unwrap_or(serde_json::Value::Null)),
            Expression::Array(exprs) => {
                let values: Result<Vec<_>, _> = exprs
                    .iter()
                    .map(|e| self.evaluate_expression(e, doc))
                    .collect();
                Ok(serde_json::Value::Array(values?))
            }
            Expression::Object(fields) => {
                let mut obj = serde_json::Map::new();
                for (k, e) in fields {
                    obj.insert(k.clone(), self.evaluate_expression(e, doc)?);
                }
                Ok(serde_json::Value::Object(obj))
            }
            Expression::Add(exprs) => {
                let mut sum = 0.0;
                for e in exprs {
                    let v = self.evaluate_expression(e, doc)?;
                    sum += v.as_f64().unwrap_or(0.0);
                }
                Ok(serde_json::json!(sum))
            }
            Expression::Subtract(left, right) => {
                let l = self.evaluate_expression(left, doc)?.as_f64().unwrap_or(0.0);
                let r = self
                    .evaluate_expression(right, doc)?
                    .as_f64()
                    .unwrap_or(0.0);
                Ok(serde_json::json!(l - r))
            }
            Expression::Multiply(exprs) => {
                let mut product = 1.0;
                for e in exprs {
                    let v = self.evaluate_expression(e, doc)?;
                    product *= v.as_f64().unwrap_or(1.0);
                }
                Ok(serde_json::json!(product))
            }
            Expression::Divide(left, right) => {
                let l = self.evaluate_expression(left, doc)?.as_f64().unwrap_or(0.0);
                let r = self
                    .evaluate_expression(right, doc)?
                    .as_f64()
                    .unwrap_or(1.0);
                Ok(serde_json::json!(l / r))
            }
            Expression::Concat(exprs) => {
                let mut result = String::new();
                for e in exprs {
                    let v = self.evaluate_expression(e, doc)?;
                    if let Some(s) = v.as_str() {
                        result.push_str(s);
                    }
                }
                Ok(serde_json::Value::String(result))
            }
            Expression::Cond { r#if, then, r#else } => {
                let condition = self.evaluate_expression(r#if, doc)?;
                let is_true = match condition {
                    serde_json::Value::Bool(b) => b,
                    serde_json::Value::Null => false,
                    serde_json::Value::Number(n) => n.as_f64().unwrap_or(0.0) != 0.0,
                    _ => true,
                };
                if is_true {
                    self.evaluate_expression(then, doc)
                } else {
                    self.evaluate_expression(r#else, doc)
                }
            }
            Expression::ToUpper(e) => {
                let v = self.evaluate_expression(e, doc)?;
                Ok(serde_json::Value::String(
                    v.as_str().unwrap_or("").to_uppercase(),
                ))
            }
            Expression::ToLower(e) => {
                let v = self.evaluate_expression(e, doc)?;
                Ok(serde_json::Value::String(
                    v.as_str().unwrap_or("").to_lowercase(),
                ))
            }
            Expression::Size(e) => {
                let v = self.evaluate_expression(e, doc)?;
                let size = match v {
                    serde_json::Value::Array(arr) => arr.len(),
                    serde_json::Value::String(s) => s.len(),
                    _ => 0,
                };
                Ok(serde_json::json!(size))
            }
            Expression::IfNull(expr, replacement) => {
                let v = self.evaluate_expression(expr, doc)?;
                if v.is_null() {
                    self.evaluate_expression(replacement, doc)
                } else {
                    Ok(v)
                }
            }
            Expression::Mod(left, right) => {
                let l = self.evaluate_expression(left, doc)?.as_f64().unwrap_or(0.0);
                let r = self
                    .evaluate_expression(right, doc)?
                    .as_f64()
                    .unwrap_or(1.0);
                if r == 0.0 {
                    Ok(serde_json::Value::Null)
                } else {
                    Ok(serde_json::json!(l % r))
                }
            }
            Expression::Switch { branches, default } => {
                for (case_expr, then_expr) in branches {
                    let condition = self.evaluate_expression(case_expr, doc)?;
                    let is_true = match condition {
                        serde_json::Value::Bool(b) => b,
                        serde_json::Value::Null => false,
                        serde_json::Value::Number(n) => n.as_f64().unwrap_or(0.0) != 0.0,
                        _ => true,
                    };
                    if is_true {
                        return self.evaluate_expression(then_expr, doc);
                    }
                }
                self.evaluate_expression(default, doc)
            }
            Expression::ArrayElemAt(array_expr, index_expr) => {
                let array = self.evaluate_expression(array_expr, doc)?;
                let index = self.evaluate_expression(index_expr, doc)?;

                if let (serde_json::Value::Array(arr), Some(idx)) = (array, index.as_i64()) {
                    let actual_idx = if idx < 0 {
                        (arr.len() as i64 + idx) as usize
                    } else {
                        idx as usize
                    };
                    Ok(arr
                        .get(actual_idx)
                        .cloned()
                        .unwrap_or(serde_json::Value::Null))
                } else {
                    Ok(serde_json::Value::Null)
                }
            }
            Expression::Substr(string_expr, start_expr, length_expr) => {
                let s = self.evaluate_expression(string_expr, doc)?;
                let start = self
                    .evaluate_expression(start_expr, doc)?
                    .as_i64()
                    .unwrap_or(0) as usize;
                let length = self
                    .evaluate_expression(length_expr, doc)?
                    .as_i64()
                    .unwrap_or(-1);

                if let Some(string) = s.as_str() {
                    let chars: Vec<char> = string.chars().collect();
                    if start >= chars.len() {
                        return Ok(serde_json::Value::String(String::new()));
                    }
                    let end = if length < 0 {
                        chars.len()
                    } else {
                        (start + length as usize).min(chars.len())
                    };
                    let result: String = chars[start..end].iter().collect();
                    Ok(serde_json::Value::String(result))
                } else {
                    Ok(serde_json::Value::Null)
                }
            }
            Expression::Year(date_expr) => {
                let v = self.evaluate_expression(date_expr, doc)?;
                if let Some(date_str) = v.as_str() {
                    // Parse ISO date format YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS
                    if date_str.len() >= 4 {
                        if let Ok(year) = date_str[0..4].parse::<i32>() {
                            return Ok(serde_json::json!(year));
                        }
                    }
                } else if let Some(timestamp) = v.as_i64() {
                    // Unix timestamp in milliseconds
                    let secs = timestamp / 1000;
                    let datetime = chrono::DateTime::from_timestamp(secs, 0);
                    if let Some(dt) = datetime {
                        return Ok(serde_json::json!(dt
                            .format("%Y")
                            .to_string()
                            .parse::<i32>()
                            .unwrap_or(0)));
                    }
                }
                Ok(serde_json::Value::Null)
            }
            Expression::Month(date_expr) => {
                let v = self.evaluate_expression(date_expr, doc)?;
                if let Some(date_str) = v.as_str() {
                    if date_str.len() >= 7 {
                        if let Ok(month) = date_str[5..7].parse::<i32>() {
                            return Ok(serde_json::json!(month));
                        }
                    }
                } else if let Some(timestamp) = v.as_i64() {
                    let secs = timestamp / 1000;
                    let datetime = chrono::DateTime::from_timestamp(secs, 0);
                    if let Some(dt) = datetime {
                        return Ok(serde_json::json!(dt
                            .format("%m")
                            .to_string()
                            .parse::<i32>()
                            .unwrap_or(0)));
                    }
                }
                Ok(serde_json::Value::Null)
            }
            Expression::DayOfMonth(date_expr) => {
                let v = self.evaluate_expression(date_expr, doc)?;
                if let Some(date_str) = v.as_str() {
                    if date_str.len() >= 10 {
                        if let Ok(day) = date_str[8..10].parse::<i32>() {
                            return Ok(serde_json::json!(day));
                        }
                    }
                } else if let Some(timestamp) = v.as_i64() {
                    let secs = timestamp / 1000;
                    let datetime = chrono::DateTime::from_timestamp(secs, 0);
                    if let Some(dt) = datetime {
                        return Ok(serde_json::json!(dt
                            .format("%d")
                            .to_string()
                            .parse::<i32>()
                            .unwrap_or(0)));
                    }
                }
                Ok(serde_json::Value::Null)
            }
            Expression::Eq(left, right) => {
                let l = self.evaluate_expression(left, doc)?;
                let r = self.evaluate_expression(right, doc)?;
                Ok(serde_json::json!(l == r))
            }
            Expression::Ne(left, right) => {
                let l = self.evaluate_expression(left, doc)?;
                let r = self.evaluate_expression(right, doc)?;
                Ok(serde_json::json!(l != r))
            }
            Expression::Gt(left, right) => {
                let l = self.evaluate_expression(left, doc)?.as_f64();
                let r = self.evaluate_expression(right, doc)?.as_f64();
                match (l, r) {
                    (Some(lv), Some(rv)) => Ok(serde_json::json!(lv > rv)),
                    _ => Ok(serde_json::json!(false)),
                }
            }
            Expression::Gte(left, right) => {
                let l = self.evaluate_expression(left, doc)?.as_f64();
                let r = self.evaluate_expression(right, doc)?.as_f64();
                match (l, r) {
                    (Some(lv), Some(rv)) => Ok(serde_json::json!(lv >= rv)),
                    _ => Ok(serde_json::json!(false)),
                }
            }
            Expression::Lt(left, right) => {
                let l = self.evaluate_expression(left, doc)?.as_f64();
                let r = self.evaluate_expression(right, doc)?.as_f64();
                match (l, r) {
                    (Some(lv), Some(rv)) => Ok(serde_json::json!(lv < rv)),
                    _ => Ok(serde_json::json!(false)),
                }
            }
            Expression::Lte(left, right) => {
                let l = self.evaluate_expression(left, doc)?.as_f64();
                let r = self.evaluate_expression(right, doc)?.as_f64();
                match (l, r) {
                    (Some(lv), Some(rv)) => Ok(serde_json::json!(lv <= rv)),
                    _ => Ok(serde_json::json!(false)),
                }
            }
            Expression::And(exprs) => {
                for e in exprs {
                    let v = self.evaluate_expression(e, doc)?;
                    let is_true = match v {
                        serde_json::Value::Bool(b) => b,
                        serde_json::Value::Null => false,
                        serde_json::Value::Number(n) => n.as_f64().unwrap_or(0.0) != 0.0,
                        _ => true,
                    };
                    if !is_true {
                        return Ok(serde_json::json!(false));
                    }
                }
                Ok(serde_json::json!(true))
            }
            Expression::Or(exprs) => {
                for e in exprs {
                    let v = self.evaluate_expression(e, doc)?;
                    let is_true = match v {
                        serde_json::Value::Bool(b) => b,
                        serde_json::Value::Null => false,
                        serde_json::Value::Number(n) => n.as_f64().unwrap_or(0.0) != 0.0,
                        _ => true,
                    };
                    if is_true {
                        return Ok(serde_json::json!(true));
                    }
                }
                Ok(serde_json::json!(false))
            }
            Expression::Not(expr) => {
                let v = self.evaluate_expression(expr, doc)?;
                let is_true = match v {
                    serde_json::Value::Bool(b) => b,
                    serde_json::Value::Null => false,
                    serde_json::Value::Number(n) => n.as_f64().unwrap_or(0.0) != 0.0,
                    _ => true,
                };
                Ok(serde_json::json!(!is_true))
            }
            Expression::Type(expr) => {
                let v = self.evaluate_expression(expr, doc)?;
                let type_name = match v {
                    serde_json::Value::Null => "null",
                    serde_json::Value::Bool(_) => "bool",
                    serde_json::Value::Number(n) => {
                        if n.is_i64() {
                            "int"
                        } else {
                            "double"
                        }
                    }
                    serde_json::Value::String(_) => "string",
                    serde_json::Value::Array(_) => "array",
                    serde_json::Value::Object(_) => "object",
                };
                Ok(serde_json::Value::String(type_name.to_string()))
            }
        }
    }

    /// Evaluate an accumulator
    fn evaluate_accumulator(
        &self,
        acc: &Accumulator,
        docs: &[Document],
    ) -> Result<serde_json::Value, DocumentStoreError> {
        match acc {
            Accumulator::Sum(expr) => {
                let mut sum = 0.0;
                for doc in docs {
                    let v = self.evaluate_expression(expr, doc)?;
                    sum += v.as_f64().unwrap_or(0.0);
                }
                Ok(serde_json::json!(sum))
            }
            Accumulator::Avg(expr) => {
                if docs.is_empty() {
                    return Ok(serde_json::Value::Null);
                }
                let mut sum = 0.0;
                let mut count = 0;
                for doc in docs {
                    let v = self.evaluate_expression(expr, doc)?;
                    if let Some(n) = v.as_f64() {
                        sum += n;
                        count += 1;
                    }
                }
                if count == 0 {
                    Ok(serde_json::Value::Null)
                } else {
                    Ok(serde_json::json!(sum / count as f64))
                }
            }
            Accumulator::Min(expr) => {
                let mut min: Option<f64> = None;
                for doc in docs {
                    let v = self.evaluate_expression(expr, doc)?;
                    if let Some(n) = v.as_f64() {
                        min = Some(min.map(|m| m.min(n)).unwrap_or(n));
                    }
                }
                Ok(min
                    .map(|m| serde_json::json!(m))
                    .unwrap_or(serde_json::Value::Null))
            }
            Accumulator::Max(expr) => {
                let mut max: Option<f64> = None;
                for doc in docs {
                    let v = self.evaluate_expression(expr, doc)?;
                    if let Some(n) = v.as_f64() {
                        max = Some(max.map(|m| m.max(n)).unwrap_or(n));
                    }
                }
                Ok(max
                    .map(|m| serde_json::json!(m))
                    .unwrap_or(serde_json::Value::Null))
            }
            Accumulator::First(expr) => {
                if let Some(doc) = docs.first() {
                    self.evaluate_expression(expr, doc)
                } else {
                    Ok(serde_json::Value::Null)
                }
            }
            Accumulator::Last(expr) => {
                if let Some(doc) = docs.last() {
                    self.evaluate_expression(expr, doc)
                } else {
                    Ok(serde_json::Value::Null)
                }
            }
            Accumulator::Push(expr) => {
                let values: Result<Vec<_>, _> = docs
                    .iter()
                    .map(|doc| self.evaluate_expression(expr, doc))
                    .collect();
                Ok(serde_json::Value::Array(values?))
            }
            Accumulator::AddToSet(expr) => {
                let mut set = std::collections::HashSet::new();
                for doc in docs {
                    let v = self.evaluate_expression(expr, doc)?;
                    let key = serde_json::to_string(&v).unwrap_or_default();
                    set.insert(key);
                }
                let values: Vec<serde_json::Value> = set
                    .into_iter()
                    .filter_map(|s| serde_json::from_str(&s).ok())
                    .collect();
                Ok(serde_json::Value::Array(values))
            }
            Accumulator::Count => Ok(serde_json::json!(docs.len())),
            Accumulator::StdDevPop(expr) => {
                let mut values = Vec::new();
                for doc in docs {
                    let v = self.evaluate_expression(expr, doc)?;
                    if let Some(n) = v.as_f64() {
                        values.push(n);
                    }
                }
                if values.is_empty() {
                    return Ok(serde_json::Value::Null);
                }
                let mean = values.iter().sum::<f64>() / values.len() as f64;
                let variance =
                    values.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / values.len() as f64;
                Ok(serde_json::json!(variance.sqrt()))
            }
            Accumulator::StdDevSamp(expr) => {
                let mut values = Vec::new();
                for doc in docs {
                    let v = self.evaluate_expression(expr, doc)?;
                    if let Some(n) = v.as_f64() {
                        values.push(n);
                    }
                }
                if values.len() < 2 {
                    return Ok(serde_json::Value::Null);
                }
                let mean = values.iter().sum::<f64>() / values.len() as f64;
                let variance = values.iter().map(|x| (x - mean).powi(2)).sum::<f64>()
                    / (values.len() - 1) as f64;
                Ok(serde_json::json!(variance.sqrt()))
            }
        }
    }
}

impl Default for AggregationPipeline {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn create_test_collection() -> Collection {
        use super::super::collection::CollectionOptions;

        let mut collection = Collection::new("test".to_string(), CollectionOptions::default());

        let docs = vec![
            json!({ "name": "Alice", "age": 30, "city": "NYC", "score": 85 }),
            json!({ "name": "Bob", "age": 25, "city": "LA", "score": 90 }),
            json!({ "name": "Carol", "age": 35, "city": "NYC", "score": 75 }),
            json!({ "name": "Dave", "age": 28, "city": "LA", "score": 88 }),
        ];

        for doc in docs {
            let d = Document::from_json(doc).unwrap();
            collection.insert(d).unwrap();
        }

        collection
    }

    #[test]
    fn test_match_stage() {
        let collection = create_test_collection();

        let pipeline = AggregationPipeline::from_json(vec![json!({
            "$match": { "city": "NYC" }
        })])
        .unwrap();

        let results = pipeline.execute(&collection).unwrap();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_sort_limit() {
        let collection = create_test_collection();

        let pipeline = AggregationPipeline::from_json(vec![
            json!({ "$sort": { "score": -1 } }),
            json!({ "$limit": 2 }),
        ])
        .unwrap();

        let results = pipeline.execute(&collection).unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].get_field("score"), Some(&json!(90)));
    }

    #[test]
    fn test_group_sum() {
        let collection = create_test_collection();

        let pipeline = AggregationPipeline::from_json(vec![json!({
            "$group": {
                "_id": "$city",
                "totalScore": { "$sum": "$score" },
                "count": { "$sum": 1 }
            }
        })])
        .unwrap();

        let results = pipeline.execute(&collection).unwrap();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_project() {
        let collection = create_test_collection();

        let pipeline = AggregationPipeline::from_json(vec![json!({
            "$project": {
                "name": 1,
                "city": 1,
                "age": 0
            }
        })])
        .unwrap();

        let results = pipeline.execute(&collection).unwrap();
        assert_eq!(results.len(), 4);
        assert!(results[0].get_field("name").is_some());
        assert!(results[0].get_field("city").is_some());
    }

    #[test]
    fn test_count() {
        let collection = create_test_collection();

        let pipeline = AggregationPipeline::from_json(vec![
            json!({ "$match": { "city": "NYC" } }),
            json!({ "$count": "total" }),
        ])
        .unwrap();

        let results = pipeline.execute(&collection).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].get_field("total"), Some(&json!(2)));
    }
}
