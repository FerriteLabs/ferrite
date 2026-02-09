//! FerriteQL Query Planner
//!
//! Converts AST into an executable query plan.

use crate::query::ast::*;
use crate::query::{QueryConfig, QueryError, Value};

/// Query planner
pub struct QueryPlanner {
    config: QueryConfig,
}

impl QueryPlanner {
    /// Create a new query planner
    pub fn new(config: QueryConfig) -> Self {
        Self { config }
    }

    /// Plan a statement
    pub fn plan(&self, statement: &Statement) -> Result<QueryPlan, QueryError> {
        match statement {
            Statement::Select(select) => self.plan_select(select),
            Statement::Explain(inner) => {
                let plan = self.plan(inner)?;
                Ok(QueryPlan::Explain(Box::new(plan)))
            }
            Statement::Insert(insert) => self.plan_insert(insert),
            Statement::Update(update) => self.plan_update(update),
            Statement::Delete(delete) => self.plan_delete(delete),
            _ => Err(QueryError::Unsupported(
                "Statement type not yet supported".to_string(),
            )),
        }
    }

    /// Plan a statement with parameter substitution
    pub fn plan_with_params(
        &self,
        statement: &Statement,
        params: &[Value],
    ) -> Result<QueryPlan, QueryError> {
        let plan = self.plan(statement)?;
        Ok(self.substitute_params(plan, params))
    }

    /// Generate explain output for a plan
    pub fn explain(&self, plan: &QueryPlan) -> String {
        self.explain_node(plan, 0)
    }

    fn plan_select(&self, select: &SelectStatement) -> Result<QueryPlan, QueryError> {
        // Start with the FROM clause - this is our base scan
        let mut plan = if select.from.is_empty() {
            QueryPlan::EmptyRelation
        } else {
            let mut scans: Vec<QueryPlan> = select
                .from
                .iter()
                .map(|from| {
                    QueryPlan::Scan(ScanPlan {
                        pattern: from.pattern.clone(),
                        alias: from.alias.clone(),
                    })
                })
                .collect();

            if scans.len() == 1 {
                scans.remove(0)
            } else {
                // Cross join multiple sources
                let mut result = scans.remove(0);
                for scan in scans {
                    result = QueryPlan::Join(JoinPlan {
                        left: Box::new(result),
                        right: Box::new(scan),
                        join_type: PlanJoinType::Cross,
                        condition: None,
                    });
                }
                result
            }
        };

        // Handle JOINs
        for join in &select.joins {
            let right = QueryPlan::Scan(ScanPlan {
                pattern: join.source.pattern.clone(),
                alias: join.source.alias.clone(),
            });

            let join_type = match join.join_type {
                JoinType::Inner => PlanJoinType::Inner,
                JoinType::Left => PlanJoinType::Left,
                JoinType::Right => PlanJoinType::Right,
                JoinType::Full => PlanJoinType::Full,
                JoinType::Cross => PlanJoinType::Cross,
            };

            let condition = join.on.as_ref().map(|expr| self.plan_expr(expr));

            plan = QueryPlan::Join(JoinPlan {
                left: Box::new(plan),
                right: Box::new(right),
                join_type,
                condition,
            });
        }

        // WHERE clause - filter
        if let Some(where_clause) = &select.where_clause {
            plan = QueryPlan::Filter(FilterPlan {
                input: Box::new(plan),
                predicate: self.plan_expr(where_clause),
            });
        }

        // GROUP BY - aggregate
        if !select.group_by.is_empty() || self.has_aggregates(&select.columns) {
            let group_by: Vec<PlanExpr> =
                select.group_by.iter().map(|e| self.plan_expr(e)).collect();

            let aggregates: Vec<AggregatePlan> = select
                .columns
                .iter()
                .filter_map(|col| {
                    if let SelectColumn::Expr { expr, alias } = col {
                        self.extract_aggregate(expr).map(|agg| AggregatePlan {
                            function: agg.0,
                            arg: agg.1,
                            distinct: agg.2,
                            alias: alias.clone(),
                        })
                    } else {
                        None
                    }
                })
                .collect();

            plan = QueryPlan::Aggregate(AggregationPlan {
                input: Box::new(plan),
                group_by,
                aggregates,
            });

            // HAVING clause
            if let Some(having) = &select.having {
                plan = QueryPlan::Filter(FilterPlan {
                    input: Box::new(plan),
                    predicate: self.plan_expr(having),
                });
            }
        }

        // SELECT - projection
        let projections: Vec<ProjectionItem> = select
            .columns
            .iter()
            .map(|col| match col {
                SelectColumn::All => ProjectionItem::All,
                SelectColumn::AllFrom(table) => ProjectionItem::AllFrom(table.clone()),
                SelectColumn::Expr { expr, alias } => ProjectionItem::Expr {
                    expr: self.plan_expr(expr),
                    alias: alias.clone(),
                },
            })
            .collect();

        if select.distinct {
            plan = QueryPlan::Distinct(Box::new(QueryPlan::Project(ProjectionPlan {
                input: Box::new(plan),
                items: projections,
            })));
        } else {
            plan = QueryPlan::Project(ProjectionPlan {
                input: Box::new(plan),
                items: projections,
            });
        }

        // ORDER BY
        if !select.order_by.is_empty() {
            let sort_keys: Vec<SortKey> = select
                .order_by
                .iter()
                .map(|item| SortKey {
                    expr: self.plan_expr(&item.expr),
                    descending: item.direction == SortDirection::Desc,
                    nulls_first: item.nulls == Some(NullsOrder::First),
                })
                .collect();

            plan = QueryPlan::Sort(SortPlan {
                input: Box::new(plan),
                keys: sort_keys,
            });
        }

        // LIMIT and OFFSET
        if select.limit.is_some() || select.offset.is_some() {
            plan = QueryPlan::Limit(LimitPlan {
                input: Box::new(plan),
                limit: select.limit,
                offset: select.offset.unwrap_or(0),
            });
        }

        Ok(plan)
    }

    fn plan_insert(&self, insert: &InsertStatement) -> Result<QueryPlan, QueryError> {
        let values: Vec<Vec<PlanExpr>> = insert
            .values
            .iter()
            .map(|row| row.iter().map(|e| self.plan_expr(e)).collect())
            .collect();

        Ok(QueryPlan::Insert(InsertPlan {
            target: insert.target.clone(),
            columns: insert.columns.clone(),
            values,
        }))
    }

    fn plan_update(&self, update: &UpdateStatement) -> Result<QueryPlan, QueryError> {
        let assignments: Vec<(String, PlanExpr)> = update
            .assignments
            .iter()
            .map(|(col, expr)| (col.clone(), self.plan_expr(expr)))
            .collect();

        Ok(QueryPlan::Update(UpdatePlan {
            target: update.target.clone(),
            assignments,
            filter: update.where_clause.as_ref().map(|e| self.plan_expr(e)),
        }))
    }

    fn plan_delete(&self, delete: &DeleteStatement) -> Result<QueryPlan, QueryError> {
        Ok(QueryPlan::Delete(DeletePlan {
            target: delete.target.clone(),
            filter: delete.where_clause.as_ref().map(|e| self.plan_expr(e)),
        }))
    }

    fn plan_expr(&self, expr: &Expr) -> PlanExpr {
        match expr {
            Expr::Literal(lit) => PlanExpr::Literal(match lit {
                Literal::Null => Value::Null,
                Literal::Boolean(b) => Value::Bool(*b),
                Literal::Integer(n) => Value::Int(*n),
                Literal::Float(f) => Value::Float(*f),
                Literal::String(s) => Value::String(s.clone()),
            }),
            Expr::Column(col) => PlanExpr::Column {
                table: col.table.clone(),
                name: col.column.clone(),
            },
            Expr::BinaryOp { left, op, right } => PlanExpr::BinaryOp {
                left: Box::new(self.plan_expr(left)),
                op: self.plan_binary_op(op),
                right: Box::new(self.plan_expr(right)),
            },
            Expr::UnaryOp { op, expr } => PlanExpr::UnaryOp {
                op: self.plan_unary_op(op),
                expr: Box::new(self.plan_expr(expr)),
            },
            Expr::Function(func) => PlanExpr::Function {
                name: func.name.clone(),
                args: func.args.iter().map(|e| self.plan_expr(e)).collect(),
                distinct: func.distinct,
            },
            Expr::Case {
                operand,
                when_clauses,
                else_clause,
            } => PlanExpr::Case {
                operand: operand.as_ref().map(|e| Box::new(self.plan_expr(e))),
                when_clauses: when_clauses
                    .iter()
                    .map(|(w, t)| (self.plan_expr(w), self.plan_expr(t)))
                    .collect(),
                else_clause: else_clause.as_ref().map(|e| Box::new(self.plan_expr(e))),
            },
            Expr::In {
                expr,
                list,
                negated,
            } => PlanExpr::In {
                expr: Box::new(self.plan_expr(expr)),
                list: list.iter().map(|e| self.plan_expr(e)).collect(),
                negated: *negated,
            },
            Expr::InSubquery {
                expr,
                subquery,
                negated,
            } => {
                let subquery_plan = self
                    .plan_select(subquery)
                    .unwrap_or(QueryPlan::EmptyRelation);
                PlanExpr::InSubquery {
                    expr: Box::new(self.plan_expr(expr)),
                    subquery: Box::new(subquery_plan),
                    negated: *negated,
                }
            }
            Expr::Between {
                expr,
                low,
                high,
                negated,
            } => PlanExpr::Between {
                expr: Box::new(self.plan_expr(expr)),
                low: Box::new(self.plan_expr(low)),
                high: Box::new(self.plan_expr(high)),
                negated: *negated,
            },
            Expr::Like {
                expr,
                pattern,
                negated,
            } => PlanExpr::Like {
                expr: Box::new(self.plan_expr(expr)),
                pattern: Box::new(self.plan_expr(pattern)),
                negated: *negated,
            },
            Expr::IsNull { expr, negated } => PlanExpr::IsNull {
                expr: Box::new(self.plan_expr(expr)),
                negated: *negated,
            },
            Expr::Parameter(n) => PlanExpr::Parameter(*n),
            Expr::Cast { expr, data_type } => PlanExpr::Cast {
                expr: Box::new(self.plan_expr(expr)),
                data_type: data_type.clone(),
            },
            Expr::Nested(inner) => self.plan_expr(inner),
            Expr::Subquery(select) => {
                let subquery_plan = self
                    .plan_select(select)
                    .unwrap_or(QueryPlan::EmptyRelation);
                PlanExpr::InSubquery {
                    expr: Box::new(PlanExpr::Literal(Value::Null)),
                    subquery: Box::new(subquery_plan),
                    negated: false,
                }
            }
            Expr::Exists(select) => {
                let subquery_plan = self
                    .plan_select(select)
                    .unwrap_or(QueryPlan::EmptyRelation);
                PlanExpr::Exists {
                    subquery: Box::new(subquery_plan),
                    negated: false,
                }
            }
        }
    }

    fn plan_binary_op(&self, op: &BinaryOperator) -> PlanBinaryOp {
        match op {
            BinaryOperator::Add => PlanBinaryOp::Add,
            BinaryOperator::Subtract => PlanBinaryOp::Subtract,
            BinaryOperator::Multiply => PlanBinaryOp::Multiply,
            BinaryOperator::Divide => PlanBinaryOp::Divide,
            BinaryOperator::Modulo => PlanBinaryOp::Modulo,
            BinaryOperator::Equal => PlanBinaryOp::Equal,
            BinaryOperator::NotEqual => PlanBinaryOp::NotEqual,
            BinaryOperator::LessThan => PlanBinaryOp::LessThan,
            BinaryOperator::LessThanOrEqual => PlanBinaryOp::LessThanOrEqual,
            BinaryOperator::GreaterThan => PlanBinaryOp::GreaterThan,
            BinaryOperator::GreaterThanOrEqual => PlanBinaryOp::GreaterThanOrEqual,
            BinaryOperator::And => PlanBinaryOp::And,
            BinaryOperator::Or => PlanBinaryOp::Or,
            BinaryOperator::Concat => PlanBinaryOp::Concat,
        }
    }

    fn plan_unary_op(&self, op: &UnaryOperator) -> PlanUnaryOp {
        match op {
            UnaryOperator::Not => PlanUnaryOp::Not,
            UnaryOperator::Minus => PlanUnaryOp::Minus,
            UnaryOperator::Plus => PlanUnaryOp::Plus,
        }
    }

    fn has_aggregates(&self, columns: &[SelectColumn]) -> bool {
        columns.iter().any(|col| {
            if let SelectColumn::Expr { expr, .. } = col {
                self.contains_aggregate(expr)
            } else {
                false
            }
        })
    }

    #[allow(clippy::only_used_in_recursion)]
    fn contains_aggregate(&self, expr: &Expr) -> bool {
        match expr {
            Expr::Function(func) => {
                let name = func.name.to_uppercase();
                matches!(
                    name.as_str(),
                    "COUNT" | "SUM" | "AVG" | "MIN" | "MAX" | "ARRAY_AGG" | "STRING_AGG"
                )
            }
            Expr::BinaryOp { left, right, .. } => {
                self.contains_aggregate(left) || self.contains_aggregate(right)
            }
            Expr::UnaryOp { expr, .. } => self.contains_aggregate(expr),
            Expr::Nested(inner) => self.contains_aggregate(inner),
            _ => false,
        }
    }

    fn extract_aggregate(&self, expr: &Expr) -> Option<(String, Option<PlanExpr>, bool)> {
        if let Expr::Function(func) = expr {
            let name = func.name.to_uppercase();
            if matches!(
                name.as_str(),
                "COUNT" | "SUM" | "AVG" | "MIN" | "MAX" | "ARRAY_AGG" | "STRING_AGG"
            ) {
                let arg = func.args.first().map(|e| self.plan_expr(e));
                return Some((name, arg, func.distinct));
            }
        }
        None
    }

    fn substitute_params(&self, plan: QueryPlan, params: &[Value]) -> QueryPlan {
        // Would recursively substitute parameters in the plan
        // For now, return as-is
        let _ = params;
        plan
    }

    fn explain_node(&self, plan: &QueryPlan, indent: usize) -> String {
        let prefix = "  ".repeat(indent);
        match plan {
            QueryPlan::Scan(scan) => {
                let alias = scan
                    .alias
                    .as_ref()
                    .map(|a| format!(" AS {}", a))
                    .unwrap_or_default();
                let model = Self::detect_data_model(&scan.pattern);
                let est_keys = Self::estimate_scan_keys(&scan.pattern, self.config.max_scan_rows);
                let cost = Self::estimate_scan_cost(est_keys);
                format!(
                    "{}Scan: {}{} (model: {}, est. {} keys, cost: {:.1})\n",
                    prefix, scan.pattern, alias, model, est_keys, cost
                )
            }
            QueryPlan::Filter(filter) => {
                let predicate_str = Self::format_predicate(&filter.predicate);
                let index_info = Self::detect_index_usage(&filter.predicate);
                format!(
                    "{}Filter: {} [{}]\n{}",
                    prefix,
                    predicate_str,
                    index_info,
                    self.explain_node(&filter.input, indent + 1)
                )
            }
            QueryPlan::Project(proj) => {
                format!(
                    "{}Project: {} columns\n{}",
                    prefix,
                    proj.items.len(),
                    self.explain_node(&proj.input, indent + 1)
                )
            }
            QueryPlan::Join(join) => {
                let join_type_str = match join.join_type {
                    PlanJoinType::Inner => "Inner",
                    PlanJoinType::Left => "Left",
                    PlanJoinType::Right => "Right",
                    PlanJoinType::Full => "Full",
                    PlanJoinType::Cross => "Cross",
                };
                let cond = join
                    .condition
                    .as_ref()
                    .map(|c| format!(" on {}", Self::format_predicate(c)))
                    .unwrap_or_default();
                format!(
                    "{}Join: {} {}{}\n{}{}",
                    prefix,
                    join_type_str,
                    if matches!(join.join_type, PlanJoinType::Cross) {
                        "(cartesian product)"
                    } else {
                        "join"
                    },
                    cond,
                    self.explain_node(&join.left, indent + 1),
                    self.explain_node(&join.right, indent + 1)
                )
            }
            QueryPlan::Aggregate(agg) => {
                let agg_names: Vec<String> = agg
                    .aggregates
                    .iter()
                    .map(|a| {
                        let arg_str = a
                            .arg
                            .as_ref()
                            .map(Self::format_predicate)
                            .unwrap_or_else(|| "*".into());
                        if a.distinct {
                            format!("{}(DISTINCT {})", a.function, arg_str)
                        } else {
                            format!("{}({})", a.function, arg_str)
                        }
                    })
                    .collect();
                format!(
                    "{}Aggregate: {} groups, funcs=[{}]\n{}",
                    prefix,
                    agg.group_by.len(),
                    agg_names.join(", "),
                    self.explain_node(&agg.input, indent + 1)
                )
            }
            QueryPlan::Sort(sort) => {
                let keys_str: Vec<String> = sort
                    .keys
                    .iter()
                    .map(|k| {
                        let dir = if k.descending { "DESC" } else { "ASC" };
                        format!("{} {}", Self::format_predicate(&k.expr), dir)
                    })
                    .collect();
                format!(
                    "{}Sort: [{}] (est. memory: in-memory)\n{}",
                    prefix,
                    keys_str.join(", "),
                    self.explain_node(&sort.input, indent + 1)
                )
            }
            QueryPlan::Limit(limit) => {
                let limit_str = limit
                    .limit
                    .map(|l| format!("LIMIT {}", l))
                    .unwrap_or_default();
                let offset_str = if limit.offset > 0 {
                    format!(" OFFSET {}", limit.offset)
                } else {
                    String::new()
                };
                format!(
                    "{}{}{}\n{}",
                    prefix,
                    limit_str,
                    offset_str,
                    self.explain_node(&limit.input, indent + 1)
                )
            }
            QueryPlan::Distinct(input) => {
                format!(
                    "{}Distinct (hash-based dedup)\n{}",
                    prefix,
                    self.explain_node(input, indent + 1)
                )
            }
            QueryPlan::EmptyRelation => format!("{}Empty\n", prefix),
            QueryPlan::Explain(inner) => {
                format!(
                    "{}Explain\n{}",
                    prefix,
                    self.explain_node(inner, indent + 1)
                )
            }
            QueryPlan::Insert(insert) => {
                format!(
                    "{}Insert: {} ({} rows)\n",
                    prefix,
                    insert.target,
                    insert.values.len()
                )
            }
            QueryPlan::Update(update) => {
                format!(
                    "{}Update: {} ({} assignments)\n",
                    prefix,
                    update.target,
                    update.assignments.len()
                )
            }
            QueryPlan::Delete(delete) => {
                format!("{}Delete: {}\n", prefix, delete.target)
            }
        }
    }

    /// Detect the data model from a key pattern.
    fn detect_data_model(pattern: &str) -> &'static str {
        let lower = pattern.to_lowercase();
        if lower.contains("ts:") || lower.contains("timeseries") || lower.contains("metric") {
            "timeseries"
        } else if lower.contains("graph:") || lower.contains("edge:") || lower.contains("node:") {
            "graph"
        } else if lower.contains("doc:") || lower.contains("document") {
            "document"
        } else {
            "kv"
        }
    }

    /// Estimate the number of keys a scan will examine.
    fn estimate_scan_keys(pattern: &str, max_rows: usize) -> usize {
        if pattern.contains('*') || pattern.contains('?') {
            // Wildcard scan — estimate based on pattern specificity
            let prefix_len = pattern.find('*').unwrap_or(pattern.len());
            if prefix_len == 0 {
                max_rows.min(100_000)
            } else {
                // Longer prefix = narrower scan
                max_rows.min(10_000 / prefix_len.max(1))
            }
        } else {
            // Exact key lookup
            1
        }
    }

    /// Estimate the I/O cost of scanning `n` keys.
    fn estimate_scan_cost(est_keys: usize) -> f64 {
        est_keys as f64 * 0.1 // 0.1 cost units per key
    }

    /// Detect whether a filter predicate can use an index.
    fn detect_index_usage(predicate: &PlanExpr) -> &'static str {
        match predicate {
            PlanExpr::BinaryOp { op, left, .. } => {
                if matches!(
                    op,
                    PlanBinaryOp::Equal | PlanBinaryOp::LessThan | PlanBinaryOp::GreaterThan
                        | PlanBinaryOp::LessThanOrEqual | PlanBinaryOp::GreaterThanOrEqual
                ) && matches!(**left, PlanExpr::Column { .. })
                {
                    return "index: possible";
                }
                "index: none (full scan)"
            }
            PlanExpr::Between { expr, .. } => {
                if matches!(**expr, PlanExpr::Column { .. }) {
                    "index: range scan"
                } else {
                    "index: none"
                }
            }
            PlanExpr::In { expr, .. } => {
                if matches!(**expr, PlanExpr::Column { .. }) {
                    "index: multi-point lookup"
                } else {
                    "index: none"
                }
            }
            _ => "index: none",
        }
    }

    /// Format a plan expression as a human-readable string for EXPLAIN output.
    fn format_predicate(expr: &PlanExpr) -> String {
        match expr {
            PlanExpr::Column { table, name } => {
                if let Some(t) = table {
                    format!("{}.{}", t, name)
                } else {
                    name.clone()
                }
            }
            PlanExpr::Literal(v) => v.to_string(),
            PlanExpr::BinaryOp { left, op, right } => {
                let op_str = match op {
                    PlanBinaryOp::Equal => "=",
                    PlanBinaryOp::NotEqual => "!=",
                    PlanBinaryOp::LessThan => "<",
                    PlanBinaryOp::LessThanOrEqual => "<=",
                    PlanBinaryOp::GreaterThan => ">",
                    PlanBinaryOp::GreaterThanOrEqual => ">=",
                    PlanBinaryOp::And => "AND",
                    PlanBinaryOp::Or => "OR",
                    PlanBinaryOp::Add => "+",
                    PlanBinaryOp::Subtract => "-",
                    PlanBinaryOp::Multiply => "*",
                    PlanBinaryOp::Divide => "/",
                    PlanBinaryOp::Modulo => "%",
                    PlanBinaryOp::Concat => "||",
                };
                format!(
                    "{} {} {}",
                    Self::format_predicate(left),
                    op_str,
                    Self::format_predicate(right)
                )
            }
            PlanExpr::Function { name, args, .. } => {
                let arg_strs: Vec<String> =
                    args.iter().map(Self::format_predicate).collect();
                format!("{}({})", name, arg_strs.join(", "))
            }
            PlanExpr::IsNull { expr, negated } => {
                if *negated {
                    format!("{} IS NOT NULL", Self::format_predicate(expr))
                } else {
                    format!("{} IS NULL", Self::format_predicate(expr))
                }
            }
            PlanExpr::Between {
                expr, low, high, ..
            } => {
                format!(
                    "{} BETWEEN {} AND {}",
                    Self::format_predicate(expr),
                    Self::format_predicate(low),
                    Self::format_predicate(high)
                )
            }
            _ => format!("{:?}", expr),
        }
    }
}

/// Query execution plan
#[derive(Clone, Debug)]
pub enum QueryPlan {
    /// Scan keys matching a pattern
    Scan(ScanPlan),
    /// Filter rows
    Filter(FilterPlan),
    /// Project columns
    Project(ProjectionPlan),
    /// Join two relations
    Join(JoinPlan),
    /// Aggregate
    Aggregate(AggregationPlan),
    /// Sort
    Sort(SortPlan),
    /// Limit/offset
    Limit(LimitPlan),
    /// Distinct
    Distinct(Box<QueryPlan>),
    /// Empty relation
    EmptyRelation,
    /// Explain plan
    Explain(Box<QueryPlan>),
    /// Insert
    Insert(InsertPlan),
    /// Update
    Update(UpdatePlan),
    /// Delete
    Delete(DeletePlan),
}

/// Scan plan
#[derive(Clone, Debug)]
pub struct ScanPlan {
    /// Key pattern to scan (e.g. glob pattern).
    pub pattern: String,
    /// Optional alias for the scanned relation.
    pub alias: Option<String>,
}

/// Filter plan
#[derive(Clone, Debug)]
pub struct FilterPlan {
    /// Input plan to filter.
    pub input: Box<QueryPlan>,
    /// Filter predicate expression.
    pub predicate: PlanExpr,
}

/// Projection plan
#[derive(Clone, Debug)]
pub struct ProjectionPlan {
    /// Input plan to project from.
    pub input: Box<QueryPlan>,
    /// Projection items (columns or expressions).
    pub items: Vec<ProjectionItem>,
}

/// Projection item
#[derive(Clone, Debug)]
pub enum ProjectionItem {
    /// Select all columns (`*`).
    All,
    /// Select all columns from a specific table (`table.*`).
    AllFrom(String),
    /// A computed expression with an optional alias.
    Expr {
        /// The expression to project.
        expr: PlanExpr,
        /// Optional output alias.
        alias: Option<String>,
    },
}

/// Join plan
#[derive(Clone, Debug)]
pub struct JoinPlan {
    /// Left input relation.
    pub left: Box<QueryPlan>,
    /// Right input relation.
    pub right: Box<QueryPlan>,
    /// Type of join to perform.
    pub join_type: PlanJoinType,
    /// Optional join condition expression.
    pub condition: Option<PlanExpr>,
}

/// Join type in plan
#[derive(Clone, Debug)]
pub enum PlanJoinType {
    /// Inner join.
    Inner,
    /// Left outer join.
    Left,
    /// Right outer join.
    Right,
    /// Full outer join.
    Full,
    /// Cross join (cartesian product).
    Cross,
}

/// Aggregation plan
#[derive(Clone, Debug)]
pub struct AggregationPlan {
    /// Input plan to aggregate over.
    pub input: Box<QueryPlan>,
    /// Expressions to group by.
    pub group_by: Vec<PlanExpr>,
    /// Aggregate functions to compute.
    pub aggregates: Vec<AggregatePlan>,
}

/// Individual aggregate
#[derive(Clone, Debug)]
pub struct AggregatePlan {
    /// Aggregate function name (e.g. `COUNT`, `SUM`).
    pub function: String,
    /// Optional argument expression for the aggregate.
    pub arg: Option<PlanExpr>,
    /// Whether the aggregate uses `DISTINCT`.
    pub distinct: bool,
    /// Optional output alias.
    pub alias: Option<String>,
}

/// Sort plan
#[derive(Clone, Debug)]
pub struct SortPlan {
    /// Input plan to sort.
    pub input: Box<QueryPlan>,
    /// Sort keys with ordering.
    pub keys: Vec<SortKey>,
}

/// Sort key
#[derive(Clone, Debug)]
pub struct SortKey {
    /// Expression to sort by.
    pub expr: PlanExpr,
    /// Whether to sort in descending order.
    pub descending: bool,
    /// Whether nulls should sort first.
    pub nulls_first: bool,
}

/// Limit plan
#[derive(Clone, Debug)]
pub struct LimitPlan {
    /// Input plan to limit.
    pub input: Box<QueryPlan>,
    /// Maximum number of rows to return.
    pub limit: Option<u64>,
    /// Number of rows to skip.
    pub offset: u64,
}

/// Insert plan
#[derive(Clone, Debug)]
pub struct InsertPlan {
    /// Target table or key pattern.
    pub target: String,
    /// Column names for the insert.
    pub columns: Vec<String>,
    /// Rows of values to insert.
    pub values: Vec<Vec<PlanExpr>>,
}

/// Update plan
#[derive(Clone, Debug)]
pub struct UpdatePlan {
    /// Target table or key pattern.
    pub target: String,
    /// Column-expression assignment pairs.
    pub assignments: Vec<(String, PlanExpr)>,
    /// Optional WHERE filter expression.
    pub filter: Option<PlanExpr>,
}

/// Delete plan
#[derive(Clone, Debug)]
pub struct DeletePlan {
    /// Target table or key pattern.
    pub target: String,
    /// Optional WHERE filter expression.
    pub filter: Option<PlanExpr>,
}

/// Plan expression
#[derive(Clone, Debug)]
pub enum PlanExpr {
    /// A literal value.
    Literal(Value),
    /// A column reference.
    Column {
        /// Optional qualifying table name.
        table: Option<String>,
        /// Column name.
        name: String,
    },
    /// A binary operation.
    BinaryOp {
        /// Left-hand operand.
        left: Box<PlanExpr>,
        /// Binary operator.
        op: PlanBinaryOp,
        /// Right-hand operand.
        right: Box<PlanExpr>,
    },
    /// A unary operation.
    UnaryOp {
        /// Unary operator.
        op: PlanUnaryOp,
        /// Operand expression.
        expr: Box<PlanExpr>,
    },
    /// A function call.
    Function {
        /// Function name.
        name: String,
        /// Function arguments.
        args: Vec<PlanExpr>,
        /// Whether the function uses `DISTINCT`.
        distinct: bool,
    },
    /// A CASE expression.
    Case {
        /// Optional operand for simple CASE form.
        operand: Option<Box<PlanExpr>>,
        /// WHEN condition / THEN result pairs.
        when_clauses: Vec<(PlanExpr, PlanExpr)>,
        /// Optional ELSE result.
        else_clause: Option<Box<PlanExpr>>,
    },
    /// IN expression with a list of values
    In {
        /// Expression to test.
        expr: Box<PlanExpr>,
        /// List of values to test against.
        list: Vec<PlanExpr>,
        /// Whether the IN is negated (`NOT IN`).
        negated: bool,
    },
    /// IN expression with a subquery.
    InSubquery {
        /// Expression to test.
        expr: Box<PlanExpr>,
        /// Subquery plan that produces the value list.
        subquery: Box<QueryPlan>,
        /// Whether the IN is negated (`NOT IN`).
        negated: bool,
    },
    /// EXISTS subquery expression.
    Exists {
        /// Subquery plan to test for non-empty results.
        subquery: Box<QueryPlan>,
        /// Whether the EXISTS is negated.
        negated: bool,
    },
    /// A BETWEEN range expression.
    Between {
        /// Expression to test.
        expr: Box<PlanExpr>,
        /// Lower bound of the range.
        low: Box<PlanExpr>,
        /// Upper bound of the range.
        high: Box<PlanExpr>,
        /// Whether the BETWEEN is negated (`NOT BETWEEN`).
        negated: bool,
    },
    /// A LIKE pattern match expression.
    Like {
        /// Expression to match.
        expr: Box<PlanExpr>,
        /// Pattern expression.
        pattern: Box<PlanExpr>,
        /// Whether the LIKE is negated (`NOT LIKE`).
        negated: bool,
    },
    /// An IS NULL test.
    IsNull {
        /// Expression to test for null.
        expr: Box<PlanExpr>,
        /// Whether the test is negated (`IS NOT NULL`).
        negated: bool,
    },
    /// A type cast expression.
    Cast {
        /// Expression to cast.
        expr: Box<PlanExpr>,
        /// Target data type name.
        data_type: String,
    },
    /// A positional parameter placeholder.
    Parameter(usize),
}

/// Binary operator in plan
#[derive(Clone, Debug)]
pub enum PlanBinaryOp {
    /// Addition (`+`).
    Add,
    /// Subtraction (`-`).
    Subtract,
    /// Multiplication (`*`).
    Multiply,
    /// Division (`/`).
    Divide,
    /// Modulo (`%`).
    Modulo,
    /// Equality (`=`).
    Equal,
    /// Inequality (`!=` / `<>`).
    NotEqual,
    /// Less than (`<`).
    LessThan,
    /// Less than or equal (`<=`).
    LessThanOrEqual,
    /// Greater than (`>`).
    GreaterThan,
    /// Greater than or equal (`>=`).
    GreaterThanOrEqual,
    /// Logical AND.
    And,
    /// Logical OR.
    Or,
    /// String concatenation (`||`).
    Concat,
}

/// Unary operator in plan
#[derive(Clone, Debug)]
pub enum PlanUnaryOp {
    /// Logical NOT.
    Not,
    /// Numeric negation (`-`).
    Minus,
    /// Numeric identity (`+`).
    Plus,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::parser::QueryParser;

    #[test]
    fn test_plan_simple_select() {
        let parser = QueryParser::new();
        let stmt = parser.parse("SELECT * FROM users").unwrap();

        let planner = QueryPlanner::new(QueryConfig::default());
        let plan = planner.plan(&stmt).unwrap();

        let explain = planner.explain(&plan);
        assert!(explain.contains("Scan"));
        assert!(explain.contains("Project"));
    }

    #[test]
    fn test_plan_with_filter() {
        let parser = QueryParser::new();
        let stmt = parser.parse("SELECT * FROM users WHERE age > 21").unwrap();

        let planner = QueryPlanner::new(QueryConfig::default());
        let plan = planner.plan(&stmt).unwrap();

        let explain = planner.explain(&plan);
        assert!(explain.contains("Filter"));
    }

    #[test]
    fn test_plan_with_join() {
        let parser = QueryParser::new();
        let stmt = parser
            .parse("SELECT * FROM users JOIN orders ON users.id = orders.user_id")
            .unwrap();

        let planner = QueryPlanner::new(QueryConfig::default());
        let plan = planner.plan(&stmt).unwrap();

        let explain = planner.explain(&plan);
        assert!(explain.contains("Join"));
    }

    #[test]
    fn test_plan_with_aggregate() {
        let parser = QueryParser::new();
        let stmt = parser
            .parse("SELECT category, COUNT(*) FROM products GROUP BY category")
            .unwrap();

        let planner = QueryPlanner::new(QueryConfig::default());
        let plan = planner.plan(&stmt).unwrap();

        let explain = planner.explain(&plan);
        assert!(explain.contains("Aggregate"));
    }

    // ───── Enhanced EXPLAIN Tests ─────

    #[test]
    fn test_explain_includes_data_model() {
        let parser = QueryParser::new();
        let stmt = parser.parse("SELECT * FROM users:*").unwrap();

        let planner = QueryPlanner::new(QueryConfig::default());
        let plan = planner.plan(&stmt).unwrap();
        let explain = planner.explain(&plan);

        assert!(
            explain.contains("model: kv"),
            "Should detect kv model: {}",
            explain
        );
    }

    #[test]
    fn test_explain_timeseries_model() {
        let parser = QueryParser::new();
        let stmt = parser.parse("SELECT * FROM ts:metrics:*").unwrap();

        let planner = QueryPlanner::new(QueryConfig::default());
        let plan = planner.plan(&stmt).unwrap();
        let explain = planner.explain(&plan);

        assert!(
            explain.contains("model: timeseries"),
            "Should detect timeseries model: {}",
            explain
        );
    }

    #[test]
    fn test_explain_includes_cost_estimate() {
        let parser = QueryParser::new();
        let stmt = parser.parse("SELECT * FROM users:*").unwrap();

        let planner = QueryPlanner::new(QueryConfig::default());
        let plan = planner.plan(&stmt).unwrap();
        let explain = planner.explain(&plan);

        assert!(
            explain.contains("cost:"),
            "Should include cost estimate: {}",
            explain
        );
        assert!(
            explain.contains("est."),
            "Should include key estimate: {}",
            explain
        );
    }

    #[test]
    fn test_explain_filter_shows_index_info() {
        let parser = QueryParser::new();
        let stmt = parser
            .parse("SELECT * FROM users WHERE age > 21")
            .unwrap();

        let planner = QueryPlanner::new(QueryConfig::default());
        let plan = planner.plan(&stmt).unwrap();
        let explain = planner.explain(&plan);

        assert!(
            explain.contains("index:"),
            "Should include index info: {}",
            explain
        );
    }

    #[test]
    fn test_explain_join_shows_type() {
        let parser = QueryParser::new();
        let stmt = parser
            .parse("SELECT * FROM users:* AS u LEFT JOIN orders:* AS o ON o.user_id = u.id")
            .unwrap();

        let planner = QueryPlanner::new(QueryConfig::default());
        let plan = planner.plan(&stmt).unwrap();
        let explain = planner.explain(&plan);

        assert!(
            explain.contains("Left"),
            "Should show join type: {}",
            explain
        );
    }

    #[test]
    fn test_explain_sort_shows_keys() {
        let parser = QueryParser::new();
        let stmt = parser
            .parse("SELECT * FROM users ORDER BY name ASC, age DESC")
            .unwrap();

        let planner = QueryPlanner::new(QueryConfig::default());
        let plan = planner.plan(&stmt).unwrap();
        let explain = planner.explain(&plan);

        assert!(
            explain.contains("ASC"),
            "Should show sort direction: {}",
            explain
        );
        assert!(
            explain.contains("DESC"),
            "Should show DESC direction: {}",
            explain
        );
    }

    #[test]
    fn test_explain_aggregate_shows_functions() {
        let parser = QueryParser::new();
        let stmt = parser
            .parse("SELECT category, COUNT(*), SUM(price) FROM products GROUP BY category")
            .unwrap();

        let planner = QueryPlanner::new(QueryConfig::default());
        let plan = planner.plan(&stmt).unwrap();
        let explain = planner.explain(&plan);

        assert!(
            explain.contains("COUNT"),
            "Should show COUNT: {}",
            explain
        );
        assert!(explain.contains("SUM"), "Should show SUM: {}", explain);
    }

    #[test]
    fn test_explain_distinct_shows_dedup() {
        let parser = QueryParser::new();
        let stmt = parser
            .parse("SELECT DISTINCT status FROM users:*")
            .unwrap();

        let planner = QueryPlanner::new(QueryConfig::default());
        let plan = planner.plan(&stmt).unwrap();
        let explain = planner.explain(&plan);

        assert!(
            explain.contains("Distinct"),
            "Should show Distinct: {}",
            explain
        );
    }

    // ───── Subquery Planner Tests ─────

    #[test]
    fn test_plan_in_subquery() {
        let parser = QueryParser::new();
        let stmt = parser
            .parse("SELECT * FROM users WHERE id IN (SELECT user_id FROM active_users)")
            .unwrap();

        let planner = QueryPlanner::new(QueryConfig::default());
        let plan = planner.plan(&stmt).unwrap();
        let explain = planner.explain(&plan);

        assert!(
            explain.contains("Filter"),
            "Should have filter: {}",
            explain
        );
    }

    #[test]
    fn test_plan_exists_subquery() {
        let parser = QueryParser::new();
        let stmt = parser
            .parse("SELECT * FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)")
            .unwrap();

        let planner = QueryPlanner::new(QueryConfig::default());
        let plan = planner.plan(&stmt).unwrap();
        let explain = planner.explain(&plan);

        assert!(
            explain.contains("Filter"),
            "Should have filter: {}",
            explain
        );
    }
}
