//! FerriteQL Query Optimizer
//!
//! Optimizes query plans for better performance.

use crate::query::planner::*;
use crate::query::Value;

/// Query optimizer
pub struct QueryOptimizer {
    /// Enable predicate pushdown
    pub predicate_pushdown: bool,
    /// Enable projection pushdown
    pub projection_pushdown: bool,
    /// Enable join reordering
    pub join_reorder: bool,
    /// Enable constant folding
    pub constant_folding: bool,
}

impl QueryOptimizer {
    /// Create a new optimizer with all optimizations enabled
    pub fn new() -> Self {
        Self {
            predicate_pushdown: true,
            projection_pushdown: true,
            join_reorder: true,
            constant_folding: true,
        }
    }

    /// Optimize a query plan
    pub fn optimize(&self, plan: QueryPlan) -> QueryPlan {
        let mut optimized = plan;

        // Apply optimization passes
        if self.constant_folding {
            optimized = self.fold_constants(optimized);
        }

        if self.predicate_pushdown {
            optimized = self.push_predicates(optimized);
        }

        if self.projection_pushdown {
            optimized = self.push_projections(optimized);
        }

        if self.join_reorder {
            optimized = self.reorder_joins(optimized);
        }

        optimized
    }

    /// Fold constant expressions
    fn fold_constants(&self, plan: QueryPlan) -> QueryPlan {
        match plan {
            QueryPlan::Filter(mut filter) => {
                filter.predicate = self.fold_expr(filter.predicate);
                filter.input = Box::new(self.fold_constants(*filter.input));

                // If predicate is always true, eliminate the filter
                if let PlanExpr::Literal(Value::Bool(true)) = filter.predicate {
                    return *filter.input;
                }

                // If predicate is always false, return empty
                if let PlanExpr::Literal(Value::Bool(false)) = filter.predicate {
                    return QueryPlan::EmptyRelation;
                }

                QueryPlan::Filter(filter)
            }
            QueryPlan::Project(mut proj) => {
                proj.input = Box::new(self.fold_constants(*proj.input));
                proj.items = proj
                    .items
                    .into_iter()
                    .map(|item| match item {
                        ProjectionItem::Expr { expr, alias } => ProjectionItem::Expr {
                            expr: self.fold_expr(expr),
                            alias,
                        },
                        other => other,
                    })
                    .collect();
                QueryPlan::Project(proj)
            }
            QueryPlan::Join(mut join) => {
                join.left = Box::new(self.fold_constants(*join.left));
                join.right = Box::new(self.fold_constants(*join.right));
                if let Some(cond) = join.condition {
                    join.condition = Some(self.fold_expr(cond));
                }
                QueryPlan::Join(join)
            }
            QueryPlan::Aggregate(mut agg) => {
                agg.input = Box::new(self.fold_constants(*agg.input));
                agg.group_by = agg
                    .group_by
                    .into_iter()
                    .map(|e| self.fold_expr(e))
                    .collect();
                QueryPlan::Aggregate(agg)
            }
            QueryPlan::Sort(mut sort) => {
                sort.input = Box::new(self.fold_constants(*sort.input));
                QueryPlan::Sort(sort)
            }
            QueryPlan::Limit(mut limit) => {
                limit.input = Box::new(self.fold_constants(*limit.input));
                QueryPlan::Limit(limit)
            }
            QueryPlan::Distinct(input) => {
                QueryPlan::Distinct(Box::new(self.fold_constants(*input)))
            }
            other => other,
        }
    }

    /// Fold an expression
    fn fold_expr(&self, expr: PlanExpr) -> PlanExpr {
        match expr {
            PlanExpr::BinaryOp { left, op, right } => {
                let left = self.fold_expr(*left);
                let right = self.fold_expr(*right);

                // Try to fold constant operations
                if let (PlanExpr::Literal(lv), PlanExpr::Literal(rv)) = (&left, &right) {
                    if let Some(result) = self.fold_binary_op(lv, &op, rv) {
                        return PlanExpr::Literal(result);
                    }
                }

                // Boolean simplifications
                match (&op, &left, &right) {
                    // x AND true => x
                    (PlanBinaryOp::And, _, PlanExpr::Literal(Value::Bool(true))) => left,
                    (PlanBinaryOp::And, PlanExpr::Literal(Value::Bool(true)), _) => right,
                    // x AND false => false
                    (PlanBinaryOp::And, _, PlanExpr::Literal(Value::Bool(false)))
                    | (PlanBinaryOp::And, PlanExpr::Literal(Value::Bool(false)), _) => {
                        PlanExpr::Literal(Value::Bool(false))
                    }
                    // x OR true => true
                    (PlanBinaryOp::Or, _, PlanExpr::Literal(Value::Bool(true)))
                    | (PlanBinaryOp::Or, PlanExpr::Literal(Value::Bool(true)), _) => {
                        PlanExpr::Literal(Value::Bool(true))
                    }
                    // x OR false => x
                    (PlanBinaryOp::Or, _, PlanExpr::Literal(Value::Bool(false))) => left,
                    (PlanBinaryOp::Or, PlanExpr::Literal(Value::Bool(false)), _) => right,
                    _ => PlanExpr::BinaryOp {
                        left: Box::new(left),
                        op,
                        right: Box::new(right),
                    },
                }
            }
            PlanExpr::UnaryOp { op, expr } => {
                let expr = self.fold_expr(*expr);

                if let PlanExpr::Literal(v) = &expr {
                    if let Some(result) = self.fold_unary_op(&op, v) {
                        return PlanExpr::Literal(result);
                    }
                }

                // NOT NOT x => x
                if let (
                    PlanUnaryOp::Not,
                    PlanExpr::UnaryOp {
                        op: PlanUnaryOp::Not,
                        expr: inner,
                    },
                ) = (&op, &expr)
                {
                    return *inner.clone();
                }

                PlanExpr::UnaryOp {
                    op,
                    expr: Box::new(expr),
                }
            }
            PlanExpr::Case {
                operand,
                when_clauses,
                else_clause,
            } => {
                let operand = operand.map(|e| Box::new(self.fold_expr(*e)));
                let when_clauses: Vec<_> = when_clauses
                    .into_iter()
                    .map(|(w, t)| (self.fold_expr(w), self.fold_expr(t)))
                    .collect();
                let else_clause = else_clause.map(|e| Box::new(self.fold_expr(*e)));

                // If operand is constant, we might be able to simplify
                if operand.is_none() {
                    for (when, then) in &when_clauses {
                        if let PlanExpr::Literal(Value::Bool(true)) = when {
                            return then.clone();
                        }
                    }
                }

                PlanExpr::Case {
                    operand,
                    when_clauses,
                    else_clause,
                }
            }
            other => other,
        }
    }

    fn fold_binary_op(&self, left: &Value, op: &PlanBinaryOp, right: &Value) -> Option<Value> {
        match op {
            PlanBinaryOp::Add => match (left, right) {
                (Value::Int(a), Value::Int(b)) => Some(Value::Int(a + b)),
                (Value::Float(a), Value::Float(b)) => Some(Value::Float(a + b)),
                _ => None,
            },
            PlanBinaryOp::Subtract => match (left, right) {
                (Value::Int(a), Value::Int(b)) => Some(Value::Int(a - b)),
                (Value::Float(a), Value::Float(b)) => Some(Value::Float(a - b)),
                _ => None,
            },
            PlanBinaryOp::Multiply => match (left, right) {
                (Value::Int(a), Value::Int(b)) => Some(Value::Int(a * b)),
                (Value::Float(a), Value::Float(b)) => Some(Value::Float(a * b)),
                _ => None,
            },
            PlanBinaryOp::Divide => match (left, right) {
                (Value::Int(a), Value::Int(b)) if *b != 0 => Some(Value::Int(a / b)),
                (Value::Float(a), Value::Float(b)) if *b != 0.0 => Some(Value::Float(a / b)),
                _ => None,
            },
            PlanBinaryOp::Equal => Some(Value::Bool(left == right)),
            PlanBinaryOp::NotEqual => Some(Value::Bool(left != right)),
            PlanBinaryOp::LessThan => Some(Value::Bool(left < right)),
            PlanBinaryOp::LessThanOrEqual => Some(Value::Bool(left <= right)),
            PlanBinaryOp::GreaterThan => Some(Value::Bool(left > right)),
            PlanBinaryOp::GreaterThanOrEqual => Some(Value::Bool(left >= right)),
            PlanBinaryOp::And => match (left, right) {
                (Value::Bool(a), Value::Bool(b)) => Some(Value::Bool(*a && *b)),
                _ => None,
            },
            PlanBinaryOp::Or => match (left, right) {
                (Value::Bool(a), Value::Bool(b)) => Some(Value::Bool(*a || *b)),
                _ => None,
            },
            PlanBinaryOp::Concat => match (left, right) {
                (Value::String(a), Value::String(b)) => Some(Value::String(format!("{}{}", a, b))),
                _ => None,
            },
            _ => None,
        }
    }

    fn fold_unary_op(&self, op: &PlanUnaryOp, val: &Value) -> Option<Value> {
        match op {
            PlanUnaryOp::Not => match val {
                Value::Bool(b) => Some(Value::Bool(!b)),
                _ => None,
            },
            PlanUnaryOp::Minus => match val {
                Value::Int(n) => Some(Value::Int(-n)),
                Value::Float(f) => Some(Value::Float(-f)),
                _ => None,
            },
            PlanUnaryOp::Plus => Some(val.clone()),
        }
    }

    /// Push predicates down toward data sources
    fn push_predicates(&self, plan: QueryPlan) -> QueryPlan {
        match plan {
            QueryPlan::Filter(filter) => {
                let input = self.push_predicates(*filter.input);

                // Try to push the filter into the input
                match input {
                    QueryPlan::Scan(scan) => {
                        // Can't push further, keep filter on top of scan
                        QueryPlan::Filter(FilterPlan {
                            input: Box::new(QueryPlan::Scan(scan)),
                            predicate: filter.predicate,
                        })
                    }
                    QueryPlan::Join(join) => {
                        // Try to push predicates into join sides
                        let (left_preds, right_preds, join_preds, remaining) =
                            self.split_predicates(&filter.predicate, &join);

                        let mut left: QueryPlan = *join.left;
                        let mut right: QueryPlan = *join.right;

                        // Push left predicates
                        for pred in left_preds {
                            left = QueryPlan::Filter(FilterPlan {
                                input: Box::new(left),
                                predicate: pred,
                            });
                        }

                        // Push right predicates
                        for pred in right_preds {
                            right = QueryPlan::Filter(FilterPlan {
                                input: Box::new(right),
                                predicate: pred,
                            });
                        }

                        // Combine join predicates with existing condition
                        let join_condition = if join_preds.is_empty() {
                            join.condition
                        } else {
                            let mut cond = join.condition;
                            for pred in join_preds {
                                cond = Some(if let Some(existing) = cond {
                                    PlanExpr::BinaryOp {
                                        left: Box::new(existing),
                                        op: PlanBinaryOp::And,
                                        right: Box::new(pred),
                                    }
                                } else {
                                    pred
                                });
                            }
                            cond
                        };

                        let new_join = QueryPlan::Join(JoinPlan {
                            left: Box::new(left),
                            right: Box::new(right),
                            join_type: join.join_type,
                            condition: join_condition,
                        });

                        // Apply remaining predicates on top
                        if remaining.is_empty() {
                            new_join
                        } else {
                            let combined =
                                remaining.into_iter().reduce(|a, b| PlanExpr::BinaryOp {
                                    left: Box::new(a),
                                    op: PlanBinaryOp::And,
                                    right: Box::new(b),
                                });

                            if let Some(pred) = combined {
                                QueryPlan::Filter(FilterPlan {
                                    input: Box::new(new_join),
                                    predicate: pred,
                                })
                            } else {
                                new_join
                            }
                        }
                    }
                    QueryPlan::Filter(inner_filter) => {
                        // Combine consecutive filters
                        QueryPlan::Filter(FilterPlan {
                            input: inner_filter.input,
                            predicate: PlanExpr::BinaryOp {
                                left: Box::new(inner_filter.predicate),
                                op: PlanBinaryOp::And,
                                right: Box::new(filter.predicate),
                            },
                        })
                    }
                    other => QueryPlan::Filter(FilterPlan {
                        input: Box::new(other),
                        predicate: filter.predicate,
                    }),
                }
            }
            QueryPlan::Project(mut proj) => {
                proj.input = Box::new(self.push_predicates(*proj.input));
                QueryPlan::Project(proj)
            }
            QueryPlan::Join(mut join) => {
                join.left = Box::new(self.push_predicates(*join.left));
                join.right = Box::new(self.push_predicates(*join.right));
                QueryPlan::Join(join)
            }
            QueryPlan::Aggregate(mut agg) => {
                agg.input = Box::new(self.push_predicates(*agg.input));
                QueryPlan::Aggregate(agg)
            }
            QueryPlan::Sort(mut sort) => {
                sort.input = Box::new(self.push_predicates(*sort.input));
                QueryPlan::Sort(sort)
            }
            QueryPlan::Limit(mut limit) => {
                limit.input = Box::new(self.push_predicates(*limit.input));
                QueryPlan::Limit(limit)
            }
            QueryPlan::Distinct(input) => {
                QueryPlan::Distinct(Box::new(self.push_predicates(*input)))
            }
            other => other,
        }
    }

    /// Split predicates based on which side of a join they reference
    fn split_predicates(
        &self,
        predicate: &PlanExpr,
        _join: &JoinPlan,
    ) -> (Vec<PlanExpr>, Vec<PlanExpr>, Vec<PlanExpr>, Vec<PlanExpr>) {
        // Simplified: for now, keep all predicates as "remaining"
        // A full implementation would analyze column references
        let conjuncts = self.extract_conjuncts(predicate);
        (vec![], vec![], vec![], conjuncts)
    }

    /// Extract conjuncts from an AND expression
    #[allow(clippy::only_used_in_recursion)]
    fn extract_conjuncts(&self, expr: &PlanExpr) -> Vec<PlanExpr> {
        match expr {
            PlanExpr::BinaryOp {
                left,
                op: PlanBinaryOp::And,
                right,
            } => {
                let mut conjuncts = self.extract_conjuncts(left);
                conjuncts.extend(self.extract_conjuncts(right));
                conjuncts
            }
            _ => vec![expr.clone()],
        }
    }

    /// Push projections down to reduce data movement
    #[allow(clippy::only_used_in_recursion)]
    fn push_projections(&self, plan: QueryPlan) -> QueryPlan {
        // Simplified: just recursively process children
        match plan {
            QueryPlan::Project(mut proj) => {
                proj.input = Box::new(self.push_projections(*proj.input));
                QueryPlan::Project(proj)
            }
            QueryPlan::Filter(mut filter) => {
                filter.input = Box::new(self.push_projections(*filter.input));
                QueryPlan::Filter(filter)
            }
            QueryPlan::Join(mut join) => {
                join.left = Box::new(self.push_projections(*join.left));
                join.right = Box::new(self.push_projections(*join.right));
                QueryPlan::Join(join)
            }
            QueryPlan::Aggregate(mut agg) => {
                agg.input = Box::new(self.push_projections(*agg.input));
                QueryPlan::Aggregate(agg)
            }
            QueryPlan::Sort(mut sort) => {
                sort.input = Box::new(self.push_projections(*sort.input));
                QueryPlan::Sort(sort)
            }
            QueryPlan::Limit(mut limit) => {
                limit.input = Box::new(self.push_projections(*limit.input));
                QueryPlan::Limit(limit)
            }
            QueryPlan::Distinct(input) => {
                QueryPlan::Distinct(Box::new(self.push_projections(*input)))
            }
            other => other,
        }
    }

    /// Reorder joins for better performance
    #[allow(clippy::only_used_in_recursion)]
    fn reorder_joins(&self, plan: QueryPlan) -> QueryPlan {
        // Simplified: just recursively process children
        // A full implementation would use dynamic programming for join ordering
        match plan {
            QueryPlan::Project(mut proj) => {
                proj.input = Box::new(self.reorder_joins(*proj.input));
                QueryPlan::Project(proj)
            }
            QueryPlan::Filter(mut filter) => {
                filter.input = Box::new(self.reorder_joins(*filter.input));
                QueryPlan::Filter(filter)
            }
            QueryPlan::Join(mut join) => {
                join.left = Box::new(self.reorder_joins(*join.left));
                join.right = Box::new(self.reorder_joins(*join.right));
                QueryPlan::Join(join)
            }
            QueryPlan::Aggregate(mut agg) => {
                agg.input = Box::new(self.reorder_joins(*agg.input));
                QueryPlan::Aggregate(agg)
            }
            QueryPlan::Sort(mut sort) => {
                sort.input = Box::new(self.reorder_joins(*sort.input));
                QueryPlan::Sort(sort)
            }
            QueryPlan::Limit(mut limit) => {
                limit.input = Box::new(self.reorder_joins(*limit.input));
                QueryPlan::Limit(limit)
            }
            QueryPlan::Distinct(input) => QueryPlan::Distinct(Box::new(self.reorder_joins(*input))),
            other => other,
        }
    }
}

impl Default for QueryOptimizer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_constant_folding_arithmetic() {
        let optimizer = QueryOptimizer::new();

        let expr = PlanExpr::BinaryOp {
            left: Box::new(PlanExpr::Literal(Value::Int(5))),
            op: PlanBinaryOp::Add,
            right: Box::new(PlanExpr::Literal(Value::Int(3))),
        };

        let folded = optimizer.fold_expr(expr);
        assert!(matches!(folded, PlanExpr::Literal(Value::Int(8))));
    }

    #[test]
    fn test_constant_folding_boolean() {
        let optimizer = QueryOptimizer::new();

        // x AND true => x
        let col = PlanExpr::Column {
            table: None,
            name: "x".to_string(),
        };
        let expr = PlanExpr::BinaryOp {
            left: Box::new(col.clone()),
            op: PlanBinaryOp::And,
            right: Box::new(PlanExpr::Literal(Value::Bool(true))),
        };

        let folded = optimizer.fold_expr(expr);
        assert!(matches!(folded, PlanExpr::Column { .. }));
    }

    #[test]
    fn test_filter_elimination() {
        let optimizer = QueryOptimizer::new();

        let plan = QueryPlan::Filter(FilterPlan {
            input: Box::new(QueryPlan::Scan(ScanPlan {
                pattern: "test:*".to_string(),
                alias: None,
            })),
            predicate: PlanExpr::Literal(Value::Bool(true)),
        });

        let optimized = optimizer.optimize(plan);

        // Filter with true should be eliminated
        assert!(matches!(optimized, QueryPlan::Scan(_)));
    }

    #[test]
    fn test_false_filter_returns_empty() {
        let optimizer = QueryOptimizer::new();

        let plan = QueryPlan::Filter(FilterPlan {
            input: Box::new(QueryPlan::Scan(ScanPlan {
                pattern: "test:*".to_string(),
                alias: None,
            })),
            predicate: PlanExpr::Literal(Value::Bool(false)),
        });

        let optimized = optimizer.optimize(plan);

        // Filter with false should return empty relation
        assert!(matches!(optimized, QueryPlan::EmptyRelation));
    }
}
