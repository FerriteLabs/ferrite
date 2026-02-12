//! Cypher query planner
//!
//! Basic query planning and optimization for Cypher queries. Analyzes AST
//! nodes to choose efficient execution strategies.

use super::ast::*;

/// A query execution plan.
#[derive(Debug, Clone)]
pub struct QueryPlan {
    /// The original query (may be reordered for efficiency)
    pub query: CypherQuery,
    /// Estimated cost
    pub estimated_cost: f64,
    /// Plan steps for informational purposes
    pub steps: Vec<PlanStep>,
}

/// A step in the query plan.
#[derive(Debug, Clone)]
pub enum PlanStep {
    /// Full scan of all vertices
    VertexScan {
        /// Variable being bound
        variable: String,
        /// Optional label filter
        label: Option<String>,
        /// Estimated rows
        estimated_rows: usize,
    },
    /// Index lookup by label
    LabelIndexLookup {
        /// Variable being bound
        variable: String,
        /// Label to look up
        label: String,
    },
    /// Index lookup by property
    PropertyIndexLookup {
        /// Variable being bound
        variable: String,
        /// Label
        label: String,
        /// Property name
        property: String,
    },
    /// Expand along relationships
    Expand {
        /// Source variable
        from: String,
        /// Target variable
        to: String,
        /// Relationship type filter
        rel_type: Option<String>,
        /// Direction
        direction: RelDirection,
    },
    /// Variable-length path expansion
    VarLengthExpand {
        /// Source variable
        from: String,
        /// Target variable
        to: String,
        /// Relationship type filter
        rel_type: Option<String>,
        /// Min hops
        min_hops: usize,
        /// Max hops
        max_hops: usize,
    },
    /// Apply WHERE filter
    Filter {
        /// Description of filter
        description: String,
    },
    /// Apply aggregation
    Aggregate {
        /// Aggregate functions used
        functions: Vec<String>,
    },
    /// Sort results
    Sort {
        /// Keys being sorted on
        keys: Vec<String>,
    },
    /// Limit results
    Limit(usize),
    /// Skip results
    Skip(usize),
    /// Project (select columns)
    Project {
        /// Columns to project
        columns: Vec<String>,
    },
}

/// Plan a Cypher query for execution.
pub fn plan(query: &CypherQuery) -> QueryPlan {
    let mut steps = Vec::new();
    let mut cost = 0.0;

    // Plan MATCH clause
    for pattern_part in &query.match_clause.patterns {
        plan_pattern_part(pattern_part, &mut steps, &mut cost);
    }

    // Plan WHERE clause
    if let Some(ref where_expr) = query.where_clause {
        steps.push(PlanStep::Filter {
            description: format_where(where_expr),
        });
        cost += 1.0; // Filter is cheap
    }

    // Plan aggregation
    let has_agg = query
        .return_clause
        .items
        .iter()
        .any(|item| matches!(item.expr, Expr::Aggregate(_, _)));
    if has_agg {
        let funcs: Vec<String> = query
            .return_clause
            .items
            .iter()
            .filter_map(|item| match &item.expr {
                Expr::Aggregate(func, _) => Some(format!("{:?}", func)),
                _ => None,
            })
            .collect();
        steps.push(PlanStep::Aggregate { functions: funcs });
        cost += 5.0;
    }

    // Plan ORDER BY
    if let Some(ref order_items) = query.order_by {
        let keys: Vec<String> = order_items
            .iter()
            .map(|item| {
                let dir = if item.ascending { "ASC" } else { "DESC" };
                format!("{} {}", expr_name(&item.expr), dir)
            })
            .collect();
        steps.push(PlanStep::Sort { keys });
        cost += 10.0; // Sort is more expensive
    }

    // Plan SKIP
    if let Some(skip) = query.skip {
        steps.push(PlanStep::Skip(skip));
    }

    // Plan LIMIT
    if let Some(limit) = query.limit {
        steps.push(PlanStep::Limit(limit));
        cost *= 0.5; // Limit reduces work
    }

    // Plan projection
    let columns: Vec<String> = query
        .return_clause
        .items
        .iter()
        .map(|item| item.alias.clone().unwrap_or_else(|| expr_name(&item.expr)))
        .collect();
    steps.push(PlanStep::Project { columns });

    QueryPlan {
        query: query.clone(),
        estimated_cost: cost,
        steps,
    }
}

fn plan_pattern_part(part: &PatternPart, steps: &mut Vec<PlanStep>, cost: &mut f64) {
    // Start node
    let start_var = part
        .start
        .variable
        .clone()
        .unwrap_or_else(|| "_anon".to_string());

    if !part.start.labels.is_empty() {
        let label = part.start.labels[0].clone();
        if part.start.properties.is_some() {
            // If we have both label and property, could use composite index
            steps.push(PlanStep::LabelIndexLookup {
                variable: start_var.clone(),
                label: label.clone(),
            });
            *cost += 5.0;
        } else {
            steps.push(PlanStep::LabelIndexLookup {
                variable: start_var.clone(),
                label,
            });
            *cost += 10.0;
        }
    } else {
        steps.push(PlanStep::VertexScan {
            variable: start_var.clone(),
            label: None,
            estimated_rows: 1000,
        });
        *cost += 100.0;
    }

    // Chain of relationships
    for (rel, node) in &part.chain {
        let to_var = node.variable.clone().unwrap_or_else(|| "_anon".to_string());

        if let Some((min, max)) = rel.var_length {
            steps.push(PlanStep::VarLengthExpand {
                from: start_var.clone(),
                to: to_var,
                rel_type: rel.rel_type.clone(),
                min_hops: min,
                max_hops: max,
            });
            *cost += 50.0 * max as f64;
        } else {
            steps.push(PlanStep::Expand {
                from: start_var.clone(),
                to: to_var,
                rel_type: rel.rel_type.clone(),
                direction: rel.direction,
            });
            *cost += 20.0;
        }
    }
}

fn format_where(expr: &WhereExpr) -> String {
    match expr {
        WhereExpr::Comparison { left, op, right } => {
            let op_str = match op {
                CypherOp::Eq => "=",
                CypherOp::Ne => "<>",
                CypherOp::Lt => "<",
                CypherOp::Le => "<=",
                CypherOp::Gt => ">",
                CypherOp::Ge => ">=",
                CypherOp::Contains => "CONTAINS",
                CypherOp::StartsWith => "STARTS WITH",
                CypherOp::EndsWith => "ENDS WITH",
            };
            format!(
                "{}.{} {} {}",
                left.variable,
                left.property,
                op_str,
                expr_name(right)
            )
        }
        WhereExpr::And(a, b) => format!("({} AND {})", format_where(a), format_where(b)),
        WhereExpr::Or(a, b) => format!("({} OR {})", format_where(a), format_where(b)),
        WhereExpr::Not(inner) => format!("NOT ({})", format_where(inner)),
    }
}

fn expr_name(expr: &Expr) -> String {
    match expr {
        Expr::Variable(v) => v.clone(),
        Expr::Property(pa) => format!("{}.{}", pa.variable, pa.property),
        Expr::Literal(v) => format!("{}", v),
        Expr::Aggregate(func, inner) => {
            let func_name = match func {
                AggregateFunc::Count => "count",
                AggregateFunc::Sum => "sum",
                AggregateFunc::Avg => "avg",
                AggregateFunc::Min => "min",
                AggregateFunc::Max => "max",
            };
            format!("{}({})", func_name, expr_name(inner))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cypher::parser::CypherParser;

    fn plan_cypher(input: &str) -> QueryPlan {
        let stmt = CypherParser::parse(input).unwrap();
        match stmt {
            CypherStatement::Query(q) => plan(&q),
            _ => panic!("expected query"),
        }
    }

    #[test]
    fn test_plan_simple_match() {
        let plan = plan_cypher("MATCH (n:Person) RETURN n");
        assert!(!plan.steps.is_empty());
        assert!(plan.estimated_cost > 0.0);

        // Should have a label index lookup
        assert!(plan
            .steps
            .iter()
            .any(|s| matches!(s, PlanStep::LabelIndexLookup { .. })));
    }

    #[test]
    fn test_plan_with_relationship() {
        let plan = plan_cypher("MATCH (a:Person)-[:KNOWS]->(b) RETURN a, b");
        assert!(plan
            .steps
            .iter()
            .any(|s| matches!(s, PlanStep::Expand { .. })));
    }

    #[test]
    fn test_plan_with_where() {
        let plan = plan_cypher("MATCH (n:Person) WHERE n.age > 25 RETURN n");
        assert!(plan
            .steps
            .iter()
            .any(|s| matches!(s, PlanStep::Filter { .. })));
    }

    #[test]
    fn test_plan_with_order_limit() {
        let plan = plan_cypher("MATCH (n) RETURN n ORDER BY n.name LIMIT 10");
        assert!(plan
            .steps
            .iter()
            .any(|s| matches!(s, PlanStep::Sort { .. })));
        assert!(plan.steps.iter().any(|s| matches!(s, PlanStep::Limit(10))));
    }

    #[test]
    fn test_plan_var_length_path() {
        let plan = plan_cypher("MATCH (a)-[:KNOWS*1..3]->(b) RETURN b");
        assert!(plan
            .steps
            .iter()
            .any(|s| matches!(s, PlanStep::VarLengthExpand { .. })));
    }

    #[test]
    fn test_plan_aggregation() {
        let plan = plan_cypher("MATCH (n:Person) RETURN count(n)");
        assert!(plan
            .steps
            .iter()
            .any(|s| matches!(s, PlanStep::Aggregate { .. })));
    }
}
