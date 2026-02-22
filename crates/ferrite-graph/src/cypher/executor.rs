//! Cypher query executor
//!
//! Executes a Cypher AST against a [`GraphStorage`] instance.

use std::collections::HashMap;

use crate::graph::edge::next_edge_id;
use crate::graph::vertex::next_vertex_id;
use crate::graph::{
    Edge, GraphStorage, PropertyValue, QueryResult, QueryRow, QueryValue, Vertex, VertexId,
};

use super::ast::*;
use super::planner::QueryPlan;

/// Execute a Cypher statement against a graph storage.
pub fn execute(stmt: &CypherStatement, storage: &mut GraphStorage) -> Result<QueryResult, String> {
    match stmt {
        CypherStatement::Query(query) => execute_query(query, storage),
        CypherStatement::Create(create) => execute_create(create, storage, &HashMap::new()),
        CypherStatement::MatchCreate {
            match_clause,
            create_clause,
        } => execute_match_create(match_clause, create_clause, storage),
    }
}

/// Execute a read-only query (does not mutate storage).
pub fn execute_read_only(
    query: &CypherQuery,
    storage: &GraphStorage,
) -> Result<QueryResult, String> {
    execute_query(query, storage)
}

/// Execute a query plan.
pub fn execute_plan(plan: &QueryPlan, storage: &GraphStorage) -> Result<QueryResult, String> {
    execute_query(&plan.query, storage)
}

/// A single binding row: variable name -> bound value.
type Bindings = HashMap<String, BoundValue>;

#[derive(Debug, Clone)]
enum BoundValue {
    Vertex(Vertex),
    Edge(Edge),
}

fn execute_query(query: &CypherQuery, storage: &GraphStorage) -> Result<QueryResult, String> {
    let start = std::time::Instant::now();

    // 1. Expand MATCH patterns into bindings
    let mut bindings = expand_match(&query.match_clause, storage)?;

    // 2. Apply WHERE filter
    if let Some(ref where_expr) = query.where_clause {
        bindings.retain(|b| evaluate_where(where_expr, b, storage));
    }

    // 3. Check if any return item uses aggregation
    let has_aggregation = query
        .return_clause
        .items
        .iter()
        .any(|item| matches!(item.expr, Expr::Aggregate(_, _)));

    if has_aggregation {
        return execute_aggregation(query, &bindings, start);
    }

    // 4. Apply ORDER BY
    if let Some(ref order_items) = query.order_by {
        bindings.sort_by(|a, b| {
            for item in order_items {
                let va = eval_expr(&item.expr, a);
                let vb = eval_expr(&item.expr, b);
                let cmp = cmp_property_values(&va, &vb);
                if cmp != std::cmp::Ordering::Equal {
                    return if item.ascending { cmp } else { cmp.reverse() };
                }
            }
            std::cmp::Ordering::Equal
        });
    }

    // 5. Apply SKIP
    if let Some(skip) = query.skip {
        if skip < bindings.len() {
            bindings = bindings[skip..].to_vec();
        } else {
            bindings.clear();
        }
    }

    // 6. Apply LIMIT
    if let Some(limit) = query.limit {
        bindings.truncate(limit);
    }

    // 7. Build result
    let columns: Vec<String> = query
        .return_clause
        .items
        .iter()
        .map(|item| {
            item.alias
                .clone()
                .unwrap_or_else(|| expr_display_name(&item.expr))
        })
        .collect();

    let rows: Vec<QueryRow> = bindings
        .iter()
        .map(|binding| {
            let mut values = HashMap::new();
            for (idx, item) in query.return_clause.items.iter().enumerate() {
                let col = &columns[idx];
                let val = binding_to_query_value(&item.expr, binding);
                values.insert(col.clone(), val);
            }
            QueryRow { values }
        })
        .collect();

    Ok(QueryResult {
        rows,
        columns,
        took_ms: start.elapsed().as_millis() as u64,
    })
}

fn execute_aggregation(
    query: &CypherQuery,
    bindings: &[Bindings],
    start: std::time::Instant,
) -> Result<QueryResult, String> {
    let columns: Vec<String> = query
        .return_clause
        .items
        .iter()
        .map(|item| {
            item.alias
                .clone()
                .unwrap_or_else(|| expr_display_name(&item.expr))
        })
        .collect();

    // Determine grouping keys (non-aggregate items)
    let group_keys: Vec<usize> = query
        .return_clause
        .items
        .iter()
        .enumerate()
        .filter(|(_, item)| !matches!(item.expr, Expr::Aggregate(_, _)))
        .map(|(i, _)| i)
        .collect();

    if group_keys.is_empty() {
        // No grouping — single aggregate row
        let mut values = HashMap::new();
        let refs: Vec<&Bindings> = bindings.iter().collect();
        for (idx, item) in query.return_clause.items.iter().enumerate() {
            let val = compute_aggregate(&item.expr, &refs);
            values.insert(columns[idx].clone(), val);
        }
        return Ok(QueryResult {
            rows: vec![QueryRow { values }],
            columns,
            took_ms: start.elapsed().as_millis() as u64,
        });
    }

    // Group by non-aggregate keys
    let mut groups: Vec<(Vec<Option<PropertyValue>>, Vec<&Bindings>)> = Vec::new();
    for binding in bindings {
        let key: Vec<Option<PropertyValue>> = group_keys
            .iter()
            .map(|&i| eval_expr(&query.return_clause.items[i].expr, binding))
            .collect();

        if let Some(group) = groups.iter_mut().find(|(k, _)| *k == key) {
            group.1.push(binding);
        } else {
            groups.push((key, vec![binding]));
        }
    }

    let mut rows = Vec::new();
    for (_, group_bindings) in &groups {
        let mut values = HashMap::new();
        for (idx, item) in query.return_clause.items.iter().enumerate() {
            let val = if matches!(item.expr, Expr::Aggregate(_, _)) {
                compute_aggregate(&item.expr, group_bindings)
            } else {
                binding_to_query_value(&item.expr, group_bindings[0])
            };
            values.insert(columns[idx].clone(), val);
        }
        rows.push(QueryRow { values });
    }

    Ok(QueryResult {
        rows,
        columns,
        took_ms: start.elapsed().as_millis() as u64,
    })
}

fn compute_aggregate(expr: &Expr, bindings: &[&Bindings]) -> QueryValue {
    match expr {
        Expr::Aggregate(func, inner) => {
            let values: Vec<Option<PropertyValue>> =
                bindings.iter().map(|b| eval_expr(inner, b)).collect();

            match func {
                AggregateFunc::Count => {
                    QueryValue::Property(PropertyValue::Integer(values.len() as i64))
                }
                AggregateFunc::Sum => {
                    let sum: f64 = values
                        .iter()
                        .filter_map(|v| v.as_ref().and_then(|p| p.as_float()))
                        .sum();
                    QueryValue::Property(PropertyValue::Float(sum))
                }
                AggregateFunc::Avg => {
                    let nums: Vec<f64> = values
                        .iter()
                        .filter_map(|v| v.as_ref().and_then(|p| p.as_float()))
                        .collect();
                    if nums.is_empty() {
                        QueryValue::Null
                    } else {
                        let avg = nums.iter().sum::<f64>() / nums.len() as f64;
                        QueryValue::Property(PropertyValue::Float(avg))
                    }
                }
                AggregateFunc::Min => {
                    let min = values
                        .iter()
                        .filter_map(|v| v.as_ref().and_then(|p| p.as_float()))
                        .fold(f64::INFINITY, f64::min);
                    if min == f64::INFINITY {
                        QueryValue::Null
                    } else {
                        QueryValue::Property(PropertyValue::Float(min))
                    }
                }
                AggregateFunc::Max => {
                    let max = values
                        .iter()
                        .filter_map(|v| v.as_ref().and_then(|p| p.as_float()))
                        .fold(f64::NEG_INFINITY, f64::max);
                    if max == f64::NEG_INFINITY {
                        QueryValue::Null
                    } else {
                        QueryValue::Property(PropertyValue::Float(max))
                    }
                }
            }
        }
        _ => QueryValue::Null,
    }
}

fn expand_match(
    match_pattern: &MatchPattern,
    storage: &GraphStorage,
) -> Result<Vec<Bindings>, String> {
    let mut bindings: Vec<Bindings> = vec![HashMap::new()];

    for pattern_part in &match_pattern.patterns {
        bindings = expand_pattern_part(pattern_part, &bindings, storage)?;
    }

    Ok(bindings)
}

fn expand_pattern_part(
    part: &PatternPart,
    bindings: &[Bindings],
    storage: &GraphStorage,
) -> Result<Vec<Bindings>, String> {
    // First expand the start node
    let mut current = expand_node(&part.start, bindings, storage)?;

    // Then expand each (rel, node) pair in the chain
    for (rel_pattern, node_pattern) in &part.chain {
        current = expand_relationship(rel_pattern, node_pattern, &current, storage)?;
    }

    Ok(current)
}

fn expand_node(
    node: &NodePatternAst,
    bindings: &[Bindings],
    storage: &GraphStorage,
) -> Result<Vec<Bindings>, String> {
    let mut results = Vec::new();

    for binding in bindings {
        // If variable already bound, just check it matches
        if let Some(ref var) = node.variable {
            if let Some(BoundValue::Vertex(existing)) = binding.get(var) {
                if node_matches(node, existing) {
                    results.push(binding.clone());
                }
                continue;
            }
        }

        // Find matching vertices
        for vertex in storage.all_vertices() {
            if node_matches(node, &vertex) {
                let mut new_binding = binding.clone();
                if let Some(ref var) = node.variable {
                    new_binding.insert(var.clone(), BoundValue::Vertex(vertex));
                }
                results.push(new_binding);
            }
        }
    }

    Ok(results)
}

fn node_matches(pattern: &NodePatternAst, vertex: &Vertex) -> bool {
    // Check labels
    for label in &pattern.labels {
        if !vertex.labels.contains(label) {
            return false;
        }
    }

    // Check properties
    if let Some(ref props) = pattern.properties {
        for (key, value) in props {
            if vertex.properties.get(key) != Some(value) {
                return false;
            }
        }
    }

    true
}

fn expand_relationship(
    rel: &RelPatternAst,
    target_node: &NodePatternAst,
    bindings: &[Bindings],
    storage: &GraphStorage,
) -> Result<Vec<Bindings>, String> {
    let mut results = Vec::new();

    for binding in bindings {
        // We need the previously bound node (the last node pattern's variable)
        // Find the source vertex from the most recently bound node variable
        let source_id = find_last_bound_vertex(binding);
        let source_id = match source_id {
            Some(id) => id,
            None => continue,
        };

        if let Some((min, max)) = rel.var_length {
            // Variable-length path
            let reachable = find_variable_length_paths(
                storage,
                source_id,
                &rel.rel_type,
                &rel.direction,
                min,
                max,
            );

            for target_id in reachable {
                if let Some(target) = storage.get_vertex(target_id) {
                    if node_matches(target_node, &target) {
                        // Check if target variable is already bound
                        if let Some(ref var) = target_node.variable {
                            if let Some(BoundValue::Vertex(existing)) = binding.get(var) {
                                if existing.id != target_id {
                                    continue;
                                }
                            }
                        }

                        let mut new_binding = binding.clone();
                        if let Some(ref var) = target_node.variable {
                            new_binding.insert(var.clone(), BoundValue::Vertex(target));
                        }
                        results.push(new_binding);
                    }
                }
            }
        } else {
            // Single-hop relationship
            let edge_ids = match rel.direction {
                RelDirection::Out => storage.get_out_edges(source_id),
                RelDirection::In => storage.get_in_edges(source_id),
                RelDirection::Both => storage.get_edges(source_id),
            };

            for eid in edge_ids {
                if let Some(edge) = storage.get_edge(eid) {
                    // Check relationship type
                    if let Some(ref rt) = rel.rel_type {
                        if edge.label != *rt {
                            continue;
                        }
                    }

                    // Check relationship properties
                    if let Some(ref props) = rel.properties {
                        let mut matches = true;
                        for (key, value) in props {
                            if edge.properties.get(key) != Some(value) {
                                matches = false;
                                break;
                            }
                        }
                        if !matches {
                            continue;
                        }
                    }

                    let target_id = match rel.direction {
                        RelDirection::Out => edge.to,
                        RelDirection::In => edge.from,
                        RelDirection::Both => edge.other(source_id).unwrap_or(edge.to),
                    };

                    if let Some(target) = storage.get_vertex(target_id) {
                        if node_matches(target_node, &target) {
                            // Check if target variable already bound
                            if let Some(ref var) = target_node.variable {
                                if let Some(BoundValue::Vertex(existing)) = binding.get(var) {
                                    if existing.id != target_id {
                                        continue;
                                    }
                                }
                            }

                            let mut new_binding = binding.clone();
                            if let Some(ref var) = rel.variable {
                                new_binding.insert(var.clone(), BoundValue::Edge(edge));
                            }
                            if let Some(ref var) = target_node.variable {
                                new_binding.insert(var.clone(), BoundValue::Vertex(target));
                            }
                            results.push(new_binding);
                        }
                    }
                }
            }
        }
    }

    Ok(results)
}

fn find_last_bound_vertex(binding: &Bindings) -> Option<VertexId> {
    // Return the last vertex found in the binding
    binding
        .values()
        .filter_map(|v| match v {
            BoundValue::Vertex(vertex) => Some(vertex.id),
            _ => None,
        })
        .last()
}

fn find_variable_length_paths(
    storage: &GraphStorage,
    start: VertexId,
    rel_type: &Option<String>,
    direction: &RelDirection,
    min: usize,
    max: usize,
) -> Vec<VertexId> {
    use std::collections::HashSet;

    let mut results = HashSet::new();
    let mut visited = HashSet::new();
    visited.insert(start);

    // BFS with depth tracking
    let mut frontier = vec![(start, 0usize)];

    while let Some((current, depth)) = frontier.pop() {
        if depth >= max {
            continue;
        }

        let edge_ids = match direction {
            RelDirection::Out => storage.get_out_edges(current),
            RelDirection::In => storage.get_in_edges(current),
            RelDirection::Both => storage.get_edges(current),
        };

        for eid in edge_ids {
            if let Some(edge) = storage.get_edge_ref(eid) {
                if let Some(ref rt) = rel_type {
                    if edge.label != *rt {
                        continue;
                    }
                }

                let next = match direction {
                    RelDirection::Out => edge.to,
                    RelDirection::In => edge.from,
                    RelDirection::Both => edge.other(current).unwrap_or(edge.to),
                };

                let new_depth = depth + 1;
                if new_depth >= min {
                    results.insert(next);
                }

                if !visited.contains(&next) && new_depth < max {
                    visited.insert(next);
                    frontier.push((next, new_depth));
                }
            }
        }
    }

    results.into_iter().collect()
}

fn evaluate_where(expr: &WhereExpr, binding: &Bindings, _storage: &GraphStorage) -> bool {
    match expr {
        WhereExpr::Comparison { left, op, right } => {
            let left_val = get_property_from_binding(binding, &left.variable, &left.property);
            let right_val = match right {
                Expr::Literal(v) => Some(v.clone()),
                Expr::Property(pa) => {
                    get_property_from_binding(binding, &pa.variable, &pa.property)
                }
                _ => None,
            };

            match (left_val, right_val) {
                (Some(l), Some(r)) => compare_with_op(&l, op, &r),
                _ => false,
            }
        }
        WhereExpr::And(a, b) => {
            evaluate_where(a, binding, _storage) && evaluate_where(b, binding, _storage)
        }
        WhereExpr::Or(a, b) => {
            evaluate_where(a, binding, _storage) || evaluate_where(b, binding, _storage)
        }
        WhereExpr::Not(inner) => !evaluate_where(inner, binding, _storage),
    }
}

fn get_property_from_binding(
    binding: &Bindings,
    variable: &str,
    property: &str,
) -> Option<PropertyValue> {
    match binding.get(variable)? {
        BoundValue::Vertex(v) => v.properties.get(property).cloned(),
        BoundValue::Edge(e) => e.properties.get(property).cloned(),
    }
}

fn compare_with_op(left: &PropertyValue, op: &CypherOp, right: &PropertyValue) -> bool {
    match op {
        CypherOp::Eq => left == right,
        CypherOp::Ne => left != right,
        CypherOp::Lt => {
            cmp_property_values(&Some(left.clone()), &Some(right.clone()))
                == std::cmp::Ordering::Less
        }
        CypherOp::Le => {
            cmp_property_values(&Some(left.clone()), &Some(right.clone()))
                != std::cmp::Ordering::Greater
        }
        CypherOp::Gt => {
            cmp_property_values(&Some(left.clone()), &Some(right.clone()))
                == std::cmp::Ordering::Greater
        }
        CypherOp::Ge => {
            cmp_property_values(&Some(left.clone()), &Some(right.clone()))
                != std::cmp::Ordering::Less
        }
        CypherOp::Contains => {
            if let (PropertyValue::String(a), PropertyValue::String(b)) = (left, right) {
                a.contains(b.as_str())
            } else {
                false
            }
        }
        CypherOp::StartsWith => {
            if let (PropertyValue::String(a), PropertyValue::String(b)) = (left, right) {
                a.starts_with(b.as_str())
            } else {
                false
            }
        }
        CypherOp::EndsWith => {
            if let (PropertyValue::String(a), PropertyValue::String(b)) = (left, right) {
                a.ends_with(b.as_str())
            } else {
                false
            }
        }
    }
}

fn eval_expr(expr: &Expr, binding: &Bindings) -> Option<PropertyValue> {
    match expr {
        Expr::Property(pa) => get_property_from_binding(binding, &pa.variable, &pa.property),
        Expr::Literal(v) => Some(v.clone()),
        Expr::Variable(_) => None,
        Expr::Aggregate(_, _) => None,
    }
}

fn binding_to_query_value(expr: &Expr, binding: &Bindings) -> QueryValue {
    match expr {
        Expr::Variable(var) => match binding.get(var) {
            Some(BoundValue::Vertex(v)) => QueryValue::Vertex(v.clone()),
            Some(BoundValue::Edge(e)) => QueryValue::Edge(e.clone()),
            None => QueryValue::Null,
        },
        Expr::Property(pa) => {
            match get_property_from_binding(binding, &pa.variable, &pa.property) {
                Some(v) => QueryValue::Property(v),
                None => QueryValue::Null,
            }
        }
        Expr::Literal(v) => QueryValue::Property(v.clone()),
        Expr::Aggregate(_, _) => QueryValue::Null,
    }
}

fn cmp_property_values(a: &Option<PropertyValue>, b: &Option<PropertyValue>) -> std::cmp::Ordering {
    match (a, b) {
        (None, None) => std::cmp::Ordering::Equal,
        (None, Some(_)) => std::cmp::Ordering::Less,
        (Some(_), None) => std::cmp::Ordering::Greater,
        (Some(a), Some(b)) => match (a, b) {
            (PropertyValue::Integer(a), PropertyValue::Integer(b)) => a.cmp(b),
            (PropertyValue::Float(a), PropertyValue::Float(b)) => {
                a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal)
            }
            (PropertyValue::Integer(a), PropertyValue::Float(b)) => (*a as f64)
                .partial_cmp(b)
                .unwrap_or(std::cmp::Ordering::Equal),
            (PropertyValue::Float(a), PropertyValue::Integer(b)) => a
                .partial_cmp(&(*b as f64))
                .unwrap_or(std::cmp::Ordering::Equal),
            (PropertyValue::String(a), PropertyValue::String(b)) => a.cmp(b),
            (PropertyValue::Boolean(a), PropertyValue::Boolean(b)) => a.cmp(b),
            _ => std::cmp::Ordering::Equal,
        },
    }
}

fn expr_display_name(expr: &Expr) -> String {
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
            format!("{}({})", func_name, expr_display_name(inner))
        }
    }
}

fn execute_create(
    create: &CreateClause,
    storage: &mut GraphStorage,
    bindings: &Bindings,
) -> Result<QueryResult, String> {
    let start = std::time::Instant::now();
    let mut created_bindings: Bindings = bindings.clone();

    for element in &create.elements {
        match element {
            CreateElement::Node(node_pattern) => {
                let id = next_vertex_id();
                let labels = if node_pattern.labels.is_empty() {
                    vec![String::new()]
                } else {
                    node_pattern.labels.clone()
                };
                let mut vertex = Vertex::with_labels(id, labels);
                if let Some(ref props) = node_pattern.properties {
                    vertex.properties = props.clone();
                }
                storage.add_vertex(vertex).map_err(|e| format!("{}", e))?;

                if let Some(ref var) = node_pattern.variable {
                    if let Some(v) = storage.get_vertex(id) {
                        created_bindings.insert(var.clone(), BoundValue::Vertex(v));
                    }
                }
            }
            CreateElement::Relationship {
                from,
                rel_type,
                to,
                properties,
            } => {
                let from_id = match created_bindings.get(from) {
                    Some(BoundValue::Vertex(v)) => v.id,
                    _ => return Err(format!("variable '{}' not bound to a node", from)),
                };
                let to_id = match created_bindings.get(to) {
                    Some(BoundValue::Vertex(v)) => v.id,
                    _ => return Err(format!("variable '{}' not bound to a node", to)),
                };

                let id = next_edge_id();
                let mut edge = Edge::new(id, from_id, to_id, rel_type);
                if let Some(props) = properties {
                    edge.properties = props.clone();
                }
                storage.add_edge(edge).map_err(|e| format!("{}", e))?;
            }
        }
    }

    Ok(QueryResult {
        rows: Vec::new(),
        columns: Vec::new(),
        took_ms: start.elapsed().as_millis() as u64,
    })
}

fn execute_match_create(
    match_clause: &MatchPattern,
    create_clause: &CreateClause,
    storage: &mut GraphStorage,
) -> Result<QueryResult, String> {
    let start = std::time::Instant::now();

    // First expand match bindings (read-only)
    let bindings = expand_match(match_clause, storage)?;

    // Execute create for each binding
    for binding in &bindings {
        execute_create(create_clause, storage, binding)?;
    }

    Ok(QueryResult {
        rows: Vec::new(),
        columns: Vec::new(),
        took_ms: start.elapsed().as_millis() as u64,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cypher::parser::CypherParser;

    fn setup_test_storage() -> GraphStorage {
        let mut storage = GraphStorage::new();

        let mut alice = Vertex::with_labels(VertexId::new(100), vec!["Person".to_string()]);
        alice.set("name", "Alice");
        alice.set("age", 30i64);
        storage.add_vertex(alice).unwrap();

        let mut bob = Vertex::with_labels(VertexId::new(101), vec!["Person".to_string()]);
        bob.set("name", "Bob");
        bob.set("age", 25i64);
        storage.add_vertex(bob).unwrap();

        let mut charlie = Vertex::with_labels(VertexId::new(102), vec!["Person".to_string()]);
        charlie.set("name", "Charlie");
        charlie.set("age", 35i64);
        storage.add_vertex(charlie).unwrap();

        let mut acme = Vertex::with_labels(VertexId::new(103), vec!["Company".to_string()]);
        acme.set("name", "Acme Corp");
        storage.add_vertex(acme).unwrap();

        use crate::graph::edge::EdgeId;
        storage
            .add_edge(Edge::new(
                EdgeId::new(200),
                VertexId::new(100),
                VertexId::new(101),
                "KNOWS",
            ))
            .unwrap();
        storage
            .add_edge(Edge::new(
                EdgeId::new(201),
                VertexId::new(101),
                VertexId::new(102),
                "KNOWS",
            ))
            .unwrap();
        storage
            .add_edge(Edge::new(
                EdgeId::new(202),
                VertexId::new(100),
                VertexId::new(103),
                "WORKS_AT",
            ))
            .unwrap();
        storage
            .add_edge(Edge::new(
                EdgeId::new(203),
                VertexId::new(101),
                VertexId::new(103),
                "WORKS_AT",
            ))
            .unwrap();

        storage
    }

    fn run_query(storage: &GraphStorage, cypher: &str) -> QueryResult {
        let stmt = CypherParser::parse(cypher).unwrap();
        match stmt {
            CypherStatement::Query(ref q) => execute_read_only(q, storage).unwrap(),
            _ => panic!("expected query"),
        }
    }

    #[test]
    fn test_match_by_label() {
        let storage = setup_test_storage();
        let result = run_query(&storage, "MATCH (n:Person) RETURN n");
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn test_match_with_property_filter() {
        let storage = setup_test_storage();
        let result = run_query(&storage, "MATCH (n:Person {name: 'Alice'}) RETURN n");
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn test_match_relationship() {
        let storage = setup_test_storage();
        let result = run_query(&storage, "MATCH (a)-[:KNOWS]->(b) RETURN a, b");
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_match_where_gt() {
        let storage = setup_test_storage();
        let result = run_query(&storage, "MATCH (n:Person) WHERE n.age > 28 RETURN n");
        assert_eq!(result.len(), 2); // Alice (30) and Charlie (35)
    }

    #[test]
    fn test_match_where_eq() {
        let storage = setup_test_storage();
        let result = run_query(&storage, "MATCH (n:Person) WHERE n.name = 'Bob' RETURN n");
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn test_count_aggregation() {
        let storage = setup_test_storage();
        let result = run_query(&storage, "MATCH (n:Person) RETURN count(n)");
        assert_eq!(result.len(), 1);
        let row = &result.rows[0];
        let val = row.values.values().next().unwrap();
        match val {
            QueryValue::Property(PropertyValue::Integer(3)) => {}
            _ => panic!("expected count=3, got {:?}", val),
        }
    }

    #[test]
    fn test_order_by_limit() {
        let storage = setup_test_storage();
        let result = run_query(
            &storage,
            "MATCH (n:Person) RETURN n.name ORDER BY n.age LIMIT 2",
        );
        assert_eq!(result.len(), 2);
        // Bob (25) should come first, then Alice (30)
        let names: Vec<_> = result
            .rows
            .iter()
            .filter_map(|r| {
                r.values.values().next().and_then(|v| match v {
                    QueryValue::Property(PropertyValue::String(s)) => Some(s.clone()),
                    _ => None,
                })
            })
            .collect();
        assert_eq!(names[0], "Bob");
        assert_eq!(names[1], "Alice");
    }

    #[test]
    fn test_variable_length_path() {
        let storage = setup_test_storage();
        let result = run_query(
            &storage,
            "MATCH (a:Person {name: 'Alice'})-[:KNOWS*1..3]->(b) RETURN b",
        );
        // Alice->Bob (1 hop), Alice->Bob->Charlie (2 hops)
        assert!(result.len() >= 2);
    }

    #[test]
    fn test_create_node() {
        let mut storage = setup_test_storage();
        let stmt = CypherParser::parse("CREATE (n:Person {name: 'Diana', age: 28})").unwrap();
        execute(&stmt, &mut storage).unwrap();
        assert_eq!(storage.vertex_count(), 5);
    }

    #[test]
    fn test_match_create_edge() {
        let mut storage = setup_test_storage();
        let stmt = CypherParser::parse(
            "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Charlie'}) CREATE (a)-[:FRIENDS]->(b)",
        )
        .unwrap();
        let prev_edges = storage.edge_count();
        execute(&stmt, &mut storage).unwrap();
        assert_eq!(storage.edge_count(), prev_edges + 1);
    }

    #[test]
    fn test_sum_aggregation() {
        let storage = setup_test_storage();
        let result = run_query(&storage, "MATCH (n:Person) RETURN sum(n.age) AS total");
        assert_eq!(result.len(), 1);
        let val = result.rows[0].values.get("total").unwrap();
        match val {
            QueryValue::Property(PropertyValue::Float(f)) => {
                assert!((f - 90.0).abs() < 0.01); // 30 + 25 + 35
            }
            _ => panic!("expected float sum, got {:?}", val),
        }
    }

    #[test]
    fn test_avg_aggregation() {
        let storage = setup_test_storage();
        let result = run_query(&storage, "MATCH (n:Person) RETURN avg(n.age) AS average");
        assert_eq!(result.len(), 1);
        let val = result.rows[0].values.get("average").unwrap();
        match val {
            QueryValue::Property(PropertyValue::Float(f)) => {
                assert!((f - 30.0).abs() < 0.01); // (30+25+35)/3 = 30
            }
            _ => panic!("expected float avg, got {:?}", val),
        }
    }

    #[test]
    fn test_where_and() {
        let storage = setup_test_storage();
        let result = run_query(
            &storage,
            "MATCH (n:Person) WHERE n.age > 24 AND n.age < 31 RETURN n",
        );
        assert_eq!(result.len(), 2); // Bob (25) and Alice (30)
    }

    #[test]
    fn test_return_property() {
        let storage = setup_test_storage();
        let result = run_query(
            &storage,
            "MATCH (n:Person) WHERE n.name = 'Alice' RETURN n.name, n.age",
        );
        assert_eq!(result.len(), 1);
    }
}
