//! Graph query language
//!
//! Provides a Cypher-like query language for graphs.

use super::*;
use std::collections::HashMap;

/// A graph query
#[derive(Debug, Clone)]
pub struct GraphQuery {
    /// Match clauses
    pub match_clauses: Vec<MatchClause>,
    /// Where clause
    pub where_clause: Option<WhereClause>,
    /// Return clause
    pub return_clause: Option<ReturnClause>,
    /// Order by clause
    pub order_by: Option<OrderByClause>,
    /// Limit
    pub limit: Option<usize>,
    /// Skip
    pub skip: Option<usize>,
}

impl GraphQuery {
    /// Create a new empty query
    pub fn new() -> Self {
        Self {
            match_clauses: Vec::new(),
            where_clause: None,
            return_clause: None,
            order_by: None,
            limit: None,
            skip: None,
        }
    }

    /// Parse a query string
    pub fn parse(query: &str) -> Result<Self> {
        QueryParser::parse(query)
    }

    /// Execute the query
    pub fn execute(&self, storage: &GraphStorage) -> Result<QueryResult> {
        let start = std::time::Instant::now();
        let mut bindings: Vec<HashMap<String, QueryValue>> = vec![HashMap::new()];

        // Process match clauses
        for match_clause in &self.match_clauses {
            bindings = self.execute_match(storage, &bindings, match_clause)?;
        }

        // Apply where clause
        if let Some(ref where_clause) = self.where_clause {
            bindings.retain(|binding| self.evaluate_where(storage, binding, where_clause));
        }

        // Apply ordering
        if let Some(ref order_by) = self.order_by {
            bindings.sort_by(|a, b| {
                for (var, asc) in &order_by.keys {
                    let va = a.get(var);
                    let vb = b.get(var);
                    let cmp = compare_query_values(va, vb);
                    if cmp != std::cmp::Ordering::Equal {
                        return if *asc { cmp } else { cmp.reverse() };
                    }
                }
                std::cmp::Ordering::Equal
            });
        }

        // Apply skip
        if let Some(skip) = self.skip {
            if skip < bindings.len() {
                bindings = bindings[skip..].to_vec();
            } else {
                bindings.clear();
            }
        }

        // Apply limit
        if let Some(limit) = self.limit {
            bindings.truncate(limit);
        }

        // Build result
        let columns = self
            .return_clause
            .as_ref()
            .map(|r| {
                r.items
                    .iter()
                    .map(|i| i.alias.clone().unwrap_or_else(|| i.variable.clone()))
                    .collect()
            })
            .unwrap_or_default();

        let rows = bindings
            .into_iter()
            .map(|binding| {
                let values = if let Some(ref ret) = self.return_clause {
                    ret.items
                        .iter()
                        .map(|item| {
                            let alias = item.alias.clone().unwrap_or_else(|| item.variable.clone());
                            let value = binding
                                .get(&item.variable)
                                .cloned()
                                .unwrap_or(QueryValue::Null);
                            (alias, value)
                        })
                        .collect()
                } else {
                    binding
                };
                QueryRow { values }
            })
            .collect();

        Ok(QueryResult {
            rows,
            columns,
            took_ms: start.elapsed().as_millis() as u64,
        })
    }

    fn execute_match(
        &self,
        storage: &GraphStorage,
        bindings: &[HashMap<String, QueryValue>],
        match_clause: &MatchClause,
    ) -> Result<Vec<HashMap<String, QueryValue>>> {
        let mut new_bindings = Vec::new();

        for binding in bindings {
            match match_clause {
                MatchClause::Node {
                    variable,
                    labels,
                    properties,
                } => {
                    // Find all matching nodes
                    for vertex in storage.all_vertices() {
                        // Check labels (vertex must have all required labels)
                        let mut label_match = true;
                        for label in labels {
                            if !vertex.labels.contains(label) {
                                label_match = false;
                                break;
                            }
                        }
                        if !label_match {
                            continue;
                        }

                        // Check properties
                        if let Some(props) = properties {
                            let mut matches = true;
                            for (key, value) in props {
                                if vertex.properties.get(key) != Some(value) {
                                    matches = false;
                                    break;
                                }
                            }
                            if !matches {
                                continue;
                            }
                        }

                        let mut new_binding = binding.clone();
                        new_binding.insert(variable.clone(), QueryValue::Vertex(vertex));
                        new_bindings.push(new_binding);
                    }
                }
                MatchClause::Relationship {
                    from,
                    edge,
                    to,
                    direction,
                    labels,
                    properties,
                } => {
                    // Get the from vertex from bindings
                    let from_vertex = match binding.get(from) {
                        Some(QueryValue::Vertex(v)) => v.clone(),
                        _ => continue,
                    };

                    // Find matching edges
                    let edge_ids = match direction {
                        Direction::Out => storage.get_out_edges(from_vertex.id),
                        Direction::In => storage.get_in_edges(from_vertex.id),
                        Direction::Both => storage.get_edges(from_vertex.id),
                    };

                    for eid in edge_ids {
                        if let Some(edge_data) = storage.get_edge(eid) {
                            // Check labels
                            if !labels.is_empty() && !labels.contains(&edge_data.label) {
                                continue;
                            }

                            // Check properties
                            if let Some(props) = properties {
                                let mut matches = true;
                                for (key, value) in props {
                                    if edge_data.properties.get(key) != Some(value) {
                                        matches = false;
                                        break;
                                    }
                                }
                                if !matches {
                                    continue;
                                }
                            }

                            // Get target vertex
                            let target_id = match direction {
                                Direction::Out => edge_data.to,
                                Direction::In => edge_data.from,
                                Direction::Both => {
                                    edge_data.other(from_vertex.id).unwrap_or(edge_data.to)
                                }
                            };

                            if let Some(target_vertex) = storage.get_vertex(target_id) {
                                let mut new_binding = binding.clone();
                                if let Some(e) = edge {
                                    new_binding.insert(e.clone(), QueryValue::Edge(edge_data));
                                }
                                new_binding.insert(to.clone(), QueryValue::Vertex(target_vertex));
                                new_bindings.push(new_binding);
                            }
                        }
                    }
                }
            }
        }

        Ok(new_bindings)
    }

    #[allow(clippy::only_used_in_recursion)]
    fn evaluate_where(
        &self,
        _storage: &GraphStorage,
        binding: &HashMap<String, QueryValue>,
        where_clause: &WhereClause,
    ) -> bool {
        match where_clause {
            WhereClause::Property {
                variable,
                property,
                operator,
                value,
            } => {
                let actual = match binding.get(variable) {
                    Some(QueryValue::Vertex(v)) => v.properties.get(property).cloned(),
                    Some(QueryValue::Edge(e)) => e.properties.get(property).cloned(),
                    _ => None,
                };

                match actual {
                    Some(actual_value) => match operator.as_str() {
                        "=" => actual_value == *value,
                        "<>" | "!=" => actual_value != *value,
                        "<" => {
                            compare_property_values_ord(&actual_value, value)
                                == std::cmp::Ordering::Less
                        }
                        "<=" => {
                            compare_property_values_ord(&actual_value, value)
                                != std::cmp::Ordering::Greater
                        }
                        ">" => {
                            compare_property_values_ord(&actual_value, value)
                                == std::cmp::Ordering::Greater
                        }
                        ">=" => {
                            compare_property_values_ord(&actual_value, value)
                                != std::cmp::Ordering::Less
                        }
                        "CONTAINS" => {
                            if let (PropertyValue::String(a), PropertyValue::String(b)) =
                                (&actual_value, value)
                            {
                                a.contains(b)
                            } else {
                                false
                            }
                        }
                        "STARTS WITH" => {
                            if let (PropertyValue::String(a), PropertyValue::String(b)) =
                                (&actual_value, value)
                            {
                                a.starts_with(b)
                            } else {
                                false
                            }
                        }
                        "ENDS WITH" => {
                            if let (PropertyValue::String(a), PropertyValue::String(b)) =
                                (&actual_value, value)
                            {
                                a.ends_with(b)
                            } else {
                                false
                            }
                        }
                        _ => false,
                    },
                    None => false,
                }
            }
            WhereClause::And(clauses) => clauses
                .iter()
                .all(|c| self.evaluate_where(_storage, binding, c)),
            WhereClause::Or(clauses) => clauses
                .iter()
                .any(|c| self.evaluate_where(_storage, binding, c)),
            WhereClause::Not(clause) => !self.evaluate_where(_storage, binding, clause),
            WhereClause::Exists { variable } => binding.contains_key(variable),
            WhereClause::IsNull { variable, property } => match binding.get(variable) {
                Some(QueryValue::Vertex(v)) => v
                    .properties
                    .get(property)
                    .map(|p| p.is_null())
                    .unwrap_or(true),
                Some(QueryValue::Edge(e)) => e
                    .properties
                    .get(property)
                    .map(|p| p.is_null())
                    .unwrap_or(true),
                _ => true,
            },
        }
    }
}

impl Default for GraphQuery {
    fn default() -> Self {
        Self::new()
    }
}

/// Match clause
#[derive(Debug, Clone)]
pub enum MatchClause {
    /// Match a node
    Node {
        variable: String,
        labels: Vec<String>,
        properties: Option<HashMap<String, PropertyValue>>,
    },
    /// Match a relationship
    Relationship {
        from: String,
        edge: Option<String>,
        to: String,
        direction: Direction,
        labels: Vec<String>,
        properties: Option<HashMap<String, PropertyValue>>,
    },
}

/// Where clause
#[derive(Debug, Clone)]
pub enum WhereClause {
    /// Property comparison
    Property {
        variable: String,
        property: String,
        operator: String,
        value: PropertyValue,
    },
    /// AND
    And(Vec<WhereClause>),
    /// OR
    Or(Vec<WhereClause>),
    /// NOT
    Not(Box<WhereClause>),
    /// EXISTS
    Exists { variable: String },
    /// IS NULL
    IsNull { variable: String, property: String },
}

/// Return clause
#[derive(Debug, Clone)]
pub struct ReturnClause {
    /// Items to return
    pub items: Vec<ReturnItem>,
    /// Distinct
    pub distinct: bool,
}

/// Return item
#[derive(Debug, Clone)]
pub struct ReturnItem {
    /// Variable name
    pub variable: String,
    /// Property (if any)
    pub property: Option<String>,
    /// Alias
    pub alias: Option<String>,
}

/// Order by clause
#[derive(Debug, Clone)]
pub struct OrderByClause {
    /// Order keys (variable, ascending)
    pub keys: Vec<(String, bool)>,
}

/// Query parser
pub struct QueryParser;

impl QueryParser {
    /// Parse a query string
    pub fn parse(query: &str) -> Result<GraphQuery> {
        let query = query.trim();

        // Very simplified parser for demonstration
        // In production, you'd use a proper parser combinator or grammar
        let mut result = GraphQuery::new();

        // Parse MATCH clause
        if query.to_uppercase().starts_with("MATCH") {
            let match_part = Self::extract_clause(
                query,
                "MATCH",
                &["WHERE", "RETURN", "ORDER", "LIMIT", "SKIP"],
            );

            // Parse node pattern like (n:Person {name: "Alice"})
            if let Some(node_match) = Self::parse_node_pattern(&match_part) {
                result.match_clauses.push(node_match);
            }

            // Parse relationship pattern like (a)-[r:KNOWS]->(b)
            if let Some(rel_match) = Self::parse_relationship_pattern(&match_part) {
                result.match_clauses.push(rel_match);
            }
        }

        // Parse WHERE clause
        if query.to_uppercase().contains("WHERE") {
            let where_part =
                Self::extract_clause(query, "WHERE", &["RETURN", "ORDER", "LIMIT", "SKIP"]);
            if let Some(where_clause) = Self::parse_where(&where_part) {
                result.where_clause = Some(where_clause);
            }
        }

        // Parse RETURN clause
        if query.to_uppercase().contains("RETURN") {
            let return_part = Self::extract_clause(query, "RETURN", &["ORDER", "LIMIT", "SKIP"]);
            if let Some(return_clause) = Self::parse_return(&return_part) {
                result.return_clause = Some(return_clause);
            }
        }

        // Parse LIMIT
        if query.to_uppercase().contains("LIMIT") {
            let limit_part = Self::extract_clause(query, "LIMIT", &["SKIP"]);
            if let Ok(limit) = limit_part.trim().parse::<usize>() {
                result.limit = Some(limit);
            }
        }

        // Parse SKIP
        if query.to_uppercase().contains("SKIP") {
            let skip_part = Self::extract_clause(query, "SKIP", &[]);
            if let Ok(skip) = skip_part.trim().parse::<usize>() {
                result.skip = Some(skip);
            }
        }

        Ok(result)
    }

    fn extract_clause(query: &str, start: &str, ends: &[&str]) -> String {
        let upper = query.to_uppercase();
        let start_idx = upper.find(start).unwrap_or(0) + start.len();

        let end_idx = ends
            .iter()
            .filter_map(|&end| upper[start_idx..].find(end).map(|i| start_idx + i))
            .min()
            .unwrap_or(query.len());

        query[start_idx..end_idx].trim().to_string()
    }

    fn parse_node_pattern(pattern: &str) -> Option<MatchClause> {
        // Parse (n:Label {prop: value})
        let pattern = pattern.trim();
        if !pattern.starts_with('(') || !pattern.ends_with(')') {
            return None;
        }

        let inner = &pattern[1..pattern.len() - 1];
        let parts: Vec<&str> = inner.splitn(2, ':').collect();

        let variable = parts[0].trim().to_string();
        let mut labels = Vec::new();
        let mut properties = None;

        if parts.len() > 1 {
            let rest = parts[1];
            // Check for properties
            if let Some(prop_start) = rest.find('{') {
                let label_part = &rest[..prop_start];
                labels.push(label_part.trim().to_string());

                // Parse properties (simplified)
                let prop_end = rest.rfind('}').unwrap_or(rest.len());
                let prop_str = &rest[prop_start + 1..prop_end];
                properties = Some(Self::parse_properties(prop_str));
            } else {
                labels.push(rest.trim().to_string());
            }
        }

        Some(MatchClause::Node {
            variable,
            labels,
            properties,
        })
    }

    fn parse_relationship_pattern(pattern: &str) -> Option<MatchClause> {
        // Parse (a)-[r:KNOWS]->(b) or (a)<-[r:KNOWS]-(b)
        if !pattern.contains("->") && !pattern.contains("<-") {
            return None;
        }

        let direction = if pattern.contains("->") {
            Direction::Out
        } else {
            Direction::In
        };

        // Split by arrow
        let parts: Vec<&str> = if direction == Direction::Out {
            pattern.splitn(2, "->").collect()
        } else {
            pattern.splitn(2, "<-").collect()
        };

        if parts.len() != 2 {
            return None;
        }

        // Parse from node
        let from_part = parts[0].trim();
        let from_start = from_part.find('(')? + 1;
        let from_end = from_part.find(')')?;
        let from = from_part[from_start..from_end]
            .split(':')
            .next()?
            .trim()
            .to_string();

        // Parse edge
        let edge_start = from_part.rfind('[');
        let edge_end = from_part.rfind(']');
        let (edge_var, edge_labels) = if let (Some(start), Some(end)) = (edge_start, edge_end) {
            let edge_inner = &from_part[start + 1..end];
            let edge_parts: Vec<&str> = edge_inner.splitn(2, ':').collect();
            let var = if edge_parts[0].is_empty() {
                None
            } else {
                Some(edge_parts[0].to_string())
            };
            let labels = if edge_parts.len() > 1 {
                vec![edge_parts[1].to_string()]
            } else {
                Vec::new()
            };
            (var, labels)
        } else {
            (None, Vec::new())
        };

        // Parse to node
        let to_part = parts[1].trim();
        let to_start = to_part.find('(')? + 1;
        let to_end = to_part.find(')')?;
        let to = to_part[to_start..to_end]
            .split(':')
            .next()?
            .trim()
            .to_string();

        Some(MatchClause::Relationship {
            from,
            edge: edge_var,
            to,
            direction,
            labels: edge_labels,
            properties: None,
        })
    }

    fn parse_where(clause: &str) -> Option<WhereClause> {
        // Simplified where parsing: variable.property = value
        let parts: Vec<&str> = clause.split_whitespace().collect();
        if parts.len() < 3 {
            return None;
        }

        // Parse variable.property
        let var_prop: Vec<&str> = parts[0].split('.').collect();
        if var_prop.len() != 2 {
            return None;
        }

        let variable = var_prop[0].to_string();
        let property = var_prop[1].to_string();
        let operator = parts[1].to_string();
        let value_str = parts[2..].join(" ");

        // Parse value
        let value = if value_str.starts_with('"') || value_str.starts_with('\'') {
            PropertyValue::String(
                value_str
                    .trim_matches(|c| c == '"' || c == '\'')
                    .to_string(),
            )
        } else if let Ok(i) = value_str.parse::<i64>() {
            PropertyValue::Integer(i)
        } else if let Ok(f) = value_str.parse::<f64>() {
            PropertyValue::Float(f)
        } else if value_str == "true" {
            PropertyValue::Boolean(true)
        } else if value_str == "false" {
            PropertyValue::Boolean(false)
        } else {
            PropertyValue::String(value_str)
        };

        Some(WhereClause::Property {
            variable,
            property,
            operator,
            value,
        })
    }

    fn parse_return(clause: &str) -> Option<ReturnClause> {
        let distinct = clause.to_uppercase().starts_with("DISTINCT");
        let clause = if distinct { clause[8..].trim() } else { clause };

        let items: Vec<ReturnItem> = clause
            .split(',')
            .map(|item| {
                let item = item.trim();
                let parts: Vec<&str> = item.split(" AS ").collect();
                let var_prop: Vec<&str> = parts[0].split('.').collect();

                ReturnItem {
                    variable: var_prop[0].to_string(),
                    property: var_prop.get(1).map(|s| s.to_string()),
                    alias: parts.get(1).map(|s| s.trim().to_string()),
                }
            })
            .collect();

        Some(ReturnClause { items, distinct })
    }

    fn parse_properties(props: &str) -> HashMap<String, PropertyValue> {
        let mut result = HashMap::new();

        for pair in props.split(',') {
            let parts: Vec<&str> = pair.split(':').collect();
            if parts.len() == 2 {
                let key = parts[0].trim().to_string();
                let value_str = parts[1].trim();

                let value = if value_str.starts_with('"') || value_str.starts_with('\'') {
                    PropertyValue::String(
                        value_str
                            .trim_matches(|c| c == '"' || c == '\'')
                            .to_string(),
                    )
                } else if let Ok(i) = value_str.parse::<i64>() {
                    PropertyValue::Integer(i)
                } else if let Ok(f) = value_str.parse::<f64>() {
                    PropertyValue::Float(f)
                } else {
                    PropertyValue::String(value_str.to_string())
                };

                result.insert(key, value);
            }
        }

        result
    }
}

/// Query builder for fluent query construction
pub struct QueryBuilder {
    query: GraphQuery,
}

impl QueryBuilder {
    /// Create a new query builder
    pub fn new() -> Self {
        Self {
            query: GraphQuery::new(),
        }
    }

    /// Add a MATCH node clause
    pub fn match_node(mut self, variable: &str) -> Self {
        self.query.match_clauses.push(MatchClause::Node {
            variable: variable.to_string(),
            labels: Vec::new(),
            properties: None,
        });
        self
    }

    /// Add a MATCH node with label
    pub fn match_node_with_label(mut self, variable: &str, label: &str) -> Self {
        self.query.match_clauses.push(MatchClause::Node {
            variable: variable.to_string(),
            labels: vec![label.to_string()],
            properties: None,
        });
        self
    }

    /// Add WHERE clause
    pub fn where_property(
        mut self,
        variable: &str,
        property: &str,
        op: &str,
        value: PropertyValue,
    ) -> Self {
        self.query.where_clause = Some(WhereClause::Property {
            variable: variable.to_string(),
            property: property.to_string(),
            operator: op.to_string(),
            value,
        });
        self
    }

    /// Add RETURN clause
    pub fn return_var(mut self, variable: &str) -> Self {
        let item = ReturnItem {
            variable: variable.to_string(),
            property: None,
            alias: None,
        };

        match &mut self.query.return_clause {
            Some(ret) => ret.items.push(item),
            None => {
                self.query.return_clause = Some(ReturnClause {
                    items: vec![item],
                    distinct: false,
                });
            }
        }
        self
    }

    /// Set LIMIT
    pub fn limit(mut self, n: usize) -> Self {
        self.query.limit = Some(n);
        self
    }

    /// Set SKIP
    pub fn skip(mut self, n: usize) -> Self {
        self.query.skip = Some(n);
        self
    }

    /// Build the query
    pub fn build(self) -> GraphQuery {
        self.query
    }
}

impl Default for QueryBuilder {
    fn default() -> Self {
        Self::new()
    }
}

fn compare_query_values(a: Option<&QueryValue>, b: Option<&QueryValue>) -> std::cmp::Ordering {
    match (a, b) {
        (None, None) => std::cmp::Ordering::Equal,
        (None, Some(_)) => std::cmp::Ordering::Less,
        (Some(_), None) => std::cmp::Ordering::Greater,
        (Some(a), Some(b)) => match (a, b) {
            (QueryValue::Property(a), QueryValue::Property(b)) => compare_property_values_ord(a, b),
            _ => std::cmp::Ordering::Equal,
        },
    }
}

fn compare_property_values_ord(a: &PropertyValue, b: &PropertyValue) -> std::cmp::Ordering {
    match (a, b) {
        (PropertyValue::Integer(a), PropertyValue::Integer(b)) => a.cmp(b),
        (PropertyValue::Float(a), PropertyValue::Float(b)) => {
            a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal)
        }
        (PropertyValue::String(a), PropertyValue::String(b)) => a.cmp(b),
        _ => std::cmp::Ordering::Equal,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_storage() -> GraphStorage {
        let mut storage = GraphStorage::new();

        let mut v1 = Vertex::new(VertexId::new(1), "Person");
        v1.set("name", "Alice");
        v1.set("age", 30i64);
        storage.add_vertex(v1).unwrap();

        let mut v2 = Vertex::new(VertexId::new(2), "Person");
        v2.set("name", "Bob");
        v2.set("age", 25i64);
        storage.add_vertex(v2).unwrap();

        storage
            .add_edge(Edge::new(
                EdgeId::new(1),
                VertexId::new(1),
                VertexId::new(2),
                "KNOWS",
            ))
            .unwrap();

        storage
    }

    #[test]
    fn test_query_parser_node() {
        let query = GraphQuery::parse("MATCH (n:Person) RETURN n").unwrap();

        assert!(!query.match_clauses.is_empty());
        assert!(query.return_clause.is_some());
    }

    #[test]
    fn test_query_parser_where() {
        let query = GraphQuery::parse("MATCH (n:Person) WHERE n.age = 30 RETURN n").unwrap();

        assert!(query.where_clause.is_some());
    }

    #[test]
    fn test_query_parser_limit() {
        let query = GraphQuery::parse("MATCH (n) RETURN n LIMIT 10").unwrap();

        assert_eq!(query.limit, Some(10));
    }

    #[test]
    fn test_query_execute() {
        let storage = create_test_storage();

        let query = QueryBuilder::new()
            .match_node_with_label("n", "Person")
            .return_var("n")
            .build();

        let result = query.execute(&storage).unwrap();
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_query_with_where() {
        let storage = create_test_storage();

        let query = QueryBuilder::new()
            .match_node_with_label("n", "Person")
            .where_property("n", "name", "=", PropertyValue::String("Alice".to_string()))
            .return_var("n")
            .build();

        let result = query.execute(&storage).unwrap();
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn test_query_with_limit() {
        let storage = create_test_storage();

        let query = QueryBuilder::new()
            .match_node_with_label("n", "Person")
            .return_var("n")
            .limit(1)
            .build();

        let result = query.execute(&storage).unwrap();
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn test_query_builder() {
        let query = QueryBuilder::new()
            .match_node("n")
            .return_var("n")
            .limit(10)
            .skip(5)
            .build();

        assert!(!query.match_clauses.is_empty());
        assert!(query.return_clause.is_some());
        assert_eq!(query.limit, Some(10));
        assert_eq!(query.skip, Some(5));
    }

    #[test]
    fn test_where_clause_types() {
        let storage = create_test_storage();

        // Test integer comparison
        let query = QueryBuilder::new()
            .match_node_with_label("n", "Person")
            .where_property("n", "age", ">", PropertyValue::Integer(26))
            .return_var("n")
            .build();

        let result = query.execute(&storage).unwrap();
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn test_query_result_columns() {
        let storage = create_test_storage();

        let query = QueryBuilder::new()
            .match_node_with_label("n", "Person")
            .return_var("n")
            .build();

        let result = query.execute(&storage).unwrap();
        assert!(result.columns.contains(&"n".to_string()));
    }
}
