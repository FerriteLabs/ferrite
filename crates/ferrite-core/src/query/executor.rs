//! FerriteQL Query Executor
//!
//! Executes query plans against the storage engine.

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;

use crate::query::functions::FunctionRegistry;
use crate::query::planner::*;
use crate::query::{QueryConfig, QueryError, QueryStats, ResultSet, Row, Value};
use crate::storage::Store;
use crate::storage::Value as StorageValue;

/// Query executor
pub struct QueryExecutor {
    store: Arc<Store>,
    config: QueryConfig,
    functions: FunctionRegistry,
}

impl QueryExecutor {
    /// Create a new query executor
    pub fn new(store: Arc<Store>, config: QueryConfig) -> Self {
        Self {
            store,
            config,
            functions: FunctionRegistry::new(),
        }
    }

    /// Execute a query plan
    pub async fn execute(&self, plan: &QueryPlan, db: u8) -> Result<ResultSet, QueryError> {
        let mut stats = QueryStats::default();
        let start = std::time::Instant::now();

        let result = self.execute_plan(plan, db, &mut stats).await?;

        stats.execution_time_us = start.elapsed().as_micros() as u64;
        Ok(result.with_stats(stats))
    }

    fn execute_plan<'a>(
        &'a self,
        plan: &'a QueryPlan,
        db: u8,
        stats: &'a mut QueryStats,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<ResultSet, QueryError>> + Send + 'a>,
    > {
        Box::pin(async move {
            match plan {
                QueryPlan::Scan(scan) => self.execute_scan(scan, db, stats).await,
                QueryPlan::Filter(filter) => self.execute_filter(filter, db, stats).await,
                QueryPlan::Project(proj) => self.execute_project(proj, db, stats).await,
                QueryPlan::Join(join) => self.execute_join(join, db, stats).await,
                QueryPlan::Aggregate(agg) => self.execute_aggregate(agg, db, stats).await,
                QueryPlan::Sort(sort) => self.execute_sort(sort, db, stats).await,
                QueryPlan::Limit(limit) => self.execute_limit(limit, db, stats).await,
                QueryPlan::Distinct(input) => self.execute_distinct(input, db, stats).await,
                QueryPlan::EmptyRelation => Ok(ResultSet::empty()),
                QueryPlan::Explain(inner) => self.execute_explain(inner),
                QueryPlan::Insert(insert) => self.execute_insert(insert, db, stats).await,
                QueryPlan::Update(update) => self.execute_update(update, db, stats).await,
                QueryPlan::Delete(delete) => self.execute_delete(delete, db, stats).await,
            }
        })
    }

    async fn execute_scan(
        &self,
        scan: &ScanPlan,
        db: u8,
        stats: &mut QueryStats,
    ) -> Result<ResultSet, QueryError> {
        let pattern = &scan.pattern;

        // Get keys matching the pattern
        let keys = self.scan_keys(pattern, db, stats)?;

        let mut rows = Vec::new();
        let mut columns = vec!["_key".to_string(), "_value".to_string()];

        // Add alias prefix if specified
        if let Some(alias) = &scan.alias {
            columns = columns
                .into_iter()
                .map(|c| format!("{}.{}", alias, c))
                .collect();
        }

        for key in keys {
            if rows.len() >= self.config.max_scan_rows {
                return Err(QueryError::RowLimitExceeded);
            }

            // Get the value for each key
            let key_bytes = Bytes::from(key.clone());
            if let Some(storage_value) = self.store.get(db, &key_bytes) {
                let value_str = self.storage_value_to_string(&storage_value);

                // Try to parse as JSON for structured data
                let row = if let Ok(json) = serde_json::from_str::<serde_json::Value>(&value_str) {
                    // If JSON, expand to columns
                    self.json_to_row(&key, &json, scan.alias.as_deref())
                } else {
                    // Simple key-value
                    Row::new(vec![Value::String(key), Value::String(value_str)])
                };

                rows.push(row);
                stats.rows_examined += 1;
            }
        }

        stats.rows_returned = rows.len() as u64;
        Ok(ResultSet::new(columns, rows))
    }

    /// Convert storage Value to a string representation
    fn storage_value_to_string(&self, value: &StorageValue) -> String {
        match value {
            StorageValue::String(bytes) => String::from_utf8_lossy(bytes).to_string(),
            StorageValue::List(list) => {
                let items: Vec<String> = list
                    .iter()
                    .map(|b| String::from_utf8_lossy(b).to_string())
                    .collect();
                serde_json::to_string(&items).unwrap_or_else(|_| "[]".to_string())
            }
            StorageValue::Hash(hash) => {
                let map: HashMap<String, String> = hash
                    .iter()
                    .map(|(k, v)| {
                        (
                            String::from_utf8_lossy(k).to_string(),
                            String::from_utf8_lossy(v).to_string(),
                        )
                    })
                    .collect();
                serde_json::to_string(&map).unwrap_or_else(|_| "{}".to_string())
            }
            StorageValue::Set(set) => {
                let items: Vec<String> = set
                    .iter()
                    .map(|b| String::from_utf8_lossy(b).to_string())
                    .collect();
                serde_json::to_string(&items).unwrap_or_else(|_| "[]".to_string())
            }
            StorageValue::SortedSet { by_member, .. } => {
                let items: Vec<(String, f64)> = by_member
                    .iter()
                    .map(|(k, v)| (String::from_utf8_lossy(k).to_string(), *v))
                    .collect();
                serde_json::to_string(&items).unwrap_or_else(|_| "[]".to_string())
            }
            StorageValue::Stream(stream) => {
                format!("stream(length={})", stream.length)
            }
            StorageValue::HyperLogLog(_) => "hyperloglog".to_string(),
        }
    }

    /// Convert query Value to storage Value
    #[allow(dead_code)] // Planned for v0.2 — used for query result materialization
    fn query_value_to_storage(&self, value: &Value) -> StorageValue {
        match value {
            Value::String(s) => StorageValue::String(Bytes::from(s.clone())),
            Value::Int(n) => StorageValue::String(Bytes::from(n.to_string())),
            Value::Float(f) => StorageValue::String(Bytes::from(f.to_string())),
            Value::Bool(b) => StorageValue::String(Bytes::from(if *b { "true" } else { "false" })),
            Value::Null => StorageValue::String(Bytes::new()),
            Value::Bytes(b) => StorageValue::String(Bytes::copy_from_slice(b)),
            Value::Array(_) | Value::Map(_) => {
                // Serialize complex types to JSON
                let json = self.value_to_json(value);
                StorageValue::String(Bytes::from(
                    serde_json::to_string(&json).unwrap_or_default(),
                ))
            }
        }
    }

    fn scan_keys(
        &self,
        pattern: &str,
        db: u8,
        stats: &mut QueryStats,
    ) -> Result<Vec<String>, QueryError> {
        let all_keys = self.store.keys(db);
        let mut keys = Vec::new();

        for key in all_keys {
            if let Ok(key_str) = std::str::from_utf8(&key) {
                if self.glob_match(pattern, key_str) {
                    keys.push(key_str.to_string());
                    stats.keys_scanned += 1;

                    if keys.len() >= self.config.max_scan_rows {
                        break;
                    }
                }
            }
        }

        Ok(keys)
    }

    /// Glob-style pattern matching
    fn glob_match(&self, pattern: &str, text: &str) -> bool {
        self.glob_match_impl(pattern.as_bytes(), text.as_bytes())
    }

    fn glob_match_impl(&self, pattern: &[u8], text: &[u8]) -> bool {
        let mut p = 0;
        let mut t = 0;
        let mut star_p = None;
        let mut star_t = 0;

        while t < text.len() {
            if p < pattern.len() {
                match pattern[p] {
                    b'*' => {
                        star_p = Some(p);
                        star_t = t;
                        p += 1;
                        continue;
                    }
                    b'?' => {
                        p += 1;
                        t += 1;
                        continue;
                    }
                    c => {
                        if c == text[t] {
                            p += 1;
                            t += 1;
                            continue;
                        }
                    }
                }
            }

            // No match - try to backtrack to last *
            if let Some(sp) = star_p {
                p = sp + 1;
                star_t += 1;
                t = star_t;
            } else {
                return false;
            }
        }

        // Check remaining pattern (should only be *)
        while p < pattern.len() && pattern[p] == b'*' {
            p += 1;
        }

        p == pattern.len()
    }

    fn json_to_row(&self, key: &str, json: &serde_json::Value, alias: Option<&str>) -> Row {
        let mut values = vec![Value::String(key.to_string())];

        if let serde_json::Value::Object(map) = json {
            for (_k, v) in map {
                values.push(self.json_value_to_value(v));
            }
        } else {
            values.push(self.json_value_to_value(json));
        }

        let _ = alias; // Would be used for column naming
        Row::new(values)
    }

    #[allow(clippy::only_used_in_recursion)]
    fn json_value_to_value(&self, json: &serde_json::Value) -> Value {
        match json {
            serde_json::Value::Null => Value::Null,
            serde_json::Value::Bool(b) => Value::Bool(*b),
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Value::Int(i)
                } else if let Some(f) = n.as_f64() {
                    Value::Float(f)
                } else {
                    Value::String(n.to_string())
                }
            }
            serde_json::Value::String(s) => Value::String(s.clone()),
            serde_json::Value::Array(arr) => {
                Value::Array(arr.iter().map(|v| self.json_value_to_value(v)).collect())
            }
            serde_json::Value::Object(map) => Value::Map(
                map.iter()
                    .map(|(k, v)| (k.clone(), self.json_value_to_value(v)))
                    .collect(),
            ),
        }
    }

    async fn execute_filter(
        &self,
        filter: &FilterPlan,
        db: u8,
        stats: &mut QueryStats,
    ) -> Result<ResultSet, QueryError> {
        let input = self.execute_plan(&filter.input, db, stats).await?;

        // Pre-materialize any subqueries in the predicate
        let materialized = self
            .materialize_subqueries(&filter.predicate, db, stats)
            .await?;

        let filtered_rows: Vec<Row> = input
            .rows
            .into_iter()
            .filter(|row| {
                let context = RowContext::new(&input.columns, row);
                self.evaluate_predicate(&materialized, &context)
                    .map(|v| v.is_truthy())
                    .unwrap_or(false)
            })
            .collect();

        stats.rows_returned = filtered_rows.len() as u64;
        Ok(ResultSet::new(input.columns, filtered_rows))
    }

    /// Materialize subqueries within an expression, replacing them with literal IN lists.
    fn materialize_subqueries<'a>(
        &'a self,
        expr: &'a PlanExpr,
        db: u8,
        stats: &'a mut QueryStats,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<PlanExpr, QueryError>> + Send + 'a>>
    {
        Box::pin(async move {
            match expr {
                PlanExpr::InSubquery {
                    expr: inner_expr,
                    subquery,
                    negated,
                } => {
                    let result = self.execute_plan(subquery, db, stats).await?;
                    let list: Vec<PlanExpr> = result
                        .rows
                        .iter()
                        .filter_map(|row| row.values.first().cloned())
                        .map(PlanExpr::Literal)
                        .collect();
                    Ok(PlanExpr::In {
                        expr: Box::new(*inner_expr.clone()),
                        list,
                        negated: *negated,
                    })
                }
                PlanExpr::Exists { subquery, negated } => {
                    let result = self.execute_plan(subquery, db, stats).await?;
                    let exists = !result.rows.is_empty();
                    let val = if *negated { !exists } else { exists };
                    Ok(PlanExpr::Literal(Value::Bool(val)))
                }
                PlanExpr::BinaryOp { left, op, right } => {
                    let left = self.materialize_subqueries(left, db, stats).await?;
                    let right = self.materialize_subqueries(right, db, stats).await?;
                    Ok(PlanExpr::BinaryOp {
                        left: Box::new(left),
                        op: op.clone(),
                        right: Box::new(right),
                    })
                }
                PlanExpr::UnaryOp { op, expr: inner } => {
                    let inner = self.materialize_subqueries(inner, db, stats).await?;
                    Ok(PlanExpr::UnaryOp {
                        op: op.clone(),
                        expr: Box::new(inner),
                    })
                }
                _ => Ok(expr.clone()),
            }
        })
    }

    async fn execute_project(
        &self,
        proj: &ProjectionPlan,
        db: u8,
        stats: &mut QueryStats,
    ) -> Result<ResultSet, QueryError> {
        let input = self.execute_plan(&proj.input, db, stats).await?;

        let mut new_columns = Vec::new();
        #[allow(clippy::type_complexity)]
        let mut projectors: Vec<Box<dyn Fn(&RowContext) -> Value + Send + Sync>> = Vec::new();

        for item in &proj.items {
            match item {
                ProjectionItem::All => {
                    for (i, col) in input.columns.iter().enumerate() {
                        new_columns.push(col.clone());
                        let idx = i;
                        projectors.push(Box::new(move |ctx: &RowContext| {
                            ctx.row.get(idx).cloned().unwrap_or(Value::Null)
                        }));
                    }
                }
                ProjectionItem::AllFrom(table) => {
                    for (i, col) in input.columns.iter().enumerate() {
                        if col.starts_with(&format!("{}.", table)) {
                            new_columns.push(col.clone());
                            let idx = i;
                            projectors.push(Box::new(move |ctx: &RowContext| {
                                ctx.row.get(idx).cloned().unwrap_or(Value::Null)
                            }));
                        }
                    }
                }
                ProjectionItem::Expr { expr, alias } => {
                    let col_name = alias.clone().unwrap_or_else(|| self.expr_to_name(expr));
                    new_columns.push(col_name);

                    let expr = expr.clone();
                    let funcs = FunctionRegistry::new();
                    projectors.push(Box::new(move |ctx: &RowContext| {
                        Self::evaluate_expr_static(&expr, ctx, &funcs).unwrap_or(Value::Null)
                    }));
                }
            }
        }

        let new_rows: Vec<Row> = input
            .rows
            .iter()
            .map(|row| {
                let context = RowContext::new(&input.columns, row);
                let values: Vec<Value> = projectors.iter().map(|p| p(&context)).collect();
                Row::new(values)
            })
            .collect();

        stats.rows_returned = new_rows.len() as u64;
        Ok(ResultSet::new(new_columns, new_rows))
    }

    async fn execute_join(
        &self,
        join: &JoinPlan,
        db: u8,
        stats: &mut QueryStats,
    ) -> Result<ResultSet, QueryError> {
        let left = self.execute_plan(&join.left, db, stats).await?;
        let right = self.execute_plan(&join.right, db, stats).await?;

        // Combine column names
        let mut columns = left.columns.clone();
        columns.extend(right.columns.clone());

        let mut rows = Vec::new();

        match join.join_type {
            PlanJoinType::Cross => {
                // Cross join - cartesian product
                for left_row in &left.rows {
                    for right_row in &right.rows {
                        let mut values = left_row.values.clone();
                        values.extend(right_row.values.clone());
                        rows.push(Row::new(values));
                    }
                }
            }
            PlanJoinType::Inner => {
                // Inner join - only matching rows
                for left_row in &left.rows {
                    for right_row in &right.rows {
                        let mut values = left_row.values.clone();
                        values.extend(right_row.values.clone());
                        let combined = Row::new(values);

                        if let Some(ref condition) = join.condition {
                            let context = RowContext::new(&columns, &combined);
                            if self.evaluate_predicate(condition, &context)?.is_truthy() {
                                rows.push(combined);
                            }
                        } else {
                            rows.push(combined);
                        }
                    }
                }
            }
            PlanJoinType::Left => {
                // Left join - all left rows, matching right rows or nulls
                for left_row in &left.rows {
                    let mut matched = false;
                    for right_row in &right.rows {
                        let mut values = left_row.values.clone();
                        values.extend(right_row.values.clone());
                        let combined = Row::new(values);

                        if let Some(ref condition) = join.condition {
                            let context = RowContext::new(&columns, &combined);
                            if self.evaluate_predicate(condition, &context)?.is_truthy() {
                                rows.push(combined);
                                matched = true;
                            }
                        } else {
                            rows.push(combined);
                            matched = true;
                        }
                    }

                    if !matched {
                        let mut values = left_row.values.clone();
                        values.extend(vec![Value::Null; right.columns.len()]);
                        rows.push(Row::new(values));
                    }
                }
            }
            _ => {
                // For now, treat other join types as inner joins
                for left_row in &left.rows {
                    for right_row in &right.rows {
                        let mut values = left_row.values.clone();
                        values.extend(right_row.values.clone());
                        let combined = Row::new(values);

                        if let Some(ref condition) = join.condition {
                            let context = RowContext::new(&columns, &combined);
                            if self.evaluate_predicate(condition, &context)?.is_truthy() {
                                rows.push(combined);
                            }
                        } else {
                            rows.push(combined);
                        }
                    }
                }
            }
        }

        stats.rows_returned = rows.len() as u64;
        Ok(ResultSet::new(columns, rows))
    }

    async fn execute_aggregate(
        &self,
        agg: &AggregationPlan,
        db: u8,
        stats: &mut QueryStats,
    ) -> Result<ResultSet, QueryError> {
        let input = self.execute_plan(&agg.input, db, stats).await?;

        // Group rows by group-by keys
        let mut groups: HashMap<Vec<String>, Vec<&Row>> = HashMap::new();

        for row in &input.rows {
            let context = RowContext::new(&input.columns, row);
            let key: Vec<String> = agg
                .group_by
                .iter()
                .map(|expr| {
                    self.evaluate_expr(expr, &context)
                        .map(|v| v.to_string())
                        .unwrap_or_default()
                })
                .collect();

            groups.entry(key).or_default().push(row);
        }

        // If no groups and no group by, treat all rows as one group
        if groups.is_empty() && agg.group_by.is_empty() {
            groups.insert(vec![], input.rows.iter().collect());
        }

        // Build result columns
        let mut columns: Vec<String> = agg
            .group_by
            .iter()
            .enumerate()
            .map(|(i, _)| format!("group_{}", i))
            .collect();

        for a in &agg.aggregates {
            columns.push(a.alias.clone().unwrap_or_else(|| a.function.clone()));
        }

        // Compute aggregates for each group
        let mut rows = Vec::new();

        for (key, group_rows) in groups {
            let mut values: Vec<Value> = key.into_iter().map(Value::String).collect();

            for aggregate_plan in &agg.aggregates {
                let mut agg_func = self
                    .functions
                    .get_aggregate(&aggregate_plan.function)
                    .ok_or_else(|| {
                        QueryError::UnknownColumn(format!(
                            "Unknown aggregate: {}",
                            aggregate_plan.function
                        ))
                    })?;

                for row in &group_rows {
                    let context = RowContext::new(&input.columns, row);
                    let value = if let Some(ref arg) = aggregate_plan.arg {
                        self.evaluate_expr(arg, &context)?
                    } else {
                        Value::Int(1) // For COUNT(*)
                    };
                    agg_func.update(&value);
                }

                values.push(agg_func.finalize());
            }

            rows.push(Row::new(values));
        }

        stats.rows_returned = rows.len() as u64;
        Ok(ResultSet::new(columns, rows))
    }

    async fn execute_sort(
        &self,
        sort: &SortPlan,
        db: u8,
        stats: &mut QueryStats,
    ) -> Result<ResultSet, QueryError> {
        let mut result = self.execute_plan(&sort.input, db, stats).await?;

        let columns = result.columns.clone();

        result.rows.sort_by(|a, b| {
            for key in &sort.keys {
                let ctx_a = RowContext::new(&columns, a);
                let ctx_b = RowContext::new(&columns, b);

                let val_a = self.evaluate_expr(&key.expr, &ctx_a).unwrap_or(Value::Null);
                let val_b = self.evaluate_expr(&key.expr, &ctx_b).unwrap_or(Value::Null);

                let cmp = val_a
                    .partial_cmp(&val_b)
                    .unwrap_or(std::cmp::Ordering::Equal);

                let cmp = if key.descending { cmp.reverse() } else { cmp };

                if cmp != std::cmp::Ordering::Equal {
                    return cmp;
                }
            }
            std::cmp::Ordering::Equal
        });

        Ok(result)
    }

    async fn execute_limit(
        &self,
        limit: &LimitPlan,
        db: u8,
        stats: &mut QueryStats,
    ) -> Result<ResultSet, QueryError> {
        let mut result = self.execute_plan(&limit.input, db, stats).await?;

        let offset = limit.offset as usize;
        let count = limit.limit.map(|l| l as usize).unwrap_or(usize::MAX);

        result.rows = result.rows.into_iter().skip(offset).take(count).collect();

        stats.rows_returned = result.rows.len() as u64;
        Ok(result)
    }

    async fn execute_distinct(
        &self,
        input: &QueryPlan,
        db: u8,
        stats: &mut QueryStats,
    ) -> Result<ResultSet, QueryError> {
        let mut result = self.execute_plan(input, db, stats).await?;

        let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
        result.rows.retain(|row| {
            let key = row
                .values
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join("|");
            seen.insert(key)
        });

        stats.rows_returned = result.rows.len() as u64;
        Ok(result)
    }

    fn execute_explain(&self, plan: &QueryPlan) -> Result<ResultSet, QueryError> {
        let planner = crate::query::planner::QueryPlanner::new(self.config.clone());
        let explain_text = planner.explain(plan);

        let rows: Vec<Row> = explain_text
            .lines()
            .map(|line| Row::new(vec![Value::String(line.to_string())]))
            .collect();

        Ok(ResultSet::new(vec!["QUERY PLAN".to_string()], rows))
    }

    async fn execute_insert(
        &self,
        insert: &InsertPlan,
        db: u8,
        stats: &mut QueryStats,
    ) -> Result<ResultSet, QueryError> {
        let mut count = 0;

        for row_values in &insert.values {
            let key = format!("{}:{}", insert.target, count);

            let value: serde_json::Value = if insert.columns.is_empty() {
                // Simple value
                if let Some(expr) = row_values.first() {
                    let v = self.evaluate_expr(expr, &RowContext::empty())?;
                    self.value_to_json(&v)
                } else {
                    serde_json::Value::Null
                }
            } else {
                // Object with named columns
                let mut obj = serde_json::Map::new();
                for (i, col) in insert.columns.iter().enumerate() {
                    if let Some(expr) = row_values.get(i) {
                        let v = self.evaluate_expr(expr, &RowContext::empty())?;
                        obj.insert(col.clone(), self.value_to_json(&v));
                    }
                }
                serde_json::Value::Object(obj)
            };

            let value_str = serde_json::to_string(&value)
                .map_err(|e| QueryError::Execution(format!("Failed to serialize value: {}", e)))?;

            let storage_value = StorageValue::String(Bytes::from(value_str));
            self.store.set(db, Bytes::from(key), storage_value);
            count += 1;
        }

        stats.rows_returned = count as u64;
        Ok(ResultSet::new(
            vec!["rows_affected".to_string()],
            vec![Row::new(vec![Value::Int(count as i64)])],
        ))
    }

    async fn execute_update(
        &self,
        update: &UpdatePlan,
        db: u8,
        stats: &mut QueryStats,
    ) -> Result<ResultSet, QueryError> {
        let keys = self.scan_keys(&update.target, db, stats)?;
        let mut count = 0;

        for key in keys {
            let key_bytes = Bytes::from(key.clone());

            // Check filter if present
            if let Some(ref filter) = update.filter {
                if let Some(storage_value) = self.store.get(db, &key_bytes) {
                    let value_str = self.storage_value_to_string(&storage_value);
                    let row = Row::new(vec![Value::String(key.clone()), Value::String(value_str)]);
                    let columns = vec!["_key".to_string(), "_value".to_string()];
                    let context = RowContext::new(&columns, &row);

                    if !self.evaluate_predicate(filter, &context)?.is_truthy() {
                        continue;
                    }
                }
            }

            // Apply updates
            let mut obj = if let Some(storage_value) = self.store.get(db, &key_bytes) {
                let value_str = self.storage_value_to_string(&storage_value);
                serde_json::from_str(&value_str)
                    .unwrap_or(serde_json::Value::Object(serde_json::Map::new()))
            } else {
                serde_json::Value::Object(serde_json::Map::new())
            };

            if let serde_json::Value::Object(ref mut map) = obj {
                for (col, expr) in &update.assignments {
                    let value = self.evaluate_expr(expr, &RowContext::empty())?;
                    map.insert(col.clone(), self.value_to_json(&value));
                }
            }

            let value_str = serde_json::to_string(&obj)
                .map_err(|e| QueryError::Execution(format!("Failed to serialize value: {}", e)))?;

            let storage_value = StorageValue::String(Bytes::from(value_str));
            self.store.set(db, key_bytes, storage_value);
            count += 1;
        }

        stats.rows_returned = count as u64;
        Ok(ResultSet::new(
            vec!["rows_affected".to_string()],
            vec![Row::new(vec![Value::Int(count as i64)])],
        ))
    }

    async fn execute_delete(
        &self,
        delete: &DeletePlan,
        db: u8,
        stats: &mut QueryStats,
    ) -> Result<ResultSet, QueryError> {
        let keys = self.scan_keys(&delete.target, db, stats)?;
        let mut count = 0;
        let mut keys_to_delete = Vec::new();

        for key in keys {
            let key_bytes = Bytes::from(key.clone());

            // Check filter if present
            if let Some(ref filter) = delete.filter {
                if let Some(storage_value) = self.store.get(db, &key_bytes) {
                    let value_str = self.storage_value_to_string(&storage_value);
                    let row = Row::new(vec![Value::String(key.clone()), Value::String(value_str)]);
                    let columns = vec!["_key".to_string(), "_value".to_string()];
                    let context = RowContext::new(&columns, &row);

                    if !self.evaluate_predicate(filter, &context)?.is_truthy() {
                        continue;
                    }
                }
            }

            keys_to_delete.push(key_bytes);
            count += 1;
        }

        // Delete all matching keys at once
        if !keys_to_delete.is_empty() {
            self.store.del(db, &keys_to_delete);
        }

        stats.rows_returned = count as u64;
        Ok(ResultSet::new(
            vec!["rows_affected".to_string()],
            vec![Row::new(vec![Value::Int(count as i64)])],
        ))
    }

    #[allow(clippy::only_used_in_recursion)]
    fn value_to_json(&self, value: &Value) -> serde_json::Value {
        match value {
            Value::Null => serde_json::Value::Null,
            Value::Bool(b) => serde_json::Value::Bool(*b),
            Value::Int(n) => serde_json::Value::Number(serde_json::Number::from(*n)),
            Value::Float(f) => serde_json::Number::from_f64(*f)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null),
            Value::String(s) => serde_json::Value::String(s.clone()),
            Value::Bytes(b) => serde_json::Value::String(String::from_utf8_lossy(b).to_string()),
            Value::Array(arr) => {
                serde_json::Value::Array(arr.iter().map(|v| self.value_to_json(v)).collect())
            }
            Value::Map(m) => {
                let obj: serde_json::Map<String, serde_json::Value> = m
                    .iter()
                    .map(|(k, v)| (k.clone(), self.value_to_json(v)))
                    .collect();
                serde_json::Value::Object(obj)
            }
        }
    }

    fn evaluate_predicate(
        &self,
        expr: &PlanExpr,
        context: &RowContext,
    ) -> Result<Value, QueryError> {
        self.evaluate_expr(expr, context)
    }

    fn evaluate_expr(&self, expr: &PlanExpr, context: &RowContext) -> Result<Value, QueryError> {
        Self::evaluate_expr_static(expr, context, &self.functions)
    }

    fn evaluate_expr_static(
        expr: &PlanExpr,
        context: &RowContext,
        functions: &FunctionRegistry,
    ) -> Result<Value, QueryError> {
        match expr {
            PlanExpr::Literal(v) => Ok(v.clone()),
            PlanExpr::Column { table, name } => {
                let col_name = if let Some(t) = table {
                    format!("{}.{}", t, name)
                } else {
                    name.clone()
                };

                // Try exact match first
                if let Some(idx) = context.columns.iter().position(|c| c == &col_name) {
                    return Ok(context.row.get(idx).cloned().unwrap_or(Value::Null));
                }

                // Try without table prefix
                if let Some(idx) = context
                    .columns
                    .iter()
                    .position(|c| c == name || c.ends_with(&format!(".{}", name)))
                {
                    return Ok(context.row.get(idx).cloned().unwrap_or(Value::Null));
                }

                Ok(Value::Null)
            }
            PlanExpr::BinaryOp { left, op, right } => {
                let left_val = Self::evaluate_expr_static(left, context, functions)?;
                let right_val = Self::evaluate_expr_static(right, context, functions)?;
                Self::apply_binary_op(&left_val, op, &right_val)
            }
            PlanExpr::UnaryOp { op, expr } => {
                let val = Self::evaluate_expr_static(expr, context, functions)?;
                Self::apply_unary_op(op, &val)
            }
            PlanExpr::Function { name, args, .. } => {
                let arg_values: Vec<Value> = args
                    .iter()
                    .map(|a| Self::evaluate_expr_static(a, context, functions))
                    .collect::<Result<_, _>>()?;
                functions.call_scalar(name, &arg_values)
            }
            PlanExpr::Case {
                operand,
                when_clauses,
                else_clause,
            } => {
                let operand_val = operand
                    .as_ref()
                    .map(|e| Self::evaluate_expr_static(e, context, functions))
                    .transpose()?;

                for (when_expr, then_expr) in when_clauses {
                    let when_val = Self::evaluate_expr_static(when_expr, context, functions)?;

                    let matches = if let Some(ref op) = operand_val {
                        op == &when_val
                    } else {
                        when_val.is_truthy()
                    };

                    if matches {
                        return Self::evaluate_expr_static(then_expr, context, functions);
                    }
                }

                if let Some(else_expr) = else_clause {
                    Self::evaluate_expr_static(else_expr, context, functions)
                } else {
                    Ok(Value::Null)
                }
            }
            PlanExpr::In {
                expr,
                list,
                negated,
            } => {
                let val = Self::evaluate_expr_static(expr, context, functions)?;
                let in_list = list.iter().any(|item| {
                    Self::evaluate_expr_static(item, context, functions)
                        .map(|v| v == val)
                        .unwrap_or(false)
                });
                let result = if *negated { !in_list } else { in_list };
                Ok(Value::Bool(result))
            }
            PlanExpr::InSubquery { negated, .. } => {
                // Subquery evaluation requires async execution context.
                // When reached from static eval, the subquery has not been materialized.
                // Return false (no match) as a safe default; full subquery evaluation
                // is performed by the async execute_filter path.
                Ok(Value::Bool(*negated))
            }
            PlanExpr::Exists { negated, .. } => {
                // Same as InSubquery: full evaluation is async.
                Ok(Value::Bool(*negated))
            }
            PlanExpr::Between {
                expr,
                low,
                high,
                negated,
            } => {
                let val = Self::evaluate_expr_static(expr, context, functions)?;
                let low_val = Self::evaluate_expr_static(low, context, functions)?;
                let high_val = Self::evaluate_expr_static(high, context, functions)?;

                let in_range = val >= low_val && val <= high_val;
                let result = if *negated { !in_range } else { in_range };
                Ok(Value::Bool(result))
            }
            PlanExpr::Like {
                expr,
                pattern,
                negated,
            } => {
                let val = Self::evaluate_expr_static(expr, context, functions)?;
                let pat = Self::evaluate_expr_static(pattern, context, functions)?;

                let matches = if let (Value::String(s), Value::String(p)) = (&val, &pat) {
                    Self::like_match(s, p)
                } else {
                    false
                };

                let result = if *negated { !matches } else { matches };
                Ok(Value::Bool(result))
            }
            PlanExpr::IsNull { expr, negated } => {
                let val = Self::evaluate_expr_static(expr, context, functions)?;
                let is_null = val.is_null();
                let result = if *negated { !is_null } else { is_null };
                Ok(Value::Bool(result))
            }
            PlanExpr::Cast { expr, data_type } => {
                let val = Self::evaluate_expr_static(expr, context, functions)?;
                Self::cast_value(&val, data_type)
            }
            PlanExpr::Parameter(_) => {
                // Parameters should have been substituted by now
                Ok(Value::Null)
            }
        }
    }

    fn apply_binary_op(
        left: &Value,
        op: &PlanBinaryOp,
        right: &Value,
    ) -> Result<Value, QueryError> {
        match op {
            PlanBinaryOp::Add => match (left, right) {
                (Value::Int(a), Value::Int(b)) => Ok(Value::Int(a + b)),
                (Value::Float(a), Value::Float(b)) => Ok(Value::Float(a + b)),
                (Value::Int(a), Value::Float(b)) => Ok(Value::Float(*a as f64 + b)),
                (Value::Float(a), Value::Int(b)) => Ok(Value::Float(a + *b as f64)),
                _ => Ok(Value::Null),
            },
            PlanBinaryOp::Subtract => match (left, right) {
                (Value::Int(a), Value::Int(b)) => Ok(Value::Int(a - b)),
                (Value::Float(a), Value::Float(b)) => Ok(Value::Float(a - b)),
                (Value::Int(a), Value::Float(b)) => Ok(Value::Float(*a as f64 - b)),
                (Value::Float(a), Value::Int(b)) => Ok(Value::Float(a - *b as f64)),
                _ => Ok(Value::Null),
            },
            PlanBinaryOp::Multiply => match (left, right) {
                (Value::Int(a), Value::Int(b)) => Ok(Value::Int(a * b)),
                (Value::Float(a), Value::Float(b)) => Ok(Value::Float(a * b)),
                (Value::Int(a), Value::Float(b)) => Ok(Value::Float(*a as f64 * b)),
                (Value::Float(a), Value::Int(b)) => Ok(Value::Float(a * *b as f64)),
                _ => Ok(Value::Null),
            },
            PlanBinaryOp::Divide => match (left, right) {
                (Value::Int(a), Value::Int(b)) if *b != 0 => Ok(Value::Int(a / b)),
                (Value::Float(a), Value::Float(b)) if *b != 0.0 => Ok(Value::Float(a / b)),
                (Value::Int(a), Value::Float(b)) if *b != 0.0 => Ok(Value::Float(*a as f64 / b)),
                (Value::Float(a), Value::Int(b)) if *b != 0 => Ok(Value::Float(a / *b as f64)),
                _ => Ok(Value::Null),
            },
            PlanBinaryOp::Modulo => match (left, right) {
                (Value::Int(a), Value::Int(b)) if *b != 0 => Ok(Value::Int(a % b)),
                _ => Ok(Value::Null),
            },
            PlanBinaryOp::Equal => Ok(Value::Bool(left == right)),
            PlanBinaryOp::NotEqual => Ok(Value::Bool(left != right)),
            PlanBinaryOp::LessThan => Ok(Value::Bool(left < right)),
            PlanBinaryOp::LessThanOrEqual => Ok(Value::Bool(left <= right)),
            PlanBinaryOp::GreaterThan => Ok(Value::Bool(left > right)),
            PlanBinaryOp::GreaterThanOrEqual => Ok(Value::Bool(left >= right)),
            PlanBinaryOp::And => Ok(Value::Bool(left.is_truthy() && right.is_truthy())),
            PlanBinaryOp::Or => Ok(Value::Bool(left.is_truthy() || right.is_truthy())),
            PlanBinaryOp::Concat => {
                let s = format!("{}{}", left, right);
                Ok(Value::String(s))
            }
        }
    }

    fn apply_unary_op(op: &PlanUnaryOp, val: &Value) -> Result<Value, QueryError> {
        match op {
            PlanUnaryOp::Not => Ok(Value::Bool(!val.is_truthy())),
            PlanUnaryOp::Minus => match val {
                Value::Int(n) => Ok(Value::Int(-n)),
                Value::Float(f) => Ok(Value::Float(-f)),
                _ => Ok(Value::Null),
            },
            PlanUnaryOp::Plus => Ok(val.clone()),
        }
    }

    fn like_match(text: &str, pattern: &str) -> bool {
        // Convert SQL LIKE pattern to simple matching
        // % matches any sequence, _ matches single character
        let regex_pattern = pattern.replace('%', ".*").replace('_', ".");

        // Simple glob-style match for now
        let pattern_chars: Vec<char> = regex_pattern.chars().collect();
        let text_chars: Vec<char> = text.chars().collect();
        Self::match_helper(&pattern_chars, &text_chars)
    }

    fn match_helper(pattern: &[char], text: &[char]) -> bool {
        match (pattern.first(), text.first()) {
            (None, None) => true,
            (Some('.'), _) if pattern.get(1) == Some(&'*') => {
                Self::match_helper(&pattern[2..], text)
                    || (!text.is_empty() && Self::match_helper(pattern, &text[1..]))
            }
            (Some('.'), Some(_)) => Self::match_helper(&pattern[1..], &text[1..]),
            (Some(p), Some(t)) if *p == *t => Self::match_helper(&pattern[1..], &text[1..]),
            (Some(_), _) => false,
            (None, Some(_)) => false,
        }
    }

    fn cast_value(value: &Value, data_type: &str) -> Result<Value, QueryError> {
        match data_type.to_uppercase().as_str() {
            "INTEGER" | "INT" | "BIGINT" => match value {
                Value::Int(n) => Ok(Value::Int(*n)),
                Value::Float(f) => Ok(Value::Int(*f as i64)),
                Value::String(s) => s
                    .parse::<i64>()
                    .map(Value::Int)
                    .map_err(|_| QueryError::TypeError(format!("Cannot cast '{}' to INTEGER", s))),
                Value::Bool(b) => Ok(Value::Int(if *b { 1 } else { 0 })),
                _ => Ok(Value::Null),
            },
            "FLOAT" | "DOUBLE" | "REAL" => match value {
                Value::Float(f) => Ok(Value::Float(*f)),
                Value::Int(n) => Ok(Value::Float(*n as f64)),
                Value::String(s) => s
                    .parse::<f64>()
                    .map(Value::Float)
                    .map_err(|_| QueryError::TypeError(format!("Cannot cast '{}' to FLOAT", s))),
                _ => Ok(Value::Null),
            },
            "STRING" | "VARCHAR" | "TEXT" => Ok(Value::String(value.to_string())),
            "BOOLEAN" | "BOOL" => Ok(Value::Bool(value.is_truthy())),
            _ => Err(QueryError::TypeError(format!(
                "Unknown type: {}",
                data_type
            ))),
        }
    }

    fn expr_to_name(&self, expr: &PlanExpr) -> String {
        match expr {
            PlanExpr::Column { name, .. } => name.clone(),
            PlanExpr::Function { name, .. } => name.clone(),
            PlanExpr::Literal(v) => v.to_string(),
            _ => "expr".to_string(),
        }
    }
}

/// Row context for expression evaluation
pub struct RowContext<'a> {
    columns: &'a [String],
    row: &'a Row,
}

impl<'a> RowContext<'a> {
    /// Creates a new row context with the given column names and row data.
    pub fn new(columns: &'a [String], row: &'a Row) -> Self {
        Self { columns, row }
    }

    /// Creates an empty row context with no columns or data.
    pub fn empty() -> Self {
        static EMPTY_COLS: Vec<String> = Vec::new();
        static EMPTY_ROW: std::sync::LazyLock<Row> =
            std::sync::LazyLock::new(|| Row::new(Vec::new()));
        Self {
            columns: &EMPTY_COLS,
            row: &EMPTY_ROW,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_binary_ops() {
        assert_eq!(
            QueryExecutor::apply_binary_op(&Value::Int(5), &PlanBinaryOp::Add, &Value::Int(3))
                .unwrap(),
            Value::Int(8)
        );

        assert_eq!(
            QueryExecutor::apply_binary_op(&Value::Int(5), &PlanBinaryOp::Equal, &Value::Int(5))
                .unwrap(),
            Value::Bool(true)
        );

        assert_eq!(
            QueryExecutor::apply_binary_op(
                &Value::String("a".to_string()),
                &PlanBinaryOp::Concat,
                &Value::String("b".to_string())
            )
            .unwrap(),
            Value::String("ab".to_string())
        );
    }

    #[test]
    fn test_like_match() {
        assert!(QueryExecutor::like_match("hello", "hello"));
        assert!(QueryExecutor::like_match("hello", "%"));
        assert!(QueryExecutor::like_match("hello", "h%"));
        assert!(QueryExecutor::like_match("hello", "%o"));
        assert!(QueryExecutor::like_match("hello", "h%o"));
        assert!(!QueryExecutor::like_match("hello", "x%"));
    }

    #[test]
    fn test_cast_value() {
        assert_eq!(
            QueryExecutor::cast_value(&Value::String("42".to_string()), "INTEGER").unwrap(),
            Value::Int(42)
        );

        assert_eq!(
            QueryExecutor::cast_value(&Value::Int(42), "STRING").unwrap(),
            Value::String("42".to_string())
        );

        assert_eq!(
            QueryExecutor::cast_value(&Value::String("true".to_string()), "BOOLEAN").unwrap(),
            Value::Bool(true)
        );
    }
}
