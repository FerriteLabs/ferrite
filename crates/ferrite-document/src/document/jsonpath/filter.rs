//! Filter expression evaluation for JSONPath

use serde_json::Value;

use super::ast::{ComparisonOp, FilterExpr, FilterOperand, FilterPath, LiteralValue};

/// Evaluate a filter expression against a JSON value (the current element `@`)
pub fn evaluate_filter(expr: &FilterExpr, current: &Value) -> bool {
    match expr {
        FilterExpr::Comparison { left, op, right } => {
            let left_val = resolve_operand(left, current);
            let right_val = resolve_operand(right, current);
            compare(&left_val, op, &right_val)
        }
        FilterExpr::And(a, b) => evaluate_filter(a, current) && evaluate_filter(b, current),
        FilterExpr::Or(a, b) => evaluate_filter(a, current) || evaluate_filter(b, current),
        FilterExpr::Not(e) => !evaluate_filter(e, current),
        FilterExpr::Exists(path) => resolve_path(path, current).is_some(),
    }
}

/// Resolve a filter operand to a JSON value
fn resolve_operand(operand: &FilterOperand, current: &Value) -> Option<Value> {
    match operand {
        FilterOperand::Path(path) => resolve_path(path, current).cloned(),
        FilterOperand::Literal(lit) => Some(literal_to_value(lit)),
    }
}

/// Resolve a filter path against the current element
fn resolve_path<'a>(path: &FilterPath, current: &'a Value) -> Option<&'a Value> {
    let mut val = current;
    for seg in &path.segments {
        match val {
            Value::Object(map) => {
                val = map.get(seg)?;
            }
            _ => return None,
        }
    }
    Some(val)
}

/// Convert a literal to a serde_json::Value
fn literal_to_value(lit: &LiteralValue) -> Value {
    match lit {
        LiteralValue::String(s) => Value::String(s.clone()),
        LiteralValue::Integer(i) => Value::Number((*i).into()),
        LiteralValue::Float(f) => serde_json::Number::from_f64(*f)
            .map(Value::Number)
            .unwrap_or(Value::Null),
        LiteralValue::Bool(b) => Value::Bool(*b),
        LiteralValue::Null => Value::Null,
    }
}

/// Compare two optional JSON values with a comparison operator
fn compare(left: &Option<Value>, op: &ComparisonOp, right: &Option<Value>) -> bool {
    match (left, right) {
        (Some(l), Some(r)) => compare_values(l, op, r),
        (None, None) => matches!(op, ComparisonOp::Eq),
        _ => matches!(op, ComparisonOp::Ne),
    }
}

/// Compare two JSON values
fn compare_values(left: &Value, op: &ComparisonOp, right: &Value) -> bool {
    match op {
        ComparisonOp::Eq => values_equal(left, right),
        ComparisonOp::Ne => !values_equal(left, right),
        ComparisonOp::Lt => values_cmp(left, right) == Some(std::cmp::Ordering::Less),
        ComparisonOp::Le => matches!(
            values_cmp(left, right),
            Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
        ),
        ComparisonOp::Gt => values_cmp(left, right) == Some(std::cmp::Ordering::Greater),
        ComparisonOp::Ge => matches!(
            values_cmp(left, right),
            Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)
        ),
    }
}

/// Check if two values are equal (with numeric coercion)
fn values_equal(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Number(n1), Value::Number(n2)) => {
            n1.as_f64()
                .zip(n2.as_f64())
                .is_some_and(|(a, b)| (a - b).abs() < f64::EPSILON)
        }
        _ => a == b,
    }
}

/// Compare two values for ordering
fn values_cmp(a: &Value, b: &Value) -> Option<std::cmp::Ordering> {
    match (a, b) {
        (Value::Number(n1), Value::Number(n2)) => {
            let f1 = n1.as_f64()?;
            let f2 = n2.as_f64()?;
            f1.partial_cmp(&f2)
        }
        (Value::String(s1), Value::String(s2)) => Some(s1.cmp(s2)),
        (Value::Bool(b1), Value::Bool(b2)) => Some(b1.cmp(b2)),
        _ => None,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn test_numeric_comparison() {
        let expr = FilterExpr::Comparison {
            left: FilterOperand::Path(FilterPath {
                segments: vec!["price".into()],
            }),
            op: ComparisonOp::Lt,
            right: FilterOperand::Literal(LiteralValue::Integer(10)),
        };

        let item = serde_json::json!({"price": 5});
        assert!(evaluate_filter(&expr, &item));

        let item2 = serde_json::json!({"price": 15});
        assert!(!evaluate_filter(&expr, &item2));
    }

    #[test]
    fn test_string_equality() {
        let expr = FilterExpr::Comparison {
            left: FilterOperand::Path(FilterPath {
                segments: vec!["author".into()],
            }),
            op: ComparisonOp::Eq,
            right: FilterOperand::Literal(LiteralValue::String("Tolkien".into())),
        };

        let item = serde_json::json!({"author": "Tolkien"});
        assert!(evaluate_filter(&expr, &item));

        let item2 = serde_json::json!({"author": "Rowling"});
        assert!(!evaluate_filter(&expr, &item2));
    }

    #[test]
    fn test_logical_and() {
        let expr = FilterExpr::And(
            Box::new(FilterExpr::Comparison {
                left: FilterOperand::Path(FilterPath {
                    segments: vec!["price".into()],
                }),
                op: ComparisonOp::Gt,
                right: FilterOperand::Literal(LiteralValue::Integer(5)),
            }),
            Box::new(FilterExpr::Comparison {
                left: FilterOperand::Path(FilterPath {
                    segments: vec!["price".into()],
                }),
                op: ComparisonOp::Lt,
                right: FilterOperand::Literal(LiteralValue::Integer(15)),
            }),
        );

        let item = serde_json::json!({"price": 10});
        assert!(evaluate_filter(&expr, &item));

        let item2 = serde_json::json!({"price": 20});
        assert!(!evaluate_filter(&expr, &item2));
    }

    #[test]
    fn test_exists() {
        let expr = FilterExpr::Exists(FilterPath {
            segments: vec!["isbn".into()],
        });

        let with_isbn = serde_json::json!({"isbn": "123"});
        assert!(evaluate_filter(&expr, &with_isbn));

        let without_isbn = serde_json::json!({"title": "Test"});
        assert!(!evaluate_filter(&expr, &without_isbn));
    }
}
