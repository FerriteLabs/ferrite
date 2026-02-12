//! JSONPath evaluator — evaluates parsed paths against serde_json::Value

use serde_json::Value;

use super::ast::{JsonPath, Segment};
use super::filter::evaluate_filter;

/// Evaluate a JSONPath against a JSON value, returning all matched values
pub fn evaluate<'a>(path: &JsonPath, root: &'a Value) -> Vec<&'a Value> {
    let mut results = vec![root];

    for segment in &path.segments {
        let mut next = Vec::new();
        for current in results {
            match segment {
                Segment::Child(key) => {
                    if let Some(val) = child(current, key) {
                        next.push(val);
                    }
                }
                Segment::Index(idx) => {
                    if let Some(val) = index(current, *idx) {
                        next.push(val);
                    }
                }
                Segment::Slice { start, end, step } => {
                    next.extend(slice(current, *start, *end, *step));
                }
                Segment::Wildcard => {
                    next.extend(wildcard(current));
                }
                Segment::RecursiveDescent(key) => {
                    next.extend(recursive_descent(current, key));
                }
                Segment::RecursiveWildcard => {
                    next.extend(recursive_wildcard(current));
                }
                Segment::Filter(expr) => {
                    if let Value::Array(arr) = current {
                        for item in arr {
                            if evaluate_filter(expr, item) {
                                next.push(item);
                            }
                        }
                    }
                }
            }
        }
        results = next;
    }

    results
}

/// Evaluate a JSONPath and return cloned values
pub fn evaluate_owned(path: &JsonPath, root: &Value) -> Vec<Value> {
    evaluate(path, root).into_iter().cloned().collect()
}

fn child<'a>(value: &'a Value, key: &str) -> Option<&'a Value> {
    match value {
        Value::Object(map) => map.get(key),
        _ => None,
    }
}

fn index(value: &Value, idx: i64) -> Option<&Value> {
    match value {
        Value::Array(arr) => {
            let resolved = if idx < 0 {
                arr.len().checked_sub((-idx) as usize)?
            } else {
                idx as usize
            };
            arr.get(resolved)
        }
        _ => None,
    }
}

fn slice(
    value: &Value,
    start: Option<i64>,
    end: Option<i64>,
    step: Option<i64>,
) -> Vec<&Value> {
    let arr = match value {
        Value::Array(arr) => arr,
        _ => return vec![],
    };

    let len = arr.len() as i64;
    let step = step.unwrap_or(1);
    if step == 0 {
        return vec![];
    }

    let resolve = |idx: i64| -> usize {
        let resolved = if idx < 0 { (len + idx).max(0) } else { idx.min(len) };
        resolved as usize
    };

    if step > 0 {
        let s = resolve(start.unwrap_or(0));
        let e = resolve(end.unwrap_or(len));
        let mut results = Vec::new();
        let mut i = s;
        while i < e && i < arr.len() {
            results.push(&arr[i]);
            i += step as usize;
        }
        results
    } else {
        let s = resolve(start.unwrap_or(len - 1));
        let e = end.map(resolve).unwrap_or(0);
        let mut results = Vec::new();
        let mut i = s as i64;
        let e_i = if end.is_some() { e as i64 } else { -1 };
        while i > e_i && i >= 0 && (i as usize) < arr.len() {
            results.push(&arr[i as usize]);
            i += step;
        }
        results
    }
}

fn wildcard(value: &Value) -> Vec<&Value> {
    match value {
        Value::Object(map) => map.values().collect(),
        Value::Array(arr) => arr.iter().collect(),
        _ => vec![],
    }
}

fn recursive_descent<'a>(value: &'a Value, key: &str) -> Vec<&'a Value> {
    let mut results = Vec::new();
    collect_recursive(value, key, &mut results);
    results
}

fn collect_recursive<'a>(value: &'a Value, key: &str, results: &mut Vec<&'a Value>) {
    match value {
        Value::Object(map) => {
            if let Some(val) = map.get(key) {
                results.push(val);
            }
            for v in map.values() {
                collect_recursive(v, key, results);
            }
        }
        Value::Array(arr) => {
            for item in arr {
                collect_recursive(item, key, results);
            }
        }
        _ => {}
    }
}

fn recursive_wildcard(value: &Value) -> Vec<&Value> {
    let mut results = Vec::new();
    collect_all_recursive(value, &mut results);
    results
}

fn collect_all_recursive<'a>(value: &'a Value, results: &mut Vec<&'a Value>) {
    match value {
        Value::Object(map) => {
            for v in map.values() {
                results.push(v);
                collect_all_recursive(v, results);
            }
        }
        Value::Array(arr) => {
            for item in arr {
                results.push(item);
                collect_all_recursive(item, results);
            }
        }
        _ => {}
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::document::jsonpath::parser::parse;
    use serde_json::json;

    fn store_data() -> Value {
        json!({
            "store": {
                "book": [
                    {
                        "category": "reference",
                        "author": "Nigel Rees",
                        "title": "Sayings of the Century",
                        "price": 8.95
                    },
                    {
                        "category": "fiction",
                        "author": "Evelyn Waugh",
                        "title": "Sword of Honour",
                        "price": 12.99
                    },
                    {
                        "category": "fiction",
                        "author": "Herman Melville",
                        "title": "Moby Dick",
                        "isbn": "0-553-21311-3",
                        "price": 8.99
                    },
                    {
                        "category": "fiction",
                        "author": "J. R. R. Tolkien",
                        "title": "The Lord of the Rings",
                        "isbn": "0-395-19395-8",
                        "price": 22.99
                    }
                ],
                "bicycle": {
                    "color": "red",
                    "price": 19.95
                }
            }
        })
    }

    #[test]
    fn test_child_access() {
        let data = store_data();
        let path = parse("$.store.bicycle.color").unwrap();
        let results = evaluate(&path, &data);
        assert_eq!(results, vec![&json!("red")]);
    }

    #[test]
    fn test_array_index() {
        let data = store_data();
        let path = parse("$.store.book[0].title").unwrap();
        let results = evaluate(&path, &data);
        assert_eq!(results, vec![&json!("Sayings of the Century")]);
    }

    #[test]
    fn test_negative_index() {
        let data = store_data();
        let path = parse("$.store.book[-1].title").unwrap();
        let results = evaluate(&path, &data);
        assert_eq!(results, vec![&json!("The Lord of the Rings")]);
    }

    #[test]
    fn test_array_slice() {
        let data = store_data();
        let path = parse("$.store.book[0:2]").unwrap();
        let results = evaluate(&path, &data);
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_wildcard() {
        let data = store_data();
        let path = parse("$.store.book[*].author").unwrap();
        let results = evaluate(&path, &data);
        assert_eq!(results.len(), 4);
        assert_eq!(results[0], &json!("Nigel Rees"));
    }

    #[test]
    fn test_recursive_descent() {
        let data = store_data();
        let path = parse("$.store..price").unwrap();
        let results = evaluate(&path, &data);
        assert_eq!(results.len(), 5); // 4 books + 1 bicycle
    }

    #[test]
    fn test_filter_less_than() {
        let data = store_data();
        let path = parse("$.store.book[?@.price < 10]").unwrap();
        let results = evaluate(&path, &data);
        assert_eq!(results.len(), 2); // 8.95 and 8.99
    }

    #[test]
    fn test_filter_equality() {
        let data = json!({
            "store": {
                "book": [
                    {"author": "Tolkien", "title": "LOTR"},
                    {"author": "Rowling", "title": "HP"},
                ]
            }
        });
        let path = parse("$.store.book[?@.author == \"Tolkien\"]").unwrap();
        let results = evaluate(&path, &data);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0]["title"], json!("LOTR"));
    }

    #[test]
    fn test_root_only() {
        let data = json!({"a": 1});
        let path = parse("$").unwrap();
        let results = evaluate(&path, &data);
        assert_eq!(results, vec![&data]);
    }

    #[test]
    fn test_recursive_wildcard() {
        let data = json!({"a": {"b": 1, "c": {"d": 2}}});
        let path = parse("$..*").unwrap();
        let results = evaluate(&path, &data);
        // a, b-value(1), c, d-value(2)
        assert!(results.len() >= 4);
    }

    #[test]
    fn test_empty_array_slice() {
        let data = json!({"arr": [1, 2, 3, 4, 5]});
        let path = parse("$.arr[1:4]").unwrap();
        let results = evaluate(&path, &data);
        assert_eq!(results, vec![&json!(2), &json!(3), &json!(4)]);
    }

    #[test]
    fn test_missing_field() {
        let data = json!({"a": 1});
        let path = parse("$.b").unwrap();
        let results = evaluate(&path, &data);
        assert!(results.is_empty());
    }
}
