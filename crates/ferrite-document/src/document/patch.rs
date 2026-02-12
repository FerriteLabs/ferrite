//! JSON Patch (RFC 6902) and JSON Merge Patch (RFC 7396)

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::DocumentStoreError;

/// A single JSON Patch operation (RFC 6902)
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "lowercase")]
pub enum PatchOp {
    /// Add a value at a path
    Add {
        path: String,
        value: Value,
    },
    /// Remove a value at a path
    Remove {
        path: String,
    },
    /// Replace a value at a path
    Replace {
        path: String,
        value: Value,
    },
    /// Move a value from one path to another
    Move {
        from: String,
        path: String,
    },
    /// Copy a value from one path to another
    Copy {
        from: String,
        path: String,
    },
    /// Test that a value at a path matches
    Test {
        path: String,
        value: Value,
    },
}

/// Apply a JSON Patch (RFC 6902) to a document
///
/// Operations are applied atomically — if any operation fails,
/// the document is left unchanged.
pub fn apply_patch(doc: &mut Value, ops: &[PatchOp]) -> Result<(), DocumentStoreError> {
    let backup = doc.clone();

    for op in ops {
        if let Err(e) = apply_single_op(doc, op) {
            *doc = backup;
            return Err(e);
        }
    }

    Ok(())
}

fn apply_single_op(doc: &mut Value, op: &PatchOp) -> Result<(), DocumentStoreError> {
    match op {
        PatchOp::Add { path, value } => {
            pointer_add(doc, path, value.clone())
        }
        PatchOp::Remove { path } => {
            pointer_remove(doc, path)?;
            Ok(())
        }
        PatchOp::Replace { path, value } => {
            pointer_replace(doc, path, value.clone())
        }
        PatchOp::Move { from, path } => {
            let val = pointer_remove(doc, from)?;
            pointer_add(doc, path, val)
        }
        PatchOp::Copy { from, path } => {
            let val = pointer_get(doc, from)?.clone();
            pointer_add(doc, path, val)
        }
        PatchOp::Test { path, value } => {
            let actual = pointer_get(doc, path)?;
            if actual != value {
                return Err(DocumentStoreError::InvalidUpdate(format!(
                    "Test failed at '{}': expected {}, got {}",
                    path, value, actual
                )));
            }
            Ok(())
        }
    }
}

/// Apply a JSON Merge Patch (RFC 7396) to a document
pub fn apply_merge_patch(doc: &mut Value, patch: &Value) {
    if let Value::Object(patch_obj) = patch {
        if !doc.is_object() {
            *doc = Value::Object(serde_json::Map::new());
        }
        if let Value::Object(doc_obj) = doc {
            for (key, value) in patch_obj {
                if value.is_null() {
                    doc_obj.remove(key);
                } else if value.is_object() {
                    let entry = doc_obj
                        .entry(key.clone())
                        .or_insert_with(|| Value::Object(serde_json::Map::new()));
                    apply_merge_patch(entry, value);
                } else {
                    doc_obj.insert(key.clone(), value.clone());
                }
            }
        }
    } else {
        *doc = patch.clone();
    }
}

/// Parse a JSON Pointer path (RFC 6901) into segments
fn parse_pointer(path: &str) -> Result<Vec<String>, DocumentStoreError> {
    if path.is_empty() {
        return Ok(vec![]);
    }
    if !path.starts_with('/') {
        return Err(DocumentStoreError::InvalidUpdate(format!(
            "JSON Pointer must start with '/': {}",
            path
        )));
    }
    Ok(path[1..]
        .split('/')
        .map(|s| s.replace("~1", "/").replace("~0", "~"))
        .collect())
}

/// Get a value by JSON Pointer
fn pointer_get<'a>(doc: &'a Value, path: &str) -> Result<&'a Value, DocumentStoreError> {
    let segments = parse_pointer(path)?;
    let mut current = doc;

    for seg in &segments {
        current = match current {
            Value::Object(map) => map.get(seg).ok_or_else(|| {
                DocumentStoreError::InvalidUpdate(format!("Path not found: {}", path))
            })?,
            Value::Array(arr) => {
                let idx = parse_array_index(seg, arr.len())?;
                arr.get(idx).ok_or_else(|| {
                    DocumentStoreError::InvalidUpdate(format!("Index out of bounds: {}", path))
                })?
            }
            _ => {
                return Err(DocumentStoreError::InvalidUpdate(format!(
                    "Cannot traverse into non-container at: {}",
                    path
                )))
            }
        };
    }

    Ok(current)
}

/// Add a value at a JSON Pointer path
fn pointer_add(doc: &mut Value, path: &str, value: Value) -> Result<(), DocumentStoreError> {
    if path.is_empty() {
        *doc = value;
        return Ok(());
    }

    let segments = parse_pointer(path)?;
    let (parent_segs, last) = segments.split_at(segments.len() - 1);
    let last = &last[0];

    let parent = navigate_to_mut(doc, parent_segs, path)?;

    match parent {
        Value::Object(map) => {
            map.insert(last.clone(), value);
            Ok(())
        }
        Value::Array(arr) => {
            if last == "-" {
                arr.push(value);
            } else {
                let idx = parse_array_index(last, arr.len() + 1)?;
                if idx > arr.len() {
                    return Err(DocumentStoreError::InvalidUpdate(format!(
                        "Index out of bounds for add: {}",
                        path
                    )));
                }
                arr.insert(idx, value);
            }
            Ok(())
        }
        _ => Err(DocumentStoreError::InvalidUpdate(format!(
            "Cannot add to non-container at: {}",
            path
        ))),
    }
}

/// Remove a value at a JSON Pointer path and return it
fn pointer_remove(doc: &mut Value, path: &str) -> Result<Value, DocumentStoreError> {
    let segments = parse_pointer(path)?;
    if segments.is_empty() {
        return Err(DocumentStoreError::InvalidUpdate(
            "Cannot remove root document".into(),
        ));
    }

    let (parent_segs, last) = segments.split_at(segments.len() - 1);
    let last = &last[0];

    let parent = navigate_to_mut(doc, parent_segs, path)?;

    match parent {
        Value::Object(map) => map.remove(last).ok_or_else(|| {
            DocumentStoreError::InvalidUpdate(format!("Path not found for remove: {}", path))
        }),
        Value::Array(arr) => {
            let idx = parse_array_index(last, arr.len())?;
            if idx >= arr.len() {
                return Err(DocumentStoreError::InvalidUpdate(format!(
                    "Index out of bounds for remove: {}",
                    path
                )));
            }
            Ok(arr.remove(idx))
        }
        _ => Err(DocumentStoreError::InvalidUpdate(format!(
            "Cannot remove from non-container at: {}",
            path
        ))),
    }
}

/// Replace a value at a JSON Pointer path
fn pointer_replace(doc: &mut Value, path: &str, value: Value) -> Result<(), DocumentStoreError> {
    if path.is_empty() {
        *doc = value;
        return Ok(());
    }

    let segments = parse_pointer(path)?;
    let (parent_segs, last) = segments.split_at(segments.len() - 1);
    let last = &last[0];

    let parent = navigate_to_mut(doc, parent_segs, path)?;

    match parent {
        Value::Object(map) => {
            if !map.contains_key(last) {
                return Err(DocumentStoreError::InvalidUpdate(format!(
                    "Path not found for replace: {}",
                    path
                )));
            }
            map.insert(last.clone(), value);
            Ok(())
        }
        Value::Array(arr) => {
            let idx = parse_array_index(last, arr.len())?;
            if idx >= arr.len() {
                return Err(DocumentStoreError::InvalidUpdate(format!(
                    "Index out of bounds for replace: {}",
                    path
                )));
            }
            arr[idx] = value;
            Ok(())
        }
        _ => Err(DocumentStoreError::InvalidUpdate(format!(
            "Cannot replace in non-container at: {}",
            path
        ))),
    }
}

/// Navigate to a mutable reference at the given path segments
fn navigate_to_mut<'a>(
    doc: &'a mut Value,
    segments: &[String],
    full_path: &str,
) -> Result<&'a mut Value, DocumentStoreError> {
    let mut current = doc;

    for seg in segments {
        current = match current {
            Value::Object(map) => map.get_mut(seg).ok_or_else(|| {
                DocumentStoreError::InvalidUpdate(format!("Path not found: {}", full_path))
            })?,
            Value::Array(arr) => {
                let idx = parse_array_index(seg, arr.len())?;
                arr.get_mut(idx).ok_or_else(|| {
                    DocumentStoreError::InvalidUpdate(format!(
                        "Index out of bounds: {}",
                        full_path
                    ))
                })?
            }
            _ => {
                return Err(DocumentStoreError::InvalidUpdate(format!(
                    "Cannot traverse non-container at: {}",
                    full_path
                )))
            }
        };
    }

    Ok(current)
}

fn parse_array_index(seg: &str, _len: usize) -> Result<usize, DocumentStoreError> {
    seg.parse::<usize>()
        .map_err(|_| DocumentStoreError::InvalidUpdate(format!("Invalid array index: {}", seg)))
}

/// Parse a JSON Patch document from a JSON array
pub fn parse_patch(value: &Value) -> Result<Vec<PatchOp>, DocumentStoreError> {
    let arr = value
        .as_array()
        .ok_or_else(|| DocumentStoreError::InvalidUpdate("JSON Patch must be an array".into()))?;

    arr.iter()
        .map(|op| {
            serde_json::from_value(op.clone()).map_err(|e| {
                DocumentStoreError::InvalidUpdate(format!("Invalid patch operation: {}", e))
            })
        })
        .collect()
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_add_object_member() {
        let mut doc = json!({"foo": "bar"});
        let ops = vec![PatchOp::Add {
            path: "/baz".into(),
            value: json!("qux"),
        }];
        apply_patch(&mut doc, &ops).unwrap();
        assert_eq!(doc, json!({"foo": "bar", "baz": "qux"}));
    }

    #[test]
    fn test_add_array_element() {
        let mut doc = json!({"foo": [1, 2, 3]});
        let ops = vec![PatchOp::Add {
            path: "/foo/1".into(),
            value: json!(42),
        }];
        apply_patch(&mut doc, &ops).unwrap();
        assert_eq!(doc, json!({"foo": [1, 42, 2, 3]}));
    }

    #[test]
    fn test_add_array_end() {
        let mut doc = json!({"foo": [1, 2]});
        let ops = vec![PatchOp::Add {
            path: "/foo/-".into(),
            value: json!(3),
        }];
        apply_patch(&mut doc, &ops).unwrap();
        assert_eq!(doc, json!({"foo": [1, 2, 3]}));
    }

    #[test]
    fn test_remove() {
        let mut doc = json!({"foo": "bar", "baz": "qux"});
        let ops = vec![PatchOp::Remove {
            path: "/baz".into(),
        }];
        apply_patch(&mut doc, &ops).unwrap();
        assert_eq!(doc, json!({"foo": "bar"}));
    }

    #[test]
    fn test_replace() {
        let mut doc = json!({"foo": "bar", "baz": "qux"});
        let ops = vec![PatchOp::Replace {
            path: "/baz".into(),
            value: json!("boo"),
        }];
        apply_patch(&mut doc, &ops).unwrap();
        assert_eq!(doc, json!({"foo": "bar", "baz": "boo"}));
    }

    #[test]
    fn test_move_value() {
        let mut doc = json!({"foo": {"bar": "baz"}, "qux": {"corge": "grault"}});
        let ops = vec![PatchOp::Move {
            from: "/foo/bar".into(),
            path: "/qux/thud".into(),
        }];
        apply_patch(&mut doc, &ops).unwrap();
        assert_eq!(
            doc,
            json!({"foo": {}, "qux": {"corge": "grault", "thud": "baz"}})
        );
    }

    #[test]
    fn test_copy_value() {
        let mut doc = json!({"foo": {"bar": "baz"}});
        let ops = vec![PatchOp::Copy {
            from: "/foo/bar".into(),
            path: "/foo/qux".into(),
        }];
        apply_patch(&mut doc, &ops).unwrap();
        assert_eq!(doc, json!({"foo": {"bar": "baz", "qux": "baz"}}));
    }

    #[test]
    fn test_test_success() {
        let mut doc = json!({"foo": "bar"});
        let ops = vec![PatchOp::Test {
            path: "/foo".into(),
            value: json!("bar"),
        }];
        apply_patch(&mut doc, &ops).unwrap();
    }

    #[test]
    fn test_test_failure() {
        let mut doc = json!({"foo": "bar"});
        let ops = vec![PatchOp::Test {
            path: "/foo".into(),
            value: json!("baz"),
        }];
        assert!(apply_patch(&mut doc, &ops).is_err());
    }

    #[test]
    fn test_atomic_rollback() {
        let mut doc = json!({"foo": "bar"});
        let original = doc.clone();
        let ops = vec![
            PatchOp::Add {
                path: "/baz".into(),
                value: json!("qux"),
            },
            PatchOp::Remove {
                path: "/nonexistent".into(),
            },
        ];
        assert!(apply_patch(&mut doc, &ops).is_err());
        assert_eq!(doc, original);
    }

    #[test]
    fn test_nested_add() {
        let mut doc = json!({"a": {"b": {"c": 1}}});
        let ops = vec![PatchOp::Add {
            path: "/a/b/d".into(),
            value: json!(2),
        }];
        apply_patch(&mut doc, &ops).unwrap();
        assert_eq!(doc["a"]["b"]["d"], json!(2));
    }

    #[test]
    fn test_replace_root() {
        let mut doc = json!({"foo": "bar"});
        let ops = vec![PatchOp::Replace {
            path: "".into(),
            value: json!({"completely": "new"}),
        }];
        apply_patch(&mut doc, &ops).unwrap();
        assert_eq!(doc, json!({"completely": "new"}));
    }

    // Merge Patch tests
    #[test]
    fn test_merge_patch_basic() {
        let mut doc = json!({"a": "b", "c": {"d": "e", "f": "g"}});
        let patch = json!({"a": "z", "c": {"f": null}});
        apply_merge_patch(&mut doc, &patch);
        assert_eq!(doc, json!({"a": "z", "c": {"d": "e"}}));
    }

    #[test]
    fn test_merge_patch_add_field() {
        let mut doc = json!({"a": 1});
        let patch = json!({"b": 2});
        apply_merge_patch(&mut doc, &patch);
        assert_eq!(doc, json!({"a": 1, "b": 2}));
    }

    #[test]
    fn test_merge_patch_nested() {
        let mut doc = json!({"a": {"b": 1}});
        let patch = json!({"a": {"c": 2}});
        apply_merge_patch(&mut doc, &patch);
        assert_eq!(doc, json!({"a": {"b": 1, "c": 2}}));
    }

    #[test]
    fn test_merge_patch_replace_non_object() {
        let mut doc = json!({"a": "string"});
        let patch = json!({"a": {"nested": true}});
        apply_merge_patch(&mut doc, &patch);
        assert_eq!(doc, json!({"a": {"nested": true}}));
    }

    #[test]
    fn test_parse_patch() {
        let patch_json = json!([
            {"op": "add", "path": "/foo", "value": "bar"},
            {"op": "remove", "path": "/baz"}
        ]);
        let ops = parse_patch(&patch_json).unwrap();
        assert_eq!(ops.len(), 2);
    }

    #[test]
    fn test_escaped_pointer() {
        let mut doc = json!({});
        let ops = vec![PatchOp::Add {
            path: "/a~1b".into(),
            value: json!(1),
        }];
        apply_patch(&mut doc, &ops).unwrap();
        assert_eq!(doc["a/b"], json!(1));
    }
}
