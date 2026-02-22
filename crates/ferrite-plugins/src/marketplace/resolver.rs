//! Dependency resolution for plugin installation.

use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

use super::PluginDependency;

/// Result of dependency resolution.
#[derive(Debug, Clone)]
pub struct ResolutionResult {
    /// Plugins to install in order.
    pub install_order: Vec<String>,
    /// Any conflicts detected.
    pub conflicts: Vec<DependencyConflict>,
}

/// A dependency conflict.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DependencyConflict {
    pub plugin: String,
    pub required_by: String,
    pub version_required: String,
    pub version_available: Option<String>,
}

/// Resolves dependencies for a set of plugins using topological sort.
#[allow(clippy::implicit_hasher)]
pub fn resolve_dependencies(
    target: &str,
    dependencies: &HashMap<String, Vec<PluginDependency>>,
    available_versions: &HashMap<String, Vec<String>>,
) -> ResolutionResult {
    let mut install_order = Vec::new();
    let mut visited = HashSet::new();
    let mut conflicts = Vec::new();

    resolve_recursive(
        target,
        dependencies,
        available_versions,
        &mut visited,
        &mut install_order,
        &mut conflicts,
    );

    ResolutionResult {
        install_order,
        conflicts,
    }
}

fn resolve_recursive(
    plugin: &str,
    dependencies: &HashMap<String, Vec<PluginDependency>>,
    available_versions: &HashMap<String, Vec<String>>,
    visited: &mut HashSet<String>,
    install_order: &mut Vec<String>,
    conflicts: &mut Vec<DependencyConflict>,
) {
    if visited.contains(plugin) {
        return;
    }
    visited.insert(plugin.to_string());

    if let Some(deps) = dependencies.get(plugin) {
        for dep in deps {
            if dep.optional {
                continue;
            }
            // Check if dependency is available
            match available_versions.get(&dep.name) {
                Some(versions) if !versions.is_empty() => {
                    resolve_recursive(
                        &dep.name,
                        dependencies,
                        available_versions,
                        visited,
                        install_order,
                        conflicts,
                    );
                }
                _ => {
                    conflicts.push(DependencyConflict {
                        plugin: dep.name.clone(),
                        required_by: plugin.to_string(),
                        version_required: dep.version_req.clone(),
                        version_available: None,
                    });
                }
            }
        }
    }

    install_order.push(plugin.to_string());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_no_dependencies() {
        let deps = HashMap::new();
        let available = HashMap::new();
        let result = resolve_dependencies("plugin-a", &deps, &available);
        assert_eq!(result.install_order, vec!["plugin-a"]);
        assert!(result.conflicts.is_empty());
    }

    #[test]
    fn test_simple_dependency() {
        let mut deps = HashMap::new();
        deps.insert(
            "plugin-a".to_string(),
            vec![PluginDependency {
                name: "plugin-b".to_string(),
                version_req: ">=1.0.0".to_string(),
                optional: false,
            }],
        );

        let mut available = HashMap::new();
        available.insert("plugin-b".to_string(), vec!["1.0.0".to_string()]);

        let result = resolve_dependencies("plugin-a", &deps, &available);
        assert_eq!(result.install_order, vec!["plugin-b", "plugin-a"]);
        assert!(result.conflicts.is_empty());
    }

    #[test]
    fn test_missing_dependency() {
        let mut deps = HashMap::new();
        deps.insert(
            "plugin-a".to_string(),
            vec![PluginDependency {
                name: "missing-plugin".to_string(),
                version_req: ">=1.0.0".to_string(),
                optional: false,
            }],
        );

        let available = HashMap::new();
        let result = resolve_dependencies("plugin-a", &deps, &available);
        assert_eq!(result.conflicts.len(), 1);
        assert_eq!(result.conflicts[0].plugin, "missing-plugin");
    }

    #[test]
    fn test_optional_dependency_skipped() {
        let mut deps = HashMap::new();
        deps.insert(
            "plugin-a".to_string(),
            vec![PluginDependency {
                name: "optional-dep".to_string(),
                version_req: ">=1.0.0".to_string(),
                optional: true,
            }],
        );

        let available = HashMap::new();
        let result = resolve_dependencies("plugin-a", &deps, &available);
        assert!(result.conflicts.is_empty());
    }
}
