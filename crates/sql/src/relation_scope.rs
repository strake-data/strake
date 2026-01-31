//! Relation Scope Tracking for SQL Generation
//!
//! This module provides the `RelationScope` struct, which tracks visible relations
//! at different levels of query nesting. It allows resolving column references
//! to the appropriate table alias based on visibility rules.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion::common::{Column, Result, TableReference};
use datafusion::error::DataFusionError;

/// Normalizes a TableReference to a consistent string representation (table name only for bare/partial)
pub fn normalize_relation(rel: &TableReference) -> String {
    rel.table().to_string()
}

/// Represents a fully qualified column reference resolved to a specific relation
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QualifiedColumn {
    pub relation: Option<TableReference>,
    pub name: String,
}

/// Utility for parsing and identifying derived column names injected by the remapper.
pub struct DerivedNameParser;

impl DerivedNameParser {
    /// Parse "derived_sq_1_d_name" -> (Some("d"), "name")
    pub fn parse(col_name: &str) -> Option<(Option<TableReference>, String)> {
        if !col_name.contains("derived_sq_") {
            return None;
        }

        let after_prefix = col_name.split("derived_sq_").last()?;
        let (_, rest) = after_prefix.split_once('_')?; // Skip the number

        // Try to split qualifier_name
        let parts: Vec<&str> = rest.rsplitn(2, '_').collect();
        if parts.len() == 2 && parts[1].len() == 1 {
            Some((Some(TableReference::bare(parts[1])), parts[0].to_string()))
        } else {
            Some((None, rest.to_string()))
        }
    }

    pub fn is_derived(name: &str) -> bool {
        name.contains("derived_sq_")
    }
}

impl From<QualifiedColumn> for Column {
    fn from(val: QualifiedColumn) -> Self {
        Column::new(val.relation, val.name)
    }
}

/// Represents visible relations at a given scope level in SQL generation.
///
/// When unparsing logical plans that contain subqueries:
/// - Inner subquery creates a new child scope
/// - Parent scope relations become invisible inside child (unless correlated - handled via parent pointer)
/// - Child scope exports alias as only visible relation to parent
#[derive(Debug, Clone, Default)]
pub struct RelationScope {
    /// Relations visible at this scope level (column_name -> list of table_references)
    visible_relations: HashMap<Arc<str>, Vec<TableReference>>,

    /// Unqualified columns that exist without table qualifiers.
    unqualified_columns: HashSet<Arc<str>>,

    /// Mappings from hidden inner relations to their outer aliases (normalized string -> normalized string)
    pub alias_mappings: HashMap<String, String>,

    /// Mappings for specific columns (relation_name, col_name) -> new_col_name
    pub column_mappings: HashMap<(Option<String>, String), String>,

    /// String-based mappings for fallback resolution "qual.name" -> new_name
    pub string_mappings: HashMap<String, String>,

    /// Parent scope for lexical scoping (outer query access)
    parent: Option<Box<RelationScope>>,
}

impl RelationScope {
    /// Create a new empty root scope
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a mapping from an inner relation to an outer alias
    pub fn add_mapping(&mut self, inner: TableReference, outer: TableReference) {
        self.alias_mappings
            .insert(normalize_relation(&inner), normalize_relation(&outer));
    }

    /// Register a column mapping
    pub fn add_column_mapping(
        &mut self,
        relation: Option<TableReference>,
        old_name: String,
        new_name: String,
    ) {
        let rel_key = relation.as_ref().map(normalize_relation);
        self.column_mappings.insert((rel_key, old_name), new_name);
    }

    /// Register a string-based mapping (e.g. "d.name" -> "derived_sq_1_d_name")
    pub fn add_string_mapping(&mut self, key: String, val: String) {
        self.string_mappings.insert(key, val);
    }

    /// Create a new child scope from the current one
    pub fn new_child(&self) -> Self {
        Self {
            visible_relations: HashMap::new(),
            unqualified_columns: std::collections::HashSet::new(),
            alias_mappings: HashMap::new(),
            column_mappings: HashMap::new(),
            string_mappings: HashMap::new(),
            parent: Some(Box::new(self.clone())),
        }
    }

    /// Push a new subquery context
    pub fn push_subquery(&self, alias: String, inner_columns: Vec<Column>) -> Self {
        let mut child_scope = Self {
            visible_relations: HashMap::new(),
            unqualified_columns: std::collections::HashSet::new(),
            alias_mappings: HashMap::new(),
            column_mappings: HashMap::new(),
            string_mappings: HashMap::new(),
            parent: Some(Box::new(self.clone())),
        };

        let table_ref = TableReference::bare(alias);
        for col in inner_columns {
            child_scope
                .visible_relations
                .entry(Arc::from(col.name))
                .or_default()
                .push(table_ref.clone());
        }

        child_scope
    }

    pub fn resolve_column(&self, col: &Column) -> Result<QualifiedColumn> {
        if col.name == "department_id"
            || col.name == "budget"
            || col.name == "amount"
            || col.name == "total_amount"
        {
            tracing::trace!(
                target: "sql_gen",
                col = ?col,
                "Resolving column in scope"
            );
        }

        let name = col.name.to_string();
        let name_arc: Arc<str> = Arc::from(col.name.as_str());
        let req_rel = col.relation.clone();
        let rel_key = col.relation.as_ref().map(normalize_relation);

        // 1. Try a direct mapping lookup (this should handle most cases after ensure_aliased)
        if let Some(mapped_name) = self.column_mappings.get(&(rel_key.clone(), name.clone())) {
            return Ok(QualifiedColumn {
                relation: None, // Column is now renamed, likely flat or in subquery
                name: mapped_name.clone(),
            });
        }

        // 2. Check string mappings for fallback "qual.name"
        if let Some(ref rel) = req_rel {
            let key = format!("{}.{}", rel, col.name);
            if let Some(mapped) = self.string_mappings.get(&key) {
                return Ok(QualifiedColumn {
                    relation: None,
                    name: mapped.clone(),
                });
            }

            // Check alias mappings (e.g. "users" -> "derived_sq_1")
            let rel_str = normalize_relation(rel);
            if let Some(outer_alias) = self.alias_mappings.get(&rel_str) {
                return Ok(QualifiedColumn {
                    relation: Some(TableReference::bare(outer_alias.clone())),
                    name: name.clone(),
                });
            }
        } else {
            // Unqualified lookup in column mappings
            if let Some(mapped_name) = self.column_mappings.get(&(None, name.clone())) {
                return Ok(QualifiedColumn {
                    relation: None,
                    name: mapped_name.clone(),
                });
            }
        }

        // 3. Try to find a strict match in the CURRENT scope
        if let Some(candidates) = self.visible_relations.get(&name_arc) {
            if let Some(ref rel) = req_rel {
                let req_str = normalize_relation(rel);
                for candidate in candidates {
                    if normalize_relation(candidate) == req_str {
                        return Ok(QualifiedColumn {
                            relation: Some(candidate.clone()),
                            name: name.clone(),
                        });
                    }
                }
            } else if candidates.len() == 1 {
                return Ok(QualifiedColumn {
                    relation: Some(candidates[0].clone()),
                    name: name.clone(),
                });
            }
        }

        // 4. Check if it's a known unqualified column
        if self.unqualified_columns.contains(&name_arc) {
            return Ok(QualifiedColumn {
                relation: None,
                name: name.clone(),
            });
        }

        // 5. Check parent scopes (correlation)
        if let Some(parent) = &self.parent {
            if let Ok(resolved) = parent.resolve_column(col) {
                return Ok(resolved);
            }
        }

        // 6. Fallback to unique local candidate
        if let Some(candidates) = self.visible_relations.get(&name_arc) {
            if req_rel.is_some() && candidates.len() == 1 {
                return Ok(QualifiedColumn {
                    relation: Some(candidates[0].clone()),
                    name: name.clone(),
                });
            } else if candidates.len() > 1 {
                return Err(DataFusionError::Plan(format!(
                    "Ambiguous column reference '{}'. Match found in multiple relations: {:?}",
                    col.name, candidates
                )));
            }
        }

        let mappings_debug: Vec<_> = self.alias_mappings.keys().map(|k| k.to_string()).collect();
        let visible_debug: Vec<_> = self.visible_relations.keys().cloned().collect();

        Err(DataFusionError::Plan(format!(
            "Unresolved column '{}' in current scope. Relation: {:?}. Mappings: {:?}. Visible: {:?}",
            col.name, col.relation, mappings_debug, visible_debug
        )))
    }

    pub fn with_parent(mut self, parent: Option<Box<RelationScope>>) -> Self {
        self.parent = parent;
        self
    }

    pub fn set_parent(&mut self, parent: Option<Box<RelationScope>>) {
        self.parent = parent;
    }

    pub fn parent(&self) -> &Option<Box<RelationScope>> {
        &self.parent
    }

    pub fn visible_relations(&self) -> &HashMap<Arc<str>, Vec<TableReference>> {
        &self.visible_relations
    }

    /// Register a relation and its columns in the current scope
    pub fn register_relation(&mut self, alias: TableReference, columns: Vec<String>) {
        for col_name in columns {
            self.visible_relations
                .entry(Arc::from(col_name))
                .or_default()
                .push(alias.clone());
        }
    }

    /// Register an unqualified column in the current scope
    pub fn register_unqualified_column(&mut self, column_name: String) {
        self.unqualified_columns.insert(Arc::from(column_name));
    }

    /// Merges another scope into this one (e.g. for Joins)
    pub fn merge(&mut self, other: &RelationScope) {
        for (name, rels) in &other.visible_relations {
            let entry = self.visible_relations.entry(name.clone()).or_default();
            for rel in rels {
                if !entry.contains(rel) {
                    entry.push(rel.clone());
                }
            }
        }
        for col in &other.unqualified_columns {
            self.unqualified_columns.insert(col.clone());
        }

        // Merge mappings
        for (k, v) in &other.alias_mappings {
            self.alias_mappings.insert(k.clone(), v.clone());
        }
        for (k, v) in &other.column_mappings {
            self.column_mappings.insert(k.clone(), v.clone());
        }
        for (k, v) in &other.string_mappings {
            self.string_mappings.insert(k.clone(), v.clone());
        }

        // Merge parent if this scope doesn't have one
        if self.parent.is_none() && other.parent.is_some() {
            self.parent = other.parent.clone();
        }
    }

    /// Inherit alias_mappings and column_mappings from another scope
    /// Used to preserve mappings when creating new output scopes
    pub fn inherit_mappings_from(&mut self, parent: &RelationScope) {
        for (k, v) in &parent.alias_mappings {
            self.alias_mappings.insert(k.clone(), v.clone());
        }
        for (k, v) in &parent.column_mappings {
            self.column_mappings.insert(k.clone(), v.clone());
        }
        for (k, v) in &parent.string_mappings {
            self.string_mappings.insert(k.clone(), v.clone());
        }
    }
}
