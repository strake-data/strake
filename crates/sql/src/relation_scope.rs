//! Relation Scope Tracking for SQL Generation
//!
//! This module provides the `RelationScope` struct, which tracks visible relations
//! at different levels of query nesting. It allows resolving column references
//! to the appropriate table alias based on visibility rules.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion::common::{Column, Result, TableReference};
use datafusion::error::DataFusionError;

/// Represents a fully qualified column reference resolved to a specific relation
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QualifiedColumn {
    pub relation: Option<TableReference>,
    pub name: String,
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

    /// Mappings from hidden inner relations to their outer aliases.
    pub alias_mappings: HashMap<TableReference, TableReference>,

    /// Mappings for specific columns (relation, col_name) -> new_col_name
    pub column_mappings: HashMap<(Option<TableReference>, Arc<str>), Arc<str>>,

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
        self.alias_mappings.insert(inner, outer);
    }

    /// Register a column mapping
    pub fn add_column_mapping(
        &mut self,
        relation: Option<TableReference>,
        old_name: String,
        new_name: String,
    ) {
        self.column_mappings
            .insert((relation, Arc::from(old_name)), Arc::from(new_name));
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
        if col.name == "department_id" || col.name == "budget" || col.name == "amount" {
             tracing::trace!(
                target: "sql_gen",
                col = ?col,
                "Resolving column in scope"
            );
        }

        let name_arc: Arc<str> = Arc::from(col.name.as_str());

        let effective_name = if let Some(mapped_name) = self
            .column_mappings
            .get(&(col.relation.clone(), name_arc.clone()))
        {
            mapped_name.clone()
        } else {
            name_arc.clone()
        };

        if let Some(req_rel) = &col.relation {
            // Check alias mappings using string representation for robustness
            let req_str = req_rel.to_string();
            
            // FALLBACK: Try string lookup "qual.name"
            // This handles cases where Aggregate references col with inner qualifier
            // that isn't directly resolvable via standard mappings
            let key = format!("{}.{}", req_str, col.name);
            if let Some(mapped) = self.string_mappings.get(&key) {
                tracing::debug!(
                    target: "sql_gen::scope",
                    ?col,
                    ?key,
                    ?mapped,
                    "Resolved column via string fallback"
                );
                // Return as resolved to current alias (or just the mapped name if we don't know the alias here)
                // Ususally mapped name includes the alias prefix if done by ensure_aliased
                return Ok(QualifiedColumn {
                    relation: None, // The mapped name should be sufficient or we might need context
                    name: mapped.clone(),
                });
            }

            for (inner, outer) in &self.alias_mappings {
                if inner == req_rel || inner.to_string() == req_str {
                    return Ok(QualifiedColumn {
                        relation: Some(outer.clone()),
                        name: effective_name.to_string(),
                    });
                }
            }
        }

        if let Some(candidates) = self.visible_relations.get(&effective_name) {
            if let Some(req_rel) = &col.relation {
                for candidate in candidates {
                    // Check for exact match or canonical string match to handle Bare vs Partial variants
                    if req_rel == candidate || req_rel.to_string() == candidate.to_string() {
                        return Ok(QualifiedColumn {
                            relation: Some(candidate.clone()),
                            name: effective_name.to_string(),
                        });
                    }
                }

                // If we are here, the requested qualifier `u` is NOT in the visible candidates.
                // This happens when `u` is an alias from an inner scope that was hidden (alias leakage).
                // Or if we are in a subquery and the qualifier is the subquery alias itself.
                
                // Fallback to checking if the resolution is unique among all visible relations.
                if candidates.len() == 1 {
                    let candidate = &candidates[0];
                    tracing::debug!(
                        target: "sql_gen",
                        col_name = %col.name,
                        req_rel = ?req_rel,
                        visible_rel = ?candidate,
                        "Auto-correcting hidden qualifier to visible unique candidate"
                    );
                    return Ok(QualifiedColumn {
                        relation: Some(candidate.clone()),
                        name: effective_name.to_string(),
                    });
                } else {
                    return Err(DataFusionError::Plan(format!(
                        "Ambiguous column reference '{}'. The qualifier '{:?}' is not visible, and the column exists in multiple visible relations: {:?}",
                        col.name, req_rel, candidates
                    )));
                }
            } else {
                if candidates.len() == 1 {
                    return Ok(QualifiedColumn {
                        relation: Some(candidates[0].clone()),
                        name: effective_name.to_string(),
                    });
                } else {
                    return Err(DataFusionError::Plan(format!(
                        "Ambiguous column reference '{}'. Found in relations: {:?}",
                        col.name, candidates
                    )));
                }
            }
        }

        // 2. Check if this is a known unqualified column (like aggregate outputs or aliases)
        // This only applies if the column wasn't found in qualified form above.
        if self.unqualified_columns.contains(&effective_name) {
            return Ok(QualifiedColumn {
                relation: None,
                name: effective_name.to_string(),
            });
        }

        // 3. Check parent scopes (outer references)
        if let Some(parent) = &self.parent {
            return parent.resolve_column(col);
        }

        // 4. Not found

        let mappings_debug: Vec<_> = self.alias_mappings.keys().map(|k| k.to_string()).collect();
        let visible_debug: Vec<_> = self.visible_relations.keys().cloned().collect();

        tracing::error!(
            target: "sql_gen",
            col = ?col,
            mappings = ?mappings_debug,
            visible = ?visible_debug,
            unqualified = ?self.unqualified_columns,
            "Failed to resolve column in scope"
        );

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
