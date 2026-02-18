use crate::sql_generator::error::SqlGenError;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::Column;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct ColumnEntry {
    pub name: Arc<str>,
    pub data_type: DataType,
    /// The alias of the table/relation where this column originates.
    /// Used to resolve `t0` vs `t1` in joins.
    pub source_alias: Arc<str>,
    /// Chain of aliases/qualifiers this column has passed through.
    /// Used for disambiguation in complex joins.
    pub provenance: Vec<String>,
    /// Global unique identifier for this specific column instance.
    pub unique_id: usize,
}

#[derive(Debug, Clone)]
pub struct Scope {
    /// The alias of this scope (e.g., "t0")
    pub alias: String,
    /// Columns exposed by this scope
    pub columns: Arc<[ColumnEntry]>,
    /// Whether this scope represents a derived table / subquery
    pub is_derived: bool,
    /// Original relation names that this scope represents (e.g. "users", "orders")
    /// Used for qualified column resolution logic (fallback)
    pub qualifiers: Vec<String>,
}

pub struct GeneratorContext {
    /// Global counter for deterministic aliases (t0, t1...)
    counter: usize,
    /// Global counter for unique column IDs
    column_id_counter: usize,
    /// Stack of visible scopes, from outermost to innermost
    scope_stack: Vec<Scope>,
}

/// RAII Guard for Scope management.
/// Pops the scope when dropped, unless committed.
pub struct ScopeGuard<'a> {
    context: &'a mut GeneratorContext,
    expected_alias: String,
    committed: bool,
}

impl<'a> ScopeGuard<'a> {
    pub fn new(
        context: &'a mut GeneratorContext,
        alias: String,
        columns: Arc<[ColumnEntry]>,
        qualifiers: Vec<String>,
    ) -> Self {
        let alias_clone = alias.clone();
        context.push_scope(alias, columns, qualifiers);
        Self {
            context,
            expected_alias: alias_clone,
            committed: false,
        }
    }

    /// Prevent the scope from being popped on drop.
    /// Useful for when the scope ownership is transferred or persisted.
    #[allow(dead_code)] // May be used in future
    pub fn commit(mut self) {
        self.committed = true;
    }
}

impl<'a> Drop for ScopeGuard<'a> {
    fn drop(&mut self) {
        if !self.committed {
            if let Some(top) = self.context.current_scope() {
                if top.alias != self.expected_alias {
                    tracing::error!(
                        target: "sql_generator",
                        expected = %self.expected_alias,
                        actual = %top.alias,
                        "Scope stack corruption detected"
                    );
                    #[cfg(debug_assertions)]
                    panic!(
                        "Scope stack corruption: expected {}, got {}",
                        self.expected_alias, top.alias
                    );
                }
            }
            self.context.pop_scope();
        }
    }
}

/// Represents a state in the scope stack that can be rolled back to.
pub struct Checkpoint {
    pub(crate) stack_len: usize,
}

impl Default for GeneratorContext {
    fn default() -> Self {
        Self::new()
    }
}

impl GeneratorContext {
    pub fn new() -> Self {
        Self {
            counter: 0,
            column_id_counter: 0,
            scope_stack: Vec::new(),
        }
    }

    /// Assign next systematic alias and increment counter
    pub fn next_alias(&mut self) -> String {
        let alias = format!("rel_{}", self.counter);
        self.counter += 1;
        alias
    }

    /// Assign next unique column ID and increment counter
    pub fn next_column_id(&mut self) -> usize {
        let id = self.column_id_counter;
        self.column_id_counter += 1;
        id
    }

    /// Enter a new scope, returning a guard that will pop it when dropped.
    pub fn enter_scope(
        &mut self,
        alias: String,
        columns: Arc<[ColumnEntry]>,
        qualifiers: Vec<String>,
    ) -> ScopeGuard<'_> {
        ScopeGuard::new(self, alias, columns, qualifiers)
    }

    /// Legacy push method - usage should be migrated to enter_scope where possible for safety
    pub(crate) fn push_scope(
        &mut self,
        alias: String,
        columns: Arc<[ColumnEntry]>,
        qualifiers: Vec<String>,
    ) {
        self.scope_stack.push(Scope {
            alias,
            columns,
            is_derived: true,
            qualifiers,
        });
    }

    /// Pop the current scope (e.g. leaving a subquery)
    pub fn pop_scope(&mut self) {
        if self.scope_stack.pop().is_none() {
            tracing::warn!(target: "sql_generator", "Attempted to pop scope from empty stack");
        } else {
            tracing::trace!(target: "sql_generator", stack_len = self.scope_stack.len(), "Popped scope");
        }
    }

    /// Get the current (top) scope
    pub fn current_scope(&self) -> Option<&Scope> {
        self.scope_stack.last()
    }

    pub fn scope_stack_len(&self) -> usize {
        self.scope_stack.len()
    }

    /// Create a checkpoint of the current scope stack state.
    pub fn checkpoint(&self) -> Checkpoint {
        Checkpoint {
            stack_len: self.scope_stack.len(),
        }
    }

    /// Roll back the scope stack to a previously created checkpoint.
    pub fn rollback(&mut self, checkpoint: Checkpoint) {
        if self.scope_stack.len() > checkpoint.stack_len {
            let diff = self.scope_stack.len() - checkpoint.stack_len;
            tracing::trace!(target: "sql_generator", count = diff, "Rolling back scopes");
            self.scope_stack.truncate(checkpoint.stack_len);
        }
    }

    /// Resolve a column to the source alias defined in the scope.
    /// Returns (source_alias, column_name)
    pub fn resolve_column(
        &self,
        col: &Column,
        node_type: &'static str,
    ) -> Result<(String, String), SqlGenError> {
        // Search from top of stack down
        for scope in self.scope_stack.iter().rev() {
            // Find all matching columns in this scope
            let matches: Vec<&ColumnEntry> = scope
                .columns
                .iter()
                .filter(|e| e.name.as_ref() == col.name)
                .collect();

            if matches.is_empty() {
                continue;
            }

            // If we have a qualifier (relation), try to find a specific match
            if let Some(relation) = &col.relation {
                let table_str = relation.to_string();

                // Check exact provenance or search scope's known qualifiers
                let specific = matches.iter().find(|e| {
                    e.provenance.contains(&table_str) || e.source_alias.as_ref() == table_str
                });

                if let Some(found) = specific {
                    return Ok((found.source_alias.to_string(), col.name.clone()));
                }

                // If qualifier was provided but didn't match anything in this scope's columns,
                // we should continue searching down or check if the qualifier itself matches this scope.
                if !scope.qualifiers.contains(&table_str) && scope.alias != table_str {
                    continue;
                }
            }

            // If no qualifier or simple match found, and it's unambiguous within this scope
            if matches.len() == 1 {
                return Ok((matches[0].source_alias.to_string(), col.name.clone()));
            }

            // Ambigous within the current scope
            return Err(SqlGenError::AmbiguousColumn {
                name: col.name.clone(),
                candidates: matches
                    .iter()
                    .map(|e| format!("{}.{}", e.source_alias, e.name))
                    .collect(),
            });
        }

        Err(SqlGenError::ScopeViolation {
            col: col.to_string(),
            node_type,
            available: self
                .scope_stack
                .iter()
                .flat_map(|s| s.columns.iter().map(|c| format!("{}.{}", s.alias, c.name)))
                .collect(),
            scope_stack: self
                .scope_stack
                .iter()
                .map(|s| {
                    format!(
                        "{}: {}",
                        s.alias,
                        s.columns
                            .iter()
                            .map(|c| c.name.as_ref())
                            .collect::<Vec<_>>()
                            .join(", ")
                    )
                })
                .collect(),
        })
    }
}
