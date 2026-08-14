//! ## Usage
//!
//! ```rust
//! use strake_sql::sql_generator::context::GeneratorContext;
//! use std::sync::Arc;
//!
//! let mut ctx = GeneratorContext::new();
//! // Enter a scope with columns
//! let columns = Arc::from(vec![]);
//! let guard = ctx.enter_scope("rel_0".to_string(), columns, vec!["users".to_string()]);
//! guard.commit();
//! ```
//!
//! ## Performance Characteristics
//!
//! Column resolution is O(scopes × columns) per lookup. Alias generation is O(1)
//! via monotonic counter.
//!
//! ## Errors
//!
//! - [`SqlGenError::ScopeViolation`]: Returned by `resolve_column` when a column cannot be found in any visible scope.
//! - [`SqlGenError::AmbiguousColumn`]: Returned when a column name matches multiple entries without sufficient qualification.

use crate::sql_generator::error::SqlGenError;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::Column;
use std::sync::Arc;

/// Global unique identifier for a column instance.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct ColumnId(pub usize);

impl From<usize> for ColumnId {
    fn from(id: usize) -> Self {
        Self(id)
    }
}

impl From<ColumnId> for usize {
    fn from(id: ColumnId) -> Self {
        id.0
    }
}

impl std::fmt::Display for ColumnId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Metadata for a column in the SQL generator's scope.
#[derive(Debug, Clone)]
pub struct ColumnEntry {
    /// The name of the column as it appears in the projection or JOIN output.
    pub name: Arc<str>,
    /// The pre-computed lowercase representation of the column name for case-insensitive matching.
    pub name_lower: Arc<str>,
    /// The Arrow data type of the column.
    pub data_type: DataType,
    /// The alias of the table/relation where this column originates.
    /// Used to resolve `t0.col` vs `t1.col` in joins.
    pub source_alias: Arc<str>,
    /// Chain of aliases/qualifiers this column has passed through.
    /// Used for disambiguation in complex joins.
    pub provenance: Vec<String>,
    /// Global unique identifier for this specific column instance,
    /// used to track columns through transformations.
    pub unique_id: ColumnId,
}

/// Represents a named set of columns visible during translation.
#[derive(Debug, Clone)]
pub struct Scope {
    /// The stable alias assigned to this scope (e.g., "rel_0").
    pub alias: String,
    /// The set of columns exposed by this relation.
    pub columns: Arc<[ColumnEntry]>,
    /// True if this scope represents a derived table, subquery, or JOIN result.
    pub is_derived: bool,
    /// Original relation names that this scope represents (e.g. "users", "orders").
    /// Used for qualified column resolution fallback.
    pub qualifiers: Vec<String>,
}

/// Represents a state in the scope stack that can be rolled back to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Checkpoint {
    pub(crate) stack_len: usize,
    pub(crate) undo_len: usize,
}

/// Manages the scope stack and alias generation for SQL translation.
pub struct GeneratorContext {
    /// Global counter for deterministic aliases (rel_0, rel_1...).
    counter: usize,
    /// Global counter for unique column IDs.
    column_id_counter: usize,
    /// Stack of visible scopes, from outermost to innermost.
    pub(crate) scope_stack: Vec<Scope>,
    /// Tracks popped scopes to allow robust restoration via Checkpoints.
    undo_stack: Vec<Scope>,
}

impl Default for GeneratorContext {
    fn default() -> Self {
        Self::new()
    }
}

impl GeneratorContext {
    /// Creates a new, empty [`GeneratorContext`].
    pub fn new() -> Self {
        Self {
            counter: 0,
            column_id_counter: 0,
            scope_stack: Vec::new(),
            undo_stack: Vec::new(),
        }
    }

    /// Assign next systematic alias and increment counter
    pub fn next_alias(&mut self) -> String {
        let alias = format!("rel_{}", self.counter);
        self.counter += 1;
        alias
    }

    /// Assign next unique column ID and increment counter
    pub fn next_column_id(&mut self) -> ColumnId {
        let id = self.column_id_counter;
        self.column_id_counter += 1;
        ColumnId(id)
    }

    /// Enter a new scope, returning a guard that will pop it when dropped.
    #[must_use = "The ScopeGuard must be committed to persist the scope change"]
    pub fn enter_scope(
        &mut self,
        alias: String,
        columns: Arc<[ColumnEntry]>,
        qualifiers: Vec<String>,
    ) -> ScopeGuard<'_> {
        ScopeGuard::new(self, alias, columns, qualifiers)
    }

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

    pub(crate) fn push_existing_scope(&mut self, scope: Scope) {
        // If we are pushing back something we just popped, remove it from undo history
        if self
            .undo_stack
            .last()
            .map(|s| s.alias == scope.alias)
            .unwrap_or(false)
        {
            self.undo_stack.pop();
        }
        self.scope_stack.push(scope);
    }

    /// Pop the current scope (e.g. leaving a subquery).
    /// Returns the popped scope.
    pub fn pop_and_return_scope(&mut self) -> Option<Scope> {
        let scope = self.scope_stack.pop();
        if let Some(ref s) = scope {
            self.undo_stack.push(s.clone());
            tracing::trace!(target: "sql_generator", stack_len = self.scope_stack.len(), "Popped scope");
        } else {
            tracing::warn!(target: "sql_generator", "Attempted to pop scope from empty stack");
        }
        scope
    }

    /// Pops and discards all scopes pushed above the given checkpoint, returning
    /// the top-most scope (the output of the most recently translated subplan).
    ///
    /// Unlike [`pop_and_return_scope`], discarded scopes are NOT recorded in the
    /// undo stack: they represent completed subplan translations whose scope
    /// entries are superseded by the single replacement scope that the caller
    /// pushes after extracting the relation. Restoring them on rollback would
    /// leak stale column entries back onto the stack.
    pub(crate) fn pop_to_checkpoint(&mut self, checkpoint: Checkpoint) -> Option<Scope> {
        let mut top: Option<Scope> = None;
        while self.scope_stack.len() > checkpoint.stack_len {
            match self.scope_stack.pop() {
                Some(scope) => {
                    if top.is_none() {
                        top = Some(scope);
                    }
                }
                None => break,
            }
        }
        if top.is_none() {
            tracing::warn!(target: "sql_generator", "Attempted to pop scope to checkpoint from empty stack");
        }
        top
    }

    /// Pop the current scope (e.g. leaving a subquery)
    pub fn pop_scope(&mut self) {
        self.pop_and_return_scope();
    }

    /// Get the current (top) scope
    pub fn current_scope(&self) -> Option<&Scope> {
        self.scope_stack.last()
    }

    /// Returns the number of active scopes in the stack.
    pub fn scope_stack_len(&self) -> usize {
        self.scope_stack.len()
    }

    /// Create a checkpoint of the current scope stack state.
    pub fn checkpoint(&self) -> Checkpoint {
        Checkpoint {
            stack_len: self.scope_stack.len(),
            undo_len: self.undo_stack.len(),
        }
    }

    /// Roll back the scope stack to a previously created checkpoint.
    /// Correctly handles both extra pushes (via truncate) and extra pops (via undo_stack).
    pub fn rollback(&mut self, checkpoint: Checkpoint) {
        // 1. Remove extra pushes
        if self.scope_stack.len() > checkpoint.stack_len {
            self.scope_stack.truncate(checkpoint.stack_len);
        }

        // 2. Restore extra pops
        if self.undo_stack.len() > checkpoint.undo_len {
            let mut to_restore = Vec::new();
            while self.undo_stack.len() > checkpoint.undo_len {
                if let Some(scope) = self.undo_stack.pop() {
                    to_restore.push(scope);
                }
            }
            // Items were pushed to undo_stack in pop order.
            // newest_pop is at the end. newest_pop was the top of the stack.
            // Items must be pushed back to scope_stack in the same order they were originally.
            for scope in to_restore {
                self.scope_stack.push(scope);
            }
        }
    }

    /// Resolve a column to the source alias defined in the scope.
    /// Returns (source_alias, column_name)
    pub fn resolve_column(
        &self,
        col: &Column,
        node_type: &'static str,
    ) -> Result<&ColumnEntry, SqlGenError> {
        let lookup_name = crate::sql_generator::translator::derive_bare_name(&col.name);
        let lookup_lower = lookup_name.to_lowercase();

        for scope in self.scope_stack.iter().rev() {
            // Primary match: derive_bare_name normalization
            let mut matches: Vec<&ColumnEntry> = scope
                .columns
                .iter()
                .filter(|e| {
                    let entry_bare =
                        crate::sql_generator::translator::derive_bare_name(e.name.as_ref());
                    entry_bare == lookup_name
                })
                .collect();

            // Fallback: direct case-insensitive comparison (safety net)
            if matches.is_empty() {
                matches = scope
                    .columns
                    .iter()
                    .filter(|e| e.name_lower.as_ref() == lookup_lower.as_str())
                    .collect();
            }

            if matches.is_empty() {
                continue;
            }

            if let Some(relation) = &col.relation {
                let table_str = relation.to_string();
                let table_norm = table_str.replace('"', "").to_lowercase();

                let specific = matches.iter().find(|e| {
                    e.source_alias.as_ref().replace('"', "").to_lowercase() == table_norm
                        || e.provenance
                            .iter()
                            .any(|p| p.replace('"', "").to_lowercase() == table_norm)
                });

                if let Some(found) = specific {
                    return Ok(found);
                }

                let scope_alias_norm = scope.alias.replace('"', "").to_lowercase();
                if !scope
                    .qualifiers
                    .iter()
                    .any(|q| q.replace('"', "").to_lowercase() == table_norm)
                    && scope_alias_norm != table_norm
                {
                    continue;
                }
            }

            if matches.len() == 1 {
                return Ok(matches[0]);
            }

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

/// RAII Guard for Scope management.
/// Pops the scope when dropped, unless committed.
pub struct ScopeGuard<'a> {
    context: &'a mut GeneratorContext,
    expected_alias: String,
    committed: bool,
}

impl<'a> ScopeGuard<'a> {
    /// Creates a new [`ScopeGuard`] and pushes a new scope to the context.
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

    /// Prevents the scope from being popped on drop.
    pub fn commit(mut self) {
        self.committed = true;
    }
}

impl<'a> Drop for ScopeGuard<'a> {
    fn drop(&mut self) {
        if !self.committed {
            if let Some(top) = self.context.current_scope()
                && top.alias != self.expected_alias
            {
                tracing::error!(
                    target: "sql_generator",
                    expected = %self.expected_alias,
                    actual = %top.alias,
                    "Scope stack corruption detected"
                );
            }
            self.context.pop_scope();
        }
    }
}

/// RAII guard for manual scope stack manipulation in complex nodes like Joins.
///
/// On drop, rolls back the scope stack to the state at creation time,
/// unless `commit` was called (which updates the checkpoint to the current state).
pub struct ScopeHolder<'a> {
    ctx: &'a mut GeneratorContext,
    checkpoint: Checkpoint,
}

impl<'a> ScopeHolder<'a> {
    /// Creates a new [`ScopeHolder`] and captures a checkpoint of the current stack.
    pub fn new(ctx: &'a mut GeneratorContext) -> Self {
        let checkpoint = ctx.checkpoint();
        Self { ctx, checkpoint }
    }

    /// Pops a scope from the context.
    pub fn pop(&mut self) -> Result<Scope, SqlGenError> {
        self.ctx
            .pop_and_return_scope()
            .ok_or_else(|| SqlGenError::UnsupportedPlan {
                message: "Missing scope during manual stack manipulation".to_string(),
                node_type: "Context".to_string(),
            })
    }

    /// Pushes a scope back to the context.
    pub fn repush(&mut self, scope: Scope) {
        self.ctx.push_existing_scope(scope);
    }

    /// Provides mutable access to the underlying context.
    pub fn ctx_mut(&mut self) -> &mut GeneratorContext {
        self.ctx
    }

    /// Marks the current stack state as the new checkpoint,
    /// preventing rollback on drop.
    pub fn commit(&mut self) {
        self.checkpoint = self.ctx.checkpoint();
    }
}

impl<'a> Drop for ScopeHolder<'a> {
    fn drop(&mut self) {
        self.ctx.rollback(self.checkpoint);
    }
}
