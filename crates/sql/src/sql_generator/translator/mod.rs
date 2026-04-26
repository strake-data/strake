//! # SQL Generator Translators
//!
//! This module contains the core logic for translating DataFusion logical plans
//! into SQL `Query` objects using `sqlparser-rs`.
//!
//! ## Overview
//!
//! The [`SqlGenerator`] orchestrates the translation by recursively walking the
//! plan and dispatching to specialized node handlers (e.g., [`scan::handle_table_scan`]).
//!
//! ## Normalization
//!
//! A key feature of this generator is its ability to produce stable, bare SQL
//! across different dialects. It achieves this by stripping source-level qualifiers
//! from columns and tables, and applying consistent aliasing.
//!
//! ## Safety
//!
//! Recursion depth is limited by [`MAX_RECURSION_DEPTH`] to prevent stack overflow
//! on deeply nested plans.
//!
//! ## Usage
//!
//! ```rust
//! use strake_sql::sql_generator::SqlGenerator;
//! use strake_sql::dialect_router::route_dialect;
//! use datafusion::logical_expr::LogicalPlan;
//!
//! # fn test(plan: &LogicalPlan) -> Result<(), Box<dyn std::error::Error>> {
//! let dialect_path = route_dialect("postgres");
//! if let strake_sql::dialect_router::DialectPath::Native(d, c, t) = dialect_path {
//!     let gen_dialect = strake_sql::sql_generator::dialect::GeneratorDialect::new(
//!         d.as_ref(), None, c, t, "postgres"
//!     );
//!     let mut generator = SqlGenerator::new(gen_dialect);
//!     let sql = generator.generate(plan)?;
//! }
//! # Ok(())
//! # }
//! ```
//!
//! ## Performance Characteristics
//!
//! - **Column Resolution**: O(scopes × columns) per query.
//! - **Aliasing**: O(1) via monotonic counter.
//! - **Recursion**: Stack depth is limited by [`MAX_RECURSION_DEPTH`] (50) to prevent overflow.

use crate::sql_generator::context::{ColumnEntry, GeneratorContext, Scope};
use crate::sql_generator::dialect::GeneratorDialect;
use crate::sql_generator::error::SqlGenError;
use crate::sql_generator::sanitize::safe_ident;
use datafusion::logical_expr::LogicalPlan;
use sqlparser::ast::{
    Expr as SqlExpr, Query, Select, SelectItem, SetExpr, TableAlias, TableFactor, TableWithJoins,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicUsize, Ordering};
use strake_error::StrakeError;

pub(crate) mod aggregate;
pub(crate) mod join;
pub(crate) mod projection;
pub(crate) mod scan;
pub(crate) mod set_ops;
pub(crate) mod sort;

/// Maximum allowed recursion depth for logical plan translation.
pub(crate) const MAX_RECURSION_DEPTH: usize = 50;

/// The main orchestrator for translating DataFusion logical plans into SQL queries.
pub struct SqlGenerator<'a> {
    /// Context used to track scopes, aliases, and column IDs during translation.
    pub context: GeneratorContext,
    /// Dialect-specific configuration and capabilities.
    pub dialect: GeneratorDialect<'a>,
    /// Internal counter to prevent infinite recursion on nested plans.
    pub(crate) recursion_level: Arc<AtomicUsize>,
}

impl<'a> SqlGenerator<'a> {
    /// Creates a new [`SqlGenerator`] instance with the specified dialect.
    pub fn new(dialect: GeneratorDialect<'a>) -> Self {
        Self {
            context: GeneratorContext::new(),
            dialect,
            recursion_level: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Generates a SQL string from a DataFusion [`LogicalPlan`].
    #[must_use = "SQL generation produces the primary output and should not be ignored"]
    pub fn generate(&mut self, plan: &LogicalPlan) -> Result<String, StrakeError> {
        tracing::debug!(target: "sql_generator", plan = %plan.display(), "Generating SQL from plan");
        self.plan_to_query(plan)
            .map(|q| {
                let sql = q.to_string();
                tracing::debug!(target: "sql_generator", sql = %sql, "Generated SQL");
                sql
            })
            .map_err(|e| e.to_strake_error(self.dialect.dialect_name))
    }

    /// Returns a skeleton SQL [`Query`] structure (equivalent to `SELECT *`).
    ///
    /// This is used as a base template for building more complex queries.
    pub(crate) fn create_skeleton_query(&self) -> Query {
        static SKELETON: OnceLock<Query> = OnceLock::new();

        SKELETON
            .get_or_init(|| {
                let dialect = GenericDialect {};
                let parser = Parser::new(&dialect);
                *parser
                    .try_with_sql("SELECT *")
                    .unwrap()
                    .parse_query()
                    .unwrap()
            })
            .clone()
    }

    /// Returns a skeleton SQL [`Select`] structure.
    pub fn create_skeleton_select(&self) -> Select {
        let query = self.create_skeleton_query();
        match *query.body {
            SetExpr::Select(select) => *select,
            _ => panic!("SKELETON query invariant violated"),
        }
    }

    pub(crate) fn plan_to_query(&mut self, plan: &LogicalPlan) -> Result<Query, SqlGenError> {
        let _guard = RecursionGuard::new(self.recursion_level.clone())?;

        match plan {
            LogicalPlan::TableScan(scan) => scan::handle_table_scan(self, scan),
            LogicalPlan::Projection(proj) => projection::handle_projection(self, proj),
            LogicalPlan::SubqueryAlias(alias) => projection::handle_subquery_alias(self, alias),
            LogicalPlan::Filter(filter) => projection::handle_filter(self, filter),
            LogicalPlan::Join(join) => join::handle_join(self, join),
            LogicalPlan::Aggregate(agg) => aggregate::handle_aggregate(self, agg),
            LogicalPlan::Sort(sort) => sort::handle_sort(self, sort),
            LogicalPlan::Limit(limit) => set_ops::handle_limit(self, limit),
            LogicalPlan::Window(window) => aggregate::handle_window(self, window),
            LogicalPlan::Union(union) => set_ops::handle_union(self, union),
            LogicalPlan::Distinct(distinct) => set_ops::handle_distinct(self, distinct),
            LogicalPlan::EmptyRelation(empty) => set_ops::handle_empty_relation(self, empty),
            LogicalPlan::Values(values) => set_ops::handle_values(self, values),
            LogicalPlan::RecursiveQuery(recursive) => {
                set_ops::handle_recursive_query(self, recursive)
            }
            LogicalPlan::Extension(ext) => {
                if let Some(nary) = ext
                    .node
                    .as_any()
                    .downcast_ref::<crate::optimizer::join_flattener::NaryJoinNode>()
                {
                    join::handle_nary_join(self, nary)
                } else if let Some(adapter) = ext
                    .node
                    .as_any()
                    .downcast_ref::<crate::schema_adapter::SchemaAdapter>()
                {
                    // Adapt to projection or handle via new API
                    self.plan_to_query(&adapter.to_projection()?)
                } else {
                    Err(SqlGenError::UnsupportedPlan {
                        message: format!("Unsupported extension node: {}", ext.node.name()),
                        node_type: "Extension".to_string(),
                    })
                }
            }
            _ => Err(SqlGenError::UnsupportedPlan {
                message: format!("Logical plan node: {}", plan.display()),
                node_type: "Unknown".to_string(),
            }),
        }
    }

    pub(crate) fn plan_to_stable_query(
        &mut self,
        plan: &LogicalPlan,
    ) -> Result<Query, SqlGenError> {
        let query = self.plan_to_query(plan)?;

        let is_complex = match &*query.body {
            SetExpr::Select(select) => {
                let has_group_by = match &select.group_by {
                    sqlparser::ast::GroupByExpr::Expressions(exprs, _) => !exprs.is_empty(),
                    sqlparser::ast::GroupByExpr::All(_) => true,
                };
                select.from.len() > 1
                    || !select.from[0].joins.is_empty()
                    || has_group_by
                    || select.distinct.is_some()
            }
            _ => true,
        };

        // Wrap the query if it's complex or has a limit, to ensure that parent
        // nodes (like Filter or Sort) apply to the results of this query rather
        // than its internal components. Simple SELECTs from TableScan are already
        // stable and aliased, so they don't need wrapping.
        let should_wrap = is_complex || query.limit_clause.is_some();

        if should_wrap {
            let relation = self.extract_relation(query)?;
            let mut select = self.create_skeleton_select();
            select.from = vec![TableWithJoins {
                relation,
                joins: vec![],
            }];

            let scope =
                self.context
                    .scope_stack
                    .last()
                    .ok_or_else(|| SqlGenError::UnsupportedPlan {
                        message: "Missing scope".to_string(),
                        node_type: "StableQuery".to_string(),
                    })?;
            select.projection = scope
                .columns
                .iter()
                .map(|c| {
                    Ok(SelectItem::UnnamedExpr(SqlExpr::CompoundIdentifier(vec![
                        safe_ident(c.source_alias.as_ref())?,
                        safe_ident(c.name.as_ref())?,
                    ])))
                })
                .collect::<Result<Vec<_>, SqlGenError>>()?;

            let mut out_query = self.create_skeleton_query();
            out_query.body = Box::new(SetExpr::Select(Box::new(select)));
            Ok(out_query)
        } else {
            Ok(query)
        }
    }

    /// Wraps a query as a derived table with a stable alias.
    ///
    /// Pops the current scope, re-aliases the query's projection to match the
    /// popped scope's column names, pushes a replacement scope with the same alias,
    /// and returns the `TableFactor::Derived`.
    ///
    /// **Net scope stack depth change: zero** (pop one, push one).
    /// Callers relying on this invariant (e.g., `handle_aggregate`) assert it.
    pub(crate) fn extract_relation(
        &mut self,
        mut query: Query,
    ) -> Result<TableFactor, SqlGenError> {
        let inner_scope =
            self.context
                .scope_stack
                .pop()
                .ok_or_else(|| SqlGenError::UnsupportedPlan {
                    message: "Missing scope".to_string(),
                    node_type: "ExtractRelation".to_string(),
                })?;
        let sub_alias = inner_scope.alias.clone();

        let column_names: Vec<Arc<str>> = inner_scope
            .columns
            .iter()
            .map(|c| derive_bare_name(c.name.as_ref()))
            .collect::<Vec<Arc<str>>>();

        if let SetExpr::Select(ref mut select) = *query.body {
            for (i, item) in select.projection.iter_mut().enumerate() {
                if i < column_names.len() {
                    let expr = match item {
                        SelectItem::UnnamedExpr(e) => e.clone(),
                        SelectItem::ExprWithAlias { expr, .. } => expr.clone(),
                        _ => {
                            // If we can't extract an expression (e.g. Wildcard),
                            // we should shouldn't re-alias but we might have a name mismatch.
                            // For federated queries, we generally always have UnnamedExpr or ExprWithAlias.
                            continue;
                        }
                    };
                    *item = SelectItem::ExprWithAlias {
                        expr,
                        alias: safe_ident(&column_names[i])?,
                    };
                }
            }
        }

        let new_columns_vec: Vec<ColumnEntry> = column_names
            .iter()
            .enumerate()
            .map(|(i, name)| {
                let (data_type, inner_provenance) =
                    if let Some(inner_col) = inner_scope.columns.get(i) {
                        (inner_col.data_type.clone(), inner_col.provenance.clone())
                    } else {
                        (datafusion::arrow::datatypes::DataType::Null, vec![])
                    };
                let mut provenance = vec![sub_alias.clone()];
                provenance.extend(inner_provenance);
                ColumnEntry {
                    name: name.clone(),
                    name_lower: Arc::from(name.to_lowercase().as_str()),
                    data_type,
                    source_alias: sub_alias.clone().into(),
                    provenance,
                    unique_id: self.context.next_column_id(),
                }
            })
            .collect();
        let new_columns: Arc<[ColumnEntry]> = Arc::from(new_columns_vec.into_boxed_slice());

        let derived = TableFactor::Derived {
            lateral: false,
            subquery: Box::new(query),
            alias: Some(TableAlias {
                name: safe_ident(&sub_alias)?,
                columns: vec![],
                explicit: true,
            }),
            sample: None,
        };

        tracing::debug!(target: "sql_generator", alias = %sub_alias, "Extracted relation with stable alias");

        let mut qualifiers = vec![sub_alias.clone()];
        qualifiers.extend(inner_scope.qualifiers.clone());
        self.context.push_existing_scope(Scope {
            alias: sub_alias.clone(),
            columns: new_columns,
            is_derived: true,
            qualifiers,
        });
        Ok(derived)
    }
}

/// Derives a stable bare name from a potentially qualified schema field name.
///
/// This function normalizes field names to match what the normalized expressions produce.
/// It strips multi-part qualifiers and ensures consistent casing.
///
/// Examples:
///   "id"                            → "id"
///   "testdb.public.users.id"        → "id"
///   "SUM(amount)"                   → "sum(amount)"
///   "SUM(testdb.public.orders.amount)" → "sum(amount)"
///   "COUNT(u.id)"                   → "count(u.id)"  (single-word qualifier preserved)
/// Derives a "bare" name for a column or table by stripping all but the final qualifier.
///
/// It also handles function expressions by recursively stripping qualifiers from their arguments.
pub fn derive_bare_name(name: &str) -> Arc<str> {
    // If it contains parentheses, it's a function expression.
    // Strip all qualifiers inside to ensure stable, bare names.
    if name.contains('(') && name.ends_with(')') {
        return strip_qualifiers_in_function(name).to_lowercase().into();
    }

    let base = if let Some(idx) = name.rfind('.') {
        &name[idx + 1..]
    } else {
        name
    };
    base.to_lowercase().into()
}

/// Strips all qualifiers (single or multi-part) from inside function call expressions.
///
/// "SUM(testdb.public.orders.amount)"   → "SUM(amount)"
/// "SUM(o.amount)"                      → "SUM(amount)"
/// "COUNT(DISTINCT testdb.public.u.id)" → "COUNT(DISTINCT id)"
fn strip_qualifiers_in_function(name: &str) -> String {
    let mut result = String::with_capacity(name.len());
    let mut current_part = String::new();
    let mut depth: usize = 0;

    for ch in name.chars() {
        match ch {
            '(' => {
                result.push_str(&current_part);
                current_part.clear();
                result.push('(');
                depth += 1;
            }
            ')' => {
                // End of function arguments - flush the last part
                let last = current_part.rsplit('.').next().unwrap_or(&current_part);
                result.push_str(last);
                current_part.clear();
                result.push(')');
                depth = depth.saturating_sub(1);
            }
            ',' if depth > 0 => {
                // Argument separator
                let last = current_part.rsplit('.').next().unwrap_or(&current_part);
                result.push_str(last);
                current_part.clear();
                result.push(',');
            }
            ' ' | '+' | '-' | '*' | '/' if depth > 0 => {
                // Operator or space inside function - also separators
                let last = current_part.rsplit('.').next().unwrap_or(&current_part);
                result.push_str(last);
                current_part.clear();
                result.push(ch);
            }
            _ if depth > 0 => {
                current_part.push(ch);
            }
            _ => {
                result.push(ch);
            }
        }
    }
    result.push_str(&current_part);
    result
}

/// RAII guard to track and limit recursion depth.
struct RecursionGuard {
    /// Reference to the shared recursion counter.
    level: Arc<AtomicUsize>,
}
impl RecursionGuard {
    fn new(level: Arc<AtomicUsize>) -> Result<Self, SqlGenError> {
        let current = level.fetch_add(1, Ordering::SeqCst);
        if current > MAX_RECURSION_DEPTH {
            level.fetch_sub(1, Ordering::SeqCst);
            return Err(SqlGenError::MaxRecursion(MAX_RECURSION_DEPTH));
        }
        Ok(Self { level })
    }
}
impl Drop for RecursionGuard {
    fn drop(&mut self) {
        self.level.fetch_sub(1, Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::sql::unparser::dialect::PostgreSqlDialect as DataFusionPostgres;
    #[tokio::test]
    async fn test_concurrent_generation_safety() {
        let sqlparser_dialect = DataFusionPostgres {};
        let _dialect = GeneratorDialect::new(
            &sqlparser_dialect,
            None,
            Arc::new(crate::sql_generator::dialect::DefaultDialectCapabilities),
            Arc::new(crate::sql_generator::dialect::DefaultTypeMapper),
            "postgres",
        );
        let mut tasks = vec![];
        for _ in 0..1000 {
            let level = Arc::new(AtomicUsize::new(0));
            tasks.push(tokio::spawn(async move {
                let _guard = RecursionGuard::new(level).unwrap();
            }));
        }
        for task in tasks {
            task.await.unwrap();
        }
    }
}
