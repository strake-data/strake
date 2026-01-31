//! SQL Generation for Remote Sources.
//!
//! Converts DataFusion LogicalPlans to SQL text using the DialectRouter
//! to select the appropriate rendering strategy per source type.

use anyhow::{Context, Result};
use datafusion::common::tree_node::TreeNode;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::SessionContext;
use datafusion::sql::unparser::dialect::Dialect as UnparserDialect;
use datafusion::sql::unparser::Unparser;

use crate::dialect_router::{route_dialect, DialectPath};

/// Generates Substrait plan bytes for the given LogicalPlan.
///
/// Should be used when `is_substrait_source` returns true.
pub async fn get_substrait_for_plan(plan: &LogicalPlan, ctx: &SessionContext) -> Result<Vec<u8>> {
    crate::substrait_producer::to_substrait_bytes(plan, ctx).await
}

/// Converts a logical plan to SQL text using the appropriate dialect for the target source.
///
/// Used by the federation layer when pushing subqueries to remote databases.
pub fn get_sql_for_plan(plan: &LogicalPlan, source_type: &str) -> Result<Option<String>> {
    match route_dialect(source_type) {
        DialectPath::Native(dialect) | DialectPath::Custom(dialect) => {
            let unparser = ScopedUnparser::new(&*dialect);
            let ast = unparser
                .plan_to_sql(plan)
                .context("Failed to unparse LogicalPlan to AST")?;
            Ok(Some(ast.to_string()))
        }
        DialectPath::Substrait => Err(anyhow::anyhow!(
            "Source '{}' uses Substrait, use get_substrait_for_plan instead",
            source_type
        )),
        DialectPath::LocalExecution => {
            tracing::debug!(
                source_type = %source_type,
                "No dialect available, using local execution"
            );
            Ok(None)
        }
    }
}

// Re-export utility functions from dialect_router for backward compatibility if needed,
// but they should be used from dialect_router directly.
pub use crate::dialect_router::{is_local_execution, is_substrait_source};

// Re-export remapper logic
pub use crate::optimizer::remapper::remap_plan_for_federation;

/// Scope-aware SQL generator that properly handles column qualifiers
struct ScopedUnparser<'a> {
    dialect: &'a dyn UnparserDialect,
}

impl<'a> ScopedUnparser<'a> {
    fn new(dialect: &'a dyn UnparserDialect) -> Self {
        Self { dialect }
    }

    fn plan_to_sql(
        &self,
        plan: &LogicalPlan,
    ) -> Result<datafusion::sql::sqlparser::ast::Statement> {
        tracing::debug!("Original plan:\n{}", plan.display_indent());

        // Delegate to the optimizer module for plan remapping
        let mut remapped_plan =
            remap_plan_for_federation(plan.clone()).context("Failed to remap plan scopes")?;

        // Recursively replace SchemaAdapter extension nodes with Projections
        // so that the DataFusion Unparser can handle them.
        remapped_plan = remapped_plan
            .transform(|node| {
                if let LogicalPlan::Extension(ext) = &node {
                    if let Some(adapter) = ext
                        .node
                        .as_any()
                        .downcast_ref::<crate::schema_adapter::SchemaAdapter>()
                    {
                        return Ok(datafusion::common::tree_node::Transformed::yes(
                            adapter.to_projection()?,
                        ));
                    }
                }
                Ok(datafusion::common::tree_node::Transformed::no(node))
            })
            .map(|t| t.data)?;

        tracing::debug!(
            "Remapped and unwrapped plan:\n{}",
            remapped_plan.display_indent()
        );

        let unparser = Unparser::new(self.dialect);
        unparser
            .plan_to_sql(&remapped_plan)
            .context("Failed to unparse remapped plan")
    }
}
