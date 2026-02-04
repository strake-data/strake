//! SQL Generation for Remote Sources.
//!
//! Converts DataFusion LogicalPlans to SQL text using the DialectRouter
//! to select the appropriate rendering strategy per source type.

use anyhow::Result;
use datafusion::common::tree_node::TreeNode;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::SessionContext;

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
    let (dialect_arc, function_mapper) = match route_dialect(source_type) {
        DialectPath::Native(d) => (d, None),
        DialectPath::Custom(d, mapper) => (d, mapper), // Mapper is Option<FunctionMapper>, already matching
        DialectPath::Substrait => {
            return Err(anyhow::anyhow!(
                "Source '{}' uses Substrait, use get_substrait_for_plan instead",
                source_type
            ))
        }
        DialectPath::LocalExecution => {
            tracing::debug!(
                source_type = %source_type,
                "No dialect available, using local execution"
            );
            return Ok(None);
        }
    };

    // Recursively replace SchemaAdapter extension nodes with Projections
    // so that we deal with standard LogicalPlan nodes.
    let plan = plan
        .clone()
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

    let generator_dialect = crate::sql_generator::dialect::GeneratorDialect::new(
        dialect_arc.as_ref(),
        function_mapper.as_ref(),
        source_type,
    );
    let mut generator = crate::sql_generator::SqlGenerator::new(generator_dialect);

    let sql = generator.generate(&plan)?;
    Ok(Some(sql))
}

// Re-export utility functions from dialect_router for backward compatibility if needed,
// but they should be used from dialect_router directly.
pub use crate::dialect_router::{is_local_execution, is_substrait_source};

// Re-export remapper logic (deprecated but keeping export to avoid breaking compilation if used elsewhere)
// pub use crate::optimizer::remapper::remap_plan_for_federation;
