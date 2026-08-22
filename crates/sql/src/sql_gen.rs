//! SQL Generation for Remote Sources.
//!
//! Converts DataFusion LogicalPlans to SQL text using the DialectRouter
//! to select the appropriate rendering strategy per source type.

use anyhow::Result;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::SessionContext;

use crate::dialect_router::{DialectPath, route_dialect};

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
    let (dialect_arc, capabilities, type_mapper, function_mapper) = match route_dialect(source_type)
    {
        DialectPath::Native(d, cap, tm) => (d, cap, tm, None),
        DialectPath::Custom(d, cap, tm, mapper) => (d, cap, tm, mapper),
        DialectPath::Substrait => {
            return Err(anyhow::anyhow!(
                "Source '{}' uses Substrait, use get_substrait_for_plan instead",
                source_type
            ));
        }
        DialectPath::LocalExecution => {
            tracing::debug!(
                source_type = %source_type,
                "No dialect available, using local execution"
            );
            return Ok(None);
        }
    };

    // Flatten join trees for cleaner SQL generation
    use crate::optimizer::join_flattener::JoinTreeFlattener;
    use datafusion::optimizer::optimizer::{OptimizerContext, OptimizerRule};
    let config = OptimizerContext::default();
    let plan = JoinTreeFlattener::new()
        .rewrite(plan.clone(), &config)?
        .data;

    let generator_dialect = crate::sql_generator::dialect::GeneratorDialect::new(
        dialect_arc.as_ref(),
        function_mapper.as_ref(),
        capabilities,
        type_mapper,
        source_type,
    );
    let mut generator = crate::sql_generator::SqlGenerator::new(generator_dialect);

    let sql = generator.generate(&plan)?;
    Ok(Some(sql))
}

/// Unparses a single DataFusion expression to SQL string for the specified target dialect.
pub fn unparse_expr_to_sql(
    expr: &datafusion::logical_expr::Expr,
    source_type: &str,
) -> Result<String> {
    let (dialect_arc, capabilities, type_mapper, function_mapper) = match route_dialect(source_type)
    {
        DialectPath::Native(d, cap, tm) => (d, cap, tm, None),
        DialectPath::Custom(d, cap, tm, mapper) => (d, cap, tm, mapper),
        _ => {
            return Err(anyhow::anyhow!(
                "No dialect available to unparse expression for source '{}'",
                source_type
            ));
        }
    };

    let generator_dialect = crate::sql_generator::dialect::GeneratorDialect::new(
        dialect_arc.as_ref(),
        function_mapper.as_ref(),
        capabilities,
        type_mapper,
        source_type,
    );
    let mut context = crate::sql_generator::context::GeneratorContext::new();
    let mut translator =
        crate::sql_generator::expr::ExprTranslator::new(&mut context, &generator_dialect);
    let sql_ast = translator
        .expr_to_sql(expr)
        .map_err(|e| anyhow::anyhow!(e))?;
    Ok(sql_ast.to_string())
}

// Re-export utility functions from dialect_router for backward compatibility if needed,
// but they should be used from dialect_router directly.
pub use crate::dialect_router::{is_local_execution, is_substrait_source};

// Re-export remapper logic (deprecated but keeping export to avoid breaking compilation if used elsewhere)
// pub use crate::optimizer::remapper::remap_plan_for_federation;
