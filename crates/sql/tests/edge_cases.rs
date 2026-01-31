mod common;

use anyhow::Result;
use datafusion::common::TableReference;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::*;
use strake_sql::optimizer::remapper::remap_plan_for_federation;

#[tokio::test]
async fn test_invalid_column_reference() -> Result<()> {
    let ctx = SessionContext::new();
    common::setup_ctx_with_tables(&ctx).await?;

    // Create a plan that references a column not in the schema
    // We use Column::new directly to bypass some builder-level checks if possible,
    // or just construct a Projection manually.
    let table_scan = ctx.table("users").await?.into_optimized_plan()?;

    let invalid_col = datafusion::common::Column::new(None::<TableReference>, "non_existent");
    let proj = datafusion::logical_expr::Projection::try_new(
        vec![datafusion::logical_expr::Expr::Column(invalid_col)],
        std::sync::Arc::new(table_scan),
    );

    // Result might be an error during construction or remapping
    match proj {
        Ok(plan) => {
            let plan = LogicalPlan::Projection(plan);
            let result = remap_plan_for_federation(plan);
            // It should fail gracefully
            assert!(result.is_err());
            println!("Error as expected: {:?}", result.err());
        }
        Err(e) => {
            println!("Caught early by DataFusion: {:?}", e);
        }
    }

    Ok(())
}

#[tokio::test]
async fn test_empty_projection() -> Result<()> {
    let ctx = SessionContext::new();
    common::setup_ctx_with_tables(&ctx).await?;

    let table_scan = ctx.table("users").await?.into_optimized_plan()?;
    let proj =
        datafusion::logical_expr::Projection::try_new(vec![], std::sync::Arc::new(table_scan));

    if let Ok(plan) = proj {
        let plan = LogicalPlan::Projection(plan);
        let remapped = remap_plan_for_federation(plan)?;
        assert_eq!(remapped.schema().fields().len(), 0);
    }

    Ok(())
}

#[tokio::test]
async fn test_deeply_nested_aliases() -> Result<()> {
    let ctx = SessionContext::new();
    common::setup_ctx_with_tables(&ctx).await?;

    let mut plan = ctx.table("users").await?;
    for i in 0..5 {
        plan = plan.alias(&format!("alias_{}", i))?;
    }

    let final_plan = plan.into_optimized_plan()?;
    let remapped = remap_plan_for_federation(final_plan)?;

    // Should resolve without stack overflow or collision
    assert!(!remapped.schema().fields().is_empty());

    Ok(())
}
